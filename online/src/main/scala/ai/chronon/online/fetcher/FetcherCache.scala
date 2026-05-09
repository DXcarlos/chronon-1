package ai.chronon.online.fetcher

import ai.chronon.aggregator.windowing.{CumulatedBatchIr, FinalBatchIr}
import ai.chronon.api.GroupBy
import ai.chronon.online.KVStore.{GetRequest, TimedValue}
import ai.chronon.online.fetcher.FetcherCache._
import ai.chronon.online.GroupByServingInfoParsed
import ai.chronon.online.metrics.{FlexibleExecutionContext, Metrics}
import com.github.benmanes.caffeine.cache.{Cache => CaffeineCache}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/*
 * FetcherCache is an extension to FetcherBase that provides caching functionality. It caches KV store
 * requests to decrease feature serving latency.
 *
 * To use it,
 *  1. Set the system property `ai.chronon.fetcher.batch_ir_cache_size_elements` to the desired cache size
 * in therms of elements. This will create a cache shared across all GroupBys. To determine a size, start with a
 * small number (e.g. 1,000) and measure how much memory it uses, then adjust accordingly.
 *  2. Enable caching for a specific GroupBy by overriding `isCachingEnabled` and returning `true` for that GroupBy.
 * FetcherBase already provides an implementation of `isCachingEnabled` that uses the FlagStore.
 * */
trait FetcherCache {
  @transient private lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  val batchIrCacheName = "batch_cache"
  val cumulatedBatchIrCacheName = "cumulated_batch_cache"
  val defaultBatchIrCacheSize = "10000"

  val configuredBatchIrCacheSize: Option[Int] =
    Option(System.getProperty("ai.chronon.fetcher.batch_ir_cache_size_elements"))
      .orElse(Some(defaultBatchIrCacheSize))
      .map(_.toInt)
      .filter(_ > 0)

  val maybeBatchIrCache: Option[BatchIrCache] =
    configuredBatchIrCacheSize
      .map(size => new BatchIrCache(batchIrCacheName, size))
  val maybeCumulatedBatchIrCache: Option[CumulatedBatchIrCache] =
    configuredBatchIrCacheSize
      .map(size => new CumulatedBatchIrCache(cumulatedBatchIrCacheName, size))
  @transient private lazy val inFlightCumulatedBatchIrBuilds =
    new ConcurrentHashMap[BatchIrCache.Key, java.lang.Boolean]()

  // Caching needs to be configured globally with a cache size > 0
  def isCacheSizeConfigured: Boolean = maybeBatchIrCache.isDefined

  // Caching needs to be enabled for the specific groupBy
  def isCachingEnabled(groupBy: GroupBy): Boolean = false

  protected def cumulatedBatchIrBuildExecutionContext: ExecutionContext =
    FetcherCache.cumulatedBatchIrBuildExecutionContext

  protected lazy val cumulatedBatchIrMetricsContext: Metrics.Context =
    caffeineMetricsContext.withSuffix(cumulatedBatchIrCacheName)

  def isCumulatedBatchIrCachingEnabled(groupBy: GroupBy): Boolean =
    isCachingEnabled(groupBy) && GroupByServingInfoParsed.isCumulatedBatchIrCacheSafe(groupBy)

  protected val caffeineMetricsContext: Metrics.Context = Metrics.Context(Metrics.Environment.JoinFetching)

  /** Obtain the Map[String, AnyRef] response from a batch response.
    *
    * If batch IR caching is enabled, this method will try to fetch the IR from the cache. If it's not in the cache,
    * it will decode it from the batch bytes and store it.
    *
    * @param batchResponses the batch responses
    * @param batchBytes the batch bytes corresponding to the batchResponses. Can be `null`.
    * @param servingInfo the GroupByServingInfoParsed that contains the info to decode the bytes
    * @param decodingFunction the function to decode bytes into Map[String, AnyRef]
    * @param keys the keys used to fetch this particular batch response, for caching purposes
    */
  private[online] def getMapResponseFromBatchResponse(batchResponses: BatchResponses,
                                                      batchBytes: Array[Byte],
                                                      decodingFunction: Array[Byte] => Map[String, AnyRef],
                                                      servingInfo: GroupByServingInfoParsed,
                                                      keys: Map[String, Any]): Map[String, AnyRef] = {
    if (!isCachingEnabled(servingInfo.groupBy)) return decodingFunction(batchBytes)

    batchResponses match {
      case _: KvStoreBatchResponse =>
        val batchRequestCacheKey =
          BatchIrCache.Key(servingInfo.groupByOps.batchDataset, keys, servingInfo.batchEndTsMillis)
        val decodedBytes = decodingFunction(batchBytes)
        if (decodedBytes != null)
          maybeBatchIrCache.get.cache.put(batchRequestCacheKey, CachedMapBatchResponse(decodedBytes))
        decodedBytes
      case cachedResponse: CachedBatchResponse =>
        cachedResponse match {
          case CachedFinalIrBatchResponse(_: FinalBatchIr)              => decodingFunction(batchBytes)
          case CachedMapBatchResponse(mapResponse: Map[String, AnyRef]) => mapResponse
        }
    }
  }

  /** Obtain the FinalBatchIr from a batch response.
    *
    * If batch IR caching is enabled, this method will try to fetch the IR from the cache. If it's not in the cache,
    * it will decode it from the batch bytes and store it.
    *
    * @param batchResponses the batch responses
    * @param batchBytes the batch bytes corresponding to the batchResponses. Can be `null`.
    * @param servingInfo the GroupByServingInfoParsed that contains the info to decode the bytes
    * @param decodingFunction the function to decode bytes into FinalBatchIr
    * @param keys the keys used to fetch this particular batch response, for caching purposes
    */
  private[online] def getBatchIrFromBatchResponse(
      batchResponses: BatchResponses,
      batchBytes: Array[Byte],
      servingInfo: GroupByServingInfoParsed,
      decodingFunction: (Array[Byte], GroupByServingInfoParsed) => FinalBatchIr,
      keys: Map[String, Any]): FinalBatchIr = {
    if (!isCachingEnabled(servingInfo.groupBy)) return decodingFunction(batchBytes, servingInfo)

    batchResponses match {
      case _: KvStoreBatchResponse =>
        val batchRequestCacheKey =
          BatchIrCache.Key(servingInfo.groupByOps.batchDataset, keys, servingInfo.batchEndTsMillis)
        val decodedBytes = decodingFunction(batchBytes, servingInfo)
        if (decodedBytes != null)
          maybeBatchIrCache.get.cache.put(batchRequestCacheKey, CachedFinalIrBatchResponse(decodedBytes))
        decodedBytes
      case cachedResponse: CachedBatchResponse =>
        cachedResponse match {
          case CachedFinalIrBatchResponse(finalBatchIr: FinalBatchIr) => finalBatchIr
          case CachedMapBatchResponse(_: Map[String, AnyRef])         => decodingFunction(batchBytes, servingInfo)
      }
    }
  }

  private[online] def getCumulatedBatchIrFromBatchIr(finalBatchIr: FinalBatchIr,
                                                     servingInfo: GroupByServingInfoParsed,
                                                     keys: Map[String, Any]): Option[CumulatedBatchIr] = {
    if (finalBatchIr == null || !isCachingEnabled(servingInfo.groupBy) || !servingInfo.isCumulatedBatchIrCacheSafe) {
      return None
    }

    val batchRequestCacheKey =
      BatchIrCache.Key(servingInfo.groupByOps.batchDataset, keys, servingInfo.batchEndTsMillis)
    maybeCumulatedBatchIrCache match {
      case None => None
      case Some(cache) =>
        val cumulatedBatchIr = cache.cache.getIfPresent(batchRequestCacheKey)
        if (cumulatedBatchIr != null) {
          cumulatedBatchIrMetricsContext.increment("hits")
          Some(cumulatedBatchIr)
        } else {
          cumulatedBatchIrMetricsContext.increment("misses")
          scheduleCumulatedBatchIrBuild(batchRequestCacheKey,
                                        finalBatchIr,
                                        servingInfo,
                                        cache,
                                        cumulatedBatchIrMetricsContext)
          None
        }
    }
  }

  private def scheduleCumulatedBatchIrBuild(batchRequestCacheKey: BatchIrCache.Key,
                                            finalBatchIr: FinalBatchIr,
                                            servingInfo: GroupByServingInfoParsed,
                                            cache: CumulatedBatchIrCache,
                                            metricsContext: Metrics.Context): Unit = {
    if (inFlightCumulatedBatchIrBuilds.putIfAbsent(batchRequestCacheKey, java.lang.Boolean.TRUE) != null) {
      metricsContext.increment("async_build_in_flight")
      return
    }

    metricsContext.increment("async_build_scheduled")
    try {
      Future {
        val buildStartTime = System.currentTimeMillis()
        val cumulatedBatchIr = servingInfo.cumulateBatchIrForServing(finalBatchIr)
        cache.cache.put(batchRequestCacheKey, cumulatedBatchIr)
        metricsContext.distribution("async_build.latency.millis", System.currentTimeMillis() - buildStartTime)
      }(cumulatedBatchIrBuildExecutionContext).andThen {
        case Success(_) =>
          metricsContext.increment("async_build_successes")
          inFlightCumulatedBatchIrBuilds.remove(batchRequestCacheKey)
        case Failure(ex) =>
          metricsContext.increment("async_build_failures")
          metricsContext.incrementException(ex)(logger)
          inFlightCumulatedBatchIrBuilds.remove(batchRequestCacheKey)
      }(cumulatedBatchIrBuildExecutionContext)
    } catch {
      case NonFatal(ex) =>
        metricsContext.increment("async_build_failures")
        metricsContext.incrementException(ex)(logger)
        inFlightCumulatedBatchIrBuilds.remove(batchRequestCacheKey)
    }
  }

  /** Given a list of GetRequests, return a map of GetRequests to cached FinalBatchIrs.
    */
  def getCachedRequests(
      groupByRequestToKvRequest: Seq[(Fetcher.Request, Try[LambdaKvRequest])]): Map[GetRequest, CachedBatchResponse] = {

    def empty = Map.empty[GetRequest, CachedBatchResponse]

    if (!isCacheSizeConfigured) return empty

    groupByRequestToKvRequest
      .map {

        case (request, Success(LambdaKvRequest(servingInfo, _, batchRequest, _, _, _, _)))
            if isCachingEnabled(servingInfo.groupBy) =>
          val batchRequestCacheKey =
            BatchIrCache.Key(batchRequest.dataset, request.keys, servingInfo.batchEndTsMillis)

          // Metrics so we can get per-group-by cache metrics
          val metricsContext =
            request.context.getOrElse(Metrics.Context(Metrics.Environment.JoinFetching, servingInfo.groupBy))

          maybeBatchIrCache.get.cache.getIfPresent(batchRequestCacheKey) match {

            case null =>
              metricsContext.increment(s"${batchIrCacheName}_gb_misses")
              empty

            case cachedIr: CachedBatchResponse =>
              metricsContext.increment(s"${batchIrCacheName}_gb_hits")
              Map(batchRequest -> cachedIr)

            case _ =>
              metricsContext.increment(s"${batchIrCacheName}_gb_misses")
              empty

          }

        case _ => empty

      }
      .foldLeft(empty)(_ ++ _)
  }
}

object FetcherCache {
  private[online] lazy val cumulatedBatchIrBuildExecutionContext: ExecutionContext =
    FlexibleExecutionContext.buildExecutionContext

  private[online] class BatchIrCache(val cacheName: String, val maximumSize: Int = 10000) {
    import BatchIrCache._

    val cache: CaffeineCache[Key, Value] =
      LRUCache[Key, Value](cacheName = cacheName, maximumSize = maximumSize)
  }

  private[online] class CumulatedBatchIrCache(val cacheName: String, val maximumSize: Int = 10000) {
    import BatchIrCache._

    val cache: CaffeineCache[Key, CumulatedBatchIr] =
      LRUCache[Key, CumulatedBatchIr](cacheName = cacheName, maximumSize = maximumSize)
  }

  private[online] object BatchIrCache {
    // We use the dataset, keys, and batchEndTsMillis to identify a batch request.
    // There's one edge case to be aware of: if a batch job is re-run in the same day, the batchEndTsMillis will
    // be the same but the underlying data may have have changed. If that new batch data is needed immediately, the
    // Fetcher service should be restarted.
    case class Key(dataset: String, keys: Map[String, Any], batchEndTsMillis: Long)

    // FinalBatchIr is for GroupBys using temporally accurate aggregation.
    // Map[String, Any] is for GroupBys using snapshot accurate aggregation or no aggregation.
    type Value = BatchResponses
  }

  /** Encapsulates the response for a GetRequest for batch data. This response could be the values received from
    * a KV Store request, or cached values.
    *
    * (The fetcher uses these batch values to construct the response for a request for feature values.)
    */
  sealed abstract class BatchResponses {
    def getBatchBytes(batchEndTsMillis: Long): Array[Byte]
  }
  object BatchResponses {
    def apply(kvStoreResponse: Try[Seq[TimedValue]]): KvStoreBatchResponse = KvStoreBatchResponse(kvStoreResponse)
    def apply(cachedResponse: FinalBatchIr): CachedFinalIrBatchResponse = CachedFinalIrBatchResponse(cachedResponse)
    def apply(cachedResponse: Map[String, AnyRef]): CachedMapBatchResponse = CachedMapBatchResponse(cachedResponse)
  }

  /** Encapsulates batch response values received from a KV Store request. */
  case class KvStoreBatchResponse(response: Try[Seq[TimedValue]]) extends BatchResponses {
    def getBatchBytes(batchEndTsMillis: Long): Array[Byte] = response match {
      case Success(timedValues) =>
        if (timedValues == null) return null

        var resultBytes: Array[Byte] = null
        var maxTs = 0L

        // Add a one-day buffer (86400000 ms) to avoid accidentally soft-deleting keys
        // that should remain present during batch upload processes
        val oneDayInMillis = 86400000L
        val softDeletionThreshold = batchEndTsMillis - oneDayInMillis

        val iter = timedValues.iterator
        while (iter.hasNext) {
          val tv = iter.next()
          // Relax the deletion condition: only filter out values older than (batchEndTsMillis - 1 day)
          if (tv.millis >= softDeletionThreshold && tv.millis > maxTs) {
            resultBytes = tv.bytes
            maxTs = tv.millis
          }
        }

        resultBytes

      case _ => null
    }
  }

  /** Encapsulates a batch response that was found in the Fetcher's internal IR cache. */
  sealed abstract class CachedBatchResponse extends BatchResponses {
    // This is the case where we don't have bytes because the decoded IR was cached so we didn't hit the KV store again.
    def getBatchBytes(batchEndTsMillis: Long): Null = null
  }

  /** Encapsulates a decoded batch response that was found in the Fetcher's internal IR cache. */
  case class CachedFinalIrBatchResponse(response: FinalBatchIr) extends CachedBatchResponse

  /** Encapsulates a decoded batch response that was found in the Fetcher's internal IR cache */
  case class CachedMapBatchResponse(response: Map[String, AnyRef]) extends CachedBatchResponse
}
