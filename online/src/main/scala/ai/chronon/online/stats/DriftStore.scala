package ai.chronon.online.stats

import ai.chronon.api
import ai.chronon.api.Extensions.{JoinOps, MetadataOps, WindowOps}
import ai.chronon.api._
import ai.chronon.api.thrift.TSerializer
import ai.chronon.api.Constants
import ai.chronon.observability._
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.GetRequest
import ai.chronon.online.fetcher.{FetchContext, MetadataStore}
import org.slf4j.LoggerFactory

import scala.collection.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class DriftStore(kvStore: KVStore,
                 summaryDataset: String = Constants.TiledSummaryDataset,
                 metadataDataset: String = Constants.MetadataDataset) {

  private val fetchContext = FetchContext(kvStore, metadataDataset, timeoutMillis = 1000L)
  private val metadataStore = new MetadataStore(fetchContext)
  implicit private val executionContext: ExecutionContext = fetchContext.getOrCreateExecutionContext

  @transient private lazy val logger = LoggerFactory.getLogger(this.getClass)

  def tileKeysForJoin(join: api.Join,
                      slice: Option[String] = None,
                      columnNamePrefix: Option[String] = None): Map[String, Array[TileKey]] = {
    val joinName = join.getMetaData.getName
    val tileSize = join.getMetaData.driftTileSize

    require(tileSize.nonEmpty, s"Drift tile size not set for join $joinName")

    val tileSizeMillis = tileSize.get.millis

    // output columns by groupBy
    val outputValueColumnsMap = join.outputColumnsByGroup

    outputValueColumnsMap.mapValues {
      _.filter { col => columnNamePrefix.forall(col.startsWith) }
        .map { column =>
          val key = new TileKey()
          slice.foreach(key.setSlice)
          key.setName(joinName)
          key.setColumn(column)
          key.setSizeMillis(tileSizeMillis)
          key
        }
    }.toMap
  }

  private case class SummaryRequestContext(request: GetRequest, tileKey: TileKey, groupName: String)

  private case class SummaryResponseContext(summaries: Array[(TileSummary, Long)], tileKey: TileKey, groupName: String)

  case class TileSummaryInfo(key: TileKey, summaries: Array[(TileSummary, Long)]) {
    def percentileToIndex(percentile: String): Int = {
      // Convert "p5" to 5, "p95" to 95, etc.
      val value = percentile.stripPrefix("p").toInt
      // Convert percentile to index (20 total percentiles, from p0 to p100 in steps of 5)
      value / 5
    }

    def filterPercentiles(summary: TileSummary,
                          requestedPercentiles: Seq[String] = Seq("p5", "p50", "p95")): TileSummary = {
      val filtered = new TileSummary(summary)
      if (summary.getPercentiles != null) {
        val filteredPercentiles = new java.util.ArrayList[java.lang.Double]()
        // Convert percentile strings to indices
        val indices = requestedPercentiles.map(percentileToIndex)
        indices.foreach(i => filteredPercentiles.add(summary.getPercentiles.get(i)))
        filtered.setPercentiles(filteredPercentiles)
      }
      filtered
    }

    def toDriftSeries(driftMetric: DriftMetric, lookBack: Window, startMs: Long): TileDriftSeries = {
      val driftsArray = TileDriftCalculator.toTileDrifts(summaries, driftMetric, startMs, lookBack)
      val result = PivotUtils.pivot(driftsArray)

      // Convert TileKey to TileSeriesKey for TileDriftSeries
      val tileSeriesKey = new TileSeriesKey()
      tileSeriesKey.setSlice(key.getSlice)
      tileSeriesKey.setColumn(key.getColumn)
      tileSeriesKey.setNodeName(key.getName)
      // Note: groupName would need to be passed separately if needed

      result.setKey(tileSeriesKey)
    }

    def toSeries(requestedPercentiles: Seq[String] = Constants.DefaultPercentiles): TileSummarySeries = {
      // Filter percentiles before pivoting
      val filteredSummaries = summaries.map { case (summary, timestamp) =>
        (filterPercentiles(summary, requestedPercentiles), timestamp)
      }
      val result = PivotUtils.pivot(filteredSummaries)
      result.setKey(key)
    }
  }

  // scatter gathers via a single multi-get
  def getSummaries(joinConf: api.Join,
                   startMs: Option[Long],
                   endMs: Option[Long],
                   columnPrefix: Option[String]): Future[Seq[TileSummaryInfo]] = {

    val serializer: TSerializer = SerdeUtils.compactSerializer.get()
    val tileKeyMap = tileKeysForJoin(joinConf, None, columnPrefix)
    val requestContextMap: Map[GetRequest, SummaryRequestContext] = tileKeyMap.flatMap { case (group, keys) =>
      // Only create requests for keys that match our column prefix
      keys
        .filter(key => columnPrefix.forall(prefix => key.getColumn == prefix))
        .map { key =>
          val keyBytes = serializer.serialize(key)
          val get = GetRequest(keyBytes, summaryDataset, startTsMillis = startMs, endTsMillis = endMs)
          get -> SummaryRequestContext(get, key, group)
        }
    }

    val responseFuture = kvStore.multiGet(requestContextMap.keys.toSeq)

    responseFuture.map { responses =>
      val deserializer = SerdeUtils.compactDeserializer.get()
      // deserialize the responses and surround with context
      val responseContextTries: Seq[Try[SummaryResponseContext]] = responses.map { response =>
        val valuesTry = response.values
        val request = response.request
        val requestContext = requestContextMap(request)
        val tileKey = requestContext.tileKey
        val groupName = requestContext.groupName
        valuesTry.map { values =>
          val summaries =
            if (values == null)
              null
            else
              values.map { value =>
                val summary = new TileSummary()
                deserializer.deserialize(summary, value.bytes)
                summary -> value.millis
              }.toArray

          SummaryResponseContext(summaries, tileKey, groupName)
        }
      }

      // handle failures
      val responseContexts: Seq[SummaryResponseContext] = responseContextTries.flatMap {
        _ match {
          case Success(responseContext) => Some(responseContext)
          // TODO instrument failures
          case Failure(exception) =>
            logger.error("Failed to fetch summary response", exception)
            None
        }
      }

      responseContexts.map { responseContext =>
        val tileKey = new TileKey()
        tileKey.setSlice(responseContext.tileKey.getSlice)
        tileKey.setName(joinConf.getMetaData.name)
        tileKey.setColumn(responseContext.tileKey.getColumn)
        tileKey.setSizeMillis(responseContext.tileKey.getSizeMillis)

        TileSummaryInfo(tileKey, responseContext.summaries)
      }
    }
  }

  case class Range(startMs: Long, endMs: Long) {
    // if lookBack is too large and the range is too small, we will make separate queries
    // e.g. if we have a range of 1 day and lookBack of 1 week, we will make two queries (8 days ago, 7 days ago) + (1 day ago, today)
    // if the range is too large and the lookBack is too small, we will make single query
    // e.g. if we have a range of 1 week and lookBack of 1 day, we will make single query - (8 days ago, today)
    def lookBack(lookBackMs: Long): (Range, Option[Range]) = {
      if (endMs - lookBackMs >= startMs) { // single query
        Range(startMs - lookBackMs, endMs) -> None
      } else {
        Range(startMs - lookBackMs, endMs - lookBackMs) -> Some(Range(startMs, endMs))
      }
    }
  }

  private def getSummariesForRange(join: api.Join,
                                   range: Range,
                                   lookBack: Long,
                                   columnPrefix: Option[String] = None): Future[Seq[TileSummaryInfo]] = {
    val (currentRange, oldRangeOpt) = range.lookBack(lookBack)
    val currentSummaries = getSummaries(join, Some(currentRange.startMs), Some(currentRange.endMs), columnPrefix)
    if (oldRangeOpt.isEmpty) {
      currentSummaries
    } else {
      val oldRange = oldRangeOpt.get
      val oldSummaries = getSummaries(join, Some(oldRange.startMs), Some(oldRange.endMs), columnPrefix)
      Future.sequence(Seq(currentSummaries, oldSummaries)).map { case Seq(current, old) =>
        old ++ current
      }
    }
  }

  def getDriftSeries(join: String,
                     driftMetric: DriftMetric,
                     lookBack: Window,
                     startMs: Long,
                     endMs: Long,
                     columnPrefix: Option[String] = None): Try[Future[Seq[TileDriftSeries]]] = {
    metadataStore.getJoinConf(join).map { joinConf =>
      // TODO-explore: we might be over fetching if lookBack is much larger than end - start
      getSummariesForRange(joinConf.join, Range(startMs, endMs), lookBack.millis, columnPrefix).map {
        tileSummaryInfos =>
          tileSummaryInfos.map { tileSummaryInfo =>
            tileSummaryInfo.toDriftSeries(driftMetric, lookBack, startMs)
          }
      }
    }
  }

  def getSummarySeries(join: String,
                       startMs: Long,
                       endMs: Long,
                       columnPrefix: Option[String] = None,
                       percentiles: Seq[String] = Constants.DefaultPercentiles): Try[Future[Seq[TileSummarySeries]]] = {
    metadataStore.getJoinConf(join).map { joinConf =>
      getSummaries(joinConf.join, Some(startMs), Some(endMs), columnPrefix).map { tileSummaryInfos =>
        tileSummaryInfos.map { tileSummaryInfo =>
          tileSummaryInfo.toSeries(percentiles)
        }
      }
    }
  }
}

object DriftStore {
  def breaks(count: Int): Seq[String] = (0 to count).map(_ * (100 / count)).map("p" + _.toString)
}
