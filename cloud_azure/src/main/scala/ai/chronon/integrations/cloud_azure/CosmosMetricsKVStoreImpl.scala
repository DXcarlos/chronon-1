package ai.chronon.integrations.cloud_azure

import ai.chronon.integrations.cloud_azure.CosmosKVStore._
import ai.chronon.integrations.cloud_azure.CosmosKVStoreConstants._
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore._
import ai.chronon.online.metrics.Metrics
import com.azure.cosmos._
import com.azure.cosmos.models._
import com.azure.cosmos.util.CosmosPagedFlux
import org.slf4j.{Logger, LoggerFactory}

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

/** Cosmos KV store for Iceberg partition stats.
  *
  * Unlike feature batch data, partition stats are raw row-key time series:
  *   - keyBytes are strings such as <outputTable>#NULL_COUNTS or <outputTable>#schema
  *   - timestamped metric rows must support range reads
  *   - schema rows are stored as latest-value metadata
  */
class CosmosMetricsKVStoreImpl(database: CosmosAsyncDatabase, conf: Map[String, String] = Map.empty) extends KVStore {

  @transient override lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private val containerCache = new TrieMap[String, CosmosAsyncContainer]()
  private val ensuredContainers = new TrieMap[String, Boolean]()
  private val slowOperationThresholdMs = conf.getOrElse("cosmos.slow_operation_threshold_ms", "1000").toLong

  protected val metricsContext: Metrics.Context =
    Metrics.Context(Metrics.Environment.KVStore).withSuffix("cosmos_metrics")

  protected val containerToContext = new TrieMap[String, Metrics.Context]()

  override def create(dataset: String): Unit = create(dataset, Map.empty)

  override def create(dataset: String, props: Map[String, Any]): Unit = {
    val requestId = UUID.randomUUID().toString
    val containerName = metricsContainerName(dataset)
    logger.info(s"[metrics.create] [requestId=$requestId] container=$containerName, dataset=$dataset")

    val isEmulator = props.get(PropEmulatorMode).exists(_.toString.toBoolean) ||
      CosmosKVStoreConstants.isEmulator(
        sys.env.getOrElse(CosmosKVStoreConstants.EnvCosmosEndpoint,
                          props.getOrElse(CosmosKVStoreConstants.EnvCosmosEndpoint, "").toString)
      )

    val paths = new java.util.ArrayList[String]()
    paths.add("/keyHash")
    val partitionKeyDef =
      if (isEmulator) new PartitionKeyDefinition().setPaths(paths)
      else
        new PartitionKeyDefinition()
          .setPaths(paths)
          .setVersion(PartitionKeyDefinitionVersion.V2)
          .setKind(PartitionKind.HASH)

    val containerProperties = new CosmosContainerProperties(containerName, partitionKeyDef)
    val throughputProps = props.get(PropThroughput) match {
      case Some(manualRU: Int) =>
        ThroughputProperties.createManualThroughput(manualRU)
      case _ =>
        val maxRU = props.getOrElse(PropAutoscale, DefaultAutoscaleMaxRU).asInstanceOf[Int]
        ThroughputProperties.createAutoscaledThroughput(maxRU)
    }

    val startTime = System.currentTimeMillis()
    val response = Await.result(
      monoToScalaFuture(database.createContainerIfNotExists(containerProperties, throughputProps)),
      120.seconds
    )
    val duration = System.currentTimeMillis() - startTime
    logger.info(
      s"[metrics.create] [requestId=$requestId] Success in ${duration}ms, diagnostics=${response.getDiagnostics}")
    ensuredContainers.put(containerName, true)
    metricsContext.increment("create.successes")
    metricsContext.distribution("create.latency", duration)
  }

  override def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]] =
    Future.sequence(requests.zipWithIndex.map { case (request, idx) =>
      val requestId = s"${UUID.randomUUID()}-i$idx"
      if (request.startTsMillis.isDefined) queryTimeRange(request, requestId)
      else readLatest(request, requestId)
    })

  override def multiPut(requests: Seq[PutRequest]): Future[Seq[Boolean]] =
    Future.sequence(requests.zipWithIndex.map { case (request, idx) =>
      val requestId = s"${UUID.randomUUID()}-i$idx"
      upsertMetric(request, requestId)
    })

  override def list(request: ListRequest): Future[ListResponse] = {
    val containerName = metricsContainerName(request.dataset)
    val container = getOrCreateContainerRef(containerName)
    val querySpec = new SqlQuerySpec(
      """SELECT c.keyBytes, c.valueBytes
        |FROM c
        |WHERE c.dataset = @dataset""".stripMargin
    )
    querySpec.setParameters(new java.util.ArrayList[SqlParameter]())
    querySpec.getParameters.add(new SqlParameter("@dataset", request.dataset))

    val options = new CosmosQueryRequestOptions()
    val flux: CosmosPagedFlux[java.util.Map[String, Object]] =
      container.queryItems(querySpec, options, classOf[java.util.Map[String, Object]])

    monoToScalaFuture(flux.collectList())
      .map { docs =>
        val values = docs.asScala.map { doc =>
          ListValue(extractBytes(doc.get("keyBytes")), extractBytes(doc.get("valueBytes")))
        }.toSeq
        ListResponse(request, Success(values), Map.empty)
      }
      .recover { case e: Exception =>
        logger.error(s"[metrics.list] Failed for dataset ${request.dataset}", e)
        ListResponse(request, Failure(e), Map.empty)
      }
  }

  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit =
    throw new UnsupportedOperationException("bulkPut is not supported for CosmosMetricsKVStoreImpl")

  private def upsertMetric(request: PutRequest, requestId: String): Future[Boolean] = {
    val containerName = metricsContainerName(request.dataset)
    val container = getOrCreateContainerRef(containerName)
    val context = containerToContext.getOrElseUpdate(containerName, metricsContext.copy(dataset = containerName))
    val keyHash = buildKeyHash(request.keyBytes)
    val tsMillis = request.tsMillis.getOrElse(System.currentTimeMillis())
    val docId = request.tsMillis match {
      case Some(ts) => buildMetricsDocumentId(request.dataset, keyHash, ts)
      case None     => buildMetricsLatestDocumentId(request.dataset, keyHash)
    }

    val doc = new java.util.HashMap[String, Object]()
    doc.put("id", docId)
    doc.put("dataset", request.dataset)
    doc.put("keyHash", keyHash)
    doc.put("keyBytes", request.keyBytes)
    doc.put("valueBytes", request.valueBytes)
    doc.put("tsMillis", tsMillis.asInstanceOf[Object])

    val partitionKey = new PartitionKeyBuilder().add(keyHash).build()
    val startTime = System.currentTimeMillis()
    monoToScalaFuture(container.upsertItem(doc, partitionKey, new CosmosItemRequestOptions()))
      .map { response =>
        val duration = System.currentTimeMillis() - startTime
        context.increment("multiPut.successes")
        context.distribution("multiPut.latency", duration)
        if (duration > slowOperationThresholdMs) {
          logger.warn(
            s"[metrics.multiPut] [requestId=$requestId] Slow upsert ${duration}ms for dataset=${request.dataset}, docId=$docId, diagnostics=${response.getDiagnostics}")
        }
        true
      }
      .recover { case e: Exception =>
        logger.error(s"[metrics.multiPut] [requestId=$requestId] Failed for dataset=${request.dataset}, docId=$docId",
                     e)
        context.increment("multiPut.failures", Map("exception" -> e.getClass.getName))
        false
      }
  }

  private def readLatest(request: GetRequest, requestId: String): Future[GetResponse] = {
    val containerName = metricsContainerName(request.dataset)
    val container = getOrCreateContainerRef(containerName)
    val keyHash = buildKeyHash(request.keyBytes)
    val docId = buildMetricsLatestDocumentId(request.dataset, keyHash)
    val partitionKey = new PartitionKeyBuilder().add(keyHash).build()

    monoToScalaFuture(container.readItem(docId, partitionKey, classOf[java.util.Map[String, Object]]))
      .map { response =>
        val doc = response.getItem
        val valueBytes = extractBytes(doc.get("valueBytes"))
        val tsMillis = doc.get("tsMillis").asInstanceOf[Number].longValue()
        GetResponse(request, Success(Seq(TimedValue(valueBytes, tsMillis))))
      }
      .recover {
        case e: CosmosException if e.getStatusCode == 404 =>
          logger.debug(s"[metrics.multiGet] [requestId=$requestId] Latest document not found: docId=$docId")
          GetResponse(request, Success(Seq.empty))
        case e: Exception =>
          logger.error(s"[metrics.multiGet] [requestId=$requestId] Failed latest read: docId=$docId", e)
          GetResponse(request, Failure(e))
      }
  }

  private def queryTimeRange(request: GetRequest, requestId: String): Future[GetResponse] = {
    val containerName = metricsContainerName(request.dataset)
    val container = getOrCreateContainerRef(containerName)
    val keyHash = buildKeyHash(request.keyBytes)
    val startTs = request.startTsMillis.get
    val endTs = request.endTsMillis.getOrElse(System.currentTimeMillis())

    val querySpec = new SqlQuerySpec(
      """SELECT c.valueBytes, c.tsMillis
        |FROM c
        |WHERE c.dataset = @dataset
        |  AND c.keyHash = @keyHash
        |  AND c.tsMillis >= @startTs
        |  AND c.tsMillis <= @endTs""".stripMargin
    )
    querySpec.setParameters(new java.util.ArrayList[SqlParameter]())
    querySpec.getParameters.add(new SqlParameter("@dataset", request.dataset))
    querySpec.getParameters.add(new SqlParameter("@keyHash", keyHash))
    querySpec.getParameters.add(new SqlParameter("@startTs", startTs))
    querySpec.getParameters.add(new SqlParameter("@endTs", endTs))

    val options = new CosmosQueryRequestOptions()
    options.setPartitionKey(new PartitionKeyBuilder().add(keyHash).build())

    val startTime = System.currentTimeMillis()
    val flux: CosmosPagedFlux[java.util.Map[String, Object]] =
      container.queryItems(querySpec, options, classOf[java.util.Map[String, Object]])

    monoToScalaFuture(flux.collectList())
      .map { docs =>
        val duration = System.currentTimeMillis() - startTime
        if (duration > slowOperationThresholdMs) {
          logger.warn(
            s"[metrics.multiGet] [requestId=$requestId] Slow range query ${duration}ms for dataset=${request.dataset}, keyHash=$keyHash")
        }
        val values = docs.asScala
          .map { doc =>
            val valueBytes = extractBytes(doc.get("valueBytes"))
            val tsMillis = doc.get("tsMillis").asInstanceOf[Number].longValue()
            TimedValue(valueBytes, tsMillis)
          }
          .toSeq
          .sortBy(_.millis)
        GetResponse(request, Success(values))
      }
      .recover { case e: Exception =>
        logger.error(
          s"[metrics.multiGet] [requestId=$requestId] Failed range query for dataset=${request.dataset}, keyHash=$keyHash",
          e)
        GetResponse(request, Failure(e))
      }
  }

  private def getOrCreateContainerRef(containerName: String): CosmosAsyncContainer = {
    ensuredContainers.getOrElseUpdate(containerName, {
                                        create(containerName)
                                        true
                                      })
    containerCache.getOrElseUpdate(containerName, database.getContainer(containerName))
  }

  private def monoToScalaFuture[T](mono: reactor.core.publisher.Mono[T]): Future[T] = {
    val promise = Promise[T]()
    mono.subscribe(
      new reactor.core.publisher.BaseSubscriber[T]() {
        override def hookOnNext(value: T): Unit = promise.success(value)
        override def hookOnError(throwable: Throwable): Unit = promise.failure(throwable)
        override def hookOnComplete(): Unit =
          if (!promise.isCompleted)
            promise.failure(new NoSuchElementException("Mono completed without emitting a value"))
      }
    )
    promise.future
  }

  private def extractBytes(value: Any): Array[Byte] =
    value match {
      case bytes: Array[Byte] => bytes
      case str: String        => java.util.Base64.getDecoder.decode(str)
      case _                  => throw new IllegalArgumentException(s"Cannot convert ${value.getClass} to byte array")
    }

  private def metricsContainerName(dataset: String): String = dataset.toLowerCase

  private def buildMetricsDocumentId(dataset: String, keyHash: String, tsMillis: Long): String =
    s"${dataset}_${keyHash}_$tsMillis"

  private def buildMetricsLatestDocumentId(dataset: String, keyHash: String): String =
    s"${dataset}_${keyHash}_latest"
}
