package ai.chronon.integrations.cloud_gcp

import ai.chronon.online.{
  Api,
  ExternalSourceRegistry,
  FlagStore,
  FlagStoreConstants,
  GroupByServingInfoParsed,
  KVStore,
  KafkaLoggableResponseConsumer,
  LoggableResponse,
  TopicInfo
}
import ai.chronon.online.serde.{AvroConversions, AvroSerDe, SerDe}
import com.google.api.gax.core.{InstantiatingExecutorProvider, NoCredentialsProvider}
import com.google.api.gax.retrying.RetrySettings
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings
import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.BigtableDataSettings
import com.google.cloud.bigtable.data.v2.stub.metrics.NoopMetricsProvider

import java.time.Duration
import java.util
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.function.Consumer

class GcpApiImpl(conf: Map[String, String]) extends Api(conf) {

  import GcpApiImpl._

  // For now we have a flag store that relies on some hardcoded values. Over time we can replace this with something
  // more sophisticated (e.g. service / teams.json based flags)
  val tilingEnabledFlagStore: FlagStore = (flagName: String, _: util.Map[String, String]) => {
    if (flagName == FlagStoreConstants.TILING_ENABLED) {
      true
    } else {
      false
    }
  }

  // We set the flag store to always return true for tiling enabled
  setFlagStore(tilingEnabledFlagStore)

  lazy val responseConsumer: Consumer[LoggableResponse] =
    getOptional(FetcherOOCTopicInfo, conf)
      .map(t => TopicInfo.parse(t)) match {
      case Some(topicInfo) if topicInfo.messageBus.toLowerCase == "kafka" =>
        val maybeSchemaRegistryId = getOptional(SchemaRegistryId, conf).map(_.toInt)
        new KafkaLoggableResponseConsumer(topicInfo, maybeSchemaRegistryId)
      case _ =>
        // fall back to no-op consumer
        logger.info("Falling back to NoOp online/offline response consumer as FETCHER_OOC_TOPIC_INFO isn't configured")
        (_: LoggableResponse) => {}
    }

  // BigTable clients tend to be expensive to create as they spin up a lot of threads and connections. The recommendation
  // is to create a single client per process and reuse it. This isn't an issue in the fetcher / service context. However,
  // in Flink jobs we can pack multiple tasks per task manager JVM and we don't want to create a new client per task as well
  // as avoid creating a new client when the task manager restarts (.open(..) method).
  override def genKvStore: KVStore = {
    // Try to get existing shared store first
    Option(sharedKvStore.get()) match {
      case Some(existingStore) =>
        existingStore
      case None =>
        kvStoreLock.synchronized {
          // Double check if another thread created the store while we were waiting for the lock
          Option(sharedKvStore.get()) match {
            case Some(existingStore) => existingStore
            case None =>
              val newStore = createKVStore()
              sharedKvStore.set(newStore)
              newStore
          }
        }
    }
  }

  override def streamDecoder(groupByServingInfoParsed: GroupByServingInfoParsed): SerDe =
    new AvroSerDe(AvroConversions.fromChrononSchema(groupByServingInfoParsed.streamChrononSchema))

  private def createKVStore(): KVStore = {

    val projectId = getOrElseThrow(GcpProjectId, conf)
    val instanceId = getOrElseThrow(GcpBigTableInstanceId, conf)
    val maybeAppProfileId = getOptional(GcpBigTableAppProfileId, conf)

    // We skip upload clients (e.g. admin client, bq client) in non-upload contexts (e.g. streaming & fetching)
    // This flag allows us to enable them in the upload contexts
    val enableUploadClients = getOptional(EnableUploadClients, conf).exists(_.toBoolean)

    // Create settings builder based on whether we're in emulator mode (e.g. docker) or not
    val (dataSettingsBuilder, maybeAdminSettingsBuilder, maybeBQClient) = sys.env.get(BigTableEmulatorHost) match {

      case Some(emulatorHostPort) =>
        val (emulatorHost, emulatorPort) = (emulatorHostPort.split(":")(0), emulatorHostPort.split(":")(1).toInt)

        val dataSettingsBuilder =
          BigtableDataSettings
            .newBuilderForEmulator(emulatorHost, emulatorPort)
            .setCredentialsProvider(NoCredentialsProvider.create())
            .setMetricsProvider(NoopMetricsProvider.INSTANCE) // opt out of metrics in emulator

        val adminSettingsBuilder =
          BigtableTableAdminSettings
            .newBuilderForEmulator(emulatorHost, emulatorPort)
            .setCredentialsProvider(NoCredentialsProvider.create())

        (dataSettingsBuilder, Some(adminSettingsBuilder), None)

      case None =>
        val dataSettingsBuilder = BigtableDataSettings.newBuilder()
        val dataSettingsBuilderWithProfileId =
          maybeAppProfileId
            .map(profileId => dataSettingsBuilder.setAppProfileId(profileId))
            .getOrElse(dataSettingsBuilder)
        if (enableUploadClients) {
          val adminSettingsBuilder = BigtableTableAdminSettings.newBuilder()
          val bigQueryClient = BigQueryOptions.getDefaultInstance.getService
          (dataSettingsBuilderWithProfileId, Some(adminSettingsBuilder), Some(bigQueryClient))
        } else {
          (dataSettingsBuilderWithProfileId, None, None)
        }
    }

    // override the bulk read batch settings
    setBigTableBulkReadRowsSettings(dataSettingsBuilder)

    // override thread pools
    setClientThreadPools(dataSettingsBuilder, maybeAdminSettingsBuilder)

    // override retry & timeout settings
    setClientRetrySettings(dataSettingsBuilder, conf)

    val dataSettings = dataSettingsBuilder.setProjectId(projectId).setInstanceId(instanceId).build()
    val dataClient = BigtableDataClient.create(dataSettings)

    val maybeAdminClient = maybeAdminSettingsBuilder.map { adminSettingsBuilder =>
      val adminSettings = adminSettingsBuilder.setProjectId(projectId).setInstanceId(instanceId).build()
      BigtableTableAdminClient.create(adminSettings)
    }

    logger.info(
      s"Creating BigTableStore for $projectId and $instanceId. " +
        s"Params: profileId: $maybeAppProfileId, adminClientEnabled: $enableUploadClients, " +
        s"num procs = ${Runtime.getRuntime.availableProcessors()}")
    new BigTableKVStoreImpl(dataClient, maybeAdminClient, maybeBQClient, conf)
  }

  // BigTable's bulk read rows by default will batch calls and wait for a delay before sending them. This is not
  // ideal from a latency perspective, so we set the batching settings to be 1 element and no delay.
  private def setBigTableBulkReadRowsSettings(dataSettingsBuilderWithProfileId: BigtableDataSettings.Builder): Unit = {
    // Get the bulkReadRowsSettings builder
    val bulkReadRowsSettingsBuilder = dataSettingsBuilderWithProfileId
      .stubSettings()
      .bulkReadRowsSettings()

    // Update the batching settings directly on the builder
    bulkReadRowsSettingsBuilder
      .setBatchingSettings(
        bulkReadRowsSettingsBuilder.getBatchingSettings.toBuilder
          .setElementCountThreshold(1)
          .setDelayThresholdDuration(null)
          .build()
      )
  }

  private def setClientRetrySettings(dataSettingsBuilder: BigtableDataSettings.Builder,
                                     conf: Map[String, String]): Unit = {
    // pull retry settings from env vars
    val initialRpcTimeoutDuration =
      getOptional(BigTableInitialRpcTimeoutDuration, conf)
        .map(Duration.parse)
        .getOrElse(GcpApiImpl.DefaultInitialRpcTimeoutDuration)

    val rpcTimeoutMultiplier =
      getOptional(BigTableRpcTimeoutMultiplier, conf)
        .map(_.toDouble)
        .getOrElse(GcpApiImpl.DefaultRpcTimeoutMultiplier)

    val maxRpcTimeoutDuration =
      getOptional(BigTableMaxRpcTimeoutDuration, conf)
        .map(Duration.parse)
        .getOrElse(GcpApiImpl.DefaultMaxRpcTimeoutDuration)

    val totalTimeoutDuration =
      getOptional(BigTableTotalTimeoutDuration, conf)
        .map(Duration.parse)
        .getOrElse(GcpApiImpl.DefaultTotalTimeoutDuration)
    val maxAttempts =
      getOptional(BigTableMaxAttempts, conf)
        .map(_.toInt)
        .getOrElse(GcpApiImpl.DefaultMaxAttempts)

    val retrySettings =
      RetrySettings
        .newBuilder()
        // retry immediately
        .setInitialRetryDelayDuration(Duration.ZERO)
        // time we wait for the first attempt before we time out
        .setInitialRpcTimeoutDuration(initialRpcTimeoutDuration)
        // allow rpc timeouts to grow a bit more lenient
        .setRpcTimeoutMultiplier(rpcTimeoutMultiplier)
        // set a cap on how long we wait for a single rpc call
        .setMaxRpcTimeoutDuration(maxRpcTimeoutDuration)
        // absolute limit on how long to keep trying until giving up
        .setTotalTimeoutDuration(totalTimeoutDuration)
        .setMaxAttempts(maxAttempts) // we retry maxAttempt times (for a total of maxAttempt + 1 tries)
        .build()

    // Update the retry settings directly on the builder
    dataSettingsBuilder.stubSettings().readRowsSettings().setRetrySettings(retrySettings)
    dataSettingsBuilder.stubSettings().bulkReadRowsSettings().setRetrySettings(retrySettings)
    dataSettingsBuilder.stubSettings().mutateRowSettings().setRetrySettings(retrySettings)
  }

  // BigTable's client creates a thread pool with a size of cores * 4. This ends up being a lot larger than we'd like
  // so we scale these down and we also use the same in both clients
  private def setClientThreadPools(
      dataSettingsBuilderWithProfileId: BigtableDataSettings.Builder,
      maybeAdminSettingsBuilder: Option[BigtableTableAdminSettings.Builder]
  ): Unit = {
    dataSettingsBuilderWithProfileId.stubSettings().setBackgroundExecutorProvider(executorProvider)
    maybeAdminSettingsBuilder.foreach(adminSettingsBuilder =>
      adminSettingsBuilder.stubSettings().setBackgroundExecutorProvider(executorProvider))
  }

  // TODO: Load from user jar.
  @transient lazy val registry: ExternalSourceRegistry = new ExternalSourceRegistry()

  override def externalRegistry: ExternalSourceRegistry = registry

  override def logResponse(resp: LoggableResponse): Unit = responseConsumer.accept(resp)
}

object GcpApiImpl {

  private[cloud_gcp] val GcpProjectId = "GCP_PROJECT_ID"
  private[cloud_gcp] val GcpBigTableInstanceId = "GCP_BIGTABLE_INSTANCE_ID"
  private[cloud_gcp] val GcpBigTableAppProfileId = "GCP_BIGTABLE_APP_PROFILE_ID"
  private[cloud_gcp] val EnableUploadClients = "ENABLE_UPLOAD_CLIENTS"
  private[cloud_gcp] val BigTableEmulatorHost = "BIGTABLE_EMULATOR_HOST"

  private[cloud_gcp] val BigTableInitialRpcTimeoutDuration = "BIGTABLE_INITIAL_RPC_TIMEOUT_DURATION"
  private[cloud_gcp] val BigTableMaxRpcTimeoutDuration = "BIGTABLE_MAX_RPC_TIMEOUT_DURATION"
  private[cloud_gcp] val BigTableTotalTimeoutDuration = "BIGTABLE_TOTAL_TIMEOUT_DURATION"
  private[cloud_gcp] val BigTableMaxAttempts = "BIGTABLE_MAX_ATTEMPTS"
  private[cloud_gcp] val BigTableRpcTimeoutMultiplier = "BIGTABLE_RPC_TIMEOUT_MULTIPLIER"

  private[cloud_gcp] val FetcherOOCTopicInfo = "FETCHER_OOC_TOPIC_INFO"
  private[cloud_gcp] val SchemaRegistryId = "SCHEMA_REGISTRY_ID"

  private val DefaultInitialRpcTimeoutDuration = Duration.ofMillis(100L)
  private val DefaultRpcTimeoutMultiplier = 1.25
  private val DefaultMaxRpcTimeoutDuration = Duration.ofMillis(200L)
  private val DefaultTotalTimeoutDuration = Duration.ofMillis(500L)
  private val DefaultMaxAttempts = 2

  private val sharedKvStore = new AtomicReference[KVStore]()
  private val kvStoreLock = new Object()

  private[cloud_gcp] def getOptional(key: String, conf: Map[String, String]): Option[String] =
    sys.env
      .get(key)
      .orElse(conf.get(key))

  private[cloud_gcp] def getOrElseThrow(key: String, conf: Map[String, String]): String =
    sys.env
      .get(key)
      .orElse(conf.get(key))
      .getOrElse(throw new IllegalArgumentException(s"$key environment variable not set"))

  // Create a thread factory so that we can name the threads for easier debugging
  val threadFactory: ThreadFactory = new ThreadFactory {
    private val counter = new AtomicInteger(0)
    override def newThread(r: Runnable): Thread = {
      val t = new Thread(r)
      t.setName(s"chronon-bt-gax-${counter.incrementAndGet()}")
      t
    }
  }

  // override the executor provider to use a custom named thread factory
  lazy val executorProvider: InstantiatingExecutorProvider = InstantiatingExecutorProvider
    .newBuilder()
    .setExecutorThreadCount(Runtime.getRuntime.availableProcessors() * 4)
    .setThreadFactory(threadFactory)
    .build()
}
