package ai.chronon.spark.submission

import ai.chronon.api.JobStatusType
import ai.chronon.api.submission.JobSubmitterConstants._
import ai.chronon.api.submission.{FlinkJob, JobSubmitter, JobType, SparkJob, StorageClient}
import io.vertx.core.json.JsonObject
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

/** Crucible REST API based job submitter.
  *
  * Submits Spark and Flink jobs to a Crucible gateway via its REST API.
  * Crucible manages the full lifecycle: K8s CRD creation, cloud identity,
  * log archival, and UI proxy.
  *
  * @param baseUrl URL of the Crucible gateway (e.g., "http://crucible.crucible-system:8080")
  * @param namespace Crucible namespace to submit jobs into (e.g., "my-team")
  * @param sparkImage Docker image for Spark jobs
  * @param flinkImage Docker image for Flink jobs
  * @param storageClient Optional storage client for Flink checkpoint resolution
  */
class CrucibleSubmitter(
    baseUrl: String,
    namespace: String,
    sparkImage: String,
    flinkImage: String,
    override val jarName: String = "cloud_gcp_deploy.jar",
    override val onlineClass: String = "",
    override val tablePartitionsDataset: String = "",
    override val dqMetricsDataset: String = "",
    override val kvStoreApiProperties: Map[String, String] = Map.empty,
    storageClient: Option[StorageClient] = None
) extends JobSubmitter {

  @transient override lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private val client = new CrucibleClient(baseUrl, namespace)

  override def submit(
      jobType: JobType,
      submissionProperties: Map[String, String],
      jobProperties: Map[String, String],
      files: List[String],
      labels: Map[String, String],
      args: String*
  ): String = {
    val mainClass = submissionProperties.getOrElse(MainClass, "")
    val jarUri = submissionProperties.getOrElse(JarURI, "")
    val jobName = labels.getOrElse(MetadataName, s"chronon-${System.currentTimeMillis()}")

    val body = new JsonObject()
    body.put("name", sanitizeName(jobName))

    jobType match {
      case SparkJob =>
        body.put("type", "spark")
        body.put("image", sparkImage)
        body.put("mainClass", mainClass)
        body.put("jar", jarUri)

        // Pass Spark conf as the conf map.
        // Also add the jar to spark.driver.extraClassPath and
        // spark.executor.extraClassPath so catalog/format-provider classes
        // are on the system classloader. The Spark Operator downloads
        // mainApplicationFile to /opt/spark/work-dir/<filename>.
        val confObj = new JsonObject()
        jobProperties.foreach { case (k, v) => confObj.put(k, v) }
        if (jarUri.nonEmpty) {
          val localJarPath = "/opt/spark/work-dir/" + jarUri.split("/").last
          confObj.put("spark.driver.extraClassPath", localJarPath)
          confObj.put("spark.executor.extraClassPath", localJarPath)
        }
        if (confObj.size() > 0) {
          body.put("conf", confObj)
        }

      case FlinkJob =>
        body.put("type", "flink")
        body.put("image", flinkImage)

        val flinkMainClass = submissionProperties.getOrElse(MainClass, FlinkMainClass)
        body.put("mainClass", flinkMainClass)

        val flinkJarUri = submissionProperties.getOrElse(FlinkMainJarURI, jarUri)
        body.put("jar", flinkJarUri)

        // Additional deploy jars: cloud API jar (GcpApiImpl/AwsApiImpl) and
        // PubSub/Kinesis connector. These change per release and are downloaded
        // by the Crucible gateway's init container to /opt/flink/usrlib/.
        //
        // Static jars (Spark catalyst, Hadoop client, etc.) are baked into the
        // Flink image at /opt/flink/usrlib/ — see docker/flink/Dockerfile.
        val additionalJars = scala.collection.mutable.ArrayBuffer[String]()

        // Cloud API jar (contains GcpApiImpl, AwsApiImpl, etc.)
        additionalJars += jarUri

        // PubSub / Kinesis connector jar
        submissionProperties.get(FlinkPubSubConnectorJarURI).foreach(additionalJars += _)
        submissionProperties.get(FlinkKinesisConnectorJarURI).foreach(additionalJars += _)

        if (additionalJars.nonEmpty) {
          val jarsArray = new io.vertx.core.json.JsonArray()
          additionalJars.foreach(jarsArray.add)
          body.put("jars", jarsArray)
        }

        // Flink conf: merge jobProperties + checkpoint/savepoint URIs.
        // NodeSubmitter encodes env vars as spark.kubernetes.driverEnv.* Spark config.
        // Convert these to Flink-compatible containerized.master.env.* and
        // containerized.taskmanager.env.* so the Flink Operator sets them as
        // environment variables on the JM/TM pods.
        val flinkConf = new JsonObject()
        val sparkDriverEnvPrefix = "spark.kubernetes.driverEnv."
        jobProperties.foreach { case (k, v) =>
          flinkConf.put(k, v)
          if (k.startsWith(sparkDriverEnvPrefix)) {
            val envName = k.stripPrefix(sparkDriverEnvPrefix)
            flinkConf.put(s"containerized.master.env.$envName", v)
            flinkConf.put(s"containerized.taskmanager.env.$envName", v)
          }
        }
        // Override spark.driver.memory for the Flink JVM. Spark catalyst initializes
        // inside the Flink JM and reads spark.driver.memory from system properties.
        // The default from batch Spark config (512m+) exceeds the Flink JM heap (~450MB),
        // causing INVALID_DRIVER_MEMORY. We pass it via env.java.opts which the Flink
        // Operator adds to JVM startup flags for both JM and TM.
        val sparkMemOpts = " -Dspark.driver.memory=128m -Dspark.testing.reservedMemory=0"
        val existingJmOpts = Option(flinkConf.getString("env.java.opts.jobmanager")).getOrElse("")
        val existingTmOpts = Option(flinkConf.getString("env.java.opts.taskmanager")).getOrElse("")
        flinkConf.put("env.java.opts.jobmanager", existingJmOpts + sparkMemOpts)
        flinkConf.put("env.java.opts.taskmanager", existingTmOpts + sparkMemOpts)
        submissionProperties.get(FlinkCheckpointUri).foreach { uri =>
          flinkConf.put("state.checkpoints.dir", uri)
          flinkConf.put("state.savepoints.dir", uri)
        }
        submissionProperties.get(SavepointUri).foreach { uri =>
          flinkConf.put("execution.savepoint.path", uri)
        }

        if (flinkConf.size() > 0) {
          body.put("conf", flinkConf)
        }
    }

    // Application args (filtered to exclude internal submission args).
    val appArgs = JobSubmitter.getApplicationArgs(jobType, args.toArray)
    if (appArgs.nonEmpty) {
      val argsArray = new io.vertx.core.json.JsonArray()
      appArgs.foreach(argsArray.add)
      body.put("args", argsArray)
    }

    // Idempotency key from job ID if provided
    submissionProperties.get(JobId).foreach(body.put("idempotencyKey", _))

    client.submitJob(body)
  }

  override def status(jobId: String): JobStatusType = {
    try {
      val (status, httpCode) = client.getJobStatus(jobId)
      if (httpCode == 404) {
        // Job ID not found in Crucible (never created or wrong namespace).
        // Return UNKNOWN — the orchestrator will retry or error out.
        logger.warn(s"Job $jobId not found (404), returning UNKNOWN")
        return JobStatusType.UNKNOWN
      }
      mapStatus(status)
    } catch {
      case e: CrucibleApiException =>
        logger.error(s"Error getting status for job $jobId: ${e.getMessage}")
        JobStatusType.UNKNOWN
      case e: Exception =>
        logger.error(s"Unexpected error getting status for job $jobId", e)
        JobStatusType.UNKNOWN
    }
  }

  override def kill(jobId: String): Unit = {
    client.killJob(jobId)
  }

  override def close(): Unit = {
    client.close()
  }

  // --- URL methods ---

  override def getJobUrl(jobId: String): Option[String] =
    Some(s"$baseUrl/api/v1/namespaces/$namespace/jobs/$jobId")

  override def getSparkUrl(jobId: String): Option[String] =
    Some(s"$baseUrl/api/v1/namespaces/$namespace/jobs/$jobId/ui")

  override def getFlinkUrl(jobId: String): Option[String] =
    Some(s"$baseUrl/api/v1/namespaces/$namespace/jobs/$jobId/ui")

  // --- Lifecycle methods ---

  override def isClusterCreateNeeded(isLongRunning: Boolean): Boolean = false

  override def ensureClusterReady(clusterName: String, clusterConf: Option[Map[String, String]])(implicit
      ec: ExecutionContext): Option[String] = Some(clusterName)

  // --- Flink submission ---

  override def buildFlinkSubmissionProps(env: Map[String, String],
                                         version: String,
                                         artifactPrefix: String): Map[String, String] = {
    val flinkJarUri = s"$artifactPrefix/release/$version/jars/$flinkJarName"
    val flinkStateUri = env.getOrElse(
      "FLINK_STATE_URI",
      throw new IllegalArgumentException("FLINK_STATE_URI must be set for GROUP_BY_STREAMING"))
    // Static jars (Spark catalyst, Hadoop client, etc.) are baked into the Flink image
    // at /opt/flink/usrlib/ — see crucible/docker/flink/Dockerfile.
    // Only the main Flink jar and connector jars (which change per release) are passed here.
    val base = Map(
      FlinkMainJarURI -> flinkJarUri,
      FlinkCheckpointUri -> s"$flinkStateUri/checkpoints"
    )
    val enablePubSub = env.getOrElse("ENABLE_PUBSUB", "false").toBoolean
    if (enablePubSub)
      base + (FlinkPubSubConnectorJarURI -> s"$artifactPrefix/release/$version/jars/connectors_pubsub_deploy.jar")
    else base
  }

  // --- Checkpoint management ---

  override def getLatestCheckpointPath(flinkInternalJobId: String, flinkStateUri: String): Option[String] = {
    storageClient.flatMap(sc => StorageClient.resolveLatestCheckpointPath(sc, flinkInternalJobId, flinkStateUri))
  }

  // --- Helpers ---

  private def mapStatus(crucibleStatus: String): JobStatusType = crucibleStatus match {
    case "PENDING"   => JobStatusType.PENDING
    case "RUNNING"   => JobStatusType.RUNNING
    case "ARCHIVING" => JobStatusType.RUNNING // still in progress
    case "COMPLETED" => JobStatusType.SUCCEEDED
    case "FAILED"    => JobStatusType.FAILED
    case "KILLED"    => JobStatusType.CANCELLED
    case other =>
      logger.warn(s"Unknown Crucible status: $other")
      JobStatusType.UNKNOWN
  }

  /** Return the full cloud storage URI as the conf path.
    * The Spark driver reads it using Hadoop FileSystem after initialization.
    */
  override def resolveConfPath(stagedFileUri: String): String = {
    stagedFileUri
  }

  /** Sanitize job name for Crucible (lowercase, alphanumeric + dashes, max 63 chars) */
  private def sanitizeName(name: String): String = {
    name
      .toLowerCase
      .replaceAll("[^a-z0-9-]", "-")
      .replaceAll("-+", "-")
      .stripPrefix("-")
      .stripSuffix("-")
      .take(63)
  }
}

object CrucibleSubmitter {

  /** Create a CrucibleSubmitter from environment variables.
    *
    * Environment variables:
    *   CRUCIBLE_URL            — Crucible gateway URL (required)
    *   CRUCIBLE_NAMESPACE      — target namespace (default: "default")
    *   CRUCIBLE_SPARK_IMAGE    — Spark image (default: standard Crucible Spark 3.5 image)
    *   CRUCIBLE_FLINK_IMAGE    — Flink image (default: standard Crucible Flink image)
    *   CRUCIBLE_JAR_NAME       — JAR filename (default: "cloud_gcp_deploy.jar")
    *   CHRONON_ONLINE_CLASS    — Online API implementation class (cloud-specific)
    *   GCP_PROJECT_ID          — GCP project (for KV store API properties)
    *   GCP_BIGTABLE_INSTANCE_ID — Bigtable instance (for KV store API properties)
    *   GCP_REGION              — GCP region (for KV store API properties)
    */
  def fromEnv(storageClient: Option[StorageClient] = None): CrucibleSubmitter = {
    val baseUrl = sys.env.getOrElse("CRUCIBLE_URL",
      throw new IllegalArgumentException("CRUCIBLE_URL environment variable is required"))
    val namespace = sys.env.getOrElse("CRUCIBLE_NAMESPACE", "default")
    val sparkImage = sys.env.getOrElse("CRUCIBLE_SPARK_IMAGE",
      "us-docker.pkg.dev/crucible-io/crucible/spark:3.5-crucible-latest")
    val flinkImage = sys.env.getOrElse("CRUCIBLE_FLINK_IMAGE",
      "us-docker.pkg.dev/crucible-io/crucible/flink:1.19-crucible-latest")
    val jarNameVal = sys.env.getOrElse("CRUCIBLE_JAR_NAME", "cloud_gcp_deploy.jar")
    val onlineClassVal = sys.env.getOrElse("CHRONON_ONLINE_CLASS", "")

    // Build KV store API properties from available env vars (same keys DataprocSubmitter uses)
    val kvProps = Seq(
      sys.env.get("GCP_PROJECT_ID").map("GCP_PROJECT_ID" -> _),
      sys.env.get("GCP_BIGTABLE_INSTANCE_ID").map("GCP_BIGTABLE_INSTANCE_ID" -> _),
      sys.env.get("GCP_REGION").map("GCP_REGION" -> _),
      // AWS
      sys.env.get("AWS_REGION").map("AWS_REGION" -> _)
    ).flatten.toMap

    val tablePartitions = sys.env.getOrElse("CRUCIBLE_TABLE_PARTITIONS_DATASET", "TABLE_PARTITIONS")
    val dqMetrics = sys.env.getOrElse("CRUCIBLE_DQ_METRICS_DATASET", "DATA_QUALITY_METRICS")

    new CrucibleSubmitter(
      baseUrl, namespace, sparkImage, flinkImage,
      jarName = jarNameVal,
      onlineClass = onlineClassVal,
      tablePartitionsDataset = tablePartitions,
      dqMetricsDataset = dqMetrics,
      kvStoreApiProperties = kvProps,
      storageClient = storageClient
    )
  }
}
