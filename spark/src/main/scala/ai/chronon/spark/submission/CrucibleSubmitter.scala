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

        // Pass Spark conf as the conf map
        if (jobProperties.nonEmpty) {
          val confObj = new JsonObject()
          jobProperties.foreach { case (k, v) => confObj.put(k, v) }
          body.put("conf", confObj)
        }

      case FlinkJob =>
        body.put("type", "flink")
        body.put("image", flinkImage)

        val flinkMainClass = submissionProperties.getOrElse(MainClass, FlinkMainClass)
        body.put("mainClass", flinkMainClass)

        val flinkJarUri = submissionProperties.getOrElse(FlinkMainJarURI, jarUri)
        body.put("jar", flinkJarUri)

        // Flink conf: merge jobProperties + checkpoint/savepoint URIs
        val flinkConf = new JsonObject()
        jobProperties.foreach { case (k, v) => flinkConf.put(k, v) }
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
    // When resolveConfPath encoded raw JSON as gz-base64:..., rewrite the
    // --conf-path arg to --conf-gz-base64 for the driver to decode.
    val appArgs = JobSubmitter.getApplicationArgs(jobType, args.toArray).map { arg =>
      if (arg.startsWith("--conf-path=gz-base64:")) {
        val encoded = arg.substring("--conf-path=gz-base64:".length)
        s"--conf-gz-base64=$encoded"
      } else arg
    }
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

  /** When no StorageClient is available, the stagedFile is raw JSON content.
    * Gzip+Base64 encode it so BatchNodeRunner can decode via --conf-gz-base64.
    * Returns the encoded content prefixed with "gz-base64:" as a sentinel.
    */
  override def resolveConfPath(stagedFileUri: String): String = {
    if (stagedFileUri.startsWith("{")) {
      // Raw JSON content — encode it
      "gz-base64:" + ai.chronon.api.GzipCodec.encode(stagedFileUri)
    } else {
      // Normal GCS/S3 path — extract filename
      stagedFileUri.split("/").last
    }
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
