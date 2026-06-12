package ai.chronon.integrations.cloud_gcp.bt_load

import com.google.cloud.dataproc.v1._
import org.slf4j.{Logger, LoggerFactory}

import scala.jdk.CollectionConverters._

/** Submits a Spark2BigTableLoader job to Dataproc and polls until completion.
  * Used as an alternative to the BQ EXPORT path when BT_LOAD_UPLOADER=spark.
  *
  * The jar at jarUri must contain Spark2BigTableLoader and all its dependencies
  * (i.e. the cloud_gcp assembly jar uploaded to GCS).
  */
object SparkDataprocLoader {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private val MainClass = "ai.chronon.integrations.cloud_gcp.Spark2BigTableLoader"

  def run(
      projectId: String,
      region: String,
      clusterName: String,
      jarUri: String,
      tableName: String,
      dataset: String,
      endDs: String,
      instanceId: String,
      // Spark BigTable connector jar must be supplied separately — it uses ServiceLoader
      // and cannot be shaded into the assembly jar.
      extraJarUris: Seq[String] = Seq.empty
  ): Unit = {
    val endpoint = s"$region-dataproc.googleapis.com:443"
    val settings = JobControllerSettings.newBuilder().setEndpoint(endpoint).build()
    val client = JobControllerClient.create(settings)

    try {
      val args = Seq(
        "--table-name", tableName,
        "--dataset", dataset,
        "--end-ds", endDs,
        "--project-id", projectId,
        "--instance-id", instanceId
      )

      // BT connector uses a driver-side client registry keyed by these config values.
      // They must be set as Spark properties (i.e. --conf on spark-submit) so executors
      // see them when initialising the client — runtime spark.conf.set does not propagate.
      val sparkProperties = Map(
        "spark.bigtable.project.id"  -> projectId,
        "spark.bigtable.instance.id" -> instanceId
      )

      val sparkJob = SparkJob
        .newBuilder()
        .setMainClass(MainClass)
        .addAllJarFileUris((Seq(jarUri) ++ extraJarUris).asJava)
        .putAllProperties(sparkProperties.asJava)
        .addAllArgs(args.asJava)
        .build()

      val jobId = s"bt-load-spark-${endDs.replace("-", "")}-${System.currentTimeMillis()}"

      val job = Job
        .newBuilder()
        .setReference(JobReference.newBuilder().setJobId(jobId).build())
        .setPlacement(JobPlacement.newBuilder().setClusterName(clusterName).build())
        .setSparkJob(sparkJob)
        .build()

      logger.info(s"Submitting Dataproc Spark job $jobId to cluster $clusterName in $region")
      val submitted = client.submitJob(projectId, region, job)
      val submittedId = submitted.getReference.getJobId
      logger.info(
        s"Job submitted: $submittedId — https://console.cloud.google.com/dataproc/jobs/$submittedId?region=$region&project=$projectId")

      pollUntilDone(client, projectId, region, submittedId)
    } finally {
      client.shutdown()
    }
  }

  private def pollUntilDone(client: JobControllerClient, projectId: String, region: String, jobId: String): Unit = {
    val pollIntervalMs = 30000L
    var done = false
    while (!done) {
      val job = client.getJob(projectId, region, jobId)
      job.getStatus.getState match {
        case JobStatus.State.DONE =>
          logger.info(s"Dataproc job $jobId completed successfully")
          done = true
        case JobStatus.State.ERROR =>
          throw new RuntimeException(
            s"Dataproc job $jobId failed: ${job.getStatus.getDetails}")
        case JobStatus.State.CANCELLED =>
          throw new RuntimeException(s"Dataproc job $jobId was cancelled")
        case state =>
          logger.info(s"Dataproc job $jobId state: $state — waiting ${pollIntervalMs / 1000}s")
          Thread.sleep(pollIntervalMs)
      }
    }
  }
}
