package ai.chronon.integrations.cloud_gcp
import ai.chronon.api.Builders.MetaData
import ai.chronon.spark.submission.JobSubmitterConstants._
import ai.chronon.spark.submission.{JobSubmitter, JobType, FlinkJob => TypeFlinkJob, SparkJob => TypeSparkJob}
import com.google.api.gax.rpc.ApiException
import com.google.cloud.dataproc.v1._
import com.google.cloud.storage.{Storage, StorageOptions}
import com.google.protobuf.util.JsonFormat
import org.apache.hadoop.fs.Path

import scala.jdk.CollectionConverters._

case class MoreThanOneRunningFlinkJob(message: String) extends Exception(message)

case class NoRunningFlinkJob(message: String) extends Exception(message)

class DataprocSubmitter(jobControllerClient: JobControllerClient,
                        gcsClient: GCSClient,
                        val region: String,
                        val projectId: String)
    extends JobSubmitter {

  def formatDataprocLabel(label: String): String = {
    // dataproc label keys and values only allow lowercase, numbers, underscores, and dashes.
    // Replaces any character that is not:
    // - a letter (a-z or A-Z)
    // - /a digit (0-9)
    // - a dash (-)
    // - an underscore (_)
    // with an underscore (_)
    // And lowercase.
    label.replaceAll("[^a-zA-Z0-9_-]", "_").toLowerCase
  }

  def listRunningGroupByFlinkJobs(groupByName: String): List[String] = {
    val groupByNameDataprocLabel = formatDataprocLabel(groupByName)

    //  String values for filters cannot have quotations or else search doesn't work.
    // for example "labels.job-type = flink" and NOT "labels.job-type = 'flink'"
    val filters =
      s"status.state = ACTIVE AND labels.job-type = flink AND labels.metadata-name = $groupByNameDataprocLabel"

    println(s"Searching for running flink jobs with filter: [$filters]")

    val listResult = jobControllerClient.listJobs(
      ListJobsRequest
        .newBuilder()
        .setProjectId(projectId)
        .setRegion(region)
        .setFilter(filters)
        .build()
    )
    listResult.iterateAll().iterator().asScala.toList.map(_.getReference.getJobId)
  }

  def getZiplineVersionOfDataprocJob(jobId: String): String = {
    val currentJob: Job = jobControllerClient.getJob(projectId, region, jobId)
    val labels = currentJob.getLabelsMap
    labels.get(formatDataprocLabel(ZiplineVersion))
  }

  def getLatestFlinkCheckpoint(groupByName: String,
                               manifestBucketPath: String,
                               flinkCheckpointUri: String): Option[String] = {
    val manifestFileName = "manifest.txt"

    val groupByCheckpointPath = new Path(manifestBucketPath, groupByName)
    val manifestObjectPath = new Path(groupByCheckpointPath, manifestFileName).toString
    println(s"Checking for manifest file at $manifestObjectPath")

    if (!gcsClient.fileExists(manifestObjectPath)) {
      println(s"No manifest file found for $groupByName. Returning no checkpoints.")
      return None // No manifest file found
    }

    val manifestStr = new String(gcsClient.downloadObjectToMemory(manifestObjectPath))
    val manifestTuples = manifestStr.split(",")
    val flinkJobTuple = manifestTuples.find(_.startsWith("flinkJobId"))
    val flinkJobId = flinkJobTuple
      .map(_.split("=")(1))
      .getOrElse(throw new RuntimeException("Flink job id not found in manifest file."))

    val matchedFiles = gcsClient.listFiles(new Path(flinkCheckpointUri, flinkJobId).toString).toList
    val allCheckpoints = matchedFiles
      .filter(_.split("/").exists(_.startsWith("chk-")))
      .map(_.split("/").find(_.startsWith("chk-")).get)
      .distinct
      .sortBy(chk => chk.substring(4).toInt)(Ordering.Int.reverse)
    println(s"Flink checkpoints for $groupByName: $allCheckpoints")

    val latestCheckpoint = allCheckpoints.headOption
    val latestCheckpointUri = latestCheckpoint
      .map(chk => {
        val flinkJobPath = new Path(flinkCheckpointUri, flinkJobId)
        new Path(flinkJobPath, chk).toString
      })

    if (latestCheckpointUri.isEmpty) {
      println(s"No checkpoints found for $groupByName.")
    } else {
      println(s"Latest checkpoint for $groupByName: ${latestCheckpointUri.get}")
    }

    latestCheckpointUri
  }

  override def status(jobId: String): String = {
    try {

      val currentJob: Job = jobControllerClient.getJob(projectId, region, jobId)
      currentJob.getStatus.getState.toString

    } catch {

      case e: ApiException =>
        println(s"Error monitoring job: ${e.getMessage}")
        "UNKNOWN" // If there's an error, we return UNKNOWN status
    }
  }

  override def kill(jobId: String): Unit = {
    val job = jobControllerClient.cancelJob(projectId, region, jobId)
    job.getDone
  }

  override def submit(jobType: JobType,
                      submissionProperties: Map[String, String],
                      jobProperties: Map[String, String],
                      files: List[String],
                      args: String*): String = {
    val mainClass = submissionProperties.getOrElse(MainClass, throw new RuntimeException("Main class not found"))
    val jarUri = submissionProperties.getOrElse(JarURI, throw new RuntimeException("Jar URI not found"))
    val clusterName = submissionProperties.getOrElse(ClusterName, throw new RuntimeException("Cluster name not found"))

    val jobId = submissionProperties.getOrElse(JobId, throw new RuntimeException("No generated job id found"))

    val jobBuilder = jobType match {
      case TypeSparkJob => buildSparkJob(mainClass, jarUri, files, jobProperties, args: _*)
      case TypeFlinkJob =>
        val mainJarUri =
          submissionProperties.getOrElse(FlinkMainJarURI,
                                         throw new RuntimeException(s"Missing expected $FlinkMainJarURI"))
        val flinkCheckpointPath =
          submissionProperties.getOrElse(FlinkCheckpointUri,
                                         throw new RuntimeException(s"Missing expected $FlinkCheckpointUri"))
        val maybeSavepointUri = submissionProperties.get(SavepointUri)
        val maybePubSubConnectorJarUri = submissionProperties.get(FlinkPubSubConnectorJarURI)
        val maybeAdditionalJarsUri = submissionProperties.get(AdditionalJars)
        val additionalJars = maybeAdditionalJarsUri.map(_.split(",")).getOrElse(Array.empty)
        val jarUris = Array(jarUri) ++ maybePubSubConnectorJarUri.toList ++ additionalJars
        buildFlinkJob(mainClass,
                      mainJarUri,
                      jarUris,
                      flinkCheckpointPath,
                      maybeSavepointUri,
                      jobProperties,
                      (args :+ "--parent-job-id" :+ jobId): _*)
    }

    // attach the job type and metadata name
    val submissionJobType = jobType match {
      case TypeSparkJob => SparkJobType
      case TypeFlinkJob => FlinkJobType
    }
    val metadataName =
      submissionProperties.getOrElse(MetadataName, throw new RuntimeException("Metadata name not found"))

    val jobPlacement = JobPlacement
      .newBuilder()
      .setClusterName(clusterName)
      .build()

    try {

      val job = jobBuilder
        .setReference(jobReference(jobId))
        .setPlacement(jobPlacement)
        .putLabels(formatDataprocLabel(JobType), formatDataprocLabel(submissionJobType))
        .putLabels(formatDataprocLabel(MetadataName), formatDataprocLabel(metadataName))
        .putLabels(
          formatDataprocLabel(ZiplineVersion),
          formatDataprocLabel(
            submissionProperties.getOrElse(ZiplineVersion, throw new RuntimeException("Zipline version not found")))
        )
        .build()

      val submittedJob = jobControllerClient.submitJob(projectId, region, job)
      val submittedJobId = submittedJob.getReference.getJobId

      if (jobId != submittedJobId) {
        throw new IllegalStateException(s"Computed job id $jobId does not match submitted job id $submittedJobId")
      }

      submittedJobId

    } catch {
      case e: ApiException =>
        throw new RuntimeException(s"Failed to submit job: ${e.getMessage}", e)
    }
  }

  private def buildSparkJob(mainClass: String,
                            jarUri: String,
                            files: List[String],
                            jobProperties: Map[String, String],
                            args: String*): Job.Builder = {
    val sparkJob = SparkJob
      .newBuilder()
      .putAllProperties(jobProperties.asJava)
      .setMainClass(mainClass)
      .addJarFileUris(jarUri)
      .addAllFileUris(files.asJava)
      .addAllArgs(args.toIterable.asJava)
      .build()
    Job.newBuilder().setSparkJob(sparkJob)
  }

  private[cloud_gcp] def buildFlinkJob(mainClass: String,
                                       mainJarUri: String,
                                       jarUris: Array[String],
                                       flinkCheckpointUri: String,
                                       maybeSavePointUri: Option[String],
                                       jobProperties: Map[String, String],
                                       args: String*): Job.Builder = {

    // JobManager is primarily responsible for coordinating the job (task slots, checkpoint triggering) and not much else
    // so 4G should suffice.
    // We go with 64G TM containers (4 task slots per container)
    // Broadly Flink splits TM memory into:
    // 1) Metaspace, framework offheap etc
    // 2) Network buffers
    // 3) Managed Memory (rocksdb)
    // 4) JVM heap
    // We tune down the network buffers to 1G-2G (default would be ~6.3G) and use some of the extra memory for
    // managed mem + jvm heap
    // Good doc - https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/memory/mem_setup_tm
    val envProps =
      Map(
        "jobmanager.memory.process.size" -> "4G",
        "taskmanager.memory.process.size" -> "64G",
        "taskmanager.memory.network.min" -> "1G",
        "taskmanager.memory.network.max" -> "2G",
        // explicitly set the number of task slots as otherwise it defaults to the number of cores
        // we go with multiple slots per TM as it allows us to squeeze more parallelism out of our resources
        // this is something we can revisit if we update Spark settings in CatalystUtil as we occasionally see them being overridden
        "taskmanager.numberOfTaskSlots" -> "4",
        "taskmanager.memory.managed.fraction" -> "0.5f",
        // default is 256m, we seem to be close to the limit so we give ourselves some headroom
        "taskmanager.memory.jvm-metaspace.size" -> "512m",
        // bump this a bit as Kafka and KV stores often need direct buffers
        "taskmanager.memory.task.off-heap.size" -> "1G",
        "yarn.classpath.include-user-jar" -> "FIRST",
        "state.savepoints.dir" -> flinkCheckpointUri,
        "state.checkpoints.dir" -> flinkCheckpointUri,
        // override the local dir for rocksdb as the default ends up being too large file name size wise
        "state.backend.rocksdb.localdir" -> "/tmp/flink-state",
        "state.checkpoint-storage" -> "filesystem",
        "rest.flamegraph.enabled" -> "true",
        // wire up prometheus reporter - prom reporter plays well with Google ops agent that can be installed in DataProc
        // as we can have a couple of containers on a given node, we use a port range
        "metrics.reporters" -> "prom",
        "metrics.reporter.prom.factory.class" -> "org.apache.flink.metrics.prometheus.PrometheusReporterFactory",
        "metrics.reporter.prom.host" -> "localhost",
        "metrics.reporter.prom.port" -> "9250-9260",
        "metrics.reporter.statsd.interval" -> "60 SECONDS",
        "state.backend.type" -> "rocksdb",
        "state.backend.incremental" -> "true",
        "state.checkpoints.num-retained" -> MaxRetainedCheckpoints
      )

    val flinkJobBuilder = FlinkJob
      .newBuilder()
      .setMainClass(mainClass)
      .setMainJarFileUri(mainJarUri)
      .putAllProperties((envProps ++ jobProperties).asJava)
      .addAllJarFileUris(jarUris.toIterable.asJava)
      .addAllArgs(args.toIterable.asJava)

    val updatedFlinkJobBuilder =
      maybeSavePointUri match {
        case Some(savePointUri) => flinkJobBuilder.setSavepointUri(savePointUri)
        case None               => flinkJobBuilder
      }

    Job
      .newBuilder()
      .setFlinkJob(updatedFlinkJobBuilder.build())
  }

  private def jobReference(jobId: String): JobReference = {
    JobReference
      .newBuilder()
      .setJobId(jobId)
      .build()
  }
}

object DataprocSubmitter {

  def apply(jobControllerClient: JobControllerClient,
            storageClient: Storage,
            region: String,
            projectId: String): DataprocSubmitter = {
    new DataprocSubmitter(jobControllerClient, GCSClient(storageClient), region, projectId)
  }

  private[cloud_gcp] def createSubmissionPropsMap(jobType: JobType,
                                                  submitter: DataprocSubmitter,
                                                  envMap: Map[String, Option[String]] = Map.empty,
                                                  args: Array[String],
                                                  clusterName: String): Map[String, String] = {
    val jarUri = JobSubmitter
      .getArgValue(args, JarUriArgKeyword)
      .getOrElse(throw new Exception("Missing required argument: " + JarUriArgKeyword))
    val mainClass = JobSubmitter
      .getArgValue(args, MainClassKeyword)
      .getOrElse(throw new Exception("Missing required argument: " + MainClassKeyword))

    val metadataName = Option(JobSubmitter.getMetadata(args).getOrElse(MetaData()).getName).getOrElse("")

    val jobId = JobSubmitter
      .getArgValue(args, JobIdArgKeyword)
      .getOrElse(throw new Exception("Missing required argument: " + JobIdArgKeyword))

    val submissionProps = jobType match {
      case TypeSparkJob =>
        Map(MainClass -> mainClass, JarURI -> jarUri, MetadataName -> metadataName, JobId -> jobId)
      case TypeFlinkJob =>
        val flinkCheckpointUri = JobSubmitter
          .getArgValue(args, StreamingCheckpointPathArgKeyword)
          .getOrElse(throw new Exception(s"Missing required argument $StreamingCheckpointPathArgKeyword"))
        val flinkMainJarUri = JobSubmitter
          .getArgValue(args, FlinkMainJarUriArgKeyword)
          .getOrElse(throw new Exception("Missing required argument: " + FlinkMainJarUriArgKeyword))
        // pull the pubsub connector uri if it has been passed
        val maybePubSubJarUri = JobSubmitter
          .getArgValue(args, FlinkPubSubJarUriArgKeyword)
        // include additional jars if present
        val additionalJars = JobSubmitter.getArgValue(args, AdditionalJarsUriArgKeyword)

        val baseJobProps = Map(
          MainClass -> mainClass,
          JarURI -> jarUri,
          FlinkMainJarURI -> flinkMainJarUri,
          FlinkCheckpointUri -> flinkCheckpointUri,
          MetadataName -> metadataName,
          JobId -> jobId
        ) ++ (maybePubSubJarUri.map(FlinkPubSubConnectorJarURI -> _) ++ additionalJars.map(AdditionalJars -> _))

        val groupByName = JobSubmitter
          .getArgValue(args, GroupByNameArgKeyword)
          .getOrElse(throw new Exception("Missing required argument: " + GroupByNameArgKeyword))

        val userPassedSavepoint = JobSubmitter
          .getArgValue(args, StreamingCustomSavepointArgKeyword)
        val maybeSavepointUri =
          if (userPassedSavepoint.isDefined) {
            userPassedSavepoint
          } else if (args.contains(StreamingLatestSavepointArgKeyword)) {
            submitter.getLatestFlinkCheckpoint(
              groupByName = groupByName,
              manifestBucketPath = JobSubmitter
                .getArgValue(args, StreamingManifestPathArgKeyword)
                .getOrElse(throw new Exception("Missing required argument: " + StreamingManifestPathArgKeyword)),
              flinkCheckpointUri = flinkCheckpointUri
            )
          } else {
            None
          }

        if (maybeSavepointUri.isEmpty) {
          baseJobProps
        } else {
          val savepointUri = maybeSavepointUri.get
          println(s"Deploying Flink app with savepoint uri $savepointUri")
          baseJobProps + (SavepointUri -> savepointUri)
        }
      case _ => throw new Exception("Invalid job type")
    }

    // Add additional properties
    submissionProps ++ Map(
      ZiplineVersion -> JobSubmitter
        .getArgValue(args, ZiplineVersionArgKeyword)
        .getOrElse(throw new Exception("Missing required argument: " + ZiplineVersionArgKeyword)),
      ClusterName -> clusterName
    )
  }

  private[cloud_gcp] def getDataprocFilesArgs(args: Array[String] = Array.empty): List[String] = {
    val gcsFilesArgs = args.filter(_.startsWith(FilesArgKeyword))
    val gcsFiles = if (gcsFilesArgs.isEmpty) {
      Array.empty[String]
    } else {
      gcsFilesArgs(0).split("=")(1).split(",")
    }
    gcsFiles.toList
  }

  private[cloud_gcp] def getApplicationArgs(jobType: JobType,
                                            envMap: Map[String, Option[String]],
                                            args: Array[String]) = {
    val internalArgs = SharedInternalArgs
    val userArgs = args.filter(arg => !internalArgs.exists(arg.startsWith))
    val finalArgs = jobType match {
      case TypeSparkJob => {
        val bigtableInstanceId = envMap(GcpBigtableInstanceIdEnvVar).getOrElse("")
        val projectId = envMap(GcpProjectIdEnvVar).getOrElse(throw new Exception(s"GcpProjectId not set"))
        Array.concat(userArgs)
      }
      case TypeFlinkJob => {
        val additionalArgsToFilterOut = Set(
          ConfTypeArgKeyword
        )
        userArgs.filter(arg => !additionalArgsToFilterOut.exists(arg.startsWith))
      }
    }
    finalArgs
  }

  private def validateOnlyOneFlinkSavepointDeploymentStrategySet(args: Array[String]): Unit = {
    val isStreamingLatestSet = args.contains(StreamingLatestSavepointArgKeyword)
    val isStreamingWithSavepointSet = JobSubmitter.getArgValue(args, StreamingCustomSavepointArgKeyword).isDefined
    val isStreamingNoSavepointSet = args.contains(StreamingNoSavepointArgKeyword)
    List(isStreamingLatestSet, isStreamingWithSavepointSet, isStreamingNoSavepointSet).count(_ == true) match {
      case 0 => throw new Exception("No savepoint deploy strategy provided")
      case 1 => // OK
      case _ =>
        throw new Exception("Multiple savepoint deploy strategies provided. " +
          s"Only one of $StreamingLatestSavepointArgKeyword, $StreamingCustomSavepointArgKeyword, $StreamingNoSavepointArgKeyword should be provided")
    }
  }

  private def compareZiplineVersionOfRunningFlinkJob(
      args: Array[String],
      submitter: DataprocSubmitter,
      jobId: String
  ): Boolean = {
    val localZiplineVersion = submitter.formatDataprocLabel(
      JobSubmitter
        .getArgValue(args, LocalZiplineVersionArgKeyword)
        .getOrElse(throw new Exception("Missing required argument: " + LocalZiplineVersionArgKeyword)))
    val remoteZiplineVersion = submitter.getZiplineVersionOfDataprocJob(jobId)
    println(
      s"Local Zipline version: $localZiplineVersion. " +
        s"Remote Zipline version for Dataproc job $jobId: $remoteZiplineVersion")
    return localZiplineVersion == remoteZiplineVersion
  }

  private def findRunningFlinkJobId(groupByName: String, submitter: DataprocSubmitter): Option[String] = {
    val matchedIds =
      submitter.listRunningGroupByFlinkJobs(groupByName = groupByName)
    if (matchedIds.size > 1) {
      throw MoreThanOneRunningFlinkJob(
        s"Multiple running Flink jobs found for GroupBy name $groupByName. " +
          s"Only one should be active. Job ids = ${matchedIds.mkString(", ")}"
      )
    } else if (matchedIds.isEmpty) {
      println(s"No running Flink jobs found for GroupBy name $groupByName.")
      None
    } else {
      Some(matchedIds.head)
    }
  }

  /** Gets an existing Dataproc cluster or creates a new one if it doesn't exist.
    *
    * @param clusterName The name of the cluster to get or create
    * @param maybeClusterConfig Optional cluster configuration for creation
    * @param projectId The GCP project ID
    * @param region The GCP region
    * @param dataprocClient The Dataproc client for API calls
    * @return The name of the cluster
    * @throws IllegalArgumentException if parameters are invalid
    * @throws Exception if cluster operations fail
    */
  def getOrCreateCluster(clusterName: String,
                         maybeClusterConfig: Option[Map[String, String]],
                         projectId: String,
                         region: String,
                         dataprocClient: ClusterControllerClient): String = {
    require(projectId.nonEmpty, "projectId cannot be empty")
    require(region.nonEmpty, "region cannot be empty")
    require(dataprocClient != null, "dataprocClient cannot be null")
    if (clusterName != "") {
      try {
        val cluster = dataprocClient.getCluster(projectId, region, clusterName)
        if (
          cluster != null && Set(ClusterStatus.State.RUNNING,
                                 ClusterStatus.State.UPDATING,
                                 ClusterStatus.State.CREATING,
                                 ClusterStatus.State.STARTING,
                                 ClusterStatus.State.REPAIRING).contains(cluster.getStatus.getState)
        ) {
          val currentStatus = waitForClusterReadiness(clusterName, projectId, region, dataprocClient)

          println(s"Dataproc cluster $clusterName already exists and is in state ${currentStatus.getState}.")
          clusterName
        } else if (cluster != null) {
          if (maybeClusterConfig.isDefined && maybeClusterConfig.get.contains("dataproc.config")) {
            // Print to stderr so that it flushes immediately
            System.err.println(
              s"Dataproc cluster $clusterName is in bad state ${cluster.getStatus.getState}. " +
                s"Deleting and recreating it with the provided config.")
            recreateDataprocCluster(
              clusterName,
              projectId,
              region,
              dataprocClient,
              maybeClusterConfig.get.getOrElse("dataproc.config", "")
            )

          } else {
            throw new Exception(
              s"Dataproc cluster $clusterName is in bad state ${cluster.getStatus.getState}. " +
                s"Please provide a cluster config in teams.py to recreate it.")
          }
        } else if (maybeClusterConfig.isDefined && maybeClusterConfig.get.contains("dataproc.config")) {
          // Print to stderr so that it flushes immediately
          System.err.println(
            s"Dataproc cluster $clusterName does not exist or is not running. Creating it with the provided config.")
          createDataprocCluster(clusterName,
                                projectId,
                                region,
                                dataprocClient,
                                maybeClusterConfig.get.getOrElse("dataproc.config", ""))
        } else {
          throw new Exception(s"Dataproc cluster $clusterName does not exist and no cluster config provided.")
        }
      } catch {
        case _: ApiException if maybeClusterConfig.isDefined && maybeClusterConfig.get.contains("dataproc.config") =>
          // Print to stderr so that it flushes immediately
          System.err.println(s"Dataproc cluster $clusterName does not exist. Creating it with the provided config.")
          createDataprocCluster(clusterName,
                                projectId,
                                region,
                                dataprocClient,
                                maybeClusterConfig.get.getOrElse("dataproc.config", ""))
        case _: ApiException =>
          throw new Exception(s"Dataproc cluster $clusterName does not exist and no cluster config provided.")
      }
    } else if (maybeClusterConfig.isDefined && maybeClusterConfig.get.contains("dataproc.config")) {
      // Print to stderr so that it flushes immediately
      System.err.println(s"Creating a transient dataproc cluster based on config.")
      val transientClusterName = s"zipline-${java.util.UUID.randomUUID()}"
      createDataprocCluster(transientClusterName,
                            projectId,
                            region,
                            dataprocClient,
                            maybeClusterConfig.get.getOrElse("dataproc.config", ""))
    } else {
      throw new Exception(
        s"$GcpDataprocClusterNameEnvVar is not set and no cluster config was provided. " +
          s"Please set $GcpDataprocClusterNameEnvVar or provide a cluster config in teams.py.")
    }
  }

  /** Creates a Dataproc cluster with the given name, project ID, region, and configuration.
    *
    * @param clusterName The name of the cluster to create.
    * @param projectId The GCP project ID.
    * @param region The region where the cluster will be created.
    * @param dataprocClient The ClusterControllerClient to interact with the Dataproc API.
    * @param clusterConfigStr The JSON string representing the cluster configuration.
    * @return The name of the created cluster.
    */
  private[cloud_gcp] def createDataprocCluster(clusterName: String,
                                               projectId: String,
                                               region: String,
                                               dataprocClient: ClusterControllerClient,
                                               clusterConfigStr: String): String = {

    val builder = ClusterConfig.newBuilder()
    val clusterConfig =
      try {
        JsonFormat.parser().merge(clusterConfigStr, builder)
        builder.build()
      } catch {
        case e: Exception =>
          throw new IllegalArgumentException(s"Failed to parse JSON: ${e.getMessage}", e)
      }

    val cluster: Cluster = Cluster
      .newBuilder()
      .setClusterName(clusterName)
      .setProjectId(projectId)
      .setConfig(clusterConfig)
      .build()

    val createRequest = CreateClusterRequest
      .newBuilder()
      .setProjectId(projectId)
      .setRegion(region)
      .setCluster(cluster)
      .build()

    // Asynchronously create the cluster and wait for it to be ready
    try {
      val operation = dataprocClient
        .createClusterAsync(createRequest)
        .get(30, java.util.concurrent.TimeUnit.MINUTES)
      if (operation == null) {
        throw new RuntimeException("Failed to create Dataproc cluster.")
      }
      println(s"Created Dataproc cluster: $clusterName")
    } catch {
      case e: java.util.concurrent.TimeoutException =>
        throw new RuntimeException(s"Timeout waiting for cluster creation: ${e.getMessage}", e)
      case e: Exception =>
        throw new RuntimeException(s"Error creating Dataproc cluster: ${e.getMessage}", e)
    }
    val currentStatus = waitForClusterReadiness(clusterName, projectId, region, dataprocClient)
    val currentState = currentStatus.getState
    currentState match {
      case ClusterStatus.State.RUNNING =>
        println(s"Dataproc cluster $clusterName is running.")
        clusterName
      case ClusterStatus.State.ERROR =>
        throw new RuntimeException(
          s"Failed to create Dataproc cluster $clusterName: ERROR state: ${currentStatus.toString}")
      case _ =>
        throw new RuntimeException(s"Dataproc cluster $clusterName is in unexpected state: $currentState.")
    }
  }

  /** Creates a Dataproc cluster with the given name, project ID, region, and configuration.
    *
    * @param clusterName The name of the cluster to create.
    * @param projectId The GCP project ID.
    * @param region The region where the cluster will be created.
    * @param dataprocClient The ClusterControllerClient to interact with the Dataproc API.
    * @param clusterConfigStr The JSON string representing the cluster configuration.
    * @return The name of the created cluster.
    */
  def recreateDataprocCluster(clusterName: String,
                              projectId: String,
                              region: String,
                              dataprocClient: ClusterControllerClient,
                              clusterConfigStr: String): String = {
    val deleteClusterRequest = DeleteClusterRequest
      .newBuilder()
      .setProjectId(projectId)
      .setRegion(region)
      .setClusterName(clusterName)
      .build()
    dataprocClient.deleteClusterAsync(deleteClusterRequest).get()

    println(s"Dataproc cluster $clusterName deleted successfully. Creating a new cluster.")

    createDataprocCluster(clusterName, projectId, region, dataprocClient, clusterConfigStr)

  }

  def waitForClusterReadiness(clusterName: String,
                              projectId: String,
                              region: String,
                              dataprocClient: ClusterControllerClient): ClusterStatus = {
    // Check status of the cluster creation
    var currentStatus = dataprocClient.getCluster(projectId, region, clusterName).getStatus
    var currentState = currentStatus.getState
    // Maximum time to wait for the cluster to become RUNNING (30 minutes)
    val maxWaitTime = 30 * 60 * 1000
    val startTime = System.currentTimeMillis()
    while (
      currentState != ClusterStatus.State.RUNNING &&
      currentState != ClusterStatus.State.UPDATING &&
      currentState != ClusterStatus.State.ERROR &&
      currentState != ClusterStatus.State.STOPPING
    ) {
      if (System.currentTimeMillis() - startTime > maxWaitTime) {
        throw new RuntimeException(
          s"Timeout waiting for cluster $clusterName to reach healthy state"
        )
      }
      println(s"Waiting for Dataproc cluster $clusterName to be in healthy state. Current state: $currentState")
      Thread.sleep(30000) // Wait for 30 seconds before checking again
      currentStatus = dataprocClient.getCluster(projectId, region, clusterName).getStatus
      currentState = currentStatus.getState
    }
    currentStatus

  }

  def run(args: Array[String],
          submitter: DataprocSubmitter,
          envMap: Map[String, Option[String]] = Map.empty,
          maybeConf: Option[Map[String, String]] = None,
          clusterName: String): Unit = {
    // Get the job type
    val jobTypeValue = JobSubmitter
      .getArgValue(args, JobTypeArgKeyword)
      .getOrElse(throw new Exception("Missing required argument: " + JobTypeArgKeyword))
    val jobType = jobTypeValue.toLowerCase match {
      case SparkJobType => TypeSparkJob
      case FlinkJobType => TypeFlinkJob
      case _            => throw new Exception("Invalid job type")
    }

    // Additional checks for streaming
    if (jobType == TypeFlinkJob) {
      val groupByName = JobSubmitter
        .getArgValue(args, GroupByNameArgKeyword)
        .getOrElse(throw new Exception("Missing required argument: " + GroupByNameArgKeyword))

      val maybeJobId = findRunningFlinkJobId(groupByName, submitter)

      val streamingMode = JobSubmitter
        .getArgValue(args, StreamingModeArgKeyword)
        .getOrElse(throw new Exception("Missing required argument: " + StreamingModeArgKeyword))

      if (CheckIfJobIsRunning.equals(streamingMode)) {
        if (maybeJobId.isEmpty) {
          println(s"No running Flink job found for GroupBy name $groupByName.")
          throw NoRunningFlinkJob(
            s"No running Flink job found for GroupBy name $groupByName"
          )
        } else {
          println(s"One running Flink job found for GroupBy name $groupByName. Job id = ${maybeJobId.get}")
          return
        }
      } else if (StreamingDeploy.equals(streamingMode)) {
        if (args.contains(StreamingVersionCheckDeploy) && maybeJobId.isDefined) {
          if (compareZiplineVersionOfRunningFlinkJob(args, submitter, maybeJobId.get)) {
            println(s"Local Zipline version matches running Flink app's Zipline version. Exiting")
            return
          } else {
            println(s"Local Zipline version does not match remote Zipline version. Proceeding with deployment.")
          }
        }

        // Check that only one type of savepoint deploy strategy is provided
        validateOnlyOneFlinkSavepointDeploymentStrategySet(args)

        if (maybeJobId.isDefined) {
          val matchedId = maybeJobId.get
          submitter.kill(matchedId)
          println(s"Cancelled running Flink job with id $matchedId for GroupBy name $groupByName.")
        }
      }
    }

    // Filter and finalize application args

    val finalArgs = getApplicationArgs(
      jobType = jobType,
      args = args,
      envMap = envMap
    )
    println(finalArgs.mkString("Array(", ", ", ")"))

    val jobId = submitter.submit(
      jobType = jobType,
      submissionProperties = createSubmissionPropsMap(jobType = jobType,
                                                      args = args,
                                                      submitter = submitter,
                                                      envMap = envMap,
                                                      clusterName = clusterName),
      jobProperties = maybeConf.getOrElse(JobSubmitter.getModeConfigProperties(args).getOrElse(Map.empty)),
      files = getDataprocFilesArgs(args),
      finalArgs: _*
    )
    println("Dataproc submitter job id: " + jobId)
    println(
      s"Safe to exit. Follow the job status at: https://console.cloud.google.com/dataproc/jobs/${jobId}/configuration?region=${submitter.region}&project=${submitter.projectId}")
  }

  def main(args: Array[String]): Unit = {
    val clusterName = sys.env
      .getOrElse(GcpDataprocClusterNameEnvVar, "")
    val maybeClusterConfig = JobSubmitter.getClusterConfig(args)

    val projectId = sys.env.getOrElse(GcpProjectIdEnvVar, throw new Exception(s"$GcpProjectIdEnvVar not set"))
    val region = sys.env.getOrElse(GcpRegionEnvVar, throw new Exception(s"$GcpRegionEnvVar not set"))
    val dataprocClient = ClusterControllerClient.create(
      ClusterControllerSettings.newBuilder().setEndpoint(s"$region-dataproc.googleapis.com:443").build())

    val submitterClusterName = getOrCreateCluster(clusterName, maybeClusterConfig, projectId, region, dataprocClient)

    val endPoint = s"${region}-dataproc.googleapis.com:443"
    val jobControllerClient =
      JobControllerClient.create(JobControllerSettings.newBuilder().setEndpoint(endPoint).build())
    val gcsStorageClient = StorageOptions.newBuilder().setProjectId(projectId).build().getService
    val submitter = DataprocSubmitter(jobControllerClient, gcsStorageClient, region, projectId)

    val envMap = Map(
      GcpBigtableInstanceIdEnvVar -> sys.env.get(GcpBigtableInstanceIdEnvVar),
      GcpProjectIdEnvVar -> sys.env.get(GcpProjectIdEnvVar)
    )
    DataprocSubmitter.run(args = args, submitter = submitter, envMap = envMap, clusterName = submitterClusterName)
  }

}
