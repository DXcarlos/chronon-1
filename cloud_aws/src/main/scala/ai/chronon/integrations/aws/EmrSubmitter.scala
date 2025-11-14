package ai.chronon.integrations.aws

import ai.chronon.api.JobStatusType
import ai.chronon.integrations.aws.EmrSubmitter.{
  DefaultClusterIdleTimeout,
  DefaultClusterInstanceCount,
  DefaultClusterInstanceType
}
import ai.chronon.spark.submission.JobSubmitterConstants._
import ai.chronon.spark.submission.{JobSubmitter, JobType, SparkJob => TypeSparkJob}
import software.amazon.awssdk.services.emr.EmrClient
import software.amazon.awssdk.services.emr.model._

import scala.collection.JavaConverters._

class EmrSubmitter(customerId: String, emrClient: EmrClient) extends JobSubmitter {

  private val ClusterApplications = List(
    "Flink",
    "Zeppelin",
    "JupyterEnterpriseGateway",
    "Hive",
    "Hadoop",
    "Livy",
    "Spark"
  )

  // TODO: test if this works for Flink
  private val DefaultEmrReleaseLabel = "emr-7.2.0"

  // Customer specific infra configurations
  private val CustomerToSubnetIdMap = Map(
    "canary" -> "subnet-085b2af531b50db44",
    "dev" -> "subnet-085b2af531b50db44"
  )
  private val CustomerToSecurityGroupIdMap = Map(
    "canary" -> "sg-04fb79b5932a41298",
    "dev" -> "sg-04fb79b5932a41298"
  )

  private def createClusterRequestBuilder(emrReleaseLabel: String = DefaultEmrReleaseLabel,
                                          clusterIdleTimeout: Int = DefaultClusterIdleTimeout,
                                          masterInstanceType: String = DefaultClusterInstanceType,
                                          slaveInstanceType: String = DefaultClusterInstanceType,
                                          instanceCount: Int = DefaultClusterInstanceCount,
                                          clusterName: Option[String] = None) = {
    val runJobFlowRequestBuilder = if (clusterName.isDefined) {
      RunJobFlowRequest
        .builder()
        .name(clusterName.get)
    } else {
      RunJobFlowRequest
        .builder()
        .name(s"job-${java.util.UUID.randomUUID.toString}")
    }

    // Cluster infra configurations:
    val customerSecurityGroupId = CustomerToSecurityGroupIdMap.getOrElse(
      customerId,
      throw new RuntimeException(s"No security group id found for $customerId"))
    runJobFlowRequestBuilder
      .autoTerminationPolicy(
        AutoTerminationPolicy
          .builder()
          .idleTimeout(clusterIdleTimeout.toLong)
          .build())
      .configurations(
        Configuration.builder
          .classification("spark-hive-site")
          .properties(Map(
            "hive.metastore.client.factory.class" -> "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").asJava)
          .build()
      )
      .applications(ClusterApplications.map(app => Application.builder().name(app).build()): _*)
      // TODO: Could make this generalizable. or use a separate logs bucket
      .logUri(s"s3://zipline-logs-${customerId}/emr/")
      .instances(
        JobFlowInstancesConfig
          .builder()
          // Hack: We hardcode the subnet ID and sg id for each customer of Zipline. The subnet gets created from
          // Terraform so we'll need to be careful that these don't get accidentally destroyed.
          .ec2SubnetId(
            CustomerToSubnetIdMap.getOrElse(customerId,
                                            throw new RuntimeException(s"No subnet id found for $customerId")))
          .emrManagedMasterSecurityGroup(customerSecurityGroupId)
          .emrManagedSlaveSecurityGroup(customerSecurityGroupId)
          .instanceGroups(
            InstanceGroupConfig
              .builder()
              .instanceRole(InstanceRoleType.MASTER)
              .instanceType(masterInstanceType)
              .instanceCount(1)
              .build(),
            InstanceGroupConfig
              .builder()
              .instanceRole(InstanceRoleType.CORE)
              .instanceType(slaveInstanceType)
              .instanceCount(1)
              .build()
          )
          .keepJobFlowAliveWhenNoSteps(true) // Keep the cluster alive after the job is done
          .build())
      .managedScalingPolicy(
        ManagedScalingPolicy
          .builder()
          .computeLimits(
            ComputeLimits
              .builder()
              .maximumCapacityUnits(instanceCount)
              .minimumCapacityUnits(1)
              .unitType(ComputeLimitsUnitType.INSTANCES)
              .build()
          )
          .build()
      )
      .serviceRole(s"zipline_${customerId}_emr_service_role")
      .jobFlowRole(s"zipline_${customerId}_emr_profile_role")
      .releaseLabel(emrReleaseLabel)

  }

  private def createStepConfig(filesToMount: List[String],
                               mainClass: String,
                               jarUri: String,
                               args: String*): StepConfig = {
    // TODO: see if we can use the spark.files or --files instead of doing this ourselves
    // Copy files from s3 to cluster
    val awsS3CpArgs = filesToMount.map(file => s"aws s3 cp $file /mnt/zipline/")
    val sparkSubmitArgs =
      List(s"spark-submit --class $mainClass $jarUri ${args.mkString(" ")}")
    val finalArgs = List(
      "bash",
      "-c",
      (awsS3CpArgs ++ sparkSubmitArgs).mkString("; \n")
    )
    println(finalArgs)
    StepConfig
      .builder()
      .name("Run Zipline Job")
      .actionOnFailure(ActionOnFailure.CANCEL_AND_WAIT)
      .hadoopJarStep(
        HadoopJarStepConfig
          .builder()
          // Using command-runner.jar from AWS:
          // https://docs.aws.amazon.com/en_us/emr/latest/ReleaseGuide/emr-spark-submit-step.html
          .jar("command-runner.jar")
          .args(finalArgs: _*)
          .build()
      )
      .build()
  }

  override def submit(jobType: JobType,
                      submissionProperties: Map[String, String],
                      jobProperties: Map[String, String],
                      files: List[String],
                      labels: Map[String, String],
                      args: String*): String = {
    if (submissionProperties.get(ShouldCreateCluster).exists(_.toBoolean)) {
      // create cluster
      val runJobFlowBuilder = createClusterRequestBuilder(
        emrReleaseLabel = submissionProperties.getOrElse(EmrReleaseLabel, DefaultEmrReleaseLabel),
        clusterIdleTimeout =
          submissionProperties.getOrElse(ClusterIdleTimeout, DefaultClusterIdleTimeout.toString).toInt,
        masterInstanceType = submissionProperties.getOrElse(ClusterInstanceType, DefaultClusterInstanceType),
        slaveInstanceType = submissionProperties.getOrElse(ClusterInstanceType, DefaultClusterInstanceType),
        instanceCount =
          submissionProperties.getOrElse(ClusterInstanceCount, DefaultClusterInstanceCount.toString).toInt,
        clusterName = submissionProperties.get(ClusterName)
      )

      runJobFlowBuilder.steps(
        createStepConfig(files, submissionProperties(MainClass), submissionProperties(JarURI), args: _*))

      val responseJobId = emrClient.runJobFlow(runJobFlowBuilder.build()).jobFlowId()
      println("EMR job id: " + responseJobId)
      println(
        s"Safe to exit. Follow the job status at: https://console.aws.amazon.com/emr/home#/clusterDetails/$responseJobId")
      responseJobId

    } else {
      // use existing cluster
      val existingJobId = submissionProperties.getOrElse(ClusterId, throw new RuntimeException("JobFlowId not found"))
      val request = AddJobFlowStepsRequest
        .builder()
        .jobFlowId(existingJobId)
        .steps(createStepConfig(files, submissionProperties(MainClass), submissionProperties(JarURI), args: _*))
        .build()

      val responseStepId = emrClient.addJobFlowSteps(request).stepIds().get(0)

      println("EMR step id: " + responseStepId)
      println(
        s"Safe to exit. Follow the job status at: https://console.aws.amazon.com/emr/home#/clusterDetails/$existingJobId")
      responseStepId
    }
  }

  override def status(jobId: String): JobStatusType = ???

  override def kill(stepId: String): scala.Unit = {
    val resp = emrClient.cancelSteps(CancelStepsRequest.builder().stepIds(stepId).build())
  }
}

object EmrSubmitter {
  def apply(): EmrSubmitter = {
    val customerId = sys.env.getOrElse("CUSTOMER_ID", throw new Exception("CUSTOMER_ID not set")).toLowerCase

    new EmrSubmitter(customerId,
                     EmrClient
                       .builder()
                       .build())
  }

  private val ClusterInstanceTypeArgKeyword = "--cluster-instance-type"
  private val ClusterInstanceCountArgKeyword = "--cluster-instance-count"
  private val ClusterIdleTimeoutArgKeyword = "--cluster-idle-timeout"
  private val CreateClusterArgKeyword = "--create-cluster"

  private val DefaultClusterInstanceType = "m5.xlarge"
  private val DefaultClusterInstanceCount = 3
  private val DefaultClusterIdleTimeout = 60 * 60 * 1 // 1h in seconds

  def main(args: Array[String]): scala.Unit = {
    // List of args that are not application args
    val internalArgs = Set(
      ClusterInstanceTypeArgKeyword,
      ClusterInstanceCountArgKeyword,
      ClusterIdleTimeoutArgKeyword,
      CreateClusterArgKeyword
    ) ++ SharedInternalArgs

    val userArgs = args.filter(arg => !internalArgs.exists(arg.startsWith))

    val jarUri = JobSubmitter
      .getArgValue(args, JarUriArgKeyword)
      .getOrElse(throw new Exception("Missing required argument: " + JarUriArgKeyword))
    val mainClass = JobSubmitter
      .getArgValue(args, MainClassKeyword)
      .getOrElse(throw new Exception("Missing required argument: " + MainClassKeyword))
    val jobTypeValue =
      JobSubmitter
        .getArgValue(args, JobTypeArgKeyword)
        .getOrElse(throw new Exception("Missing required argument: " + JobTypeArgKeyword))

    val clusterInstanceType = JobSubmitter
      .getArgValue(args, ClusterInstanceTypeArgKeyword)
      .getOrElse(DefaultClusterInstanceType)
    val clusterInstanceCount = JobSubmitter
      .getArgValue(args, ClusterInstanceCountArgKeyword)
      .getOrElse(DefaultClusterInstanceCount.toString)
    val clusterIdleTimeout = JobSubmitter
      .getArgValue(args, ClusterIdleTimeoutArgKeyword)
      .getOrElse(DefaultClusterIdleTimeout.toString)

    val createCluster = args.exists(_.startsWith(CreateClusterArgKeyword))

    val clusterId = sys.env.get("EMR_CLUSTER_ID")

    // search args array for prefix `--gcs_files`
    val filesArgs = args.filter(_.startsWith(FilesArgKeyword))
    assert(filesArgs.length == 0 || filesArgs.length == 1)

    val files = if (filesArgs.isEmpty) {
      Array.empty[String]
    } else {
      filesArgs(0).split("=")(1).split(",")
    }

    val (jobType, submissionProps) = jobTypeValue.toLowerCase match {
      case "spark" => {
        val baseProps = Map(
          MainClass -> mainClass,
          JarURI -> jarUri,
          ClusterInstanceType -> clusterInstanceType,
          ClusterInstanceCount -> clusterInstanceCount,
          ClusterIdleTimeout -> clusterIdleTimeout,
          ShouldCreateCluster -> createCluster.toString
        )

        if (!createCluster && clusterId.isDefined) {
          (TypeSparkJob, baseProps + (ClusterId -> clusterId.get))
        } else {
          (TypeSparkJob, baseProps)
        }
      }
      // TODO: add flink
      case _ => throw new Exception("Invalid job type")
    }

    val finalArgs = userArgs.toSeq
    val modeConfigProperties = JobSubmitter.getModeConfigProperties(args)

    val emrSubmitter = EmrSubmitter()
    emrSubmitter.submit(
      jobType = jobType,
      submissionProperties = submissionProps,
      jobProperties = modeConfigProperties.getOrElse(Map.empty),
      files = files.toList,
      labels = Map.empty,
      finalArgs: _*
    )
  }
}
