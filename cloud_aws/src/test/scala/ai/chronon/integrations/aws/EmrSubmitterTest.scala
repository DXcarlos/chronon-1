package ai.chronon.integrations.aws

import ai.chronon.api.ScalaJavaConversions.ListOps
import ai.chronon.spark.submission.SparkJob
import org.scalatest.flatspec.AnyFlatSpec
import software.amazon.awssdk.services.emr.EmrClient
import ai.chronon.spark.submission.JobSubmitterConstants._
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import software.amazon.awssdk.services.emr.model.ComputeLimitsUnitType
import software.amazon.awssdk.services.emr.model.RunJobFlowRequest
import software.amazon.awssdk.services.emr.model.RunJobFlowResponse

class EmrSubmitterTest extends AnyFlatSpec with MockitoSugar {
  "EmrSubmitterClient" should "return job id when a job is submitted and assert EMR request args" in {
    val jobId = "mock-job-id"

    val mockEmrClient = mock[EmrClient]

    val requestCaptor = org.mockito.ArgumentCaptor.forClass(classOf[RunJobFlowRequest])

    when(
      mockEmrClient.runJobFlow(
        requestCaptor.capture()
      )).thenReturn(RunJobFlowResponse.builder().jobFlowId(jobId).build())

    val expectedCustomerId = "canary"
    val expectedApplicationArgs = Seq("group-by-backfill", "arg1", "arg2")
    val expectedFiles = List("s3://random-conf", "s3://random-data")
    val expectedMainClass = "some-main-class"
    val expectedJarURI = "s3://-random-jar-uri"
    val expectedIdleTimeout = 2
    val expectedClusterInstanceType = "some-type"
    val expectedClusterInstanceCount = 5

    val submitter = new EmrSubmitter(expectedCustomerId, mockEmrClient)
    val submittedJobId = submitter.submit(
      jobType = SparkJob,
      submissionProperties = Map(
        MainClass -> expectedMainClass,
        JarURI -> expectedJarURI,
        ClusterIdleTimeout -> expectedIdleTimeout.toString,
        ClusterInstanceType -> expectedClusterInstanceType,
        ClusterInstanceCount -> expectedClusterInstanceCount.toString,
        ShouldCreateCluster -> true.toString
      ),
      jobProperties = Map.empty,
      files = expectedFiles,
      labels = Map.empty,
      expectedApplicationArgs: _*
    )
    assertEquals(submittedJobId, jobId)

    val actualRequest = requestCaptor.getValue

    // "canary" specific assertions
    assertEquals(actualRequest.logUri(), "s3://zipline-logs-canary/emr/")
    assertEquals(actualRequest.instances().ec2SubnetId(), "subnet-085b2af531b50db44")
    assertEquals(actualRequest.instances().emrManagedMasterSecurityGroup(), "sg-04fb79b5932a41298")
    assertEquals(actualRequest.instances().emrManagedSlaveSecurityGroup(), "sg-04fb79b5932a41298")
    assertEquals(actualRequest.managedScalingPolicy().computeLimits().unitType(), ComputeLimitsUnitType.INSTANCES)
    assertEquals(actualRequest.managedScalingPolicy().computeLimits().minimumCapacityUnits(), 1)
    assertEquals(actualRequest.managedScalingPolicy().computeLimits().maximumCapacityUnits(),
      expectedClusterInstanceCount)

    // cluster specific assertions
    assertEquals(actualRequest.releaseLabel(), "emr-7.2.0")

    assertEquals(actualRequest.instances().keepJobFlowAliveWhenNoSteps(), true)
    assertTrue(
      actualRequest
        .applications()
        .toScala
        .map(app => app.name)
        .forall(List("Flink", "Zeppelin", "JupyterEnterpriseGateway", "Hive", "Hadoop", "Livy", "Spark").contains))
    assertEquals("spark-hive-site", actualRequest.configurations().get(0).classification())
    assertEquals(
      "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
      actualRequest.configurations().get(0).properties().get("hive.metastore.client.factory.class")
    )
    assertEquals("zipline_canary_emr_profile_role", actualRequest.jobFlowRole())
    assertEquals("zipline_canary_emr_service_role", actualRequest.serviceRole())
    assertEquals(expectedIdleTimeout.toLong, actualRequest.autoTerminationPolicy().idleTimeout())

    assertEquals(actualRequest.steps().size(), 1)

    val stepConfig = actualRequest.steps().get(0)
    assertEquals(stepConfig.actionOnFailure().name(), "CANCEL_AND_WAIT")
    assertEquals(stepConfig.name(), "Run Zipline Job")
    assertEquals(stepConfig.hadoopJarStep().jar(), "command-runner.jar")
    assertEquals(
      stepConfig.hadoopJarStep().args().toScala.mkString(" "),
      s"bash -c aws s3 cp s3://random-conf /mnt/zipline/; \naws s3 cp s3://random-data /mnt/zipline/; \nspark-submit --class some-main-class s3://-random-jar-uri group-by-backfill arg1 arg2"
    )
  }

  it should "test flink job locally" ignore {}

  it should "test flink kafka ingest job locally" ignore {}

  it should "Used to iterate locally. Do not enable this in CI/CD!" ignore {
    val emrSubmitter = new EmrSubmitter("canary",
      EmrClient
        .builder()
        .build())
    val jobId = emrSubmitter.submit(
      jobType = SparkJob,
      submissionProperties = Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://zipline-artifacts-canary/jars/cloud_aws_lib_deploy.jar",
        ClusterId -> "j-13BASWFP15TLR"
      ),
      jobProperties = Map.empty,
      files = List("s3://zipline-warehouse-canary/purchases.v1"),
      Map.empty,
      "group-by-backfill",
      "--conf-path",
      "/mnt/zipline/purchases.v1",
      "--end-date",
      "2025-02-26",
      "--conf-type",
      "group_bys",
    )
    println("EMR job id: " + jobId)
    0
  }

}
