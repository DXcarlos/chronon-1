package ai.chronon.integrations.aws

import ai.chronon.api.ScalaJavaConversions.ListOps
import ai.chronon.spark.submission.JobSubmitterConstants._
import ai.chronon.spark.submission.SparkJob
import org.junit.Assert.assertEquals
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar
import software.amazon.awssdk.services.ec2.Ec2Client
import software.amazon.awssdk.services.emr.EmrClient
import software.amazon.awssdk.services.emr.model.{AddJobFlowStepsRequest, AddJobFlowStepsResponse}

class EmrSubmitterTest extends AnyFlatSpec with MockitoSugar {
  "EmrSubmitterClient" should "return job id when a job is submitted and assert EMR request args" in {
    val stepId = "mock-step-id"
    val clusterId = "j-MOCKCLUSTERID123"

    val mockEmrClient = mock[EmrClient]
    val mockEc2Client = mock[Ec2Client]

    val requestCaptor = org.mockito.ArgumentCaptor.forClass(classOf[AddJobFlowStepsRequest])

    when(
      mockEmrClient.addJobFlowSteps(
        requestCaptor.capture()
      )).thenReturn(AddJobFlowStepsResponse.builder().stepIds(stepId).build())

    val expectedCustomerId = "canary"
    val expectedApplicationArgs = Seq("group-by-backfill", "arg1", "arg2")
    val expectedFiles = List("s3://random-conf", "s3://random-data")
    val expectedMainClass = "some-main-class"
    val expectedJarURI = "s3://-random-jar-uri"

    val expectedJobProperties = Map("spark.executor.memory" -> "4g", "spark.executor.cores" -> "2")

    val submitter = new EmrSubmitter(expectedCustomerId, mockEmrClient, mockEc2Client)
    val submittedStepId = submitter.submit(
      jobType = SparkJob,
      submissionProperties = Map(
        MainClass -> expectedMainClass,
        JarURI -> expectedJarURI,
        ClusterId -> clusterId
      ),
      jobProperties = expectedJobProperties,
      files = expectedFiles,
      labels = Map.empty,
      expectedApplicationArgs: _*
    )
    assertEquals(submittedStepId, stepId)

    val actualRequest = requestCaptor.getValue

    // Verify the cluster ID is correct
    assertEquals(actualRequest.jobFlowId(), clusterId)

    // Verify step configuration
    assertEquals(actualRequest.steps().size(), 1)

    val stepConfig = actualRequest.steps().get(0)
    assertEquals(stepConfig.actionOnFailure().name(), "CONTINUE")
    assertEquals(stepConfig.name(), "Run Zipline Job")
    assertEquals(stepConfig.hadoopJarStep().jar(), "command-runner.jar")

    val actualArgs = stepConfig.hadoopJarStep().args().toScala.mkString(" ")
    // Verify file copy commands are present
    assert(actualArgs.contains("aws s3 cp s3://random-conf /mnt/zipline/"))
    assert(actualArgs.contains("aws s3 cp s3://random-data /mnt/zipline/"))
    // Verify spark-submit with --conf args for job properties
    assert(actualArgs.contains("--conf 'spark.executor.memory=4g'"))
    assert(actualArgs.contains("--conf 'spark.executor.cores=2'"))
    // Verify spark-submit class, jar, and application args
    assert(actualArgs.contains(s"--class $expectedMainClass"))
    assert(actualArgs.contains(expectedJarURI))
    assert(actualArgs.contains(expectedApplicationArgs.mkString(" ")))
  }

  it should "test flink job locally" ignore {}

  it should "test flink kafka ingest job locally" ignore {}

  it should "Used to iterate locally. Do not enable this in CI/CD!" ignore {
    val emrSubmitter = new EmrSubmitter(
      "canary",
      EmrClient.builder().build(),
      Ec2Client.builder().build()
    )
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
