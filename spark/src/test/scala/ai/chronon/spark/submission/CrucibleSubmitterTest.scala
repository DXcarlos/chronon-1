package ai.chronon.spark.submission

import ai.chronon.api.JobStatusType
import ai.chronon.api.submission.JobSubmitterConstants._
import io.vertx.core.json.JsonObject
import org.mockito.ArgumentMatchers._
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

class CrucibleSubmitterTest extends AnyFlatSpec with MockitoSugar {

  private def createMockClient(): CrucibleClient = {
    val mockClient = mock[CrucibleClient]
    when(mockClient.baseUrl).thenReturn("http://crucible:8080")
    when(mockClient.namespace).thenReturn("test-ns")
    mockClient
  }

  private def createSubmitter(client: CrucibleClient): CrucibleSubmitter = {
    val submitter = new CrucibleSubmitter(
      baseUrl = "http://crucible:8080",
      namespace = "test-ns",
      sparkImage = "ghcr.io/zipline-ai/crucible/spark:4.1-crucible-latest",
      flinkImage = "ghcr.io/zipline-ai/crucible/flink:1.19-crucible-latest"
    )
    // Replace the internal client with the mock
    val clientField = classOf[CrucibleSubmitter].getDeclaredField("client")
    clientField.setAccessible(true)
    clientField.set(submitter, client)
    submitter
  }

  it should "submit a Spark job with correct JSON payload" in {
    val mockClient = createMockClient()
    val submitter = createSubmitter(mockClient)

    val captor = ArgumentCaptor.forClass(classOf[JsonObject])
    when(mockClient.submitJob(captor.capture())).thenReturn("spark-test-job-ab12")

    val jobId = submitter.submit(
      SparkJob,
      Map(
        MainClass -> "ai.chronon.spark.batch.BatchNodeRunner",
        JarURI -> "s3://bucket/chronon/spark_deploy.jar"
      ),
      Map("spark.executor.memory" -> "4g"),
      List.empty,
      Map(MetadataName -> "test-groupby"),
      "--conf-type=group_bys", "--local-conf-path=/tmp/conf.json"
    )

    assert(jobId == "spark-test-job-ab12")

    val body = captor.getValue
    assert(body.getString("type") == "spark")
    assert(body.getString("name") == "test-groupby")
    assert(body.getString("image") == "ghcr.io/zipline-ai/crucible/spark:4.1-crucible-latest")
    assert(body.getString("mainClass") == "ai.chronon.spark.batch.BatchNodeRunner")
    assert(body.getString("jar") == "s3://bucket/chronon/spark_deploy.jar")
    assert(body.getJsonObject("conf").getString("spark.executor.memory") == "4g")
  }

  it should "submit a Flink job with checkpoint URI" in {
    val mockClient = createMockClient()
    val submitter = createSubmitter(mockClient)

    val captor = ArgumentCaptor.forClass(classOf[JsonObject])
    when(mockClient.submitJob(captor.capture())).thenReturn("flink-streaming-cd34")

    val jobId = submitter.submit(
      FlinkJob,
      Map(
        MainClass -> "ai.chronon.flink.FlinkJob",
        FlinkMainJarURI -> "s3://bucket/chronon/flink_assembly_deploy.jar",
        FlinkCheckpointUri -> "s3://bucket/checkpoints"
      ),
      Map.empty,
      List.empty,
      Map(MetadataName -> "user-activities-stream")
    )

    assert(jobId == "flink-streaming-cd34")

    val body = captor.getValue
    assert(body.getString("type") == "flink")
    assert(body.getString("image") == "ghcr.io/zipline-ai/crucible/flink:1.19-crucible-latest")
    assert(body.getString("jar") == "s3://bucket/chronon/flink_assembly_deploy.jar")
    assert(body.getJsonObject("conf").getString("state.checkpoints.dir") == "s3://bucket/checkpoints")
  }

  it should "map Crucible statuses to JobStatusType correctly" in {
    val mockClient = createMockClient()
    val submitter = createSubmitter(mockClient)

    val cases = Map(
      "PENDING" -> JobStatusType.PENDING,
      "RUNNING" -> JobStatusType.RUNNING,
      "ARCHIVING" -> JobStatusType.RUNNING,
      "COMPLETED" -> JobStatusType.SUCCEEDED,
      "FAILED" -> JobStatusType.FAILED,
      "KILLED" -> JobStatusType.CANCELLED
    )

    cases.foreach { case (crucibleStatus, expected) =>
      when(mockClient.getJobStatus("job-1")).thenReturn((crucibleStatus, 200))
      val result = submitter.status("job-1")
      assert(result == expected, s"Expected $expected for Crucible status $crucibleStatus, got $result")
    }
  }

  it should "treat 404 as UNKNOWN (job not found)" in {
    val mockClient = createMockClient()
    val submitter = createSubmitter(mockClient)

    when(mockClient.getJobStatus("missing-job")).thenReturn(("NOT_FOUND", 404))
    assert(submitter.status("missing-job") == JobStatusType.UNKNOWN)
  }

  it should "return UNKNOWN on API exception" in {
    val mockClient = createMockClient()
    val submitter = createSubmitter(mockClient)

    when(mockClient.getJobStatus("broken-job")).thenThrow(
      CrucibleApiException("Connection failed")
    )
    assert(submitter.status("broken-job") == JobStatusType.UNKNOWN)
  }

  it should "call killJob on the client" in {
    val mockClient = createMockClient()
    val submitter = createSubmitter(mockClient)

    submitter.kill("job-to-kill")
    verify(mockClient).killJob("job-to-kill")
  }

  it should "return correct UI URLs" in {
    val mockClient = createMockClient()
    val submitter = createSubmitter(mockClient)

    assert(submitter.getSparkUrl("spark-abc") == Some("http://crucible:8080/api/v1/namespaces/test-ns/jobs/spark-abc/ui"))
    assert(submitter.getFlinkUrl("flink-xyz") == Some("http://crucible:8080/api/v1/namespaces/test-ns/jobs/flink-xyz/ui"))
    assert(submitter.getJobUrl("any-job") == Some("http://crucible:8080/api/v1/namespaces/test-ns/jobs/any-job"))
  }

  it should "not require cluster creation" in {
    val mockClient = createMockClient()
    val submitter = createSubmitter(mockClient)

    assert(!submitter.isClusterCreateNeeded(isLongRunning = false))
    assert(!submitter.isClusterCreateNeeded(isLongRunning = true))
  }

  it should "sanitize job names" in {
    val mockClient = createMockClient()
    val submitter = createSubmitter(mockClient)

    val captor = ArgumentCaptor.forClass(classOf[JsonObject])
    when(mockClient.submitJob(captor.capture())).thenReturn("job-id")

    submitter.submit(
      SparkJob,
      Map(MainClass -> "Main", JarURI -> "jar.jar"),
      Map.empty,
      List.empty,
      Map(MetadataName -> "My_GroupBy.V2__Special!")
    )

    val body = captor.getValue
    assert(body.getString("name") == "my-groupby-v2-special")
  }

  it should "pass idempotency key from submission properties" in {
    val mockClient = createMockClient()
    val submitter = createSubmitter(mockClient)

    val captor = ArgumentCaptor.forClass(classOf[JsonObject])
    when(mockClient.submitJob(captor.capture())).thenReturn("job-id")

    submitter.submit(
      SparkJob,
      Map(MainClass -> "Main", JarURI -> "jar.jar", JobId -> "unique-key-123"),
      Map.empty,
      List.empty,
      Map(MetadataName -> "test")
    )

    val body = captor.getValue
    assert(body.getString("idempotencyKey") == "unique-key-123")
  }
}
