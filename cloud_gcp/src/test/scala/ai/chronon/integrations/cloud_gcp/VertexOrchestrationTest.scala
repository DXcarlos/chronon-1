package ai.chronon.integrations.cloud_gcp

import ai.chronon.api._
import ai.chronon.online.{DeployModelRequest, TrainingRequest}
import com.google.cloud.aiplatform.v1._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Await
import scala.jdk.CollectionConverters._

class VertexOrchestrationTest extends AnyFlatSpec with Matchers {

  it should "correctly build a CustomJob with all required fields" in {
    // Setup
    val trainingSpec = new TrainingSpec()
    trainingSpec.setImage("gcr.io/my-project/trainer:v1")
    trainingSpec.setPythonModule("trainer.main")

    val jobConfigs = Map("learning_rate" -> "0.001", "epochs" -> "100").asJava
    trainingSpec.setJobConfigs(jobConfigs)

    val resourceConfig = new ResourceConfig()
    resourceConfig.setMachineType("n1-standard-4")
    resourceConfig.setMinReplicaCount(1)
    trainingSpec.setResourceConfig(resourceConfig)

    val rawTable = "random-table"
    val eventSource = new EventSource()
      .setTable(rawTable)
    val source = new Source()
    source.setEvents(eventSource)
    trainingSpec.setTrainingDataSource(source)

    trainingSpec.setTrainingDataWindow(new Window().setLength(1).setTimeUnit(TimeUnit.DAYS))

    val modelName = "test_model"
    val version = "v1"
    val date = "2025-12-09"
    val pythonPackageUri = "gs://my-bucket/builds/test_model-v1.tar.gz"
    val outputDir = "gs://my-bucket/training_output/test_model-v1/2025-12-09"

    // Execute
    val customJob = VertexOrchestration.buildCustomJob(
      trainingSpec, modelName, version, date, pythonPackageUri, outputDir
    )

    // Verify
    customJob.getDisplayName should include(modelName)
    customJob.getDisplayName should include(version)
    customJob.getDisplayName should include(date)

    val jobSpec = customJob.getJobSpec
    jobSpec.getWorkerPoolSpecsCount shouldBe 1

    val workerPoolSpec = jobSpec.getWorkerPoolSpecs(0)
    val pythonPackageSpec = workerPoolSpec.getPythonPackageSpec
    pythonPackageSpec.getExecutorImageUri shouldBe "gcr.io/my-project/trainer:v1"
    pythonPackageSpec.getPythonModule shouldBe "trainer.main"
    pythonPackageSpec.getPackageUrisCount shouldBe 1
    pythonPackageSpec.getPackageUris(0) shouldBe pythonPackageUri
    pythonPackageSpec.getArgsCount shouldBe 5
    pythonPackageSpec.getArgsList.asScala should contain("--learning_rate=0.001")
    pythonPackageSpec.getArgsList.asScala should contain("--epochs=100")
    pythonPackageSpec.getArgsList.asScala should contain(s"--input-table=$rawTable")
    pythonPackageSpec.getArgsList.asScala should contain("--start-ds=2025-12-08")
    pythonPackageSpec.getArgsList.asScala should contain(s"--end-ds=$date")

    val machineSpec = workerPoolSpec.getMachineSpec
    machineSpec.getMachineType shouldBe "n1-standard-4"
    workerPoolSpec.getReplicaCount shouldBe 1

    val baseOutputDirectory = jobSpec.getBaseOutputDirectory
    baseOutputDirectory.getOutputUriPrefix shouldBe outputDir
  }

  it should "use default python module when not specified" in {
    val trainingSpec = new TrainingSpec()
    trainingSpec.setImage("gcr.io/my-project/trainer:v1")

    val eventSource = new EventSource()
      .setTable("random-table")
    val source = new Source()
    source.setEvents(eventSource)
    trainingSpec.setTrainingDataSource(source)
    trainingSpec.setTrainingDataWindow(new Window().setLength(1).setTimeUnit(TimeUnit.DAYS))

    // Don't set pythonModule

    val customJob = VertexOrchestration.buildCustomJob(
      trainingSpec, "model", "v1", "2025-12-09",
      "gs://bucket/package.tar.gz", "gs://bucket/output"
    )

    val pythonPackageSpec = customJob.getJobSpec.getWorkerPoolSpecs(0).getPythonPackageSpec
    pythonPackageSpec.getPythonModule shouldBe "trainer.train"
  }

  it should "correctly build a Model with container spec" in {
    // Setup
    val containerConfig = new ServingContainerConfig()
    containerConfig.setImage("gcr.io/my-project/predictor:v1")
    containerConfig.setServingHealthRoute("/healthz")
    containerConfig.setServingPredictRoute("/infer")

    val envVars = Map("MODEL_PATH" -> "/models", "BATCH_SIZE" -> "32").asJava
    containerConfig.setServingContainerEnvVars(envVars)

    val modelName = "test_model"
    val version = "v1"
    val modelArtifactUri = "gs://my-bucket/models/test_model-v1"

    // Execute
    val model = VertexOrchestration.buildModel(containerConfig, modelName, version, modelArtifactUri)

    // Verify
    model.getDisplayName shouldBe "test_model-v1"
    model.getArtifactUri shouldBe modelArtifactUri

    val containerSpec = model.getContainerSpec
    containerSpec.getImageUri shouldBe "gcr.io/my-project/predictor:v1"
    containerSpec.getHealthRoute shouldBe "/healthz"
    containerSpec.getPredictRoute shouldBe "/infer"
    containerSpec.getEnvCount shouldBe 2

    val envList = containerSpec.getEnvList.asScala.map(e => e.getName -> e.getValue).toMap
    envList("MODEL_PATH") shouldBe "/models"
    envList("BATCH_SIZE") shouldBe "32"
  }

  it should "use default routes when not specified" in {
    val containerConfig = new ServingContainerConfig()
    containerConfig.setImage("gcr.io/my-project/predictor:v1")

    val model = VertexOrchestration.buildModel(containerConfig, "model", "v1", "gs://bucket/model")

    val containerSpec = model.getContainerSpec
    containerSpec.getHealthRoute shouldBe "/health"
    containerSpec.getPredictRoute shouldBe "/predict"
  }

  it should "correctly build a DeployedModel with dedicated resources" in {
    // Setup
    val modelResourceName = "projects/my-project/locations/us-central1/models/123456"
    val deployedModelDisplayName = "test_model"

    val resourceConfig = new ResourceConfig()
    resourceConfig.setMachineType("n1-standard-4")
    resourceConfig.setMinReplicaCount(2)
    resourceConfig.setMaxReplicaCount(10)

    // Execute
    val deployedModel = VertexOrchestration.buildDeployedModel(
      modelResourceName, deployedModelDisplayName, resourceConfig
    )

    // Verify
    deployedModel.getModel shouldBe modelResourceName
    deployedModel.getDisplayName shouldBe deployedModelDisplayName

    val dedicatedResources = deployedModel.getDedicatedResources
    dedicatedResources.getMachineSpec.getMachineType shouldBe "n1-standard-4"
    dedicatedResources.getMinReplicaCount shouldBe 2
    dedicatedResources.getMaxReplicaCount shouldBe 10
  }

  it should "correctly map JobState to JobStatusType" in {
    VertexOrchestration.parseJobState(JobState.JOB_STATE_SUCCEEDED) shouldBe JobStatusType.SUCCEEDED
    VertexOrchestration.parseJobState(JobState.JOB_STATE_FAILED) shouldBe JobStatusType.FAILED
    VertexOrchestration.parseJobState(JobState.JOB_STATE_CANCELLED) shouldBe JobStatusType.CANCELLED
    VertexOrchestration.parseJobState(JobState.JOB_STATE_RUNNING) shouldBe JobStatusType.RUNNING
    VertexOrchestration.parseJobState(JobState.JOB_STATE_PENDING) shouldBe JobStatusType.PENDING
    VertexOrchestration.parseJobState(JobState.JOB_STATE_QUEUED) shouldBe JobStatusType.PENDING
    VertexOrchestration.parseJobState(JobState.JOB_STATE_UNSPECIFIED) shouldBe JobStatusType.UNKNOWN
    VertexOrchestration.parseJobState(JobState.UNRECOGNIZED) shouldBe JobStatusType.UNKNOWN
  }

  it should "test end to end training & submission of a custom model locally" ignore {
    import ai.chronon.api._
    import scala.concurrent.duration._

    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

    val orchPlatform = new VertexOrchestration("canary-443022", "us-central1")

    // Load the CTR model from the compiled model file
    val modelPath = "python/test/canary/compiled/models/gcp/click_through_rate.ctr_model__1.0"

    logger.info(s"Loading model from file: $modelPath")
    val ctrModel = ThriftJsonCodec.fromJsonFile[Model](modelPath, check = false)

    val version = ctrModel.metaData.version

    // Step 1: Submit training job
    logger.info("=" * 80)
    logger.info("Step 1: Submitting training job to Vertex AI...")
    logger.info("=" * 80)
    val trainingRequest = TrainingRequest(trainingSource = null, model = ctrModel, date = "2025-12-08", window = null)
    val trainingJobName = Await.result(orchPlatform.submitTrainingJob(trainingRequest), 1.minute)
    logger.info(s"Training job name: $trainingJobName")

    val maxWaitTime = 10 * 60 * 1000
    var startTime = System.currentTimeMillis()
    var currentState = JobStatusType.UNKNOWN
    while (currentState != JobStatusType.SUCCEEDED && currentState != JobStatusType.FAILED) {
      if (System.currentTimeMillis() - startTime > maxWaitTime) {
        throw new RuntimeException(
          s"Timeout waiting training job"
        )
      }
      logger.info(s"Waiting for training job. Current state: $currentState")
      Thread.sleep(30000) // Wait for 30 seconds before checking again
      val currentStatus = Await.result(orchPlatform.getTrainingJobStatus(trainingJobName), 10.seconds)
      currentState = currentStatus.jobStatusType
    }

    // Step 2: Create endpoint
    logger.info("\n" + "=" * 80)
    logger.info("Step 2: Creating endpoint (if absent)...")
    logger.info("=" * 80)
    val endpointConfig = ctrModel.getDeploymentConf.getEndpointConfig
    val endpointResourceName = Await.result(orchPlatform.createEndpoint(endpointConfig.endpointName), 5.minutes)
    logger.info(s"Endpoint resource name: $endpointResourceName")

    // Step 3: Deploy model
    logger.info("\n" + "=" * 80)
    logger.info("Step 2: Deploying model (endpoint creation + model upload + deployment)...")
    logger.info("=" * 80)
    val deployRequest = DeployModelRequest(model = ctrModel, version = version, date = "2025-12-08")
    val deploymentId = Await.result(orchPlatform.deployModel(deployRequest), 20.minutes)
    logger.info(s"Deployment ID: $deploymentId")

    startTime = System.currentTimeMillis()
    currentState = JobStatusType.UNKNOWN
    while (currentState != JobStatusType.SUCCEEDED && currentState != JobStatusType.FAILED) {
      if (System.currentTimeMillis() - startTime > maxWaitTime) {
        throw new RuntimeException(
          s"Timeout waiting for deployment"
        )
      }
      logger.info(s"Waiting for deployment. Current state: $currentState")
      Thread.sleep(30000) // Wait for 30 seconds before checking again
      val currentStatus = Await.result(orchPlatform.getOperationStatus(deploymentId), 10.seconds)
      currentState = currentStatus.jobStatusType
    }

    logger.info("\n" + "=" * 80)
    logger.info("VertexPlatform example completed!")
    logger.info("=" * 80)
  }
}
