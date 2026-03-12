package ai.chronon.integrations.aws

import ai.chronon.api.EndpointConfig
import ai.chronon.online.metrics.FlexibleExecutionContext
import ai.chronon.online.{
  DeployModelRequest,
  ModelJobStatus,
  ModelOperation,
  ModelPlatform,
  PredictRequest,
  PredictResponse,
  TrainingRequest,
  metrics
}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeAsyncClient
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class BedrockPlatform(region: String, clientOverride: Option[BedrockRuntimeAsyncClient] = None) extends ModelPlatform {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  // Lazy so it's not serialized; recreated on each executor if needed
  @transient private lazy val bedrockClient: BedrockRuntimeAsyncClient =
    clientOverride.getOrElse(
      BedrockRuntimeAsyncClient.builder().region(Region.of(region)).build()
    )

  override def predict(predictRequest: PredictRequest): Future[PredictResponse] = {
    implicit val ec: ExecutionContext = FlexibleExecutionContext.buildExecutionContext
    Try {
      val modelParams =
        Option(predictRequest.model.inferenceSpec.modelBackendParams)
          .map(_.asScala.toMap)
          .getOrElse(Map.empty)

      val modelName = modelParams.get("model_name") match {
        case Some(name) => name
        case None =>
          return Future.successful(
            PredictResponse(predictRequest,
                            Failure(new IllegalArgumentException("model_name is required in modelBackendParams"))))
      }

      val modelType = modelParams.getOrElse("model_type", "bedrock")
      if (modelType != "bedrock") {
        return Future.successful(
          PredictResponse(predictRequest, Failure(new IllegalArgumentException(s"Unsupported model_type: $modelType"))))
      }

      val missingInstanceIndices = predictRequest.inputRequests.zipWithIndex.collect {
        case (inputRequest, index) if !inputRequest.contains("instance") => index
      }

      if (missingInstanceIndices.nonEmpty) {
        val errorMsg = s"Missing 'instance' key in input requests at indices: ${missingInstanceIndices.mkString(", ")}"
        return Future.successful(PredictResponse(predictRequest, Failure(new IllegalArgumentException(errorMsg))))
      }

      val perInstanceFutures: Seq[Future[Map[String, AnyRef]]] =
        predictRequest.inputRequests.map { inputRequest =>
          val instanceValue = inputRequest("instance")
          val bodyBytes = BedrockUtils.serializeInstance(instanceValue)

          val invokeRequest = InvokeModelRequest
            .builder()
            .modelId(modelName)
            .body(SdkBytes.fromByteArray(bodyBytes))
            .contentType("application/json")
            .accept("application/json")
            .build()

          val promise = Promise[Map[String, AnyRef]]()
          bedrockClient
            .invokeModel(invokeRequest)
            .whenComplete { (response, error) =>
              if (error != null) {
                promise.failure(error)
              } else {
                promise.complete(Try(BedrockUtils.parseResponse(response.body().asUtf8String())))
              }
            }
          promise.future
        }

      Future.sequence(perInstanceFutures).map { results =>
        PredictResponse(predictRequest, Success(results))
      }
    } match {
      case Success(f) => f.recover { case e => PredictResponse(predictRequest, Failure(e)) }
      case Failure(e) => Future.successful(PredictResponse(predictRequest, Failure(e)))
    }
  }

  override def submitTrainingJob(trainingRequest: TrainingRequest): Future[String] =
    Future.failed(new UnsupportedOperationException("Training not supported for Bedrock"))

  override def createEndpoint(endpointConfig: EndpointConfig): Future[String] =
    Future.failed(new UnsupportedOperationException("Endpoint creation not supported for Bedrock"))

  override def deployModel(deployModelRequest: DeployModelRequest): Future[String] =
    Future.failed(new UnsupportedOperationException("Model deployment not supported for Bedrock"))

  override def getJobStatus(operation: ModelOperation, id: String): Future[ModelJobStatus] =
    Future.failed(new UnsupportedOperationException("Job status not supported for Bedrock"))
}

object BedrockPlatform {
  import ai.chronon.api.{Builders => B, ModelBackend}
  import scala.concurrent.Await
  import scala.concurrent.duration._
  import scala.util.{Failure, Success}

  def main(args: Array[String]): Unit = {
    val region = "us-west-2"
    val modelName = "amazon.titan-embed-text-v1"

    val platform = new BedrockPlatform(region)
    val titanModel = B.Model(
      metaData = B.MetaData(name = "titan_embed_model"),
      inferenceSpec = B.InferenceSpec(
        modelBackend = ModelBackend.SageMaker,
        modelBackendParams = Map("model_name" -> modelName, "model_type" -> "bedrock")
      )
    )

    val inputRequests = Seq(
      Map("instance" -> Map("inputText" -> "Hello, world!").asInstanceOf[AnyRef]),
      Map("instance" -> Map("inputText" -> "Another example for Bedrock.").asInstanceOf[AnyRef])
    )

    val predictRequest = PredictRequest(titanModel, inputRequests)
    println(s"Making prediction request to Bedrock ($modelName) in region $region...")
    val predictionFuture = platform.predict(predictRequest)

    val response = Await.result(predictionFuture, 30.seconds)
    response.outputs match {
      case Success(results) =>
        println("Predictions successful:")
        results.zipWithIndex.foreach { case (result, index) =>
          println(s"  Input $index: $result")
        }
      case Failure(exception) =>
        println(s"Prediction failed: ${exception.getMessage}")
        exception.printStackTrace()
    }

    println("BedrockPlatform smoke test completed.")
  }
}

object BedrockUtils {
  // DefaultScalaModule needed to serialize Scala Maps/collections from input_mapping structs
  private val serializeMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  // Plain mapper for response parsing: produces java.util.ArrayList (not Scala List) for JSON arrays,
  // which is required by SparkInternalRowConversions.arrayConverter
  private val parseMapper = new ObjectMapper()

  def serializeInstance(instance: AnyRef): Array[Byte] =
    serializeMapper.writeValueAsBytes(instance)

  def parseResponse(responseBody: String): Map[String, AnyRef] = {
    val raw = parseMapper.readValue(responseBody, classOf[java.util.Map[String, AnyRef]])
    raw.asScala.toMap
  }
}
