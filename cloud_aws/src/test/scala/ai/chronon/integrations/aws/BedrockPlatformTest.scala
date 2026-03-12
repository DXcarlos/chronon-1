package ai.chronon.integrations.aws

import ai.chronon.api.{Builders => B, ModelBackend}
import ai.chronon.online.PredictRequest
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeAsyncClient
import software.amazon.awssdk.services.bedrockruntime.model.{InvokeModelRequest, InvokeModelResponse}

import java.util.concurrent.CompletableFuture
import scala.concurrent.ExecutionContext
import scala.util.Failure

class BedrockPlatformTest extends AnyFlatSpec with Matchers with MockitoSugar with ScalaFutures {

  implicit val ec: ExecutionContext = ExecutionContext.global

  private def mockClient: BedrockRuntimeAsyncClient = mock[BedrockRuntimeAsyncClient]

  private def makePlatform(client: BedrockRuntimeAsyncClient = mockClient): BedrockPlatform =
    new BedrockPlatform("us-east-1", Some(client))

  private def makeModel(params: Map[String, String]): ai.chronon.api.Model =
    B.Model(
      metaData = B.MetaData(name = "test_model"),
      inferenceSpec = B.InferenceSpec(
        modelBackend = ModelBackend.SageMaker,
        modelBackendParams = params
      )
    )

  it should "return Failure when model_name is missing" in {
    val platform = makePlatform()
    val model = makeModel(Map("model_type" -> "bedrock"))
    val request = PredictRequest(model, Seq(Map("instance" -> Map("inputText" -> "hello").asInstanceOf[AnyRef])))

    whenReady(platform.predict(request)) { response =>
      response.outputs shouldBe a[Failure[_]]
      response.outputs.failed.get.getMessage should include("model_name is required")
    }
  }

  it should "return Failure when model_type is unsupported" in {
    val platform = makePlatform()
    val model = makeModel(Map("model_name" -> "some-model", "model_type" -> "custom"))
    val request = PredictRequest(model, Seq(Map("instance" -> Map("inputText" -> "hello").asInstanceOf[AnyRef])))

    whenReady(platform.predict(request)) { response =>
      response.outputs shouldBe a[Failure[_]]
      response.outputs.failed.get.getMessage should include("Unsupported model_type: custom")
    }
  }

  it should "return Failure when instance key is missing at index 1" in {
    val platform = makePlatform()
    val model = makeModel(Map("model_name" -> "amazon-titan-embed-text-v1", "model_type" -> "bedrock"))
    val request = PredictRequest(
      model,
      Seq(
        Map("instance" -> Map("inputText" -> "hello").asInstanceOf[AnyRef]),
        Map("other_key" -> "invalid".asInstanceOf[AnyRef]),
        Map("instance" -> Map("inputText" -> "world").asInstanceOf[AnyRef])
      )
    )

    whenReady(platform.predict(request)) { response =>
      response.outputs shouldBe a[Failure[_]]
      response.outputs.failed.get.getMessage should include("1")
    }
  }

  it should "serialize nested Scala map to JSON bytes correctly" in {
    val instance = Map("inputText" -> "hello world", "extra" -> Map("k" -> "v").asInstanceOf[AnyRef])
    val bytes = BedrockUtils.serializeInstance(instance.asInstanceOf[AnyRef])
    val json = new String(bytes, "UTF-8")
    json should include("inputText")
    json should include("hello world")
    json should include("extra")
  }

  it should "parse JSON response string to Map" in {
    val json = """{"embedding":[0.1,0.2],"statistics":{"tokenCount":5}}"""
    val result = BedrockUtils.parseResponse(json)
    result.keys should contain("embedding")
    result.keys should contain("statistics")
  }

  it should "return Success with parsed response on happy path" in {
    val client = mockClient
    val platform = makePlatform(client)

    val responseBody = SdkBytes.fromUtf8String("""{"embedding":[0.1,0.2]}""")
    val invokeResponse = InvokeModelResponse.builder().body(responseBody).build()
    val cf = CompletableFuture.completedFuture(invokeResponse)

    when(client.invokeModel(any[InvokeModelRequest])).thenReturn(cf)

    val model = makeModel(Map("model_name" -> "amazon-titan-embed-text-v1", "model_type" -> "bedrock"))
    val request = PredictRequest(
      model,
      Seq(Map("instance" -> Map("inputText" -> "hello").asInstanceOf[AnyRef]))
    )

    whenReady(platform.predict(request)) { response =>
      response.outputs.isSuccess shouldBe true
      val results = response.outputs.get
      results should have size 1
      results.head should contain key "embedding"
    }
  }

  it should "invoke model once per input instance" in {
    val client = mockClient
    val platform = makePlatform(client)

    val responseBody = SdkBytes.fromUtf8String("""{"embedding":[0.1]}""")
    val invokeResponse = InvokeModelResponse.builder().body(responseBody).build()
    val cf = CompletableFuture.completedFuture(invokeResponse)

    when(client.invokeModel(any[InvokeModelRequest])).thenReturn(cf)

    val model = makeModel(Map("model_name" -> "amazon-titan-embed-text-v1", "model_type" -> "bedrock"))
    val request = PredictRequest(
      model,
      Seq(
        Map("instance" -> Map("inputText" -> "first").asInstanceOf[AnyRef]),
        Map("instance" -> Map("inputText" -> "second").asInstanceOf[AnyRef]),
        Map("instance" -> Map("inputText" -> "third").asInstanceOf[AnyRef])
      )
    )

    whenReady(platform.predict(request)) { response =>
      response.outputs.isSuccess shouldBe true
      response.outputs.get should have size 3
    }

    verify(client, times(3)).invokeModel(any[InvokeModelRequest])
  }

  it should "default model_type to bedrock when not specified" in {
    val client = mockClient
    val platform = makePlatform(client)

    val responseBody = SdkBytes.fromUtf8String("""{"embedding":[0.5]}""")
    val invokeResponse = InvokeModelResponse.builder().body(responseBody).build()
    val cf = CompletableFuture.completedFuture(invokeResponse)

    when(client.invokeModel(any[InvokeModelRequest])).thenReturn(cf)

    // No model_type specified — should default to "bedrock"
    val model = makeModel(Map("model_name" -> "amazon-titan-embed-text-v1"))
    val request = PredictRequest(
      model,
      Seq(Map("instance" -> Map("inputText" -> "hello").asInstanceOf[AnyRef]))
    )

    whenReady(platform.predict(request)) { response =>
      response.outputs.isSuccess shouldBe true
    }
  }
}
