package ai.chronon.integrations.cloud_gcp

import ai.chronon.api.{Builders => B, ModelBackend}
import ai.chronon.online.InferRequest
import io.vertx.core.{AsyncResult, Future => VertxFuture, Handler}
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.{JsonArray, JsonObject}
import io.vertx.ext.web.client.{HttpRequest, HttpResponse, WebClient}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, when}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.ExecutionContext
import scala.util.Failure

class VertexPlatformTest extends AnyFlatSpec with Matchers with MockitoSugar with ScalaFutures {

  implicit val ec: ExecutionContext = ExecutionContext.global

  private val mockWebClient = mock[WebClient]
  private val platform = new VertexPlatform("test-project", "us-central1", Some(mockWebClient))

  it should "handle missing model_name parameter" in {

    val model = B.Model(
      metaData = B.MetaData(name = "test_model"),
      runtime = B.ModelRuntime(
        backend = ModelBackend.VertexAI,
        params = Map(
          "model_type" -> "publisher"
          // Missing model_name
        )
      )
    )

    val inputRequests = Seq(
      Map("instance" -> Map("content" -> "hello").asInstanceOf[AnyRef])
    )

    val inferRequest = InferRequest(model, inputRequests)

    val responseFuture = platform.infer(inferRequest)

    whenReady(responseFuture) { response =>
      response.outputs shouldBe a[Failure[_]]
      response.outputs.failed.get.getMessage should include("model_name is required")
    }
  }

  it should "handle unsupported model_type" in {

    val model = B.Model(
      metaData = B.MetaData(name = "test_model"),
      runtime = B.ModelRuntime(
        backend = ModelBackend.VertexAI,
        params = Map(
          "model_name" -> "some-model",
          "model_type" -> "unsupported_type"
        )
      )
    )

    val inputRequests = Seq(
      Map("instance" -> Map("content" -> "hello").asInstanceOf[AnyRef])
    )

    val inferRequest = InferRequest(model, inputRequests)

    val responseFuture = platform.infer(inferRequest)

    whenReady(responseFuture) { response =>
      response.outputs shouldBe a[Failure[_]]
      response.outputs.failed.get.getMessage should include("Unsupported model_type: unsupported_type")
    }
  }

  it should "generate correct URLs for publisher models" in {

    val publisherTemplate = platform.urlTemplates("publisher")
    val publisherUrl = publisherTemplate.format("text-embedding-004")

    publisherUrl shouldBe "https://us-central1-aiplatform.googleapis.com/v1/projects/test-project/locations/us-central1/publishers/google/models/text-embedding-004:predict"
  }

  it should "generate correct URLs for custom models" in {

    val customTemplate = platform.urlTemplates("custom")
    val customUrl = customTemplate.format("1234567890")

    customUrl shouldBe "https://us-central1-aiplatform.googleapis.com/v1/projects/test-project/locations/us-central1/endpoints/1234567890:predict"
  }

  it should "default to publisher model type when not specified" in {
    val mockRequest = mock[HttpRequest[Buffer]]
    val mockFuture = mock[VertxFuture[HttpResponse[Buffer]]]

    val model = B.Model(
      metaData = B.MetaData(name = "test_model"),
      runtime = B.ModelRuntime(
        backend = ModelBackend.VertexAI,
        params = Map(
          "model_name" -> "text-embedding-004"
          // No model_type specified - should default to publisher
        )
      )
    )

    val inputRequests = Seq(
      Map("instance" -> Map("content" -> "hello").asInstanceOf[AnyRef])
    )

    val inferRequest = InferRequest(model, inputRequests)

    // Setup mocks to capture the URL being called
    when(mockWebClient.postAbs(any[String])).thenReturn(mockRequest)
    when(mockRequest.putHeader(any[String], any[String])).thenReturn(mockRequest)
    when(mockRequest.sendJsonObject(any[JsonObject])).thenReturn(mockFuture)
    when(mockFuture.onComplete(any[Handler[AsyncResult[HttpResponse[Buffer]]]])).thenReturn(mockFuture)

    // Execute - this will fail but we can still verify the URL was called
    platform.infer(inferRequest)

    // Verify that publisher URL template was used (default)
    verify(mockWebClient).postAbs("https://us-central1-aiplatform.googleapis.com/v1/projects/test-project/locations/us-central1/publishers/google/models/text-embedding-004:predict")
  }

  it should "filter model parameters correctly" in {

    val inputRequests = Seq(
      Map("instance" -> Map("content" -> "hello").asInstanceOf[AnyRef])
    )

    val modelParams = Map(
      "model_name" -> "text-embedding-004", // Should be filtered out
      "model_type" -> "publisher",          // Should be filtered out
      "temperature" -> "0.7",               // Should be included
      "max_tokens" -> "100"                 // Should be included
    )

    val requestBody = VertexHttpUtils.createPredictionRequestBody(inputRequests, modelParams)

    // Check that instances are correctly added
    requestBody.containsKey("instances") shouldBe true
    val instances = requestBody.getJsonArray("instances")
    instances.size() shouldBe 1

    // Check that parameters are correctly filtered
    requestBody.containsKey("parameters") shouldBe true
    val parameters = requestBody.getJsonObject("parameters")
    parameters.containsKey("temperature") shouldBe true
    parameters.containsKey("max_tokens") shouldBe true
    parameters.containsKey("model_name") shouldBe false
    parameters.containsKey("model_type") shouldBe false

    parameters.getString("temperature") shouldBe "0.7"
    parameters.getString("max_tokens") shouldBe "100"
  }

  it should "fail entire request when any input is missing instance key" in {

    val model = B.Model(
      metaData = B.MetaData(name = "test_model"),
      runtime = B.ModelRuntime(
        backend = ModelBackend.VertexAI,
        params = Map(
          "model_name" -> "text-embedding-004",
          "model_type" -> "publisher"
        )
      )
    )

    val inputRequests = Seq(
      Map("instance" -> Map("content" -> "hello").asInstanceOf[AnyRef]),
      Map("other_key" -> "invalid".asInstanceOf[AnyRef]), // Missing instance key
      Map("instance" -> Map("content" -> "world").asInstanceOf[AnyRef])
    )

    val inferRequest = InferRequest(model, inputRequests)

    val responseFuture = platform.infer(inferRequest)

    whenReady(responseFuture) { response =>
      response.outputs shouldBe a[Failure[_]]
      response.outputs.failed.get.getMessage should include("Missing 'instance' key in input requests at indices: 1")
    }
  }

  it should "map predictions correctly" in {

    val responseBody = new JsonObject()
      .put("predictions", new JsonArray()
        .add(new JsonObject().put("score", 0.8).put("label", "positive"))
        .add(new JsonObject().put("score", 0.3).put("label", "negative")))

    val results = VertexHttpUtils.extractPredictionResults(responseBody)

    results should have size 2

    // First result should be first prediction
    results(0) should contain("score" -> 0.8)
    results(0) should contain("label" -> "positive")

    // Second result should be second prediction
    results(1) should contain("score" -> 0.3)
    results(1) should contain("label" -> "negative")
  }

  it should "handle missing predictions array" in {

    val responseBody = new JsonObject()
      .put("other_field", "value")

    val exception = intercept[RuntimeException] {
      VertexHttpUtils.extractPredictionResults(responseBody)
    }

    exception.getMessage should include("No 'predictions' array found in response")
  }

  it should "convert nested Scala Maps to JSON correctly" in {
    val nestedMap = Map(
      "content" -> "test content",
      "metadata" -> Map(
        "nested_field" -> "nested_value",
        "number" -> 42.asInstanceOf[AnyRef]
      ).asInstanceOf[AnyRef]
    )

    val inputRequests = Seq(Map("instance" -> nestedMap.asInstanceOf[AnyRef]))
    val requestBody = VertexHttpUtils.createPredictionRequestBody(inputRequests, Map.empty)

    requestBody.containsKey("instances") shouldBe true
    val instances = requestBody.getJsonArray("instances")
    instances.size() shouldBe 1

    val firstInstance = instances.getJsonObject(0)
    firstInstance.getString("content") shouldBe "test content"
    firstInstance.containsKey("metadata") shouldBe true
    
    val metadata = firstInstance.getJsonObject("metadata")
    metadata.getString("nested_field") shouldBe "nested_value"
    metadata.getValue("number") shouldBe 42
  }
}