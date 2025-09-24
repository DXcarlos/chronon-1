package ai.chronon.integrations.cloud_gcp

import ai.chronon.online.{LoggableResponse, TopicInfo}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PubSubLoggableResponseConsumerTest extends AnyFlatSpec with Matchers {

  "TopicInfo.parse" should "correctly parse pubsub URL format" in {
    val topicUrl = "pubsub://my-topic/project=my-project-id/endpoint=localhost:8085"
    val topicInfo = TopicInfo.parse(topicUrl)

    topicInfo.name shouldBe "my-topic"
    topicInfo.messageBus shouldBe "pubsub"
    topicInfo.params("project") shouldBe "my-project-id"
    topicInfo.params("endpoint") shouldBe "localhost:8085"
  }

  "PubSubLoggableResponseConsumer" should "be created successfully with valid topic info" in {
    val topicInfo = TopicInfo(
      name = "test-topic",
      messageBus = "pubsub",
      params = Map(
        "project" -> "test-project",
        "endpoint" -> "localhost:8085" // PubSub emulator endpoint
      )
    )

    noException should be thrownBy {
      new PubSubLoggableResponseConsumer(topicInfo, None, "test-project")
    }
  }

  it should "work with any topic info since project ID is passed separately" in {
    val topicInfo = TopicInfo(
      name = "test-topic",
      messageBus = "pubsub",
      params = Map("endpoint" -> "localhost:8085")
    )

    noException should be thrownBy {
      new PubSubLoggableResponseConsumer(topicInfo, None, "test-project")
    }
  }

  "LoggableResponse" should "serialize to Avro bytes correctly" in {
    val response = LoggableResponse(
      keyBytes = "test-key".getBytes(),
      valueBytes = "test-value".getBytes(),
      joinName = "test-join",
      tsMillis = System.currentTimeMillis(),
      schemaHash = "test-hash"
    )

    val avroBytes = LoggableResponse.toAvroBytes(response)
    avroBytes should not be empty

    // Verify we can deserialize it back
    val deserializedResponse = LoggableResponse.fromAvroBytes(avroBytes)
    deserializedResponse.joinName shouldBe response.joinName
    deserializedResponse.tsMillis shouldBe response.tsMillis
    deserializedResponse.schemaHash shouldBe response.schemaHash
  }
}