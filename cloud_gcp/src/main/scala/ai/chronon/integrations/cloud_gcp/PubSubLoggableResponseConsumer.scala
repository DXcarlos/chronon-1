package ai.chronon.integrations.cloud_gcp

import ai.chronon.online.{LoggableResponse, TopicInfo}
import ai.chronon.online.metrics.Metrics
import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.{PubsubMessage, TopicName}
import org.slf4j.{Logger, LoggerFactory}
import com.google.api.core.ApiFutures
import com.google.api.core.ApiFutureCallback

import java.util.concurrent.TimeUnit
import java.util.function.Consumer

class PubSubLoggableResponseConsumer(topicInfo: TopicInfo, maybeSchemaRegistryId: Option[Int], projectId: String)
    extends Consumer[LoggableResponse] {

  lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  private val metricsContext: Metrics.Context =
    Metrics.Context(Metrics.Environment.JoinFetching).withSuffix("pubsub_ooc")

  private val topicName = TopicName.of(projectId, topicInfo.name)

  private lazy val publisher: Publisher = {
    try {
      val publisherBuilder = Publisher.newBuilder(topicName)

      // Apply any additional settings from params
      topicInfo.params.get("endpoint").foreach { endpoint =>
        publisherBuilder.setEndpoint(endpoint)
      }

      publisherBuilder.build()
    } catch {
      case e: Exception =>
        logger.error(s"Failed to create PubSub publisher for topic ${topicName}", e)
        throw e
    }
  }

  override def accept(response: LoggableResponse): Unit = {
    val avroBytes = LoggableResponse.toAvroBytes(response)
    val avroBytesWithOptionalPreamble = {
      // tack on the schema registry magic byte and schema ID if provided
      // https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
      maybeSchemaRegistryId
        .map { schemaId =>
          LoggableResponse.prependSchemaRegistryBytes(schemaId, avroBytes)
        }
        .getOrElse(avroBytes)
    }

    val messageBuilder = PubsubMessage
      .newBuilder()
      .setData(ByteString.copyFrom(avroBytesWithOptionalPreamble))

    // Add joinName attribute (use empty string if null since PubSub doesn't allow null values)
    messageBuilder.putAttributes("joinName", Option(response.joinName).getOrElse(""))

    val message = messageBuilder.build()

    try {
      metricsContext.increment("published_records")
      val future = publisher.publish(message)
      ApiFutures.addCallback(
        future,
        new ApiFutureCallback[String] {
          override def onSuccess(messageId: String): Unit = {
            metricsContext.increment("publish_successes")
            logger.debug(
              s"Successfully published message to PubSub topic ${topicInfo.name} for join ${response.joinName}, messageId: $messageId")
          }
          override def onFailure(t: Throwable): Unit = {
            metricsContext.increment("publish_failures")
            logger.warn(
              s"Failed to publish feature response to PubSub topic ${topicInfo.name} for join ${response.joinName}",
              t)
          }
        },
        java.util.concurrent.ForkJoinPool.commonPool()
      )
    } catch {
      case e: Exception =>
        metricsContext.increment("publish_failures")
        logger.warn(
          s"Failed to publish feature response to PubSub topic ${topicInfo.name} for join ${response.joinName}",
          e)
    }
  }

  // Cleanup method to shut down the publisher gracefully
  def shutdown(): Unit = {
    try {
      publisher.shutdown()
      if (!publisher.awaitTermination(1, TimeUnit.MINUTES)) {
        logger.warn("PubSub publisher did not terminate gracefully within 1 minute")
      }
    } catch {
      case e: Exception =>
        logger.error("Error during PubSub publisher shutdown", e)
    }
  }
}
