package ai.chronon.flink_connectors.pubsub.fastack

import com.google.cloud.pubsub.v1.Subscriber
import com.google.pubsub.v1.ProjectSubscriptionName
import org.apache.flink.api.common.serialization.{DeserializationSchema, RuntimeContextInitializationContextAdapters}
import org.apache.flink.api.common.state.CheckpointListener
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.MetricGroup
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubDeserializationSchema
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.{CountDownLatch, TimeUnit}

/** Flink source that reads messages from Pub/Sub given various params (project, subscription, etc.)
  * Uses the injected DeserializationSchema to first convert PubSubMessages -> Array[Byte] and the the underlying
  * Chronon deserialization schema to convert the bytes to the desired Chronon type (typically a Mutation)
  *
  * This source uses the streaming pull model of Pub/Sub. We create & start a subscriber in the open/run methods and
  * register a callback (MessageReceiver) that is invoked whenever a new message is pulled from Pub/Sub. The PubSub
  * subscriber will register threads and pull messages in parallel under the hood.
  *
  * This source chooses to ACK messages immediately after pulling them rather than waiting for checkpointing every n secs.
  * Allows us to stay clear of tight ACK deadlines and drowning under Pub/Sub retransmits if the message volume is high / checkpoints
  * fail a couple of times in a row, etc.
  *
  * This comes at the cost of accuracy (e.g. lost messages if the job fails after pulling but before processing), but we do
  * batch correct daily in GroupByUploads.
  */
class PubSubSource[OUT](
    groupByName: String,
    deserializationSchema: PubSubDeserializationSchema[OUT],
    projectSubscriptionName: String
) extends RichSourceFunction[OUT]
    with ResultTypeQueryable[OUT]
    with ParallelSourceFunction[OUT]
    with CheckpointListener {

  @transient protected var subscriber: Subscriber = _
  @transient protected var messageReceiver: PubSubMessageReceiver[OUT] = _
  @transient @volatile protected var isRunning: Boolean = false
  @transient private var shutdownLatch: CountDownLatch = _

  private lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  override def open(configuration: Configuration): Unit = {
    super.open(configuration)

    val metricsGroup = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", groupByName)

    val deSerRuntimeContext = RuntimeContextInitializationContextAdapters.deserializationAdapter(getRuntimeContext)
    deserializationSchema.open(deSerRuntimeContext)

    createAndSetPubSubSubscriber(metricsGroup)
    this.isRunning = true
    shutdownLatch = new CountDownLatch(1)
    logger.info("Pub/Sub source initialized for project/subscription: {}", projectSubscriptionName)
  }

  override def run(ctx: SourceFunction.SourceContext[OUT]): Unit = {
    val collector = new PubSubCollector(ctx)
    messageReceiver.open(collector)
    try {
      subscriber.startAsync().awaitRunning(60, TimeUnit.SECONDS)
    } catch {
      case e: Exception =>
        // fail the job if we can't start the subscriber
        throw new RuntimeException(s"Failed to start Pub/Sub subscriber for $projectSubscriptionName", e)
    }

    try {
      while (isRunning) {
        try {
          if (shutdownLatch.await(1, TimeUnit.SECONDS)) {
            isRunning = false
          }
        } catch {
          case _: InterruptedException =>
            // Flink will interrupt in case of ungraceful shutdown (e.g. cancellation of the source operator during failover)
            Thread.currentThread().interrupt()
            isRunning = false
        }
      }
    } finally {
      // trigger a cleanup in both graceful (cancel called first) and abrupt shutdowns
      cleanUpSubscriber()
    }
  }

  override def close(): Unit = {
    cleanUpSubscriber()
  }

  override def cancel(): Unit = {
    isRunning = false
    if (shutdownLatch != null) {
      shutdownLatch.countDown()
    }
  }

  private def cleanUpSubscriber(): Unit = {
    if (subscriber != null && subscriber.isRunning) {
      try {
        subscriber.stopAsync()
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to stop Pub/Sub subscriber for $projectSubscriptionName", e)
      }
      subscriber = null
    }
  }

  private class PubSubCollector(ctx: SourceFunction.SourceContext[OUT]) extends Collector[OUT] {
    override def collect(record: OUT): Unit = {
      // sync on checkpoint lock as we might have multiple PubSub threads trying to collect records
      ctx.getCheckpointLock.synchronized {
        ctx.collect(record)
      }
    }

    override def close(): Unit = {}
  }

  override def getProducedType: TypeInformation[OUT] = {
    deserializationSchema.getProducedType
  }

  override def notifyCheckpointComplete(checkpointId: Long): Unit = {}

  override def notifyCheckpointAborted(checkpointId: Long): Unit = {}

  private def createAndSetPubSubSubscriber(metricGroup: MetricGroup): Unit = {
    if (subscriber == null) {
      messageReceiver = new PubSubMessageReceiver(deserializationSchema)
      subscriber = Subscriber.newBuilder(projectSubscriptionName, messageReceiver).build()
    }
  }
}

object PubSubSource {
  def build[OUT](
      groupByName: String,
      deserializationSchema: DeserializationSchema[OUT],
      projectName: String,
      subscriptionName: String
  ): PubSubSource[OUT] = {

    val wrappedSchema = new DeserializationSchemaWrapper[OUT](deserializationSchema)

    new PubSubSource[OUT](
      groupByName,
      wrappedSchema,
      ProjectSubscriptionName.format(projectName, subscriptionName)
    )
  }
}
