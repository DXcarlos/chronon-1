package ai.chronon.flink
import ai.chronon.flink.types.WriteResponse
import com.codahale.metrics.ExponentiallyDecayingReservoir
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper
import org.apache.flink.metrics.Histogram
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction

/** Sink that captures metrics around feature freshness. We capture the time taken from event creation to KV store sink
  * Ideally we expect this to match the Kafka persistence -> sink time. They can diverge if the event object is created and held on
  * in the source service for some time before the event is submitted to Kafka.
  */
class MetricsSink(groupByName: String) extends RichSinkFunction[WriteResponse] {

  @transient private var eventCreatedToSinkTimeHistogram: Histogram = _
  @transient private var flinkProcessingTimeHistogram: Histogram = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val metricsGroup = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", groupByName)

    // tune the Dropwizard defaults a bit as they can hold on to samples for a longer time (hence metrics take
    // longer to come back to normal in low traffic scenarios)
    // we bump alpha up from 0.015 to 0.05 to make the decay faster
    eventCreatedToSinkTimeHistogram = metricsGroup.histogram(
      "event_created_to_sink_time",
      new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new ExponentiallyDecayingReservoir(512, 0.05))
      )
    )

    flinkProcessingTimeHistogram = metricsGroup.histogram(
      "flink_processing_time",
      new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new ExponentiallyDecayingReservoir(512, 0.05))
      )
    )
  }

  override def invoke(value: WriteResponse, context: SinkFunction.Context): Unit = {
    val currentTime = System.currentTimeMillis()
    eventCreatedToSinkTimeHistogram.update(currentTime - value.tsMillis)
    flinkProcessingTimeHistogram.update(currentTime - value.startProcessingTime)
  }
}
