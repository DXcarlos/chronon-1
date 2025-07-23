package ai.chronon.flink

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.avro.AvroInputFormat
import org.apache.flink.formats.avro.AvroSerializationSchema
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo
import org.apache.flink.formats.avro.utils.AvroKryoSerializerUtils
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.kafka.clients.producer.ProducerConfig
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.ScallopOption
import org.rogach.scallop.Serialization

// Canary test app that can point to a source data file and will emit an event to Kafka periodically with an updated timestamp
object FlinkKafkaItemEventDriver {
  // Pull in the Serialization trait to sidestep: https://github.com/scallop/scallop/issues/137
  class JobArgs(args: Seq[String]) extends ScallopConf(args) with Serialization {
    val dataFileName: ScallopOption[String] =
      opt[String](required = true, descr = "Name of the file on GCS to read data from")
    val kafkaBootstrap: ScallopOption[String] =
      opt[String](required = true, descr = "Kafka bootstrap server in host:port format")
    val kafkaTopic: ScallopOption[String] = opt[String](required = true, descr = "Kafka topic to write to")
    val parentJobId: ScallopOption[String] =
      opt[String](required = false,
                  descr = "Parent job id that invoked the Flink job. For example, the Dataproc job id.")
    val eventDelayMillis: ScallopOption[Int] =
      opt[Int](required = false,
               descr = "Delay to use between event publishes (dictates the eps)",
               default = Some(1000))

    verify()
  }

  def main(args: Array[String]): Unit = {
    val jobArgs = new JobArgs(args)
    val dataFileName = jobArgs.dataFileName()
    val bootstrapServers = jobArgs.kafkaBootstrap()
    val kafkaTopic = jobArgs.kafkaTopic()
    val parentJobId = jobArgs.parentJobId()
    val eventDelayMillis = jobArgs.eventDelayMillis()

    // Configure GCS source
    val avroFormat = new AvroInputFormat[GenericRecord](
      new Path(dataFileName),
      classOf[GenericRecord]
    )

    implicit val typeInfo: TypeInformation[GenericRecord] = new GenericRecordAvroTypeInfo(avroSchema)

    // Set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig
      .enableForceKryo() // use kryo for complex types that Flink's default ser system doesn't support (e.g case classes)
    env.getConfig.enableGenericTypes() // more permissive type checks
    env.addDefaultKryoSerializer(classOf[Schema], classOf[AvroKryoSerializerUtils.AvroSchemaSerializer])

    val stream = env
      .createInput(avroFormat)
      .setParallelism(1)

    val transformedStream: DataStream[GenericRecord] = stream
      .map(new DelayedSourceTransformFn(eventDelayMillis))
      .setParallelism(stream.getParallelism)

    // Configure Kafka sink
    val serializationSchema = KafkaRecordSerializationSchema
      .builder()
      .setTopic(kafkaTopic)
      .setValueSerializationSchema(AvroSerializationSchema.forGeneric(avroSchema))
      .build()

    val producerConfig = new java.util.Properties()
    producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all")
    producerConfig.setProperty(ProducerConfig.RETRIES_CONFIG, "3")
    producerConfig.setProperty("security.protocol", "SASL_SSL")
    producerConfig.setProperty("sasl.mechanism", "OAUTHBEARER")
    producerConfig.setProperty("sasl.login.callback.handler.class",
                               "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler")
    producerConfig.setProperty("sasl.jaas.config",
                               "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;")

    val kafkaSink = KafkaSink
      .builder()
      .setBootstrapServers(bootstrapServers)
      .setRecordSerializer(serializationSchema)
      .setKafkaProducerConfig(producerConfig)
      .build()

    // Write to Kafka
    transformedStream
      .sinkTo(kafkaSink)
      .setParallelism(transformedStream.getParallelism)

    // Execute program
    env.execute("Periodic Kafka Data Producer")
  }

  lazy val avroSchema: Schema = {
    new Schema.Parser().parse("""
    {
      "type": "record",
      "name": "Event",
      "namespace": "ai.chronon",
      "fields": [
        {"name": "event_type", "type": ["null", "string"], "default": null},
        {"name": "timestamp", "type": "long"},
        {"name": "visitor_id", "type": ["null", "string"], "default": null},
        {"name": "is_primary", "type": "boolean"},
        {"name": "logger_name", "type": ["null", "string"], "default": null},
        {"name": "source", "type": ["null", "string"], "default": null},
        {"name": "is_mobile_req", "type": ["null", "boolean"], "default": null},
        {"name": "is_mobile_device", "type": ["null", "boolean"], "default": null},
        {"name": "is_mobile_view", "type": ["null", "boolean"], "default": null},
        {"name": "item_ids", "type": ["null", {"type": "array", "items": "long"}], "default": null},
        {"name": "created_at", "type": ["null", "long"], "default": null},
        {"name": "attributes", "type": ["null", {"type": "map", "values": ["null", "string"]}], "default": null}
      ]
    }
  """)
  }
}

class DelayedSourceTransformFn(delayMs: Int) extends MapFunction[GenericRecord, GenericRecord] {
  override def map(value: GenericRecord): GenericRecord = {
    val updatedTimestamp = System.currentTimeMillis()
    // Update the timestamp field in the record
    value.put("timestamp", updatedTimestamp)
    Thread.sleep(delayMs)
    value
  }
}
