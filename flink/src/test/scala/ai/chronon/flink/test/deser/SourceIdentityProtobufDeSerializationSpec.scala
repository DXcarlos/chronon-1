package ai.chronon.flink.test.deser

import ai.chronon.api.ScalaJavaConversions.IterableOps
import ai.chronon.flink.deser.DeserializationSchemaBuilder
import ai.chronon.online.serde.{ProtobufConversions, SparkConversions}
import org.apache.flink.api.common.functions.util.ListCollector
import org.apache.spark.sql.Row
import org.junit.Assert.assertTrue
import org.scalatest.flatspec.AnyFlatSpec

import java.util
import scala.collection.JavaConverters._

class SourceIdentityProtobufDeSerializationSpec extends AnyFlatSpec {
  import ProtobufObjectCreator._

  it should "deserialize protobuf data" in {
    val userDescriptor = createUserDescriptor()
    val groupBy = makeMetadataOnlyGroupBy()
    val protobufSerdeProvider = new InMemoryProtobufDeserializationSchemaProvider(userDescriptor)
    val deserSchema = DeserializationSchemaBuilder.buildSourceIdentityDeserSchema(protobufSerdeProvider, groupBy)
    deserSchema.open(new DummyInitializationContext)

    val resultList = new util.ArrayList[Row]()
    val listCollector = new ListCollector(resultList)

    val recordBytes = createDummyUserRecordBytes()
    deserSchema.deserialize(recordBytes, listCollector)

    // check we got a row back
    assert(resultList.size() == 1)
    val row = resultList.toScala.head

    // sanity check source event schemas line up between the deSerSchema and the original schema
    val schema = deserSchema.sourceEventEncoder.schema
    val protoSchemaFields = userDescriptor.getFields.toScala
    val protoSchemaToSparkDataTypes = protoSchemaFields.map { field =>
      val chrononType = ProtobufConversions.fieldToChrononType(field)
      val sparkType = SparkConversions.fromChrononType(chrononType)
      field.getName -> sparkType
    }.toMap

    assert(schema.fieldNames sameElements protoSchemaFields.map(_.getName))
    schema.fieldNames.foreach(name => assert(schema(name).dataType == protoSchemaToSparkDataTypes(name)))

    // spot check a couple of fields
    assert(row.get(0) == 12345) // id
    assert(row.getString(1) == "johndoe") // username
  }

  it should "skip protobuf data that can't be deserialized" in {
    val userDescriptor = createUserDescriptor()
    val groupBy = makeMetadataOnlyGroupBy()

    val protobufSerdeProvider = new InMemoryProtobufDeserializationSchemaProvider(userDescriptor)
    val deserSchema = DeserializationSchemaBuilder.buildSourceIdentityDeserSchema(protobufSerdeProvider, groupBy)
    deserSchema.open(new DummyInitializationContext)

    val recordBytes = createDummyUserRecordBytes()
    // corrupt the record bytes
    val corruptedBytes = recordBytes.clone()
    corruptedBytes(0) = 0xff.toByte
    corruptedBytes(1) = 0xff.toByte
    corruptedBytes(2) = 0xff.toByte

    val resultList = new util.ArrayList[Row]()
    val listCollector = new ListCollector(resultList)

    deserSchema.deserialize(corruptedBytes, listCollector)
    assertTrue(resultList.isEmpty)
  }

  it should "handle repeated fields (lists) correctly" in {
    val userDescriptor = createUserDescriptor()
    val groupBy = makeMetadataOnlyGroupBy()
    val protobufSerdeProvider = new InMemoryProtobufDeserializationSchemaProvider(userDescriptor)
    val deserSchema = DeserializationSchemaBuilder.buildSourceIdentityDeserSchema(protobufSerdeProvider, groupBy)
    deserSchema.open(new DummyInitializationContext)

    val resultList = new util.ArrayList[Row]()
    val listCollector = new ListCollector(resultList)

    val recordBytes = createDummyUserRecordBytes()
    deserSchema.deserialize(recordBytes, listCollector)

    assert(resultList.size() == 1)
    val row = resultList.toScala.head

    // Check tags field (index 2) is a list - Spark converts to Array
    val tags = row.get(2) match {
      case arr: Array[_]           => arr.toSet
      case list: util.ArrayList[_] => list.toScala.toSet
      case seq: Seq[_]             => seq.toSet
      case other                   => fail(s"Unexpected type for tags: ${other.getClass}")
    }
    assert(tags.size == 3)
    assert(tags == Set("active", "premium", "verified"))
  }

  it should "handle map fields correctly" in {
    val userDescriptor = createUserDescriptor()
    val groupBy = makeMetadataOnlyGroupBy()
    val protobufSerdeProvider = new InMemoryProtobufDeserializationSchemaProvider(userDescriptor)
    val deserSchema = DeserializationSchemaBuilder.buildSourceIdentityDeserSchema(protobufSerdeProvider, groupBy)
    deserSchema.open(new DummyInitializationContext)

    val resultList = new util.ArrayList[Row]()
    val listCollector = new ListCollector(resultList)

    val recordBytes = createDummyUserRecordBytes()
    deserSchema.deserialize(recordBytes, listCollector)

    assert(resultList.size() == 1)
    val row = resultList.toScala.head

    // Check preferences field (index 4) is a map - could be Scala or Java map
    val preferences: Map[String, String] = row.get(4) match {
      case m: util.Map[_, _]             => m.asScala.toMap.asInstanceOf[Map[String, String]]
      case m: scala.collection.Map[_, _] => m.toMap.asInstanceOf[Map[String, String]]
      case other                         => fail(s"Unexpected type for preferences: ${other.getClass}")
    }
    assert(preferences("theme") == "dark")
    assert(preferences("language") == "en")
  }

  // ============== Proto2 Tests ==============

  it should "deserialize proto2 data" in {
    val userDescriptor = createUserDescriptorProto2()
    val groupBy = makeMetadataOnlyGroupBy()
    val protobufSerdeProvider = new InMemoryProtobufDeserializationSchemaProvider(userDescriptor)
    val deserSchema = DeserializationSchemaBuilder.buildSourceIdentityDeserSchema(protobufSerdeProvider, groupBy)
    deserSchema.open(new DummyInitializationContext)

    val resultList = new util.ArrayList[Row]()
    val listCollector = new ListCollector(resultList)

    val recordBytes = createDummyUserRecordBytesProto2()
    deserSchema.deserialize(recordBytes, listCollector)

    assert(resultList.size() == 1)
    val row = resultList.toScala.head

    // sanity check source event schemas line up
    val schema = deserSchema.sourceEventEncoder.schema
    val protoSchemaFields = userDescriptor.getFields.toScala
    val protoSchemaToSparkDataTypes = protoSchemaFields.map { field =>
      val chrononType = ProtobufConversions.fieldToChrononType(field)
      val sparkType = SparkConversions.fromChrononType(chrononType)
      field.getName -> sparkType
    }.toMap

    assert(schema.fieldNames sameElements protoSchemaFields.map(_.getName))
    schema.fieldNames.foreach(name => assert(schema(name).dataType == protoSchemaToSparkDataTypes(name)))

    // spot check required fields
    assert(row.get(0) == 12345) // id (required)
    assert(row.getString(1) == "johndoe") // username (required)
  }

  it should "handle proto2 repeated fields (lists) correctly" in {
    val userDescriptor = createUserDescriptorProto2()
    val groupBy = makeMetadataOnlyGroupBy()
    val protobufSerdeProvider = new InMemoryProtobufDeserializationSchemaProvider(userDescriptor)
    val deserSchema = DeserializationSchemaBuilder.buildSourceIdentityDeserSchema(protobufSerdeProvider, groupBy)
    deserSchema.open(new DummyInitializationContext)

    val resultList = new util.ArrayList[Row]()
    val listCollector = new ListCollector(resultList)

    val recordBytes = createDummyUserRecordBytesProto2()
    deserSchema.deserialize(recordBytes, listCollector)

    assert(resultList.size() == 1)
    val row = resultList.toScala.head

    // Check tags field (index 2) is a list
    val tags = row.get(2) match {
      case arr: Array[_]           => arr.toSet
      case list: util.ArrayList[_] => list.toScala.toSet
      case seq: Seq[_]             => seq.toSet
      case other                   => fail(s"Unexpected type for tags: ${other.getClass}")
    }
    assert(tags.size == 3)
    assert(tags == Set("active", "premium", "verified"))
  }

  it should "handle proto2 preferences as repeated message (not map)" in {
    val userDescriptor = createUserDescriptorProto2()
    val groupBy = makeMetadataOnlyGroupBy()
    val protobufSerdeProvider = new InMemoryProtobufDeserializationSchemaProvider(userDescriptor)
    val deserSchema = DeserializationSchemaBuilder.buildSourceIdentityDeserSchema(protobufSerdeProvider, groupBy)
    deserSchema.open(new DummyInitializationContext)

    val resultList = new util.ArrayList[Row]()
    val listCollector = new ListCollector(resultList)

    val recordBytes = createDummyUserRecordBytesProto2()
    deserSchema.deserialize(recordBytes, listCollector)

    assert(resultList.size() == 1)
    val row = resultList.toScala.head

    // In proto2, preferences is a repeated PreferenceEntry message (list of structs), not a map
    val preferences = row.get(4) match {
      case arr: Array[_]           => arr.toSeq
      case list: util.ArrayList[_] => list.toScala.toSeq
      case seq: Seq[_]             => seq
      case other                   => fail(s"Unexpected type for preferences: ${other.getClass}")
    }
    assert(preferences.size == 2)

    // Each preference entry is a Row with key/value fields
    val prefMap = preferences.map {
      case r: Row        => r.getString(0) -> r.getString(1)
      case arr: Array[_] => arr(0).toString -> arr(1).toString
      case other         => fail(s"Unexpected preference entry type: ${other.getClass}")
    }.toMap
    assert(prefMap("theme") == "dark")
    assert(prefMap("language") == "en")
  }

  it should "skip proto2 data that can't be deserialized" in {
    val userDescriptor = createUserDescriptorProto2()
    val groupBy = makeMetadataOnlyGroupBy()

    val protobufSerdeProvider = new InMemoryProtobufDeserializationSchemaProvider(userDescriptor)
    val deserSchema = DeserializationSchemaBuilder.buildSourceIdentityDeserSchema(protobufSerdeProvider, groupBy)
    deserSchema.open(new DummyInitializationContext)

    val recordBytes = createDummyUserRecordBytesProto2()
    // corrupt the record bytes
    val corruptedBytes = recordBytes.clone()
    corruptedBytes(0) = 0xff.toByte
    corruptedBytes(1) = 0xff.toByte
    corruptedBytes(2) = 0xff.toByte

    val resultList = new util.ArrayList[Row]()
    val listCollector = new ListCollector(resultList)

    deserSchema.deserialize(corruptedBytes, listCollector)
    assertTrue(resultList.isEmpty)
  }
}
