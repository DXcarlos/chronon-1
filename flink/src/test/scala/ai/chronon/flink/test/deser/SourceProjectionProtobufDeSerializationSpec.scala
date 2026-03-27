package ai.chronon.flink.test.deser

import ai.chronon.api.ScalaJavaConversions.ListOps
import ai.chronon.flink.deser.{DeserializationSchemaBuilder, ProjectedEvent, SourceProjectionDeserializationSchema}
import ai.chronon.online.serde.SparkConversions
import org.apache.flink.api.common.functions.util.ListCollector
import org.scalatest.flatspec.AnyFlatSpec

import java.util

class SourceProjectionProtobufDeSerializationSpec extends AnyFlatSpec {
  import ProtobufObjectCreator._

  it should "project and let through protobuf data" in {
    val userDescriptor = createUserDescriptor()
    val groupBy =
      makeGroupBy(
        Map("id" -> "id", "username" -> "username", "is_active" -> "is_active", "ts" -> "last_login_timestamp"),
        Seq("id == 12345", "is_active == true")
      )

    val resultList = new util.ArrayList[ProjectedEvent]()
    val listCollector = new ListCollector(resultList)

    val protobufSerdeProvider = new InMemoryProtobufDeserializationSchemaProvider(userDescriptor)
    val deserSchema = DeserializationSchemaBuilder.buildSourceProjectionDeserSchema(protobufSerdeProvider, groupBy)
    deserSchema.open(new DummyInitializationContext)

    val recordBytes = createDummyUserRecordBytes()
    deserSchema.deserialize(recordBytes, listCollector)

    // sanity check projected schemas is what we expect
    val projectedSchema = deserSchema.asInstanceOf[SourceProjectionDeserializationSchema].projectedSchema
    assert(projectedSchema.map(_._1).toSet == Set("id", "username", "is_active", "ts"))

    // now check the types of projected data matching up with types in source schemas
    val projectedSparkSchema = SparkConversions.fromChrononSchema(projectedSchema)
    val schema = deserSchema.sourceEventEncoder.schema

    // check ts out of band as the field name changes
    assert(projectedSparkSchema("ts").dataType == schema("last_login_timestamp").dataType)
    // check other fields
    val fieldsToCheck = Set("id", "username", "is_active")
    fieldsToCheck.foreach { name =>
      val sourceField = schema(name)
      val projectedField = projectedSparkSchema(name)
      assert(sourceField.dataType == projectedField.dataType, s"Field $name has different types")
    }

    // sanity check result data
    assert(resultList.size() == 1)
    val projectedResult = resultList.toScala.map(_.fields).head
    assert(projectedResult.nonEmpty)
    assert(projectedResult("id") == 12345)
  }

  it should "project and filter protobuf data" in {
    val userDescriptor = createUserDescriptor()
    val groupBy =
      makeGroupBy(
        Map("id" -> "id", "username" -> "username", "is_active" -> "is_active"),
        Seq("id == 45678", "is_active == true") // Filter that won't match our test data
      )
    val protobufSerdeProvider = new InMemoryProtobufDeserializationSchemaProvider(userDescriptor)
    val deserSchema = DeserializationSchemaBuilder.buildSourceProjectionDeserSchema(protobufSerdeProvider, groupBy)
    deserSchema.open(new DummyInitializationContext)

    val recordBytes = createDummyUserRecordBytes() // Has id=12345, not 45678

    val resultList = new util.ArrayList[ProjectedEvent]()
    val listCollector = new ListCollector(resultList)
    deserSchema.deserialize(recordBytes, listCollector)

    // sanity check result data - should be filtered out
    assert(resultList.isEmpty)
  }

  it should "skip protobuf data that can't be deserialized" in {
    val userDescriptor = createUserDescriptor()
    val groupBy =
      makeGroupBy(
        Map("id" -> "id", "username" -> "username", "is_active" -> "is_active"),
        Seq("id == 12345", "is_active == true")
      )

    val protobufSerdeProvider = new InMemoryProtobufDeserializationSchemaProvider(userDescriptor)
    val deserSchema = DeserializationSchemaBuilder.buildSourceProjectionDeserSchema(protobufSerdeProvider, groupBy)
    deserSchema.open(new DummyInitializationContext)

    val recordBytes = createDummyUserRecordBytes()

    // corrupt the record bytes
    val corruptedBytes = recordBytes.clone()
    corruptedBytes(0) = 0xff.toByte
    corruptedBytes(1) = 0xff.toByte

    val resultList = new util.ArrayList[ProjectedEvent]()
    val listCollector = new ListCollector(resultList)

    deserSchema.deserialize(corruptedBytes, listCollector)
    assert(resultList.isEmpty, "deserialize should skip corrupted protobuf bytes and produce no ProjectedEvent")
  }

  // ============== Proto2 Tests ==============

  it should "project and let through proto2 data" in {
    val userDescriptor = createUserDescriptorProto2()
    val groupBy =
      makeGroupByProto2(
        Map("id" -> "id", "username" -> "username", "is_active" -> "is_active", "ts" -> "last_login_timestamp"),
        Seq("id == 12345", "is_active == true")
      )

    val resultList = new util.ArrayList[ProjectedEvent]()
    val listCollector = new ListCollector(resultList)

    val protobufSerdeProvider = new InMemoryProtobufDeserializationSchemaProvider(userDescriptor)
    val deserSchema = DeserializationSchemaBuilder.buildSourceProjectionDeserSchema(protobufSerdeProvider, groupBy)
    deserSchema.open(new DummyInitializationContext)

    val recordBytes = createDummyUserRecordBytesProto2()
    deserSchema.deserialize(recordBytes, listCollector)

    // sanity check projected schemas is what we expect
    val projectedSchema = deserSchema.asInstanceOf[SourceProjectionDeserializationSchema].projectedSchema
    assert(projectedSchema.map(_._1).toSet == Set("id", "username", "is_active", "ts"))

    // now check the types of projected data matching up with types in source schemas
    val projectedSparkSchema = SparkConversions.fromChrononSchema(projectedSchema)
    val schema = deserSchema.sourceEventEncoder.schema

    // check ts out of band as the field name changes
    assert(projectedSparkSchema("ts").dataType == schema("last_login_timestamp").dataType)
    // check other fields
    val fieldsToCheck = Set("id", "username", "is_active")
    fieldsToCheck.foreach { name =>
      val sourceField = schema(name)
      val projectedField = projectedSparkSchema(name)
      assert(sourceField.dataType == projectedField.dataType, s"Field $name has different types")
    }

    // sanity check result data
    assert(resultList.size() == 1)
    val projectedResult = resultList.toScala.map(_.fields).head
    assert(projectedResult.nonEmpty)
    assert(projectedResult("id") == 12345)
  }

  it should "project and filter proto2 data" in {
    val userDescriptor = createUserDescriptorProto2()
    val groupBy =
      makeGroupByProto2(
        Map("id" -> "id", "username" -> "username", "is_active" -> "is_active"),
        Seq("id == 45678", "is_active == true") // Filter that won't match our test data
      )
    val protobufSerdeProvider = new InMemoryProtobufDeserializationSchemaProvider(userDescriptor)
    val deserSchema = DeserializationSchemaBuilder.buildSourceProjectionDeserSchema(protobufSerdeProvider, groupBy)
    deserSchema.open(new DummyInitializationContext)

    val recordBytes = createDummyUserRecordBytesProto2() // Has id=12345, not 45678

    val resultList = new util.ArrayList[ProjectedEvent]()
    val listCollector = new ListCollector(resultList)
    deserSchema.deserialize(recordBytes, listCollector)

    // sanity check result data - should be filtered out
    assert(resultList.isEmpty)
  }

  it should "skip proto2 data that can't be deserialized" in {
    val userDescriptor = createUserDescriptorProto2()
    val groupBy =
      makeGroupByProto2(
        Map("id" -> "id", "username" -> "username", "is_active" -> "is_active"),
        Seq("id == 12345", "is_active == true")
      )

    val protobufSerdeProvider = new InMemoryProtobufDeserializationSchemaProvider(userDescriptor)
    val deserSchema = DeserializationSchemaBuilder.buildSourceProjectionDeserSchema(protobufSerdeProvider, groupBy)
    deserSchema.open(new DummyInitializationContext)

    val recordBytes = createDummyUserRecordBytesProto2()

    // corrupt the record bytes
    val corruptedBytes = recordBytes.clone()
    corruptedBytes(0) = 0xff.toByte
    corruptedBytes(1) = 0xff.toByte

    val resultList = new util.ArrayList[ProjectedEvent]()
    val listCollector = new ListCollector(resultList)

    deserSchema.deserialize(corruptedBytes, listCollector)
    assert(resultList.isEmpty, "deserialize should skip corrupted proto2 bytes and produce no ProjectedEvent")
  }
}
