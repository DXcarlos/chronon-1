package ai.chronon.online.test

import ai.chronon.api._
import ai.chronon.online.serde.{AvroCodec, AvroConversions}
import org.apache.avro.generic.GenericData
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util

class AvroConversionsListTest extends AnyFlatSpec with Matchers {

  "AvroConversions" should "handle float list correctly" in {
    // Create a schema with a list of floats
    val schema = StructType(
      "TestSchema",
      Array(
        StructField("floatList", ListType(FloatType))
      )
    )
    
    val avroSchema = AvroConversions.fromChrononSchema(schema)
    val codec = new AvroCodec(avroSchema.toString)
    
    // Create test data with floats
    val floatList = new util.ArrayList[Any]()
    floatList.add(1.5f)
    floatList.add(2.5f)
    floatList.add(3.5f)
    
    val testData = Map("floatList" -> floatList.asInstanceOf[AnyRef])
    
    // Convert to Avro record
    val avroRecord = AvroConversions.fromChrononRow(
      schema.castArr(testData), 
      schema, 
      codec.schema
    ).asInstanceOf[GenericData.Record]
    
    // Encode to binary
    val encoded = codec.encodeBinary(avroRecord)
    
    // Decode back - this might return BufferBackedPrimitiveFloatList
    // Convert back to Chronon row - this is where the error would occur without our fix
    val chrononRow = codec.decodeRow(encoded)
    
    // Verify the data
    chrononRow should not be null
    val resultList = chrononRow(0).asInstanceOf[util.ArrayList[Any]]
    resultList.size() shouldBe 3
    resultList.get(0).asInstanceOf[Float] shouldBe 1.5f +- 0.001f
    resultList.get(1).asInstanceOf[Float] shouldBe 2.5f +- 0.001f
    resultList.get(2).asInstanceOf[Float] shouldBe 3.5f +- 0.001f
  }

  it should "handle mixed list types correctly" in {
    // Create a schema with multiple list types  
    // Note: Only float lists have BufferBacked implementation
    val schema = StructType(
      "MixedListSchema",
      Array(
        StructField("intList", ListType(IntType)),
        StructField("longList", ListType(LongType)),
        StructField("doubleList", ListType(DoubleType)),
        StructField("floatList", ListType(FloatType)),
        StructField("boolList", ListType(BooleanType))
      )
    )
    
    val avroSchema = AvroConversions.fromChrononSchema(schema)
    val codec = new AvroCodec(avroSchema.toString)
    
    // Create test data
    val intList = new util.ArrayList[Any]()
    intList.add(1)
    intList.add(2)
    
    val longList = new util.ArrayList[Any]()
    longList.add(100L)
    longList.add(200L)
    
    val doubleList = new util.ArrayList[Any]()
    doubleList.add(1.1)
    doubleList.add(2.2)
    
    val floatList = new util.ArrayList[Any]()
    floatList.add(1.5f)
    floatList.add(2.5f)
    
    val boolList = new util.ArrayList[Any]()
    boolList.add(true)
    boolList.add(false)
    
    val testData = Map(
      "intList" -> intList.asInstanceOf[AnyRef],
      "longList" -> longList.asInstanceOf[AnyRef],
      "doubleList" -> doubleList.asInstanceOf[AnyRef],
      "floatList" -> floatList.asInstanceOf[AnyRef],
      "boolList" -> boolList.asInstanceOf[AnyRef]
    )
    
    // Convert to Avro and back
    val avroRecord = AvroConversions.fromChrononRow(
      schema.castArr(testData), 
      schema, 
      codec.schema
    ).asInstanceOf[GenericData.Record]
    
    val encoded = codec.encodeBinary(avroRecord)
    val chrononRow = codec.decodeRow(encoded)
    
    // Verify all lists
    chrononRow should not be null
    
    val resultIntList = chrononRow(0).asInstanceOf[util.ArrayList[Any]]
    resultIntList.size() shouldBe 2
    resultIntList.get(0) shouldBe 1
    
    val resultLongList = chrononRow(1).asInstanceOf[util.ArrayList[Any]]
    resultLongList.size() shouldBe 2
    resultLongList.get(0) shouldBe 100L
    
    val resultDoubleList = chrononRow(2).asInstanceOf[util.ArrayList[Any]]
    resultDoubleList.size() shouldBe 2
    resultDoubleList.get(0).asInstanceOf[Double] shouldBe 1.1 +- 0.001
    
    val resultFloatList = chrononRow(3).asInstanceOf[util.ArrayList[Any]]
    resultFloatList.size() shouldBe 2
    resultFloatList.get(0).asInstanceOf[Float] shouldBe 1.5f +- 0.001f
    
    val resultBoolList = chrononRow(4).asInstanceOf[util.ArrayList[Any]]
    resultBoolList.size() shouldBe 2
    resultBoolList.get(0) shouldBe true
  }

  it should "handle empty lists correctly" in {
    val schema = StructType(
      "EmptyListSchema",
      Array(
        StructField("emptyFloatList", ListType(FloatType))
      )
    )
    
    val avroSchema = AvroConversions.fromChrononSchema(schema)
    val codec = new AvroCodec(avroSchema.toString)
    
    // Create empty list
    val emptyList = new util.ArrayList[Any]()
    val testData = Map("emptyFloatList" -> emptyList.asInstanceOf[AnyRef])
    
    // Convert to Avro and back
    val avroRecord = AvroConversions.fromChrononRow(
      schema.castArr(testData), 
      schema, 
      codec.schema
    ).asInstanceOf[GenericData.Record]
    
    val encoded = codec.encodeBinary(avroRecord)
    val chrononRow = codec.decodeRow(encoded)
    
    // Verify empty list
    chrononRow should not be null
    val resultList = chrononRow(0).asInstanceOf[util.ArrayList[Any]]
    resultList.size() shouldBe 0
  }

  it should "handle null lists correctly" in {
    val schema = StructType(
      "NullListSchema",
      Array(
        StructField("nullableFloatList", ListType(FloatType))
      )
    )
    
    val avroSchema = AvroConversions.fromChrononSchema(schema)
    val codec = new AvroCodec(avroSchema.toString)
    
    // Create null list
    val testData = Map("nullableFloatList" -> null)
    
    // Convert to Avro and back
    val avroRecord = AvroConversions.fromChrononRow(
      schema.castArr(testData), 
      schema, 
      codec.schema
    ).asInstanceOf[GenericData.Record]
    
    val encoded = codec.encodeBinary(avroRecord)
    val chrononRow = codec.decodeRow(encoded)
    
    // Verify null
    chrononRow should not be null
    Option(chrononRow(0)) shouldBe None
  }
}