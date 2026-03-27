package ai.chronon.online.test

import ai.chronon.api._
import ai.chronon.online.serde.{AvroCodec, AvroConversions}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import com.linkedin.avro.fastserde.FastGenericDatumReader
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LargeSchemaFastSerdeTest extends AnyFlatSpec with Matchers {

  "FastGenericDatumReader" should "handle schemas with many distinct record-typed fields" in {
    // Reproduces JVM "code too large" error in fastserde constructor when
    // a schema has many fields, each with a unique record type in a union.
    // Each unique record type creates a separate schema var in the generated
    // constructor, which can exceed the 64KB JVM method bytecode limit.
    val n = 1000
    val subFieldCount = 5

    val chrononFields = (0 until n).map { i =>
      val innerFields = (0 until subFieldCount).map { j =>
        StructField(s"sub_$j", StringType)
      }.toArray
      StructField(s"field_$i", StructType(s"InnerRecord_$i", innerFields))
    }.toArray

    val outerSchema = StructType("LargeTestSchema", chrononFields)
    val avroSchema = AvroConversions.fromChrononSchema(outerSchema)

    // This is the line that fails without the constructor splitting fix —
    // fastserde generates a Java class whose constructor exceeds 64KB.
    val reader = new FastGenericDatumReader[GenericData.Record](avroSchema)

    // Verify round-trip through AvroCodec (which uses FastGenericDatumReader/Writer)
    val codec = new AvroCodec(avroSchema.toString)
    val record = new GenericData.Record(avroSchema)
    (0 until n).foreach { i =>
      val innerSchema = avroSchema.getField(s"field_$i").schema().getTypes.get(1)
      val inner = new GenericData.Record(innerSchema)
      (0 until subFieldCount).foreach { j =>
        inner.put(s"sub_$j", s"val_${i}_$j")
      }
      record.put(s"field_$i", inner)
    }

    val encoded = codec.encodeBinary(record)
    val decoded = codec.decode(encoded)

    val inner0 = decoded.get("field_0").asInstanceOf[GenericData.Record]
    inner0.get("sub_0").toString shouldBe "val_0_0"
    val inner999 = decoded.get("field_999").asInstanceOf[GenericData.Record]
    inner999.get("sub_4").toString shouldBe "val_999_4"
  }
}
