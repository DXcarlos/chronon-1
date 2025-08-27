package ai.chronon.flink_connectors.pubsub.fastack

import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.Collector
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.{an, convertToAnyShouldWrapper}

import scala.collection.mutable.ListBuffer

class DeserializationSchemaWrapperTest extends AnyFlatSpec with MockitoSugar {

  case class TestData(value: String)

  class TestCollector[T] extends Collector[T] {
    val results: ListBuffer[T] = ListBuffer.empty[T]
    
    override def collect(record: T): Unit = results += record
    override def close(): Unit = {}
  }

  "DeserializationSchemaWrapper" should "delegate open to underlying schema" in {
    val mockSchema = mock[DeserializationSchema[TestData]]
    val wrapper = new DeserializationSchemaWrapper[TestData](mockSchema)
    val mockContext = mock[DeserializationSchema.InitializationContext]

    wrapper.open(mockContext)

    verify(mockSchema).open(mockContext)
  }

  it should "delegate isEndOfStream to underlying schema" in {
    val mockSchema = mock[DeserializationSchema[TestData]]
    val wrapper = new DeserializationSchemaWrapper[TestData](mockSchema)
    val testData = TestData("test")

    when(mockSchema.isEndOfStream(testData)).thenReturn(true)

    val result = wrapper.isEndOfStream(testData)

    result shouldBe true
    verify(mockSchema).isEndOfStream(testData)
  }

  it should "delegate getProducedType to underlying schema" in {
    val mockSchema = mock[DeserializationSchema[TestData]]
    val wrapper = new DeserializationSchemaWrapper[TestData](mockSchema)
    val mockTypeInfo = mock[TypeInformation[TestData]]

    when(mockSchema.getProducedType).thenReturn(mockTypeInfo)

    val result = wrapper.getProducedType

    result shouldBe mockTypeInfo
    verify(mockSchema).getProducedType
  }

  it should "throw UnsupportedOperationException for single deserialize method" in {
    val mockSchema = mock[DeserializationSchema[TestData]]
    val wrapper = new DeserializationSchemaWrapper[TestData](mockSchema)
    val pubsubMessage = PubsubMessage.newBuilder()
      .setData(ByteString.copyFromUtf8("test"))
      .build()

    an[UnsupportedOperationException] should be thrownBy {
      wrapper.deserialize(pubsubMessage)
    }
  }

  it should "deserialize PubsubMessage data using underlying schema" in {
    val mockSchema = mock[DeserializationSchema[TestData]]
    val wrapper = new DeserializationSchemaWrapper[TestData](mockSchema)
    val collector = new TestCollector[TestData]()
    val testBytes = "test data".getBytes
    val pubsubMessage = PubsubMessage.newBuilder()
      .setData(ByteString.copyFrom(testBytes))
      .build()

    wrapper.deserialize(pubsubMessage, collector)

    verify(mockSchema).deserialize(testBytes, collector)
  }

  it should "handle null data in PubsubMessage" in {
    val mockSchema = mock[DeserializationSchema[TestData]]
    val wrapper = new DeserializationSchemaWrapper[TestData](mockSchema)
    val collector = new TestCollector[TestData]()
    val pubsubMessage = PubsubMessage.newBuilder().build()

    wrapper.deserialize(pubsubMessage, collector)

    verify(mockSchema).deserialize(Array.empty[Byte], collector)
  }
}