package ai.chronon.online.test

import ai.chronon.api.{PartitionSpec, TimeUnit, Window}
import ai.chronon.api.Extensions.WindowOps
import ai.chronon.online.KvPartitions
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class KvPartitionsTest extends AnyFlatSpec with Matchers {

  implicit val partitionSpec: PartitionSpec =
    new PartitionSpec("ds", "yyyy-MM-dd", new Window(1, TimeUnit.DAYS).millis)

  "KvPartitions" should "round-trip serialize and deserialize with semantic hash" in {
    val original = KvPartitions(
      partitions = Seq("2020-01-01", "2020-01-02", "2020-01-03"),
      timestamp = 12345L,
      semanticHash = Some("abc123")
    )

    val serialized = original.serialize
    val deserialized = KvPartitions.deserialize(serialized, original.timestamp)

    deserialized shouldBe defined
    deserialized.get.partitions should contain theSameElementsInOrderAs original.partitions
    deserialized.get.timestamp shouldBe original.timestamp
    deserialized.get.semanticHash shouldBe Some("abc123")
  }

  it should "round-trip serialize and deserialize without semantic hash" in {
    val original = KvPartitions(
      partitions = Seq("2020-01-01", "2020-01-05"),
      timestamp = 99999L,
      semanticHash = None
    )

    val serialized = original.serialize
    val deserialized = KvPartitions.deserialize(serialized, original.timestamp)

    deserialized shouldBe defined
    deserialized.get.partitions should contain theSameElementsInOrderAs original.partitions
    deserialized.get.semanticHash shouldBe None
  }

  it should "deserialize legacy format (collapsed partitions string without JSON wrapper)" in {
    val legacyPayload = "(2020-01-01 -> 2020-01-03)"
    val timestamp = 55555L

    val deserialized = KvPartitions.deserialize(legacyPayload, timestamp)

    deserialized shouldBe defined
    deserialized.get.partitions should contain theSameElementsInOrderAs Seq("2020-01-01", "2020-01-02", "2020-01-03")
    deserialized.get.timestamp shouldBe timestamp
    deserialized.get.semanticHash shouldBe None
  }

  it should "return empty partitions for unparseable legacy payload" in {
    // expandDates returns empty for malformed input, so deserialize succeeds with empty partitions
    val invalidPayload = "not valid at all {"

    val deserialized = KvPartitions.deserialize(invalidPayload, 12345L)

    deserialized shouldBe defined
    deserialized.get.partitions shouldBe empty
  }
}
