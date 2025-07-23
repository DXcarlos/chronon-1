package ai.chronon.online.test

import ai.chronon.api.Extensions.WindowOps
import ai.chronon.api.PartitionSpec
import ai.chronon.api.TimeUnit
import ai.chronon.api.Window
import ai.chronon.api.PartitionRange
import ai.chronon.api.PartitionRange.collapseToRange
import ai.chronon.api.PartitionRange.expandDates
import ai.chronon.api.PartitionRange.collapsedPrint
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DataRangeTest extends AnyFlatSpec with Matchers {

  // Assuming you have a PartitionSpec and PartitionRange class defined somewhere
  implicit val partitionSpec: PartitionSpec = new PartitionSpec("ds", "yyyy-MM-dd", new Window(1, TimeUnit.DAYS).millis)

  "collapseToRange" should "collapse consecutive partitions into ranges" in {
    val partitions = List(
      "2020-01-01",
      "2020-01-02",
      "2020-01-03",
      "2020-01-05",
      "2020-01-07",
      "2020-01-08"
    )

    val expectedRanges = Seq(
      PartitionRange("2020-01-01", "2020-01-03"),
      PartitionRange("2020-01-05", "2020-01-05"),
      PartitionRange("2020-01-07", "2020-01-08")
    )

    val result = collapseToRange(partitions)
    println(result)
    result should have length 3
    result should contain theSameElementsInOrderAs expectedRanges
  }

  it should "handle a single partition" in {
    val partitions = List("2020-01-01")
    val expectedRanges = Seq(PartitionRange("2020-01-01", "2020-01-01"))

    val result = collapseToRange(partitions)

    result should have length 1
    result should contain theSameElementsAs expectedRanges
  }

  it should "handle empty input" in {
    val partitions = List.empty[String]

    val result = collapseToRange(partitions)

    result should be(empty)
  }

  it should "handle non-consecutive dates" in {
    val partitions = List("2020-01-01", "2020-01-03", "2020-01-05", "2020-01-07")
    val expectedRanges = Seq(
      PartitionRange("2020-01-01", "2020-01-01"),
      PartitionRange("2020-01-03", "2020-01-03"),
      PartitionRange("2020-01-05", "2020-01-05"),
      PartitionRange("2020-01-07", "2020-01-07")
    )

    val result = collapseToRange(partitions)

    result should have length 4
    result should contain theSameElementsInOrderAs expectedRanges
  }

  "expandDates" should "expand a single range correctly" in {
    val collapsedString = "(2020-01-01 -> 2020-01-03)"
    val expected = Seq("2020-01-01", "2020-01-02", "2020-01-03")

    val result = expandDates(collapsedString)

    result should contain theSameElementsInOrderAs expected
  }

  it should "expand multiple ranges correctly" in {
    val collapsedString = "(2020-01-01 -> 2020-01-03), (2020-01-05 -> 2020-01-05), (2020-01-07 -> 2020-01-08)"
    val expected = Seq("2020-01-01", "2020-01-02", "2020-01-03", "2020-01-05", "2020-01-07", "2020-01-08")

    val result = expandDates(collapsedString)

    result should contain theSameElementsInOrderAs expected
  }

  it should "handle single date ranges" in {
    val collapsedString = "(2020-01-01 -> 2020-01-01), (2020-01-03 -> 2020-01-03)"
    val expected = Seq("2020-01-01", "2020-01-03")

    val result = expandDates(collapsedString)

    result should contain theSameElementsInOrderAs expected
  }

  it should "handle empty input" in {
    val result1 = expandDates("")
    val result2 = expandDates("   ")
    val result3 = expandDates(null)

    result1 should be(empty)
    result2 should be(empty)
    result3 should be(empty)
  }

  it should "handle malformed input gracefully" in {
    val malformedInput1 = "invalid-format"
    val malformedInput2 = "(2020-01-01 -> )"
    val malformedInput3 = "(2020-01-01)"

    val result1 = expandDates(malformedInput1)
    val result2 = expandDates(malformedInput2)
    val result3 = expandDates(malformedInput3)

    result1 should be(empty)
    result2 should be(empty)
    result3 should be(empty)
  }

  it should "handle ranges with extra whitespace" in {
    val collapsedString = "( 2020-01-01  ->  2020-01-03 ), ( 2020-01-05  ->  2020-01-05 )"
    val expected = Seq("2020-01-01", "2020-01-02", "2020-01-03", "2020-01-05")

    val result = expandDates(collapsedString)

    result should contain theSameElementsInOrderAs expected
  }

  "collapsedPrint and expandDates" should "be round-trip compatible" in {
    val originalPartitions = List(
      "2020-01-01",
      "2020-01-02",
      "2020-01-03",
      "2020-01-05",
      "2020-01-07",
      "2020-01-08"
    )

    val collapsedString = collapsedPrint(originalPartitions)
    val expandedPartitions = expandDates(collapsedString)

    expandedPartitions should contain theSameElementsInOrderAs originalPartitions
  }

  it should "handle round-trip with single partition" in {
    val originalPartitions = List("2020-01-01")

    val collapsedString = collapsedPrint(originalPartitions)
    val expandedPartitions = expandDates(collapsedString)

    expandedPartitions should contain theSameElementsInOrderAs originalPartitions
  }

  it should "handle round-trip with empty list" in {
    val originalPartitions = List.empty[String]

    val collapsedString = collapsedPrint(originalPartitions)
    val expandedPartitions = expandDates(collapsedString)

    expandedPartitions should be(empty)
  }

  it should "handle round-trip with non-consecutive dates" in {
    val originalPartitions = List("2020-01-01", "2020-01-03", "2020-01-05", "2020-01-07")

    val collapsedString = collapsedPrint(originalPartitions)
    val expandedPartitions = expandDates(collapsedString)

    expandedPartitions should contain theSameElementsInOrderAs originalPartitions
  }

  it should "handle round-trip with long consecutive ranges" in {
    val originalPartitions = (1 to 10).map(i => f"2020-01-$i%02d").toList

    val collapsedString = collapsedPrint(originalPartitions)
    val expandedPartitions = expandDates(collapsedString)

    expandedPartitions should contain theSameElementsInOrderAs originalPartitions
  }
}
