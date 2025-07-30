package ai.chronon.online.test.stats

import ai.chronon.api.Constants
import ai.chronon.observability.TileDrift
import ai.chronon.observability.TileSummary
import ai.chronon.online.stats.PivotUtils.pivot
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class PivotUtilsTest extends AnyFlatSpec with Matchers {

  "pivot" should "handle empty input" in {
    val result = pivot(Array.empty[(TileSummary, Long)])
    result.getPercentiles shouldBe null
    result.getHistogram shouldBe null
    result.getCount shouldBe null
    result.getTimestamps shouldBe null
  }

  it should "handle single entry" in {
    val ts = new TileSummary()
    ts.setPercentiles(List(1.0, 2.0, 3.0).map(Double.box).asJava)
    ts.setCount(100L)
    ts.setHistogram(Map("A" -> 10L, "B" -> 20L).mapValues(Long.box).toMap.asJava)

    val timestamp = 1234L
    val result = pivot(Array((ts, timestamp)))

    result.getPercentiles
    // Check percentiles
    result.getPercentiles.asScala.map(_.asScala) shouldEqual Array(Array(1.0), Array(2.0), Array(3.0))

    // Check count
    result.getCount.asScala shouldEqual List(100L)

    // Check histogram
    val expectedHistogram = Map(
      "A" -> List(10L).asJava,
      "B" -> List(20L).asJava
    ).asJava
    result.getHistogram.asScala.mapValues(_.asScala.toList).toMap shouldEqual
      expectedHistogram.asScala.mapValues(_.asScala.toList).toMap

    // Check timestamps
    result.getTimestamps.asScala shouldEqual List(timestamp)
  }

  it should "correctly transpose percentiles for multiple entries" in {
    val ts1 = new TileSummary()
    ts1.setPercentiles(List(1.0, 2.0, 3.0).map(Double.box).asJava)

    val ts2 = new TileSummary()
    ts2.setPercentiles(List(4.0, 5.0, 6.0).map(Double.box).asJava)

    val result = pivot(
      Array(
        (ts1, 1000L),
        (ts2, 2000L)
      ))

    // After pivot, we expect:
    // [1.0, 2.0, 3.0]  -->  [[1.0, 4.0],
    // [4.0, 5.0, 6.0]      [2.0, 5.0],
    //                       [3.0, 6.0]]

    val expected = List(
      List(1.0, 4.0).asJava,
      List(2.0, 5.0).asJava,
      List(3.0, 6.0).asJava
    ).asJava

    result.getPercentiles.asScala.map(_.asScala.toList) shouldEqual
      expected.asScala.map(_.asScala.toList)
  }

  it should "handle histogram merging for multiple entries" in {
    val ts1 = new TileSummary()
    ts1.setHistogram(Map("A" -> 10L, "B" -> 20L).mapValues(Long.box).toMap.asJava)

    val ts2 = new TileSummary()
    ts2.setHistogram(Map("B" -> 30L, "C" -> 40L).mapValues(Long.box).toMap.asJava)

    val result = pivot(
      Array(
        (ts1, 1000L),
        (ts2, 2000L)
      ))

    val expectedHistogram = Map(
      "A" -> List(10L, Constants.magicNullLong).asJava,
      "B" -> List(20L, 30L).asJava,
      "C" -> List(Constants.magicNullLong, 40L).asJava
    ).asJava

    result.getHistogram.asScala.mapValues(_.asScala.toList).toMap shouldEqual
      expectedHistogram.asScala.mapValues(_.asScala.toList).toMap
  }

  it should "handle null values in input" in {
    val ts1 = new TileSummary()
    ts1.setCount(100L)

    val ts2 = new TileSummary()
    // count is null

    val ts3 = new TileSummary()
    ts3.setCount(300L)

    val result = pivot(
      Array(
        (ts1, 1000L),
        (ts2, 2000L),
        (ts3, 3000L)
      ))

    result.getCount.asScala.toList shouldEqual List(100L, Constants.magicNullLong, 300L)
  }

  it should "preserve timestamp order" in {
    val ts = new TileSummary()
    ts.setCount(100L)

    // Input timestamps in random order
    val input = Array(
      (ts, 3000L),
      (ts, 1000L),
      (ts, 2000L)
    )

    val result = pivot(input)

    // Should be sorted
    result.getTimestamps.asScala shouldEqual List(1000L, 2000L, 3000L)
    result.getCount.asScala shouldEqual List(100L, 100L, 100L)
  }

  it should "handle length percentiles and string length percentiles" in {
    val ts1 = new TileSummary()
    ts1.setLengthPercentiles(List(1, 2, 3).map(Int.box).asJava)
    ts1.setStringLengthPercentiles(List(10, 20).map(Int.box).asJava)

    val ts2 = new TileSummary()
    ts2.setLengthPercentiles(List(4, 5, 6).map(Int.box).asJava)
    ts2.setStringLengthPercentiles(List(30, 40).map(Int.box).asJava)

    val result = pivot(
      Array(
        (ts1, 1000L),
        (ts2, 2000L)
      ))

    // Check length percentiles transposition
    val expectedLengthPercentiles = List(
      List(1, 4).asJava,
      List(2, 5).asJava,
      List(3, 6).asJava
    ).asJava

    result.getLengthPercentiles.asScala.map(_.asScala.toList) shouldEqual
      expectedLengthPercentiles.asScala.map(_.asScala.toList)

    // Check string length percentiles transposition
    val expectedStringLengthPercentiles = List(
      List(10, 30).asJava,
      List(20, 40).asJava
    ).asJava

    result.getStringLengthPercentiles.asScala.map(_.asScala.toList) shouldEqual
      expectedStringLengthPercentiles.asScala.map(_.asScala.toList)
  }

  it should "handle null values in percentiles lists" in {
    val ts1 = new TileSummary()
    ts1.setPercentiles(List[java.lang.Double](1.0, null, 3.0).map(d => if (d == null) null else Double.box(d)).asJava)

    val ts2 = new TileSummary()
    ts2.setPercentiles(List[java.lang.Double](4.0, 5.0, null).map(d => if (d == null) null else Double.box(d)).asJava)

    val result = pivot(
      Array(
        (ts1, 1000L),
        (ts2, 2000L)
      ))

    // After pivot, we expect nulls to be replaced with magicNullDouble
    val expected = List(
      List(1.0, 4.0).asJava,
      List(Constants.magicNullDouble, 5.0).asJava,
      List(3.0, Constants.magicNullDouble).asJava
    ).asJava

    result.getPercentiles.asScala.map(_.asScala.toList) shouldEqual
      expected.asScala.map(_.asScala.toList)
  }

  "pivot_drift" should "handle empty input" in {
    val result = pivot(Array.empty[(TileDrift, Long)])
    result.getPercentileDriftSeries shouldBe null
    result.getHistogramDriftSeries shouldBe null
    result.getCountChangePercentSeries shouldBe null
    result.getNullRatioChangePercentSeries shouldBe null
    result.getInnerCountChangePercentSeries shouldBe null
    result.getInnerNullCountChangePercentSeries shouldBe null
    result.getLengthPercentilesDriftSeries shouldBe null
    result.getStringLengthPercentilesDriftSeries shouldBe null
    result.getTimestamps shouldBe null
  }

  it should "handle single entry" in {
    val drift = new TileDrift()
    drift.setPercentileDrift(0.5)
    drift.setHistogramDrift(0.3)
    drift.setCountChangePercent(10.0)

    val timestamp = 1234L
    val result = pivot(Array((drift, timestamp)))

    result.getPercentileDriftSeries.asScala shouldEqual List(0.5)
    result.getHistogramDriftSeries.asScala shouldEqual List(0.3)
    result.getCountChangePercentSeries.asScala shouldEqual List(10.0)
    result.getTimestamps.asScala shouldEqual List(timestamp)
  }

  it should "handle multiple entries with all fields set" in {
    val drift1 = new TileDrift()
    drift1.setPercentileDrift(0.5)
    drift1.setHistogramDrift(0.3)
    drift1.setCountChangePercent(10.0)
    drift1.setNullRatioChangePercent(5.0)
    drift1.setInnerCountChangePercent(2.0)
    drift1.setInnerNullCountChangePercent(1.0)
    drift1.setLengthPercentilesDrift(0.2)
    drift1.setStringLengthPercentilesDrift(0.1)

    val drift2 = new TileDrift()
    drift2.setPercentileDrift(0.6)
    drift2.setHistogramDrift(0.4)
    drift2.setCountChangePercent(12.0)
    drift2.setNullRatioChangePercent(6.0)
    drift2.setInnerCountChangePercent(3.0)
    drift2.setInnerNullCountChangePercent(2.0)
    drift2.setLengthPercentilesDrift(0.3)
    drift2.setStringLengthPercentilesDrift(0.2)

    val result = pivot(
      Array(
        (drift1, 1000L),
        (drift2, 2000L)
      ))

    result.getPercentileDriftSeries.asScala shouldEqual List(0.5, 0.6)
    result.getHistogramDriftSeries.asScala shouldEqual List(0.3, 0.4)
    result.getCountChangePercentSeries.asScala shouldEqual List(10.0, 12.0)
    result.getNullRatioChangePercentSeries.asScala shouldEqual List(5.0, 6.0)
    result.getInnerCountChangePercentSeries.asScala shouldEqual List(2.0, 3.0)
    result.getInnerNullCountChangePercentSeries.asScala shouldEqual List(1.0, 2.0)
    result.getLengthPercentilesDriftSeries.asScala shouldEqual List(0.2, 0.3)
    result.getStringLengthPercentilesDriftSeries.asScala shouldEqual List(0.1, 0.2)
    result.getTimestamps.asScala shouldEqual List(1000L, 2000L)
  }

  it should "handle null values in input" in {
    val drift1 = new TileDrift()
    drift1.setPercentileDrift(0.5)
    drift1.setCountChangePercent(10.0)

    val drift2 = new TileDrift()
    // all fields null

    val drift3 = new TileDrift()
    drift3.setPercentileDrift(0.7)
    drift3.setCountChangePercent(30.0)

    val result = pivot(
      Array(
        (drift1, 1000L),
        (drift2, 2000L),
        (drift3, 3000L)
      ))

    result.getPercentileDriftSeries.asScala.map(Option(_).map(_.doubleValue)) shouldEqual
      List(Some(0.5), Some(Constants.magicNullDouble), Some(0.7))

    result.getCountChangePercentSeries.asScala.map(Option(_).map(_.doubleValue)) shouldEqual
      List(Some(10.0), Some(Constants.magicNullDouble), Some(30.0))

    result.getHistogramDriftSeries.asScala shouldBe null // since no values were ever set
  }

  it should "preserve timestamp order" in {
    val drift = new TileDrift()
    drift.setPercentileDrift(0.5)

    // Input timestamps in random order
    val input = Array(
      (drift, 3000L),
      (drift, 1000L),
      (drift, 2000L)
    )

    val result = pivot(input)

    // Should be sorted
    result.getTimestamps.asScala shouldEqual List(1000L, 2000L, 3000L)
    result.getPercentileDriftSeries.asScala shouldEqual List(0.5, 0.5, 0.5)
  }

  it should "return null for series where no values were ever set" in {
    val drift1 = new TileDrift()
    drift1.setPercentileDrift(0.5) // only set percentileDrift

    val drift2 = new TileDrift()
    drift2.setPercentileDrift(0.6) // only set percentileDrift

    val result = pivot(
      Array(
        (drift1, 1000L),
        (drift2, 2000L)
      ))

    result.getPercentileDriftSeries.asScala shouldEqual List(0.5, 0.6)
    result.getHistogramDriftSeries.asScala shouldBe null // never set
    result.getCountChangePercentSeries.asScala shouldBe null // never set
    result.getTimestamps.asScala shouldEqual List(1000L, 2000L)
  }

  it should "handle Double.NaN values" in {
    val drift1 = new TileDrift()
    drift1.setPercentileDrift(Double.NaN)

    val drift2 = new TileDrift()
    drift2.setPercentileDrift(0.5)

    val result = pivot(
      Array(
        (drift1, 1000L),
        (drift2, 2000L)
      ))

    val series = result.getPercentileDriftSeries.asScala.toList
    series.size shouldBe 2
    series(0) shouldBe Constants.magicNullDouble
    series(1) shouldBe 0.5
  }

  it should "handle Long.MAX_VALUE and magicNullLong values" in {
    val ts1 = new TileSummary()
    ts1.setCount(Long.MaxValue)

    val ts2 = new TileSummary()
    // count is not set, should become magicNullLong

    val ts3 = new TileSummary()
    ts3.setCount(100L)

    val result = pivot(
      Array(
        (ts1, 1000L),
        (ts2, 2000L),
        (ts3, 3000L)
      ))

    result.getCount.asScala shouldEqual List(Long.MaxValue, Constants.magicNullLong, 100L)
  }

  it should "handle all null Long values" in {
    val ts1 = new TileSummary()
    val ts2 = new TileSummary()
    val ts3 = new TileSummary()
    // no counts set for any summary

    val result = pivot(
      Array(
        (ts1, 1000L),
        (ts2, 2000L),
        (ts3, 3000L)
      ))

    // Since all values are unset, they should all be magicNullLong rather than null
    result.getCount.asScala.toList shouldEqual List.fill(3)(Constants.magicNullLong)
  }

  it should "handle mixed null and non-null Long fields" in {
    val ts1 = new TileSummary()
    ts1.setCount(100L)
    ts1.setNullCount(10L)

    val ts2 = new TileSummary()
    // count not set
    ts2.setNullCount(20L)

    val ts3 = new TileSummary()
    ts3.setCount(300L)
    // nullCount not set

    val result = pivot(
      Array(
        (ts1, 1000L),
        (ts2, 2000L),
        (ts3, 3000L)
      ))

    result.getCount.asScala shouldEqual List(100L, Constants.magicNullLong, 300L)
    result.getNullCount.asScala shouldEqual List(10L, 20L, Constants.magicNullLong)
  }
}
