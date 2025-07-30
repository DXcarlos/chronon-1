package ai.chronon.api.test

import ai.chronon.api.Constants
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.ThriftJsonCodec
import ai.chronon.observability.TileDriftSeries
import ai.chronon.observability.TileSummarySeries
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.lang.{Double => JDouble}
import java.lang.{Long => JLong}

class TileSeriesSerializationTest extends AnyFlatSpec with Matchers {

  "TileDriftSeries" should "serialize with nulls and special values" in {
    val tileDriftSeries = new TileDriftSeries()

    val percentileDrifts: Seq[JDouble] = Seq(0.1, null, Double.PositiveInfinity, Double.NaN, 0.5)
      .map(v =>
        if (v == null || (v != null && (v.asInstanceOf[Double].isInfinite || v.asInstanceOf[Double].isNaN)))
          Constants.magicNullDouble
        else
          v.asInstanceOf[JDouble])

    val percentileDriftsList: java.util.List[JDouble] = percentileDrifts.toJava
    tileDriftSeries.setPercentileDriftSeries(percentileDriftsList)

    val jsonStr = ThriftJsonCodec.toJsonStr(tileDriftSeries)

    jsonStr should be(
      s"""{"percentileDriftSeries":[0.1,${Constants.magicNullDouble},${Constants.magicNullDouble},${Constants.magicNullDouble},0.5]}""")
  }

  it should "deserialize double values correctly" in {
    val json =
      s"""{"percentileDriftSeries":[0.1,${Constants.magicNullDouble},${Constants.magicNullDouble},${Constants.magicNullDouble},0.5]}"""

    val series =
      ThriftJsonCodec.fromJsonStr[TileDriftSeries](json, true, classOf[TileDriftSeries])(manifest[TileDriftSeries])

    val drifts = series.getPercentileDriftSeries.toScala
    drifts.size should be(5)
    drifts(0) should be(0.1)
    drifts(1) should be(Constants.magicNullDouble)
    drifts(2) should be(Constants.magicNullDouble)
    drifts(3) should be(Constants.magicNullDouble)
    drifts(4) should be(0.5)
  }

  "TileSummarySeries" should "serialize with nulls and special long values" in {
    val tileSummarySeries = new TileSummarySeries()

    val counts: Seq[JLong] = Seq(100L, null, Long.MaxValue, Constants.magicNullLong, 500L)
      .map(v => if (v == null) Constants.magicNullLong else v.asInstanceOf[JLong])

    val countsList: java.util.List[JLong] = counts.toJava
    tileSummarySeries.setCount(countsList)

    val jsonStr = ThriftJsonCodec.toJsonStr(tileSummarySeries)

    jsonStr should be(
      s"""{"count":[100,${Constants.magicNullLong},9223372036854775807,${Constants.magicNullLong},500]}""")
  }

  it should "deserialize long values correctly" in {
    val json = s"""{"count":[100,${Constants.magicNullLong},9223372036854775807,${Constants.magicNullLong},500]}"""

    val series = ThriftJsonCodec.fromJsonStr[TileSummarySeries](json, true, classOf[TileSummarySeries])(
      manifest[TileSummarySeries])

    val counts = series.getCount.toScala
    counts.size should be(5)
    counts(0) should be(100L)
    counts(1) should be(Constants.magicNullLong)
    counts(2) should be(Long.MaxValue)
    counts(3) should be(Constants.magicNullLong)
    counts(4) should be(500L)
  }

}
