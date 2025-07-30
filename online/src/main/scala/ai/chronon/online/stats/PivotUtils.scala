package ai.chronon.online.stats

import ai.chronon.api.Constants
import ai.chronon.observability.TileDrift
import ai.chronon.observability.TileDriftSeries
import ai.chronon.observability.TileSummary
import ai.chronon.observability.TileSummarySeries

import java.lang.{Double => JDouble}
import java.lang.{Long => JLong}
import java.util.{ArrayList => JArrayList}
import java.util.{HashMap => JHashMap}
import java.util.{List => JList}
import java.util.{Map => JMap}
import ai.chronon.api.ScalaJavaConversions._

// class to convert array of structs with numeric data, to single struct with arrays of numeric data
// currently supports TileSummary => TileSummarySeries
// TODO: TileDrift => TileDriftSeries
object PivotUtils {

  private def collect[T](vals: Iterator[T]): JList[T] = {
    val result = new JArrayList[T]()

    while (vals.hasNext) {
      result.add(vals.next())
    }

    result
  }

  private def transpose[T <: AnyRef](lists: Array[JList[T]]): JList[JList[T]] = {
    if (lists == null || lists.isEmpty || lists.head == null) return null

    val percentileLength = lists.head.size
    val seriesLength = lists.length

    val result = new JArrayList[JList[T]](percentileLength)
    var pctIndex = 0

    while (pctIndex < percentileLength) {
      val row = new JArrayList[T](seriesLength)
      // extract percentile for each of summary into single list
      var seriesIndex = 0
      while (seriesIndex < seriesLength) {
        val list = lists(seriesIndex)
        val value: T = if (list == null) {
          Constants.magicNullDouble.asInstanceOf[T]
        } else {
          val v = list.get(pctIndex)
          if (v == null || (v.isInstanceOf[Double] && v.asInstanceOf[Double].isNaN)) {
            Constants.magicNullDouble.asInstanceOf[T]
          } else {
            v
          }
        }
        row.add(value)
        seriesIndex += 1
      }
      result.add(row)
      pctIndex += 1
    }

    result
  }

  private def merge[T](maps: Array[JMap[String, T]]): JHashMap[String, JList[T]] = {
    if (maps == null || maps.isEmpty) return null
    val result = new JHashMap[String, JList[T]]()

    val allKeys = maps.iterator
      .flatMap(m => if (m == null) Iterator.empty else m.keySet().iterator().toScala)
      .toSet

    // entire set of keys
    allKeys.foreach { key =>
      val values = maps.iterator.map { m =>
        if (m == null || m.get(key) == null)
          Constants.magicNullLong.asInstanceOf[T]
        else
          m.get(key)
      }
      val list = new JArrayList[T](maps.length)
      values.foreach { list.add }
      result.put(key, list)
    }

    result
  }

  // for chaining functions
  implicit class PipeOps[A](val value: A) extends AnyVal {
    def |>[B](f: A => B): B = f(value)
  }

  def pivot(summariesWithTimestamps: Array[(TileSummary, Long)]): TileSummarySeries = {
    if (summariesWithTimestamps == null || summariesWithTimestamps.isEmpty) {
      return new TileSummarySeries()
    }

    // Sort by timestamp to ensure consistent ordering
    val sorted = summariesWithTimestamps.sortBy(_._2)
    val (summaries, timestamps) = sorted.unzip

    def iterator[T](func: TileSummary => T): Iterator[T] = summaries.iterator.map(func)

    def longIterator(isSetFunc: TileSummary => Boolean, extract: TileSummary => Long): Iterator[JLong] = {
      summaries.iterator.map { summary =>
        if (isSetFunc(summary)) {
          JLong.valueOf(extract(summary))
        } else {
          Constants.magicNullLong
        }
      }
    }

    new TileSummarySeries()
      .setPercentiles(iterator(_.getPercentiles).toArray |> transpose)
      .setHistogram(iterator(_.getHistogram).toArray |> merge)
      .setCount(longIterator(_.isSetCount, _.getCount) |> collect)
      .setNullCount(longIterator(_.isSetNullCount, _.getNullCount) |> collect)
      .setInnerCount(longIterator(_.isSetInnerCount, _.getInnerCount) |> collect)
      .setInnerNullCount(longIterator(_.isSetInnerNullCount, _.getInnerNullCount) |> collect)
      .setLengthPercentiles(iterator(_.getLengthPercentiles).toArray |> transpose)
      .setStringLengthPercentiles(iterator(_.getStringLengthPercentiles).toArray |> transpose)
      .setTimestamps(timestamps.iterator.map(JLong.valueOf) |> collect)
  }

  private def collectDoubles(vals: Iterator[JDouble]): JList[JDouble] = {
    val result = new JArrayList[JDouble]()

    var sawValidInput = false

    while (vals.hasNext) {

      val next = vals.next()

      // check if this is valid input - if no prior inputs are valid
      val thisIsValid = next != Constants.magicNullDouble && next != null
      sawValidInput = sawValidInput || thisIsValid

      result.add(next)
    }

    // if no valid input, return null
    if (!sawValidInput) null else result
  }

  def pivot(driftsWithTimestamps: Array[(TileDrift, Long)]): TileDriftSeries = {
    if (driftsWithTimestamps.isEmpty) {
      return new TileDriftSeries()
    }

    // Sort by timestamp to ensure consistent ordering
    val sorted = driftsWithTimestamps.sortBy(_._2)
    val (drifts, timestamps) = sorted.unzip

    def doubleIterator(isSetFunc: TileDrift => Boolean, extract: TileDrift => Double): Iterator[JDouble] = {
      drifts.iterator.map { drift =>
        if (isSetFunc(drift)) {
          val value = extract(drift)
          if (value.isInfinite || value.isNaN) {
            Constants.magicNullDouble
          } else {
            JDouble.valueOf(value)
          }
        } else {
          Constants.magicNullDouble
        }
      }
    }

    new TileDriftSeries()
      .setPercentileDriftSeries(doubleIterator(_.isSetPercentileDrift, _.getPercentileDrift) |> collectDoubles)
      .setHistogramDriftSeries(doubleIterator(_.isSetHistogramDrift, _.getHistogramDrift) |> collectDoubles)
      .setCountChangePercentSeries(doubleIterator(_.isSetCountChangePercent, _.getCountChangePercent) |> collectDoubles)
      .setNullRatioChangePercentSeries(doubleIterator(_.isSetNullRatioChangePercent,
                                                      _.getNullRatioChangePercent) |> collectDoubles)
      .setInnerCountChangePercentSeries(doubleIterator(_.isSetInnerCountChangePercent,
                                                       _.getInnerCountChangePercent) |> collectDoubles)
      .setInnerNullCountChangePercentSeries(doubleIterator(_.isSetInnerNullCountChangePercent,
                                                           _.getInnerNullCountChangePercent) |> collectDoubles)
      .setLengthPercentilesDriftSeries(doubleIterator(_.isSetLengthPercentilesDrift,
                                                      _.getLengthPercentilesDrift) |> collectDoubles)
      .setStringLengthPercentilesDriftSeries(doubleIterator(_.isSetStringLengthPercentilesDrift,
                                                            _.getStringLengthPercentilesDrift) |> collectDoubles)
      .setTimestamps(timestamps.iterator.map(JLong.valueOf) |> collect)
  }
}
