package ai.chronon.spark.join

import ai.chronon.api.Row
import ai.chronon.online.serde.{RowWrapper, SparkConversions}
import ai.chronon.spark.GenericRowHandler
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.{Row => SparkRow}

import java.util
import scala.collection.mutable

object SawtoothUdf {

  // scala's collection.groupBy assumes unsorted input
  // if we know that the collection is sorted, we can be a lot more optimal
  def sortedArrayGroupBy[T, K, V](arr: Seq[T],
                                  keyFunc: T => K,
                                  valueFunc: T => V): mutable.Buffer[(K, mutable.Buffer[V])] = {
    val result = mutable.ArrayBuffer.empty[(K, mutable.Buffer[V])]

    val it = arr.iterator

    while (it.hasNext) {

      val element = it.next()
      val key = keyFunc(element)
      val value = valueFunc(element)

      if (result.isEmpty || key != result.last._1) {
        result.append(key -> mutable.ArrayBuffer(value))
      } else {
        result.last._2.append(value)
      }

    }

    result
  }

  // create two rows - left coming in from spark, and aggregatedRight coming from chronon aggregation library
  // and creates a GenericRow efficiently - without creating any intermediate data structures
  def concatenate(leftData: RowWrapper, aggregatedRightData: Array[Any], aggregators: AggregationInfo): CGenericRow = {

    val rightData = SparkConversions
      .toSparkRow(aggregatedRightData, aggregators.aggregateChrononSchema, GenericRowHandler.func)
      .asInstanceOf[GenericRow]

    val outputLength = leftData.length + rightData.length
    val resultArray = Array.fill[Any](outputLength)(null)

    var leftIdx = 0
    while (leftIdx < leftData.length) {
      resultArray.update(leftIdx, leftData.get(leftIdx))
      leftIdx += 1
    }

    var rightIdx = 0
    while (rightIdx < rightData.length) {
      resultArray.update(leftIdx + rightIdx, rightData.get(rightIdx))
      rightIdx += 1
    }

    new CGenericRow(resultArray)

  }

  // takes a list of time-sorted rows ("ts" is a column in all rows)
  // and aggregators - constructed from GroupBy
  // and performs point-in-time aggregation
  //   - where the point-in-time is based on the left rows
  //   - and the aggregation is performed over right rows
  //
  // for example - when left represents search requests with query and timestamp
  //             - and right represents item-click events with item attributes like item prices
  //             - and GroupBy definition is aggregating multiple things simultaneously
  //                         - average item prices viewed in the last two days
  //                         - list of 20 most recently viewed item ids in the last week
  //             - this aggregation will compute
  //                         - these features "as of" the timestamp in the search event
  //                         - by filtering events from the right side: right.ts in [left.ts - window, left.ts)
  //                         - un-windowed aggregations are also supported
  def sawtoothAggregate(
      aggregators: AggregationInfo)(leftRows: Seq[SparkRow], rightRows: Seq[SparkRow]): Iterator[CGenericRow] = {

    val hopsAggregator = aggregators.hopsAggregator
    val sawtoothAggregator = aggregators.sawtoothAggregator

    // STEP-1. create hops aggregates
    var hopMaps = hopsAggregator.init()
    var rightIdx = 0
    while (rightIdx < rightRows.size) {
      hopMaps = hopsAggregator.update(hopMaps, aggregators.rightRowWrapper(rightRows(rightIdx)))
      rightIdx += 1
    }
    val hops = hopsAggregator.toTimeSortedArray(hopMaps)

    // STEP-2. group events and queries by headStart - 5minute round down of ts
    val leftByHeadStart: mutable.Buffer[(Long, mutable.Buffer[RowWrapper])] =
      sortedArrayGroupBy(
        leftRows,
        aggregators.leftBucketer,
        aggregators.leftRowWrapper
      )

    val rightByHeadStart: Map[Long, mutable.Buffer[RowWrapper]] =
      sortedArrayGroupBy(
        rightRows,
        aggregators.rightBucketer,
        aggregators.rightRowWrapper
      ).toMap

    // STEP-3. compute windows based on headStart
    // aggregates will have at-most 5 minute staleness
    val headStartTimes = leftByHeadStart.iterator.map(_._1).toArray

    // compute windows up-to 5min accuracy for the queries
    val nonRealtimeIrs = sawtoothAggregator.computeWindowsIterator(hops, headStartTimes)

    headStartTimes.indices.iterator.zip(nonRealtimeIrs).flatMap { case (i, tailIr) =>
      val headStart = headStartTimes(i)

      // join events and queries on tailEndTimes
      val endTimes: mutable.Buffer[RowWrapper] = leftByHeadStart(i)._2

      val rightHeadEvents: mutable.Buffer[Row] =
        rightByHeadStart.get(headStart).map(_.asInstanceOf[mutable.Buffer[Row]]).orNull

      sawtoothAggregator
        .cumulateAndFinalizeSortedIterator(
          rightHeadEvents,
          endTimes.asInstanceOf[mutable.Buffer[Row]],
          tailIr
        )
        .map { case (row, aggregatedData) =>
          concatenate(row.asInstanceOf[RowWrapper], aggregatedData, aggregators)
        }
    }
  }

}
