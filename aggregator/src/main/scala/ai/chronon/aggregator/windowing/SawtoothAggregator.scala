/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.aggregator.windowing

import ai.chronon.aggregator.row.{ColumnAggregator, RowAggregator}
import ai.chronon.api.{Aggregation, AggregationPart, DataType, Row, TsUtils}
import ai.chronon.api.Extensions.UnpackedAggregations
import ai.chronon.api.Extensions.WindowMapping
import ai.chronon.api.Extensions.WindowOps

import java.util
import scala.collection.mutable

// Head Sliding, Tail Hopping Window - effective window size when plotted against query timestamp
// will look the edge of sawtooth - instead of like a straight line.
//
// There are three major steps in the strategy for realtime accuracy
// 1. Roll up raw events into hops - using HopsAggregator - see buildHopsAggregator
//      Output data will look like `key -> [[IR_hop1], [IR_hop2], [IR_hop3] ... ]`
// 2. Use the hops to construct windows - see `computeWindows`.
//       We consume the hops and construct the full window of IR - but without head accuracy
//       Output data will look like `(key, hopStart) -> IR`
//    At his point resolution of head of the window is the smallest hop - 5mins
// 3. To make the head realtime use the `cumulate` method
//       We JOIN the output of
//       a. `computeWindows` - `(key, hopStart) -> IR`
//       b. the raw events on the head by hopStart - `(key, hopStart) -> [Input]`
//       c. query_times by hopStart - `(key, hopStart) -> [query_ts]`
//      And produce `key -> [query_ts, IR]`
// NOTE: Not using the `cumulate` method will result in snapshot accuracy.
object SawtoothAggregator {
  sealed trait WindowComputerKind extends Serializable {
    def name: String
    def requiresSorted: Boolean
  }

  object WindowComputerKind {
    case object Cached extends WindowComputerKind {
      override val name: String = "cached"
      override val requiresSorted: Boolean = false
    }
    case object Sliding extends WindowComputerKind {
      override val name: String = "sliding"
      override val requiresSorted: Boolean = true
    }
    case object QueryAwareSliding extends WindowComputerKind {
      override val name: String = "query-aware-sliding"
      override val requiresSorted: Boolean = true
    }

    val all: Seq[WindowComputerKind] = Seq(Cached, Sliding, QueryAwareSliding)
  }
}

class SawtoothAggregator(aggregations: Seq[Aggregation], inputSchema: Seq[(String, DataType)], resolution: Resolution)
    extends Serializable {

  import SawtoothAggregator.WindowComputerKind

  protected val hopSizes = resolution.hopSizes

  @transient lazy val unpackedAggs: UnpackedAggregations = UnpackedAggregations.from(aggregations)
  @transient lazy protected val tailHopIndices: Array[Int] = windowMappings.map { mapping =>
    hopSizes.indexOf(resolution.calculateTailHop(mapping.aggregationPart.window))
  }

  @transient lazy val windowMappings: Array[WindowMapping] = unpackedAggs.perWindow
  @transient lazy val perWindowAggs: Array[AggregationPart] = windowMappings.map(_.aggregationPart)
  @transient lazy val windowedAggregator = new RowAggregator(inputSchema, unpackedAggs.perWindow.map(_.aggregationPart))
  @transient lazy val baseAggregator = new RowAggregator(inputSchema, unpackedAggs.perBucket)
  @transient protected lazy val baseIrIndices: Array[Int] = windowMappings.map(_.baseIrIndex)

  // the cache uses this space to work out the IRs for the whole window based on hops
  // we only create this arena once, so GC kicks in fewer times
  @transient private lazy val arena =
    Array.fill(resolution.hopSizes.length)(Array.fill[Entry](windowedAggregator.length)(null))

  trait WindowComputer extends Serializable {
    def kind: WindowComputerKind
    def computeWindowsIterator(hops: HopsAggregator.OutputArrayType, endTimes: Array[Long]): Iterator[Array[Any]]
    def maxRetainedQueueEntries: Long = 0L
  }

  def windowComputer(kind: WindowComputerKind): WindowComputer =
    kind match {
      case WindowComputerKind.Cached            => new CachedWindowComputer
      case WindowComputerKind.Sliding           => new SlidingWindowComputer
      case WindowComputerKind.QueryAwareSliding => new QueryAwareSlidingWindowComputer
    }

  def computeWindowsIterator(hops: HopsAggregator.OutputArrayType, endTimes: Array[Long]): Iterator[Array[Any]] = {
    if (hops == null) return emptyWindowsIterator(endTimes)
    val kind = if (isSorted(endTimes)) WindowComputerKind.Sliding else WindowComputerKind.Cached
    windowComputer(kind).computeWindowsIterator(hops, endTimes)
  }

  def computeWindowsIterator(hops: HopsAggregator.OutputArrayType,
                             endTimes: Array[Long],
                             kind: WindowComputerKind): Iterator[Array[Any]] = {
    if (hops == null) return emptyWindowsIterator(endTimes)
    val effectiveKind = if (kind.requiresSorted && !isSorted(endTimes)) WindowComputerKind.Cached else kind
    windowComputer(effectiveKind).computeWindowsIterator(hops, endTimes)
  }

  def computeWindows(hops: HopsAggregator.OutputArrayType, endTimes: Array[Long]): Array[Array[Any]] =
    computeWindowsIterator(hops, endTimes).toArray

  def computeWindows(hops: HopsAggregator.OutputArrayType,
                     endTimes: Array[Long],
                     kind: WindowComputerKind): Array[Array[Any]] =
    computeWindowsIterator(hops, endTimes, kind).toArray

  private def isSorted(endTimes: Array[Long]): Boolean = {
    var i = 1
    while (i < endTimes.length) {
      if (endTimes(i) < endTimes(i - 1)) return false
      i += 1
    }
    true
  }

  private def emptyWindowsIterator(endTimes: Array[Long]): Iterator[Array[Any]] =
    endTimes.iterator.map(_ => windowedAggregator.init)

  private def merge(left: Any, right: Any, col: Int): Any =
    if (right == null) left else windowedAggregator(col).merge(left, right)

  // stitches multiple hops into a continuous window
  private def genIr(col: Int, endTime: Long, aggregateRange: (Int, Long, Long) => Any): Any = {
    val window = perWindowAggs(col).window
    var hopIndex = tailHopIndices(col)
    val hopMillis = hopSizes(hopIndex)
    var baseIr: Any = null
    var start = TsUtils.round(endTime - window.millis, hopMillis)
    while (hopIndex < hopSizes.length) {
      val end = TsUtils.round(endTime, hopSizes(hopIndex))
      baseIr = merge(baseIr, aggregateRange(hopIndex, start, end), col)
      start = end
      hopIndex += 1
    }
    baseIr
  }

  private class CachedWindowComputer extends WindowComputer {
    override def kind: WindowComputerKind = WindowComputerKind.Cached

    override def computeWindowsIterator(hops: HopsAggregator.OutputArrayType,
                                        endTimes: Array[Long]): Iterator[Array[Any]] = {
      if (hops == null) return emptyWindowsIterator(endTimes)
      lazy val cache = {
        val ret = new HopRangeCache(hops, windowedAggregator, baseIrIndices, arena)
        ret.reset() // clear arena
        ret
      }

      endTimes.iterator.map { endTime =>
        val result = windowedAggregator.init
        for (col <- windowedAggregator.indices) {
          result.update(col, genIr(col, endTime, (hopIndex, start, end) => cache.merge(hopIndex, col, start, end)))
        }
        result
      }
    }
  }

  private class SlidingWindowComputer extends WindowComputer {
    private var ranges: Array[Array[SlidingHopRange]] = Array.empty

    override def kind: WindowComputerKind = WindowComputerKind.Sliding

    override def computeWindowsIterator(hops: HopsAggregator.OutputArrayType,
                                        endTimes: Array[Long]): Iterator[Array[Any]] = {
      if (hops == null) return emptyWindowsIterator(endTimes)
      require(isSorted(endTimes), s"${kind.name} window computer requires sorted end times")
      ranges = Array.tabulate(windowedAggregator.length, hopSizes.length) { case (col, hopIndex) =>
        new SlidingHopRange(hops(hopIndex), windowedAggregator(col), baseIrIndices(col))
      }

      endTimes.iterator.map { endTime =>
        val result = windowedAggregator.init
        for (col <- windowedAggregator.indices) {
          val colRanges = ranges(col)
          result.update(col, genIr(col, endTime, (hopIndex, start, end) => colRanges(hopIndex).aggregate(start, end)))
        }
        result
      }
    }

    override def maxRetainedQueueEntries: Long =
      ranges.iterator.flatMap(_.iterator).map(_.maxRetainedEntries.toLong).sum
  }

  private class QueryAwareSlidingWindowComputer extends WindowComputer {
    private var ranges: Array[Array[CoalescingSlidingHopRange]] = Array.empty

    override def kind: WindowComputerKind = WindowComputerKind.QueryAwareSliding

    override def computeWindowsIterator(hops: HopsAggregator.OutputArrayType,
                                        endTimes: Array[Long]): Iterator[Array[Any]] = {
      if (hops == null) return emptyWindowsIterator(endTimes)
      require(isSorted(endTimes), s"${kind.name} window computer requires sorted end times")
      val splitPoints = computeSplitPoints(endTimes)
      ranges = Array.tabulate(windowedAggregator.length, hopSizes.length) { case (col, hopIndex) =>
        new CoalescingSlidingHopRange(hops(hopIndex),
                                      windowedAggregator(col),
                                      baseIrIndices(col),
                                      splitPoints(col)(hopIndex))
      }

      endTimes.iterator.map { endTime =>
        val result = windowedAggregator.init
        for (col <- windowedAggregator.indices) {
          val colRanges = ranges(col)
          result.update(col, genIr(col, endTime, (hopIndex, start, end) => colRanges(hopIndex).aggregate(start, end)))
        }
        result
      }
    }

    override def maxRetainedQueueEntries: Long =
      ranges.iterator.flatMap(_.iterator).map(_.maxRetainedEntries.toLong).sum

    private def appendDistinct(buffer: mutable.ArrayBuffer[Long], value: Long): Unit = {
      if (buffer.isEmpty || buffer.last != value) buffer += value
    }

    private def computeSplitPoints(endTimes: Array[Long]): Array[Array[Array[Long]]] = {
      val buffers = Array.fill(windowedAggregator.length, hopSizes.length)(mutable.ArrayBuffer.empty[Long])
      endTimes.foreach { endTime =>
        for (col <- windowedAggregator.indices) {
          val window = perWindowAggs(col).window
          var hopIndex = tailHopIndices(col)
          val hopMillis = hopSizes(hopIndex)
          var start = TsUtils.round(endTime - window.millis, hopMillis)
          while (hopIndex < hopSizes.length) {
            appendDistinct(buffers(col)(hopIndex), start)
            val end = TsUtils.round(endTime, hopSizes(hopIndex))
            start = end
            hopIndex += 1
          }
        }
      }
      buffers.map(_.map(_.toArray))
    }
  }

  // method is used to generate head-realtime ness on top of hops
  // But without the requirement that the input be sorted
  def cumulate(inputs: Iterator[Row], // don't need to be sorted
               sortedEndTimes: Array[Long], // sorted,
               baseIR: Array[Any]): Array[Array[Any]] = {
    if (sortedEndTimes == null || sortedEndTimes.isEmpty) return Array.empty[Array[Any]]
    if (inputs == null || inputs.isEmpty)
      return Array.fill[Array[Any]](sortedEndTimes.length)(baseIR)

    val result = Array.fill[Array[Any]](sortedEndTimes.length)(null)
    while (inputs.hasNext) {
      val row = inputs.next()
      val inputTs = row.ts
      var updateIndex = util.Arrays.binarySearch(sortedEndTimes, inputTs)
      if (updateIndex >= 0) {
        while (updateIndex < sortedEndTimes.length && sortedEndTimes(updateIndex) == inputTs)
          updateIndex += 1
      } else {
        // binary search didn't find an exact match
        updateIndex = math.abs(updateIndex) - 1
      }
      if (updateIndex < sortedEndTimes.length && updateIndex >= 0) {
        if (result(updateIndex) == null) {
          result.update(updateIndex, new Array[Any](baseAggregator.length))
        }
        baseAggregator.update(result(updateIndex), row)
      }
    }

    // at this point results aren't cumulated, and are one per spec, instead of one per window
    var currBase = baseIR
    for (i <- result.indices) {
      val binned = result(i)
      if (binned != null) {
        currBase = windowedAggregator.clone(currBase)
        for (col <- windowedAggregator.indices) {
          val merged = windowedAggregator(col)
            .merge(currBase(col), binned(baseIrIndices(col)))
          currBase.update(col, merged)
        }
      }
      result.update(i, currBase)
    }
    result
  }

  // method is used to generate head-realtime ness on top of hops
  // But without the requirement that the input be sorted
  def cumulateAndFinalizeSorted(sortedInputs: mutable.Buffer[Row], // don't need to be sorted
                                sortedEndTimes: mutable.Buffer[Row], // sorted,
                                baseIR: Array[Any],
                                consumer: (Row, Array[Any]) => Unit): Unit = {

    if (sortedEndTimes == null || sortedEndTimes.isEmpty) return

    if (sortedInputs == null || sortedInputs.isEmpty) {
      val finalized = windowedAggregator.finalize(baseIR)
      sortedEndTimes.foreach(query => consumer(query, finalized))
      return
    }

    var inputIdx = 0
    var queryIdx = 0

    var queryIr = if (baseIR == null) {
      new Array[Any](windowedAggregator.length)
    } else {
      baseIR
    }

    while (queryIdx < sortedEndTimes.length) {

      while (inputIdx < sortedInputs.length && sortedInputs(inputIdx).ts < sortedEndTimes(queryIdx).ts) {
        queryIr = windowedAggregator.update(queryIr, sortedInputs(inputIdx))
        inputIdx += 1
      }

      val clonedIr = windowedAggregator.clone(queryIr)
      consumer(sortedEndTimes(queryIdx), windowedAggregator.finalize(clonedIr))

      queryIdx += 1
    }
  }

  // method is used to generate head-realtime ness on top of hops
  // But without the requirement that the input be sorted
  def cumulateAndFinalizeSortedIterator(sortedInputs: mutable.Buffer[Row], // don't need to be sorted
                                        sortedEndTimes: mutable.Buffer[Row], // sorted,
                                        baseIR: Array[Any]): Iterator[(Row, Array[Any])] = {

    if (sortedEndTimes == null || sortedEndTimes.isEmpty) Iterator.empty

    if (sortedInputs == null || sortedInputs.isEmpty) {
      val finalized = windowedAggregator.finalize(baseIR)
      return sortedEndTimes.iterator.map(query => (query, finalized))
    }

    var inputIdx = 0

    var queryIr = if (baseIR == null) {
      new Array[Any](windowedAggregator.length)
    } else {
      baseIR
    }

    sortedEndTimes.indices.iterator.map { queryIdx =>
      while (inputIdx < sortedInputs.length && sortedInputs(inputIdx).ts < sortedEndTimes(queryIdx).ts) {
        queryIr = windowedAggregator.update(queryIr, sortedInputs(inputIdx))
        inputIdx += 1
      }

      // clone and finalize without intermediate collections
      val result = Array.fill[Any](windowedAggregator.length)(null)
      var i = 0
      while (i < windowedAggregator.length) {
        val colAgg = windowedAggregator.columnAggregators(i)
        val colIr = queryIr(i)
        if (colIr != null) {
          val finalized = colAgg.finalize(colAgg.clone(colIr))
          result.update(i, finalized)
        }
        i += 1
      }

      (sortedEndTimes(queryIdx), result)
    }
  }
}

private class Entry(var startIndex: Int, var endIndex: Int, var ir: Any) {}

private class IrQueueEntry(val value: Any, val aggregate: Any) {}

private[windowing] class TwoStackIrQueue(aggregator: ColumnAggregator) {
  private val inStack = new util.ArrayDeque[IrQueueEntry]()
  private val outStack = new util.ArrayDeque[IrQueueEntry]()
  private var maxSize = 0

  def clear(): Unit = {
    inStack.clear()
    outStack.clear()
  }

  private def mergedCopy(left: Any, right: Any): Any = {
    val base = if (left == null) null else aggregator.clone(left)
    aggregator.merge(base, right)
  }

  def push(ir: Any): Unit = {
    val previousAggregate = if (inStack.isEmpty) null else inStack.peek().aggregate
    inStack.push(new IrQueueEntry(ir, mergedCopy(previousAggregate, ir)))
    maxSize = math.max(maxSize, size)
  }

  def pop(): Unit = {
    if (outStack.isEmpty) {
      while (!inStack.isEmpty) {
        val entry = inStack.pop()
        val previousAggregate = if (outStack.isEmpty) null else outStack.peek().aggregate
        outStack.push(new IrQueueEntry(entry.value, mergedCopy(entry.value, previousAggregate)))
      }
    }
    if (!outStack.isEmpty) {
      outStack.pop()
    }
  }

  def aggregate: Any = {
    val outAggregate = if (outStack.isEmpty) null else outStack.peek().aggregate
    val inAggregate = if (inStack.isEmpty) null else inStack.peek().aggregate
    if (outAggregate == null) inAggregate
    else if (inAggregate == null) outAggregate
    else mergedCopy(outAggregate, inAggregate)
  }

  def size: Int = inStack.size() + outStack.size()

  def maxRetainedEntries: Int = maxSize
}

private[windowing] class SlidingHopRange(hops: Array[HopsAggregator.HopIr],
                                         aggregator: ColumnAggregator,
                                         hopIrIndex: Int) {
  private val queue = new TwoStackIrQueue(aggregator)
  private var initialized = false
  private var leftIdx = 0
  private var rightIdx = 0

  @inline
  private def ts(hop: Array[Any]): Long = hop.last.asInstanceOf[Long]

  private def lowerBound(start: Long): Int = {
    var low = 0
    var high = hops.length
    while (low < high) {
      val mid = (low + high) >>> 1
      if (ts(hops(mid)) < start) low = mid + 1
      else high = mid
    }
    low
  }

  def aggregate(start: Long, end: Long): Any = {
    if (start >= end || hops.isEmpty) return null

    if (!initialized) {
      val startIdx = lowerBound(start)
      leftIdx = startIdx
      rightIdx = startIdx
      initialized = true
    } else {
      while (leftIdx < rightIdx && ts(hops(leftIdx)) < start) {
        queue.pop()
        leftIdx += 1
      }
      if (leftIdx == rightIdx) {
        val startIdx = lowerBound(start)
        leftIdx = startIdx
        rightIdx = startIdx
        queue.clear()
      }
    }

    while (rightIdx < hops.length && ts(hops(rightIdx)) < end) {
      queue.push(hops(rightIdx)(hopIrIndex))
      rightIdx += 1
    }
    while (leftIdx < rightIdx && ts(hops(leftIdx)) < start) {
      queue.pop()
      leftIdx += 1
    }
    queue.aggregate
  }

  def maxRetainedEntries: Int = queue.maxRetainedEntries
}

private class ExpiringIrQueueEntry(val expiresAt: Long, val value: Any, val aggregate: Any) {}

private[windowing] class CoalescingTwoStackIrQueue(aggregator: ColumnAggregator) {
  private val inStack = new util.ArrayDeque[ExpiringIrQueueEntry]()
  private val outStack = new util.ArrayDeque[ExpiringIrQueueEntry]()
  private var maxSize = 0

  def clear(): Unit = {
    inStack.clear()
    outStack.clear()
  }

  private def mergedCopy(left: Any, right: Any): Any = {
    val base = if (left == null) null else aggregator.clone(left)
    aggregator.merge(base, right)
  }

  private def pushEntry(expiresAt: Long, value: Any): Unit = {
    val previousAggregate = if (inStack.isEmpty) null else inStack.peek().aggregate
    inStack.push(new ExpiringIrQueueEntry(expiresAt, value, mergedCopy(previousAggregate, value)))
  }

  def push(ir: Any, expiresAt: Long): Unit = {
    if (ir == null) return
    if (!inStack.isEmpty && inStack.peek().expiresAt == expiresAt) {
      val top = inStack.pop()
      val mergedValue = mergedCopy(top.value, ir)
      pushEntry(expiresAt, mergedValue)
    } else {
      pushEntry(expiresAt, ir)
    }
    maxSize = math.max(maxSize, size)
  }

  private def transferInToOut(): Unit = {
    while (!inStack.isEmpty) {
      val entry = inStack.pop()
      val previousAggregate = if (outStack.isEmpty) null else outStack.peek().aggregate
      outStack.push(new ExpiringIrQueueEntry(entry.expiresAt, entry.value, mergedCopy(entry.value, previousAggregate)))
    }
  }

  def popExpired(start: Long): Unit = {
    var done = false
    while (!done) {
      if (outStack.isEmpty) transferInToOut()
      if (!outStack.isEmpty && outStack.peek().expiresAt <= start) {
        outStack.pop()
      } else {
        done = true
      }
    }
  }

  def aggregate: Any = {
    val outAggregate = if (outStack.isEmpty) null else outStack.peek().aggregate
    val inAggregate = if (inStack.isEmpty) null else inStack.peek().aggregate
    if (outAggregate == null) inAggregate
    else if (inAggregate == null) outAggregate
    else mergedCopy(outAggregate, inAggregate)
  }

  def size: Int = inStack.size() + outStack.size()

  def maxRetainedEntries: Int = maxSize
}

private[windowing] class CoalescingSlidingHopRange(hops: Array[HopsAggregator.HopIr],
                                                   aggregator: ColumnAggregator,
                                                   hopIrIndex: Int,
                                                   splitPoints: Array[Long]) {
  private val queue = new CoalescingTwoStackIrQueue(aggregator)
  private var initialized = false
  private var rightIdx = 0

  @inline
  private def ts(hop: Array[Any]): Long = hop.last.asInstanceOf[Long]

  private def lowerBound(start: Long): Int = {
    var low = 0
    var high = hops.length
    while (low < high) {
      val mid = (low + high) >>> 1
      if (ts(hops(mid)) < start) low = mid + 1
      else high = mid
    }
    low
  }

  private def nextSplitAfter(timestamp: Long): Long = {
    var low = 0
    var high = splitPoints.length
    while (low < high) {
      val mid = (low + high) >>> 1
      if (splitPoints(mid) <= timestamp) low = mid + 1
      else high = mid
    }
    if (low < splitPoints.length) splitPoints(low) else Long.MaxValue
  }

  private def mergedCopy(left: Any, right: Any): Any = {
    val base = if (left == null) null else aggregator.clone(left)
    aggregator.merge(base, right)
  }

  def aggregate(start: Long, end: Long): Any = {
    if (start >= end || hops.isEmpty) return null

    if (!initialized) {
      rightIdx = lowerBound(start)
      initialized = true
    } else {
      queue.popExpired(start)
      val startIdx = lowerBound(start)
      if (startIdx >= rightIdx) {
        queue.clear()
        rightIdx = startIdx
      }
    }

    while (rightIdx < hops.length && ts(hops(rightIdx)) < end) {
      val expiresAt = nextSplitAfter(ts(hops(rightIdx)))
      var blockIr: Any = null
      while (rightIdx < hops.length && ts(hops(rightIdx)) < end && nextSplitAfter(ts(hops(rightIdx))) == expiresAt) {
        blockIr = mergedCopy(blockIr, hops(rightIdx)(hopIrIndex))
        rightIdx += 1
      }
      queue.push(blockIr, expiresAt)
    }
    queue.popExpired(start)
    queue.aggregate
  }

  def maxRetainedEntries: Int = queue.maxRetainedEntries
}

private[windowing] class HopRangeCache(hopsArrays: HopsAggregator.OutputArrayType,
                                       windowAggregator: RowAggregator,
                                       hopIrIndices: Array[Int],
                                       // arena is the memory buffer where cache entries live
                                       arena: Array[Array[Entry]]) {

  // without the reset method, recreating the arena would add to GC pressure
  def reset(): Unit = {
    for (i <- arena.indices) {
      for (j <- arena(i).indices) {
        arena(i).update(j, null)
      }
    }
  }

  @inline
  private def ts(hop: Array[Any]): Long = hop.last.asInstanceOf[Long]

  // start and end need to be multiples of hop-sizes for this to work
  // Every call to this method constructs a unique reference, but uses clone as
  // much as possible instead of
  def merge(hopIndex: Int, col: Int, start: Long, end: Long): Any = {
    val hops = hopsArrays(hopIndex)
    val cached: Entry = arena(hopIndex)(col)
    val agg = windowAggregator(col)
    val baseCol = hopIrIndices(col)

    var startIdx = if (cached == null) 0 else cached.startIndex
    while (startIdx < hops.length && ts(hops(startIdx)) < start) {
      startIdx += 1
    }

    var ir: Any = null
    var endIdx = startIdx
    if (cached != null && startIdx == cached.startIndex) {
      // un-windowed case will always degenerate to cumulative sum
      ir = agg.clone(cached.ir)
      endIdx = cached.endIndex
    }

    while (endIdx < hops.length && ts(hops(endIdx)) < end) {
      ir = agg.merge(ir, hops(endIdx)(baseCol))
      endIdx += 1
    }

    if (cached == null) {
      val newEntry = new Entry(startIdx, endIdx, ir)
      arena(hopIndex).update(col, newEntry)
    } else if (cached.startIndex != startIdx || cached.endIndex != endIdx) {
      // reusing the entry object to reduce GC pressure
      cached.startIndex = startIdx
      cached.endIndex = endIdx
      cached.ir = ir
    }

    ir
  }

}
