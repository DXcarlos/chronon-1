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

package ai.chronon.api

sealed trait DataRange {
  def toTimePoints: Array[Long]
}
case class TimeRange(start: Long, end: Long)(implicit partitionSpec: PartitionSpec) extends DataRange {
  def toTimePoints: Array[Long] = {
    Stream
      .iterate(TsUtils.round(start, partitionSpec.spanMillis))(_ + partitionSpec.spanMillis)
      .takeWhile(_ <= end)
      .toArray
  }

  def toPartitionRange: PartitionRange = {
    PartitionRange(partitionSpec.at(start), partitionSpec.at(end))
  }

  def pretty: String = s"start:[${TsUtils.toStr(start)}]-end:[${TsUtils.toStr(end)}]"
  override def toString: String = s"[${TsUtils.toStr(start)}-${TsUtils.toStr(end)}]"
}
// start and end can be null - signifies unbounded-ness
case class PartitionRange(start: String, end: String)(implicit val partitionSpec: PartitionSpec)
    extends DataRange
    with Ordered[PartitionRange] {

  def valid: Boolean = {
    (Option(start), Option(end)) match {
      case (Some(s), Some(e)) => s <= e
      case _                  => true
    }
  }

  def isSingleDay: Boolean = {
    start == end
  }

  def intersect(other: PartitionRange): PartitionRange = {
    // lots of null handling
    val newStart = (Option(start) ++ Option(other.start))
      .reduceLeftOption(Ordering[String].max)
      .orNull
    val newEnd = (Option(end) ++ Option(other.end))
      .reduceLeftOption(Ordering[String].min)
      .orNull
    // could be invalid
    PartitionRange(newStart, newEnd)
  }

  override def toTimePoints: Array[Long] = {
    assert(start != null && end != null, "Can't request timePoint conversion when PartitionRange is unbounded")
    Stream
      .iterate(start)(partitionSpec.after)
      .takeWhile(_ <= end)
      .map(partitionSpec.epochMillis)
      .toArray
  }

  def betweenClauses: String = {
    s"${partitionSpec.column} BETWEEN '$start' AND '$end'"
  }

  def whereClauses: Seq[String] = {
    (Option(start).map(s => s"${partitionSpec.column} >= '$s'") ++ Option(end).map(e =>
      s"${partitionSpec.column} <= '$e'")).toSeq
  }

  def steps(days: Int): Seq[PartitionRange] = {
    partitions
      .sliding(days, days) // sliding(x, x) => tumbling(x)
      .map { step => PartitionRange(step.head, step.last) }
      .toSeq
  }

  def partitions: Seq[String] = {
    require(wellDefined, s"Invalid partition range $this")
    Stream
      .iterate(start)(partitionSpec.after)
      .takeWhile(_ <= end)
  }

  // no nulls in start or end and start <= end - used as a pre-check before the `partitions` function
  def wellDefined: Boolean = start != null && end != null && start <= end

  def shift(days: Int): PartitionRange = {
    if (days == 0) {
      this
    } else {
      PartitionRange(partitionSpec.shift(start, days), partitionSpec.shift(end, days))
    }
  }

  def shiftMillis(millis: Long): PartitionRange = {
    if (millis == 0) {
      this
    } else {
      // Handle start date (00:00:00.000)
      val newStart = if (start == null) {
        null
      } else {
        val startTimeMillis = partitionSpec.epochMillis(start) // Already represents 00:00:00.000
        partitionSpec.at(startTimeMillis + millis)
      }

      // Handle end date (23:59:59.999)
      val newEnd = if (end == null) {
        null
      } else {
        val endTimeMillis = partitionSpec.epochMillis(end) + (24 * 60 * 60 * 1000 - 1) // End of day (23:59:59.999)
        val shiftedEndTimeMillis = endTimeMillis + millis
        // Get the date part (without time)
        partitionSpec.at(shiftedEndTimeMillis)
      }

      PartitionRange(newStart, newEnd)
    }
  }

  override def compare(that: PartitionRange): Int = {
    def compareDate(left: String, right: String): Int = {
      if (left == right) {
        0
      } else if (left == null) {
        -1
      } else if (right == null) {
        1
      } else {
        left compareTo right
      }
    }

    val compareStart = compareDate(this.start, that.start)
    if (compareStart != 0) {
      compareStart
    } else {
      compareDate(this.end, that.end)
    }
  }

  def translate(otherSpec: PartitionSpec): PartitionRange = {

    val newStart = Option(start).map(d => partitionSpec.translate(d, otherSpec)).orNull
    val newEnd = Option(end).map(d => partitionSpec.translate(d, otherSpec)).orNull

    PartitionRange(newStart, newEnd)(otherSpec)
  }

  override def toString: String = s"[$start...$end]"
}

object PartitionRange {
  def rangesToString(ranges: Iterable[PartitionRange]): String = {
    val tuples = ranges.map(r => s"(${r.start} -> ${r.end})").mkString(", ")
    s"$tuples"
  }

  // takes a list of partitions and collapses them into ranges
  // eg: ["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-05", "2020-01-07", "2020-01-08"]
  // will return: [
  //    PartitionRange("2020-01-01", "2020-01-03"),
  //    PartitionRange("2020-01-05", "2020-01-05"),
  //    PartitionRange("2020-01-07", "2020-01-08")
  // ]
  def collapseToRange(partitions: Iterable[String])(implicit partitionSpec: PartitionSpec): Seq[PartitionRange] = {
    if (partitions == null) return null
    var result = Seq.empty[PartitionRange]
    val sortedPartitions = partitions.toSeq.sorted.distinct
    if (sortedPartitions.isEmpty) return result
    var start = sortedPartitions.head
    var end = start
    sortedPartitions.tail.foreach { p =>
      if (partitionSpec.after(end) == p) {
        end = p
      } else {
        val range = PartitionRange(start, end)
        result = result :+ range
        start = p
        end = p
      }
    }
    result :+ PartitionRange(start, end)
  }

  def collapsedPrint(parts: Iterable[String])(implicit partitionSpec: PartitionSpec): String = {
    val ranges = collapseToRange(parts)
    if (ranges == null) return ""
    rangesToString(ranges)
  }

  // takes a collapsed string and expands it back to individual partitions
  // eg: "(2020-01-01 -> 2020-01-03), (2020-01-05 -> 2020-01-05), (2020-01-07 -> 2020-01-08)"
  // will return: ["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-05", "2020-01-07", "2020-01-08"]
  def expandDates(collapsedString: String)(implicit partitionSpec: PartitionSpec): Seq[String] = {
    if (collapsedString == null || collapsedString.trim.isEmpty) return Seq.empty

    try {
      // Parse the collapsed string format: "(start -> end), (start -> end), ..."
      val rangePattern = """\(([^)]+)\)""".r
      val ranges = rangePattern.findAllIn(collapsedString).map(_.stripPrefix("(").stripSuffix(")")).toSeq

      ranges.flatMap { rangeStr =>
        val parts = rangeStr.split(" -> ").map(_.trim)
        if (parts.length == 2) {
          val start = parts(0)
          val end = parts(1)
          PartitionRange(start, end).partitions
        } else {
          Seq.empty
        }
      }
    } catch {
      case _: Exception => Seq.empty
    }
  }

  def toTimeRange(partitionRange: PartitionRange): TimeRange = {
    val spec = partitionRange.partitionSpec
    val shiftedEnd = spec.after(partitionRange.end)
    TimeRange(spec.epochMillis(partitionRange.start), spec.epochMillis(shiftedEnd) - 1)(spec)
  }
}
