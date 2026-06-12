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

import ai.chronon.api.Extensions._
import org.apache.commons.lang3.time.FastDateFormat

import java.text.{ParseException, ParsePosition, SimpleDateFormat}
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.{Calendar, Locale, TimeZone}
import scala.collection.mutable.ListBuffer

/** Regular partition boundaries: `{ k * spanMillis + offsetMillis }`, with each cell covering
  * `[start, start + spanMillis)`. Independent of label format and partition column.
  */
case class PartitionGrid(spanMillis: Long, offsetMillis: Long = 0L) {
  require(spanMillis > 0, s"Partition span must be positive, found $spanMillis")
  // day-denominated reasoning (partitionsPerDay, stepsByDays, snapshot/orchestration math) relies
  // on partitions tiling the UTC day; week/month-sized partitions are deliberately unrepresentable
  // (7d grids would anchor to Thursday - epoch day zero; 30d grids drift off calendar months)
  require(
    spanMillis == WindowUtils.Day.millis || (spanMillis < WindowUtils.Day.millis && WindowUtils.Day.millis % spanMillis == 0),
    s"Partition span must divide a UTC day evenly or equal one day, found ${spanMillis}ms. " +
      s"Weekly/monthly cadences are expressed as schedules over daily partitions, not as partition spans."
  )
  require(
    offsetMillis >= 0 && offsetMillis < spanMillis,
    s"Partition offset must be in [0, span), found ${offsetMillis}ms for span ${spanMillis}ms. " +
      s"Declare the canonical offset instead of relying on modular normalization."
  )
  require(
    spanMillis < WindowUtils.Day.millis || offsetMillis == 0,
    s"Daily partitions stay midnight-anchored: offsets are only supported on sub-daily grids, " +
      s"found offset ${offsetMillis}ms on a ${spanMillis}ms span."
  )

  def isDaily: Boolean = spanMillis == WindowUtils.Day.millis && offsetMillis == 0

  /** 1 for daily spans; used to convert day-denominated configs like stepDays. */
  def partitionsPerDay: Int = math.max(1, (WindowUtils.Day.millis / spanMillis).toInt)

  /** start of the grid interval containing `millis` */
  def floor(millis: Long): Long =
    millis - Math.floorMod(millis - offsetMillis, spanMillis)

  /** True when this grid's interval is an exact multiple of the producer's interval. */
  def isExactMultipleOf(producer: PartitionGrid): Boolean =
    spanMillis >= producer.spanMillis && spanMillis % producer.spanMillis == 0

  def exactMultipleRequirement(producer: PartitionGrid): String =
    s"consumer interval ${WindowUtils.millisToString(spanMillis)} must be equal to or an exact multiple of " +
      s"producer interval ${WindowUtils.millisToString(producer.spanMillis)}"

  /** Directional alignment: this grid's offset must land on the producer's grid. */
  def isAlignedTo(producer: PartitionGrid): Boolean =
    Math.floorMod(offsetMillis - producer.offsetMillis, producer.spanMillis) == 0L

  def alignmentRequirement(producer: PartitionGrid): String =
    s"consumer grid ($show) is not aligned with producer grid (${producer.show}); " +
      "grid offsets must differ by a whole number of producer intervals."

  def canCover(producer: PartitionGrid): Boolean =
    isExactMultipleOf(producer) && isAlignedTo(producer)

  /** Conditional semantic-hash token: the historical daily-at-midnight grid contributes nothing. */
  def semanticToken: Option[String] =
    if (isDaily) None else Some(s"grid:interval_ms=$spanMillis,offset_ms=$offsetMillis")

  def show: String =
    s"interval ${WindowUtils.millisToString(spanMillis)} @ offset ${WindowUtils.millisToString(offsetMillis)}"
}

/** Table-facing partition spec: a grid plus the column/format used to parse and render labels.
  * Labels name interval starts, and `format` must be sortable and precise enough for the grid.
  */
case class PartitionSpec(column: String, format: String, spanMillis: Long, offsetMillis: Long = 0L) {

  private val partitionGrid = PartitionGrid(spanMillis, offsetMillis)
  def grid: PartitionGrid = partitionGrid

  validateFormat()

  private def partitionFormatter =
    DateTimeFormatter
      .ofPattern(format, Locale.US)
      .withZone(ZoneOffset.UTC)

  def expandRange(startDateStr: String, endDateStr: String): List[String] = {
    // Parse/format in UTC with a stable locale
    val tz = TimeZone.getTimeZone("UTC")
    val dateFormat = FastDateFormat.getInstance(format, tz, Locale.US)

    // Parse start and end dates
    val startDate = dateFormat.parse(startDateStr)
    val endDate = dateFormat.parse(endDateStr)

    if (startDate.after(endDate)) return List.empty

    // List to store all dates
    val dates = ListBuffer[String]()

    // Use Calendar for date iteration
    val calendar = Calendar.getInstance(tz, Locale.US)
    calendar.setTime(startDate)

    // Iterate from start to end date
    while (!calendar.getTime.after(endDate)) {
      // Format current date and add to list
      dates += dateFormat.format(calendar.getTime)

      // Move to the next partition interval.
      calendar.add(calendarGrain(intervalWindow), intervalWindow.length)
    }

    dates.toList
  }

  private def sdf = {
    val formatter = new SimpleDateFormat(format)
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"))
    formatter.setLenient(false)
    formatter
  }

  def epochMillis(partition: String): Long = {
    val formatter = sdf
    val position = new ParsePosition(0)
    val parsed = formatter.parse(partition, position)
    if (parsed == null || position.getIndex != partition.length) {
      val errorOffset = if (position.getErrorIndex >= 0) position.getErrorIndex else position.getIndex
      throw new ParseException(s"Unparseable date: '$partition'", errorOffset)
    }
    parsed.getTime
  }

  def isDaily: Boolean = grid.isDaily

  /** Same span and anchor; labels translate 1:1 even if column or format differs. */
  def hasSameGrid(other: PartitionSpec): Boolean = grid == other.grid

  // the partition value containing this timestamp
  def at(millis: Long): String = partitionFormatter.format(Instant.ofEpochMilli(grid.floor(millis)))

  def partitionStartMillis(partitionValue: String): Long = epochMillis(partitionValue)

  def partitionEndMillis(partitionValue: String): Long = partitionStartMillis(partitionValue) + spanMillis

  def rangeCovering(interval: PartitionInterval): Option[PartitionRange] = {
    if (interval.isEmpty) {
      None
    } else {
      Some(PartitionRange(at(interval.startMillis), at(interval.endMillis - 1))(this))
    }
  }

  def before(s: String): String = shiftPartitions(s, -1)

  def partitionsOverlapping(interval: PartitionInterval): Seq[String] =
    rangeCovering(interval).map(_.partitions).getOrElse(Seq.empty)

  def calendarGrain(window: Window): Int = window.timeUnit match {
    case TimeUnit.DAYS    => Calendar.DAY_OF_MONTH
    case TimeUnit.HOURS   => Calendar.HOUR_OF_DAY
    case TimeUnit.MINUTES => Calendar.MINUTE
  }

  // TODO-test:
  // takes a string and a window and returns the string representing the advancement by window
  def plusFast(s: String, window: Window, sign: Int = 1): String = {
    // Parse/format in UTC with a stable locale
    val tz = TimeZone.getTimeZone("UTC")
    val dateFormat = FastDateFormat.getInstance(format, tz, Locale.US)

    // Parse the given timestamp
    val date = dateFormat.parse(s)

    // Use Calendar for date math in UTC/Locale.US
    val calendar = Calendar.getInstance(tz, Locale.US)
    calendar.setTime(date)

    // Advance by the window length
    calendar.add(calendarGrain(window), sign * window.length)

    // Format back to string
    dateFormat.format(calendar.getTime)
  }

  def afterFast(s: String): String = plusFast(s, intervalWindow)

  // TODO-test:
  def minusFast(s: String, window: Window): String = plusFast(s, window, -1)

  def minus(s: String, window: Window): String = at(epochMillis(s) - window.millis)

  def plus(s: String, window: Window): String = at(epochMillis(s) + window.millis)

  def minus(partition: String, window: Option[Window]): String = {
    if (partition == null) return null
    window.map(minus(partition, _)).getOrElse(partition)
  }

  def plus(partition: String, window: Option[Window]): String = {
    if (partition == null) return null
    window.map(plus(partition, _)).getOrElse(partition)
  }

  def after(s: String): String = shiftPartitions(s, 1)

  // all partitions `count` ahead of `s` including `s` - result size will be count + 1
  // used to compute effected output partitions for a given partition
  def partitionsFrom(s: String, count: Int): Seq[String] = s +: (1 to count).map(shiftPartitions(s, _))

  def partitionsFrom(s: String, window: Window): Seq[String] = {
    val count = math.ceil(window.millis.toDouble / spanMillis).toInt
    partitionsFrom(s, count)
  }

  def before(millis: Long): String = at(millis - spanMillis)

  def shiftPartitions(partition: String, partitionCount: Int): String =
    at(epochMillis(partition) + partitionCount * spanMillis)

  def now: String = at(System.currentTimeMillis())

  def shiftPartitionsBackFromNow(partitionCount: Int): String = shiftPartitions(now, 0 - partitionCount)

  def intervalWindow: Window = {
    if (spanMillis == WindowUtils.Day.millis) WindowUtils.Day
    else if (spanMillis == WindowUtils.Hour.millis) WindowUtils.Hour
    else if (spanMillis % WindowUtils.MinuteMillis == 0) {
      new Window((spanMillis / WindowUtils.MinuteMillis).toInt, TimeUnit.MINUTES)
    } else
      throw new UnsupportedOperationException(s"Partition intervals should be minute-aligned - found ${spanMillis}ms")
  }

  /** Converts a partition value from this spec into the equivalent value in `targetSpec`.
    * The value is parsed with this spec, then rendered with `targetSpec`, including flooring
    * to the target grid when the target has a coarser or differently-offset interval.
    */
  def translate(partitionValue: String, targetSpec: PartitionSpec): String = {
    val millis = epochMillis(partitionValue)
    targetSpec.at(millis)
  }

  /** This label re-rendered canonically when it parses fully and starts a grid interval; None
    * otherwise. Lookup paths use it to admit externally-stored label parts (normalizing
    * padding quirks) without ever letting an off-grid value into coverage math.
    */
  def canonical(label: String): Option[String] =
    scala.util.Try(epochMillis(label)).toOption.filter(ms => grid.floor(ms) == ms).map(at)

  def normalize(partition: String, fallbackSpec: PartitionSpec): String = {
    normalizeStart(partition, fallbackSpec)
  }

  /** Normalizes a start label by translating from the fallback spec's interval start when needed. */
  def normalizeStart(partition: String, fallbackSpec: PartitionSpec): String = {
    if (partition == null) return null
    val startMillis =
      scala.util
        .Try(epochMillis(partition))
        .toOption
        .getOrElse(fallbackSpec.partitionStartMillis(partition))
    at(startMillis)
  }

  /** Normalizes an end label by translating from the fallback spec's interval end when needed. */
  def normalizeEnd(partition: String, fallbackSpec: PartitionSpec): String = {
    if (partition == null) return null
    val endMillis =
      scala.util
        .Try(partitionEndMillis(partition))
        .toOption
        .getOrElse(fallbackSpec.partitionEndMillis(partition))
    at(endMillis - 1)
  }

  private def validateFormat(): Unit = {
    import PartitionSpec._
    // resolution: every grid instant must round-trip exactly, else range arithmetic on labels
    // would drift (e.g. a 3h span with a date-only format collapses 8 partitions into one label)
    val t0 = grid.floor(ProbeInstantMillis)
    Seq(t0, t0 + spanMillis).foreach { t =>
      val label = partitionFormatter.format(Instant.ofEpochMilli(t))
      require(
        !label.contains("'"),
        s"partition format '$format' produces labels containing quotes; labels are embedded in SQL unescaped"
      )
      val parsed = epochMillis(label)
      require(
        parsed == t,
        s"partition format '$format' cannot represent partition boundaries for span=${spanMillis}ms " +
          s"offset=${offsetMillis}ms: '$label' parses back to ${TsUtils.toStr(parsed)} instead of " +
          s"${TsUtils.toStr(t)}. Include time fields, e.g. 'yyyy-MM-dd-HH'."
      )
    }
    // sortability heuristic: consecutive grid labels straddling hour-12, day, month and year
    // rollovers must order lexicographically (catches MM-dd-yyyy, 12-hour clocks, etc.)
    SortabilityProbeBoundaries.foreach { boundary =>
      val prev = grid.floor(boundary - 1)
      val prevLabel = partitionFormatter.format(Instant.ofEpochMilli(prev))
      val nextLabel = partitionFormatter.format(Instant.ofEpochMilli(prev + spanMillis))
      require(
        prevLabel < nextLabel,
        s"partition format '$format' is not lexicographically sortable: " +
          s"'$prevLabel' !< '$nextLabel' (instants ${TsUtils.toStr(prev)} -> ${TsUtils.toStr(prev + spanMillis)})"
      )
    }
    if (format.exists(c => c == ' ' || c == ':')) {
      logger.warn(
        s"partition format '$format' contains spaces or colons which get URL-escaped in object-store " +
          s"paths; prefer dash-separated formats like 'yyyy-MM-dd-HH'")
    }
  }
}

object PartitionSpec {
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  // mid-month, mid-day reference so truncating formats are exposed; fixed for repeatability
  private val ProbeInstantMillis: Long = 1710510443000L // 2024-03-15T13:47:23Z

  private val SortabilityProbeBoundaries: Seq[Long] = Seq(
    1710507600000L, // 2024-03-15T13:00:00Z - crosses hour 12 on 12-hour clocks
    1710547200000L, // 2024-03-16T00:00:00Z - day rollover
    1706745600000L, // 2024-02-01T00:00:00Z - month rollover
    1735689600000L // 2025-01-01T00:00:00Z - year rollover
  )

  val daily: PartitionSpec = PartitionSpec("ds", "yyyy-MM-dd", 24 * 60 * 60 * 1000)

  def hourly(column: String = "ds", format: String = "yyyy-MM-dd-HH"): PartitionSpec =
    PartitionSpec(column, format, 60 * 60 * 1000)
}
