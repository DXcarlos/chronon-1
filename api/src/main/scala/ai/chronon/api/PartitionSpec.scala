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
import org.slf4j.LoggerFactory

import java.text.SimpleDateFormat
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.{Locale, TimeZone}

/** Describes how a table is partitioned in time.
  *
  * A partition label is the formatted UTC instant of its interval *start*. The grid of valid
  * interval starts is `{ k * spanMillis + offsetMillis }`, and a label covers the half-open
  * interval `[epochMillis(label), epochMillis(label) + spanMillis)`.
  *
  * `format` must produce labels that sort lexicographically in time order (zero-padded,
  * most-significant field first, e.g. "yyyy-MM-dd-HH"). Construction probes a handful of
  * boundary-straddling grid instants to reject non-sortable or under-resolving formats, but
  * the sortability contract ultimately rests with the config author.
  */
case class PartitionSpec(column: String, format: String, spanMillis: Long, offsetMillis: Long = 0) {
  import PartitionSpec._

  require(spanMillis > 0, s"partition span must be positive, got ${spanMillis}ms")
  require(
    DayMillis % spanMillis == 0 || spanMillis % DayMillis == 0,
    s"partition span must cleanly divide 24h or be a whole number of days, got ${spanMillis}ms"
  )
  require(
    offsetMillis >= 0 && offsetMillis < spanMillis,
    s"partition anchor offset must be in [0, span), got ${offsetMillis}ms for span ${spanMillis}ms"
  )
  require(
    spanMillis % MinuteMillis == 0 && offsetMillis % MinuteMillis == 0,
    s"partition span and offset must be whole minutes, got span=${spanMillis}ms offset=${offsetMillis}ms"
  )
  validateFormat()

  private def partitionFormatter =
    DateTimeFormatter
      .ofPattern(format, Locale.US)
      .withZone(ZoneOffset.UTC)

  private def sdf = {
    val formatter = new SimpleDateFormat(format)
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"))
    formatter
  }

  private def formatInstant(millis: Long): String = partitionFormatter.format(Instant.ofEpochMilli(millis))

  /** start of the partition interval containing `millis` */
  def floor(millis: Long): Long =
    // floorDiv (not /) so pre-epoch and pre-anchor timestamps floor downward instead of toward zero
    Math.floorDiv(millis - offsetMillis, spanMillis) * spanMillis + offsetMillis

  /** label of the partition whose coverage [start, start + span) contains `millis` */
  def at(millis: Long): String = formatInstant(floor(millis))

  /** Strict by design: an off-grid label means the caller's range arithmetic would silently
    * miss physical partitions, so we fail loudly instead of snapping.
    */
  def epochMillis(partition: String): Long = {
    val parsed = sdf.parse(partition).getTime
    require(
      (parsed - offsetMillis) % spanMillis == 0,
      s"'$partition' is not aligned to the partition grid (span=${spanMillis}ms, offset=${offsetMillis}ms); " +
        s"nearest grid label: ${at(parsed)}"
    )
    parsed
  }

  /** half-open coverage of one partition, in the inclusive-millis TimeRange convention */
  def coverage(label: String): TimeRange = {
    val start = epochMillis(label)
    TimeRange(start, start + spanMillis - 1)(this)
  }

  def shift(date: String, steps: Int): String = formatInstant(epochMillis(date) + steps * spanMillis)

  def before(s: String): String = shift(s, -1)

  def after(s: String): String = shift(s, 1)

  def before(millis: Long): String = at(millis - spanMillis)

  def now: String = at(System.currentTimeMillis())

  def shiftBackFromNow(steps: Int): String = shift(now, -steps)

  // at() floors, so window arithmetic always lands back on the grid
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

  // all partitions `count` ahead of `s` including `s` - result size will be count + 1
  // used to compute effected output partitions for a given partition
  def partitionsFrom(s: String, count: Int): Seq[String] = s +: (1 to count).map(shift(s, _))

  def partitionsFrom(s: String, window: Window): Seq[String] = {
    val count = math.ceil(window.millis.toDouble / spanMillis).toInt
    partitionsFrom(s, count)
  }

  def expandRange(startLabel: String, endLabel: String): List[String] = {
    val start = epochMillis(startLabel)
    val end = epochMillis(endLabel)
    if (start > end) Nil
    else Iterator.iterate(start)(_ + spanMillis).takeWhile(_ <= end).map(at).toList
  }

  def isDaily: Boolean = spanMillis == DayMillis && offsetMillis == 0

  /** same span and anchor - labels translate 1:1 between the two specs (format may differ) */
  def gridEquals(other: PartitionSpec): Boolean =
    spanMillis == other.spanMillis && offsetMillis == other.offsetMillis

  /** 1 for daily and multi-day spans; used to convert day-denominated configs like stepDays */
  def partitionsPerDay: Int = math.max(1, (DayMillis / spanMillis).toInt)

  def intervalWindow: Window = {
    if (spanMillis % DayMillis == 0) new Window((spanMillis / DayMillis).toInt, TimeUnit.DAYS)
    else if (spanMillis % HourMillis == 0) new Window((spanMillis / HourMillis).toInt, TimeUnit.HOURS)
    else new Window((spanMillis / MinuteMillis).toInt, TimeUnit.MINUTES)
  }

  def translate(date: String, targetSpec: PartitionSpec): String = {
    val millis = epochMillis(date)
    targetSpec.at(millis)
  }

  private def validateFormat(): Unit = {
    // resolution: every grid instant must round-trip exactly, else range arithmetic on labels
    // would drift (e.g. a 3h span with a date-only format collapses 8 partitions into one label)
    val t0 = floor(ProbeInstantMillis)
    Seq(t0, t0 + spanMillis).foreach { t =>
      val label = formatInstant(t)
      require(
        !label.contains("'"),
        s"partition format '$format' produces labels containing quotes; labels are embedded in SQL unescaped"
      )
      val parsed = sdf.parse(label).getTime
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
      val prev = floor(boundary - 1)
      val prevLabel = formatInstant(prev)
      val nextLabel = formatInstant(prev + spanMillis)
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
  private val logger = LoggerFactory.getLogger(getClass)

  private val MinuteMillis: Long = 60 * 1000L
  private val HourMillis: Long = 60 * MinuteMillis
  private val DayMillis: Long = 24 * HourMillis

  // mid-month, mid-day reference so truncating formats are exposed; fixed for repeatability
  private val ProbeInstantMillis: Long = 1710510443000L // 2024-03-15T13:47:23Z

  private val SortabilityProbeBoundaries: Seq[Long] = Seq(
    1710507600000L, // 2024-03-15T13:00:00Z - crosses hour 12 on 12-hour clocks
    1710547200000L, // 2024-03-16T00:00:00Z - day rollover
    1706745600000L, // 2024-02-01T00:00:00Z - month rollover
    1735689600000L // 2025-01-01T00:00:00Z - year rollover
  )

  val daily: PartitionSpec = PartitionSpec("ds", "yyyy-MM-dd", DayMillis)

  def hourly(column: String = "ds", format: String = "yyyy-MM-dd-HH"): PartitionSpec =
    PartitionSpec(column, format, HourMillis)
}
