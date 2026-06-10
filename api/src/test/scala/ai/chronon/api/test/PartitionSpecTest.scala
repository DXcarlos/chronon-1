package ai.chronon.api.test

import ai.chronon.api.{PartitionRange, PartitionSpec, TimeUnit, Window}
import ai.chronon.api.Extensions.WindowUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PartitionSpecTest extends AnyFlatSpec with Matchers {

  private val MinuteMillis = 60 * 1000L
  private val HourMillis = 60 * MinuteMillis
  private val DayMillis = 24 * HourMillis

  private val dailySpec = PartitionSpec.daily
  private val compactSpec = PartitionSpec("ds", "yyyyMMdd", DayMillis)
  private val hourlySpec = PartitionSpec.hourly()
  private val minuteFormat = "yyyy-MM-dd-HH-mm"
  // 3-hourly grid anchored at 01:00 - boundaries 01:00, 04:00, ..., 22:00
  private val threeHourlyAt1 = PartitionSpec("ds", minuteFormat, 3 * HourMillis, HourMillis)

  private def utc(date: String, hour: Int = 0, minute: Int = 0): Long =
    java.time.Instant.parse(f"${date}T$hour%02d:$minute%02d:00Z").toEpochMilli

  "PartitionSpec construction" should "validate spans, offsets and formats" in {
    // spans must cleanly divide 24h or be whole days
    an[IllegalArgumentException] should be thrownBy PartitionSpec("ds", minuteFormat, 5 * HourMillis)
    an[IllegalArgumentException] should be thrownBy PartitionSpec("ds", minuteFormat, 7 * HourMillis)
    an[IllegalArgumentException] should be thrownBy PartitionSpec("ds", minuteFormat, 0)
    Seq(1, 2, 3, 4, 6, 8, 12).foreach { h =>
      noException should be thrownBy PartitionSpec("ds", minuteFormat, h * HourMillis)
    }
    noException should be thrownBy PartitionSpec("ds", minuteFormat, 90 * MinuteMillis)
    noException should be thrownBy PartitionSpec("ds", "yyyy-MM-dd", 7 * DayMillis)

    // offsets must sit in [0, span) and be expressible in the format
    an[IllegalArgumentException] should be thrownBy PartitionSpec("ds", minuteFormat, HourMillis, -1 * MinuteMillis)
    an[IllegalArgumentException] should be thrownBy PartitionSpec("ds", minuteFormat, HourMillis, HourMillis)
    an[IllegalArgumentException] should be thrownBy PartitionSpec("ds", "yyyy-MM-dd-HH", HourMillis,
                                                                  30 * MinuteMillis)
    noException should be thrownBy PartitionSpec("ds", minuteFormat, 3 * HourMillis, HourMillis)

    // formats must resolve the grid and sort lexicographically
    an[IllegalArgumentException] should be thrownBy PartitionSpec("ds", "yyyy-MM-dd", HourMillis)
    an[IllegalArgumentException] should be thrownBy PartitionSpec("ds", "MM-dd-yyyy", DayMillis)
    an[IllegalArgumentException] should be thrownBy PartitionSpec("ds", "yyyy-MM-dd-hh", HourMillis)
  }

  "PartitionSpec.at" should "floor instants onto the anchored grid" in {
    threeHourlyAt1.at(utc("2024-01-05", 1)) should be("2024-01-05-01-00")
    threeHourlyAt1.at(utc("2024-01-05", 3, 59)) should be("2024-01-05-01-00")
    threeHourlyAt1.at(utc("2024-01-05", 4)) should be("2024-01-05-04-00")
    // pre-anchor instants belong to the previous day's last partition
    threeHourlyAt1.at(utc("2024-01-05", 0, 59)) should be("2024-01-04-22-00")

    // daily span with a 23h anchor wraps across midnight
    val dailyAt23 = PartitionSpec("ds", minuteFormat, DayMillis, 23 * HourMillis)
    dailyAt23.at(utc("2024-01-02", 0, 30)) should be("2024-01-01-23-00")
    dailyAt23.at(utc("2024-01-02", 23, 30)) should be("2024-01-02-23-00")
  }

  "PartitionSpec.epochMillis" should "reject off-grid labels with the nearest grid label" in {
    val ex = the[IllegalArgumentException] thrownBy threeHourlyAt1.epochMillis("2024-01-05-02-30")
    ex.getMessage should include("2024-01-05-01-00")
    noException should be thrownBy threeHourlyAt1.epochMillis("2024-01-05-04-00")
  }

  "PartitionSpec" should "step whole partitions across rollovers" in {
    hourlySpec.shift("2024-01-01-23", 2) should be("2024-01-02-01")
    hourlySpec.shift("2024-01-02-01", -2) should be("2024-01-01-23")
    threeHourlyAt1.after("2024-01-05-22-00") should be("2024-01-06-01-00")
    threeHourlyAt1.before("2024-01-05-01-00") should be("2024-01-04-22-00")

    compactSpec.after("20251125") should be("20251126")
    compactSpec.before("20251201") should be("20251130")
    compactSpec.shift("20251231", 1) should be("20260101")

    // partitionsFrom ceils window length to partition steps
    val ninetyMin = new Window(90, TimeUnit.MINUTES)
    hourlySpec.partitionsFrom("2024-01-01-10", ninetyMin) should be(
      Seq("2024-01-01-10", "2024-01-01-11", "2024-01-01-12"))
  }

  it should "do window arithmetic on the grid" in {
    dailySpec.plus("2024-01-01", WindowUtils.Day) should be("2024-01-02")
    dailySpec.plus("2024-01-01", new Window(3, TimeUnit.DAYS)) should be("2024-01-04")
    dailySpec.plus("2024-01-31", WindowUtils.Day) should be("2024-02-01")
    dailySpec.after("2024-03-15") should be("2024-03-16")
    dailySpec.minus("2024-01-03", new Window(2, TimeUnit.DAYS)) should be("2024-01-01")
    dailySpec.minus("2024-02-01", WindowUtils.Day) should be("2024-01-31")
    compactSpec.minus("20251125", new Window(2, TimeUnit.DAYS)) should be("20251123")

    // 90m back from 04:00 is 02:30, which floors back onto the 01:00 grid
    threeHourlyAt1.minus("2024-01-05-04-00", new Window(90, TimeUnit.MINUTES)) should be("2024-01-05-01-00")
  }

  it should "describe its grid via partitionsPerDay and intervalWindow" in {
    dailySpec.partitionsPerDay should be(1)
    hourlySpec.partitionsPerDay should be(24)
    threeHourlyAt1.partitionsPerDay should be(8)
    PartitionSpec("ds", "yyyy-MM-dd", 7 * DayMillis).partitionsPerDay should be(1)

    dailySpec.intervalWindow should be(new Window(1, TimeUnit.DAYS))
    hourlySpec.intervalWindow should be(new Window(1, TimeUnit.HOURS))
    threeHourlyAt1.intervalWindow should be(new Window(3, TimeUnit.HOURS))
    PartitionSpec("ds", minuteFormat, 90 * MinuteMillis).intervalWindow should be(new Window(90, TimeUnit.MINUTES))
  }

  "PartitionSpec.expandRange" should "expand ranges by partition step" in {
    dailySpec.expandRange("2024-01-01", "2024-01-05") should be(
      List("2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"))
    dailySpec.expandRange("2024-01-15", "2024-01-15") should be(List("2024-01-15"))
    dailySpec.expandRange("2024-01-30", "2024-02-02") should be(
      List("2024-01-30", "2024-01-31", "2024-02-01", "2024-02-02"))

    hourlySpec.expandRange("2024-01-01-22", "2024-01-02-02") should be(
      List("2024-01-01-22", "2024-01-01-23", "2024-01-02-00", "2024-01-02-01", "2024-01-02-02"))
    threeHourlyAt1.expandRange("2024-01-31-19-00", "2024-02-01-01-00") should be(
      List("2024-01-31-19-00", "2024-01-31-22-00", "2024-02-01-01-00"))
  }

  "PartitionSpec.translate" should "translate labels across specs" in {
    dailySpec.translate("2025-11-25", compactSpec) should be("20251125")
    dailySpec.translate("2025-01-01", compactSpec) should be("20250101")
    dailySpec.translate("1970-01-01", compactSpec) should be("19700101")
    compactSpec.translate("20251125", dailySpec) should be("2025-11-25")
    compactSpec.translate(dailySpec.translate("2025-12-01", compactSpec), dailySpec) should be("2025-12-01")

    // coarser targets floor; finer targets land on the grid
    hourlySpec.translate("2024-01-01-22", dailySpec) should be("2024-01-01")
    dailySpec.translate("2024-01-01", hourlySpec) should be("2024-01-01-00")
  }

  "PartitionRange.translate" should "translate ranges across same-grain specs" in {
    val cliRange = PartitionRange("2025-11-25", "2025-12-01")(dailySpec)
    val translated = cliRange.translate(compactSpec)
    translated.start should be("20251125")
    translated.end should be("20251201")
    translated.partitionSpec should be(compactSpec)
    translated.wellDefined should be(true)
    translated.partitions.size should be(7)

    val noop = cliRange.translate(dailySpec)
    noop.start should be("2025-11-25")
    noop.end should be("2025-12-01")
  }

  "PartitionRange.coverage" should "derive half-open coverage" in {
    // daily numerically equals the previous toTimeRange semantics: [start 00:00, end+1d - 1ms]
    val daily = PartitionRange("2024-01-01", "2024-01-03")(dailySpec)
    daily.coverage.start should be(1704067200000L)
    daily.coverage.end should be(1704326400000L - 1)

    val hour = PartitionRange("2024-01-01-22", "2024-01-01-22")(hourlySpec)
    hour.coverage.start should be(utc("2024-01-01", 22))
    hour.coverage.end should be(utc("2024-01-01", 23) - 1)

    val spec = threeHourlyAt1.coverage("2024-01-05-01-00")
    spec.start should be(utc("2024-01-05", 1))
    spec.end should be(utc("2024-01-05", 4) - 1)
  }

  "PartitionRange.coveringRange" should "compute covering ranges across grains" in {
    val day = PartitionRange("2024-01-05", "2024-01-05")(dailySpec)
    day.coveringRange(dailySpec) should be theSameInstanceAs day

    // [00:00, 24:00) intersects the 3h@01:00 partitions from yesterday 22:00 through today 22:00
    val covering = day.coveringRange(threeHourlyAt1)
    covering.start should be("2024-01-04-22-00")
    covering.end should be("2024-01-05-22-00")
    covering.partitions.size should be(9)

    val slice = PartitionRange("2024-01-05-01-00", "2024-01-05-04-00")(threeHourlyAt1)
    slice.coveringRange(dailySpec).start should be("2024-01-05")
    slice.coveringRange(dailySpec).end should be("2024-01-05")

    // a partition straddling midnight needs both days
    val straddling = PartitionRange("2024-01-05-22-00", "2024-01-05-22-00")(threeHourlyAt1)
    straddling.coveringRange(dailySpec).start should be("2024-01-05")
    straddling.coveringRange(dailySpec).end should be("2024-01-06")

    val hours = PartitionRange("2024-01-05-03", "2024-01-05-05")(hourlySpec)
    hours.coveringRange(threeHourlyAt1).start should be("2024-01-05-01-00")
    hours.coveringRange(threeHourlyAt1).end should be("2024-01-05-04-00")
  }

  "PartitionRange.coveredPartitions" should "cover partitions across grains" in {
    // same grain: 1:1 translation
    PartitionRange.coveredPartitions(Seq("2024-01-05", "2024-01-06"), dailySpec, compactSpec) should be(
      Seq("20240105", "20240106"))

    // coarser labels expand into finer partitions, but only FULLY contained ones: the
    // day-straddling 22:00 partitions need both surrounding daily labels
    val covered = PartitionRange.coveredPartitions(Seq("2024-01-05"), dailySpec, threeHourlyAt1)
    covered should contain("2024-01-05-01-00")
    covered should not contain "2024-01-04-22-00" // [22:00, 01:00) also needs daily 2024-01-04
    covered should not contain "2024-01-05-22-00" // also needs daily 2024-01-06
    covered.size should be(7)

    val coveredTwoDays =
      PartitionRange.coveredPartitions(Seq("2024-01-05", "2024-01-06"), dailySpec, threeHourlyAt1)
    coveredTwoDays should contain("2024-01-05-22-00") // both covering days present now
    coveredTwoDays.size should be(15)

    // a coarser partition counts only when ALL its finer labels exist
    val allHours = (0 to 23).map(h => f"2024-01-05-$h%02d")
    PartitionRange.coveredPartitions(allHours, hourlySpec, dailySpec) should be(Seq("2024-01-05"))
    PartitionRange.coveredPartitions(allHours.drop(1), hourlySpec, dailySpec) should be(Seq.empty)
  }

  "PartitionRange.steps" should "tumble by partition count and by days" in {
    val range = PartitionRange("2024-01-01-00", "2024-01-01-05")(hourlySpec)
    range.steps(4).map(r => (r.start, r.end)) should be(
      Seq(("2024-01-01-00", "2024-01-01-03"), ("2024-01-01-04", "2024-01-01-05")))

    val dailyRange = PartitionRange("2024-01-01", "2024-01-10")(dailySpec)
    dailyRange.stepsByDays(3) should be(dailyRange.steps(3))

    val hourly = PartitionRange("2024-01-01-00", "2024-01-02-05")(hourlySpec).stepsByDays(1)
    hourly.head.partitions.size should be(24)
    hourly.last.partitions.size should be(6) // ragged tail
  }

  "TimeRange.toTimePoints" should "stride on the anchored grid" in {
    implicit val spec: PartitionSpec = threeHourlyAt1
    val tr = ai.chronon.api.TimeRange(utc("2024-01-05", 2), utc("2024-01-05", 8))
    tr.toTimePoints should be(Array(utc("2024-01-05", 1), utc("2024-01-05", 4), utc("2024-01-05", 7)))
  }

  "PartitionRange.collapseToRange" should "collapse and round-trip hourly labels with gaps" in {
    implicit val spec: PartitionSpec = hourlySpec
    val labels = Seq("2024-01-01-22", "2024-01-01-23", "2024-01-02-00", "2024-01-02-05")
    val ranges = PartitionRange.collapseToRange(labels)
    ranges.map(r => (r.start, r.end)) should be(
      Seq(("2024-01-01-22", "2024-01-02-00"), ("2024-01-02-05", "2024-01-02-05")))
    ranges.flatMap(_.partitions) should be(labels)
  }
}
