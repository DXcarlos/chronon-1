package ai.chronon.api.test

import ai.chronon.api.{PartitionRange, PartitionSpec, Window, TimeUnit}
import ai.chronon.api.Extensions.WindowUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PartitionSpecTest extends AnyFlatSpec with Matchers {

  private val dailySpec = PartitionSpec.daily
  private val compactSpec = PartitionSpec("ds", "yyyyMMdd", 24 * 60 * 60 * 1000)
  private val threeHourSpec = PartitionSpec("ds", "yyyy-MM-dd HH:mm", 3 * 60 * 60 * 1000)
  private val fifteenMinuteSpec = PartitionSpec("ds", "yyyy-MM-dd HH:mm", 15 * 60 * 1000)
  
  "PartitionSpec.expandRange" should "expand date range into individual dates" in {
    val result = dailySpec.expandRange("2024-01-01", "2024-01-05")
    val expected = List("2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05")
    result should be(expected)
  }
  
  it should "handle single day range" in {
    val result = dailySpec.expandRange("2024-01-15", "2024-01-15")
    result should be(List("2024-01-15"))
  }

  it should "expand sub-daily ranges by the partition interval" in {
    val result = threeHourSpec.expandRange("2024-01-15 00:00", "2024-01-15 09:00")
    result should be(List("2024-01-15 00:00", "2024-01-15 03:00", "2024-01-15 06:00", "2024-01-15 09:00"))
  }
  
  it should "expand range across month boundaries" in {
    val result = dailySpec.expandRange("2024-01-30", "2024-02-02")
    val expected = List("2024-01-30", "2024-01-31", "2024-02-01", "2024-02-02")
    result should be(expected)
  }
  
  "PartitionSpec.plusFast" should "add one day correctly" in {
    val result = dailySpec.plusFast("2024-01-01", WindowUtils.Day)
    result should be("2024-01-02")
  }
  
  it should "add multiple days correctly" in {
    val window = new Window(3, TimeUnit.DAYS)
    val result = dailySpec.plusFast("2024-01-01", window)
    result should be("2024-01-04")
  }
  
  it should "handle month boundary correctly when adding days" in {
    val result = dailySpec.plusFast("2024-01-31", WindowUtils.Day)
    result should be("2024-02-01")
  }
  
  "PartitionSpec.afterFast" should "return the next day" in {
    val result = dailySpec.afterFast("2024-03-15")
    result should be("2024-03-16")
  }

  it should "return the next sub-daily partition" in {
    threeHourSpec.afterFast("2024-03-15 21:00") should be("2024-03-16 00:00")
    fifteenMinuteSpec.afterFast("2024-03-15 00:45") should be("2024-03-15 01:00")
  }
  
  "PartitionSpec.minusFast" should "subtract days correctly" in {
    val window = new Window(2, TimeUnit.DAYS)
    val result = dailySpec.minusFast("2024-01-03", window)
    result should be("2024-01-01")
  }
  
  it should "handle month boundary correctly when subtracting days" in {
    val result = dailySpec.minusFast("2024-02-01", WindowUtils.Day)
    result should be("2024-01-31")
  }
  
  "PartitionSpec.calendarGrain" should "return HOUR_OF_DAY for HOURS" in {
    val window = new Window(1, TimeUnit.HOURS)
    val result = dailySpec.calendarGrain(window)
    result should be(java.util.Calendar.HOUR_OF_DAY)
  }
  
  it should "return DAY_OF_MONTH for DAYS" in {
    val window = new Window(1, TimeUnit.DAYS)
    val result = dailySpec.calendarGrain(window)
    result should be(java.util.Calendar.DAY_OF_MONTH)
  }
  
  it should "return MINUTE for MINUTES" in {
    val window = new Window(1, TimeUnit.MINUTES)
    val result = dailySpec.calendarGrain(window)
    result should be(java.util.Calendar.MINUTE)
  }

  "PartitionSpec.translate" should "convert from yyyy-MM-dd to yyyyMMdd" in {
    dailySpec.translate("2025-11-25", compactSpec) should be("20251125")
    dailySpec.translate("2025-01-01", compactSpec) should be("20250101")
    dailySpec.translate("1970-01-01", compactSpec) should be("19700101")
  }

  it should "convert from yyyyMMdd to yyyy-MM-dd" in {
    compactSpec.translate("20251125", dailySpec) should be("2025-11-25")
    compactSpec.translate("20250101", dailySpec) should be("2025-01-01")
    compactSpec.translate("19700101", dailySpec) should be("1970-01-01")
  }

  it should "convert between daily and sub-daily formats at interval starts" in {
    dailySpec.translate("2025-11-25", threeHourSpec) should be("2025-11-25 00:00")
    threeHourSpec.translate("2025-11-25 18:00", dailySpec) should be("2025-11-25")
  }

  it should "round-trip between formats" in {
    val date = "2025-12-01"
    val roundTripped = compactSpec.translate(dailySpec.translate(date, compactSpec), dailySpec)
    roundTripped should be(date)
  }

  "PartitionSpec date arithmetic" should "work correctly with yyyyMMdd format" in {
    compactSpec.after("20251125") should be("20251126")
    compactSpec.before("20251201") should be("20251130")
    compactSpec.shiftPartitions("20251231", 1) should be("20260101")
  }

  it should "support minus with window in yyyyMMdd format" in {
    val twoDays = new Window(2, TimeUnit.DAYS)
    compactSpec.minus("20251125", twoDays) should be("20251123")
  }

  "PartitionRange.coveringRange" should "convert CLI dates (yyyy-MM-dd) to yyyyMMdd" in {
    val cliRange = PartitionRange("2025-11-25", "2025-12-01")(dailySpec)
    val covering = cliRange.coveringRange(compactSpec)

    covering.start should be("20251125")
    covering.end should be("20251201")
    covering.partitionSpec should be(compactSpec)
  }

  it should "preserve range validity after translation" in {
    val cliRange = PartitionRange("2025-11-25", "2025-12-01")(dailySpec)
    val covering = cliRange.coveringRange(compactSpec)

    covering.wellDefined should be(true)
    covering.partitions.size should be(7)
    covering.partitions.head should be("20251125")
    covering.partitions.last should be("20251201")
  }

  it should "be a no-op when source and target formats match" in {
    val range = PartitionRange("2025-11-25", "2025-12-01")(dailySpec)
    val covering = range.coveringRange(dailySpec)

    covering.start should be("2025-11-25")
    covering.end should be("2025-12-01")
  }
}
