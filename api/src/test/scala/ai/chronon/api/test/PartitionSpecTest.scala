package ai.chronon.api.test

import ai.chronon.api.{PartitionSpec, Window, TimeUnit}
import ai.chronon.api.Extensions.WindowUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PartitionSpecTest extends AnyFlatSpec with Matchers {
  
  private val dailySpec = PartitionSpec.daily
  
  "PartitionSpec.expandRange" should "expand date range into individual dates" in {
    val result = dailySpec.expandRange("2024-01-01", "2024-01-05")
    val expected = List("2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05")
    result should be(expected)
  }
  
  it should "handle single day range" in {
    val result = dailySpec.expandRange("2024-01-15", "2024-01-15")
    result should be(List("2024-01-15"))
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
}