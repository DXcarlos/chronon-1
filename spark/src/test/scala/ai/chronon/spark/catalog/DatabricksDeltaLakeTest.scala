package ai.chronon.spark.catalog

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DatabricksDeltaLakeTest extends AnyFlatSpec with Matchers {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[1]")
    .appName("DatabricksDeltaLakeTest")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  "DatabricksDeltaLake" should "have tableTypeString = delta" in {
    DatabricksDeltaLake.tableTypeString shouldBe "delta"
  }

  it should "support sub-partition filters" in {
    DatabricksDeltaLake.supportSubPartitionsFilter shouldBe true
  }

  it should "return empty partitions for non-existent table" in {
    val result = DatabricksDeltaLake.partitions("non_existent_db.non_existent_table", "")(spark)
    result shouldBe List.empty
  }

  it should "return empty partitionColumnNames for non-existent table" in {
    val result = DatabricksDeltaLake.partitionColumnNames("non_existent_db.non_existent_table")(spark)
    result shouldBe Seq.empty
  }
}

class DatabricksFormatProviderTest extends AnyFlatSpec with Matchers {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[1]")
    .appName("DatabricksFormatProviderTest")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  "DatabricksFormatProvider" should "return None for non-existent table" in {
    val provider = new DatabricksFormatProvider(spark)
    provider.readFormat("non_existent_db.non_existent_table") shouldBe None
  }

  it should "return DatabricksDeltaLake as writeFormat when configured for delta" in {
    spark.conf.set("spark.chronon.table_write.format", "delta")
    try {
      val provider = new DatabricksFormatProvider(spark)
      provider.writeFormat shouldBe DatabricksDeltaLake
    } finally {
      spark.conf.set("spark.chronon.table_write.format", "")
    }
  }

  it should "return Hive as writeFormat when configured as empty" in {
    spark.conf.set("spark.chronon.table_write.format", "")
    val provider = new DatabricksFormatProvider(spark)
    provider.writeFormat shouldBe Hive
  }
}
