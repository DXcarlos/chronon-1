package ai.chronon.spark.catalog

import ai.chronon.api.{PartitionRange, PartitionSpec}
import ai.chronon.spark.submission.SparkSessionBuilder
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StringType
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class DeltaLakeTest extends AnyFlatSpec with BeforeAndAfterAll {

  private implicit lazy val spark: SparkSession =
    SparkSessionBuilder.build(
      "DeltaLakeTest",
      local = true,
      additionalConfig = Some(
        Map(
          "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
          "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        ))
    )

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  it should "derive virtual partitions from Delta log stats for a timestamp column" in {
    val dbName = s"delta_stats_${System.nanoTime()}"
    val tableName = s"$dbName.time_with_stats"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")

    try {
      spark.sql(s"""
        CREATE TABLE $tableName (
          created_at TIMESTAMP,
          user_id STRING
        ) USING DELTA
      """)
      spark.sql(s"""
        INSERT INTO $tableName VALUES
          (TIMESTAMP '2024-01-01 12:00:00', 'user1'),
          (TIMESTAMP '2024-01-03 12:00:00', 'user2')
      """)

      DeltaLake.statsDateRange(tableName, "created_at", PartitionSpec.daily) shouldBe
        Some(StatsDateRange(start = "2024-01-01", end = "2024-01-03"))
      DeltaLake.virtualPartitions(tableName, "created_at", PartitionSpec.daily) shouldBe
        List("2024-01-01", "2024-01-02")
      DeltaLake.firstAvailablePartition(tableName, "created_at", PartitionSpec.daily) shouldBe Some("2024-01-01")
      DeltaLake.lastAvailablePartition(tableName, "created_at", PartitionSpec.daily) shouldBe Some("2024-01-03")
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(s"DROP DATABASE IF EXISTS $dbName")
    }
  }

  it should "floor off-grid timestamps to the sub-daily grid in Delta log stats boundaries" in {
    val dbName = s"delta_subdaily_stats_${System.nanoTime()}"
    val tableName = s"$dbName.subdaily_time_with_stats"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")

    val threeHourSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000)
    val offsetSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000, offsetMillis = 60 * 60 * 1000)

    try {
      spark.sql(s"""
        CREATE TABLE $tableName (
          created_at TIMESTAMP,
          user_id STRING
        ) USING DELTA
      """)
      // min/max are deliberately off-grid; stats boundaries must land on the declared grid,
      // otherwise expandRange seeds an entirely off-grid partition enumeration
      spark.sql(s"""
        INSERT INTO $tableName VALUES
          (TIMESTAMP '2024-01-01 09:17:00', 'user1'),
          (TIMESTAMP '2024-01-01 14:05:00', 'user2')
      """)

      DeltaLake.statsDateRange(tableName, "created_at", threeHourSpec) shouldBe
        Some(StatsDateRange(start = "2024-01-01-09-00", end = "2024-01-01-12-00"))
      DeltaLake.virtualPartitions(tableName, "created_at", threeHourSpec) shouldBe
        List("2024-01-01-09-00")
      // sub-daily grids report the virtual partition containing maxTs; dataWatermark uses its exclusive end
      DeltaLake.lastAvailablePartition(tableName, "created_at", threeHourSpec) shouldBe Some("2024-01-01-12-00")

      // grid phased by 1h: 01:00, 04:00, 07:00, 10:00, 13:00, ...
      DeltaLake.statsDateRange(tableName, "created_at", offsetSpec) shouldBe
        Some(StatsDateRange(start = "2024-01-01-07-00", end = "2024-01-01-13-00"))
      DeltaLake.virtualPartitions(tableName, "created_at", offsetSpec) shouldBe
        List("2024-01-01-07-00", "2024-01-01-10-00")
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(s"DROP DATABASE IF EXISTS $dbName")
    }
  }

  it should "apply readiness semantics to epoch-millis numeric columns in Delta log stats" in {
    val dbName = s"delta_numeric_stats_${System.nanoTime()}"
    val tableName = s"$dbName.numeric_epoch_with_stats"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")

    val threeHourSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000)

    try {
      spark.sql(s"""
        CREATE TABLE $tableName (
          event_ms BIGINT,
          user_id STRING
        ) USING DELTA
      """)
      // epoch millis for 2024-01-01 09:17:00 and 14:05:00 UTC
      spark.sql(s"""
        INSERT INTO $tableName VALUES
          (1704100620000, 'user1'),
          (1704117900000, 'user2')
      """)

      DeltaLake.statsDateRange(tableName, "event_ms", threeHourSpec) shouldBe
        Some(StatsDateRange(start = "2024-01-01-09-00", end = "2024-01-01-12-00"))
      // sub-daily: report the virtual partition containing the max value, same as timestamps
      DeltaLake.lastAvailablePartition(tableName, "event_ms", threeHourSpec) shouldBe Some("2024-01-01-12-00")
      // daily-or-coarser: report the data-bearing partition
      DeltaLake.lastAvailablePartition(tableName, "event_ms", PartitionSpec.daily) shouldBe Some("2024-01-01")
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(s"DROP DATABASE IF EXISTS $dbName")
    }
  }

  it should "derive virtual partitions from Delta log stats for a clustered timestamp column" in {
    val dbName = s"delta_clustered_stats_${System.nanoTime()}"
    val tableName = s"$dbName.time_clustered_with_stats"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")

    try {
      spark.sql(s"""
        CREATE TABLE $tableName (
          created_at TIMESTAMP,
          user_id STRING
        ) USING DELTA
        CLUSTER BY (created_at)
      """)
      spark.sql(s"""
        INSERT INTO $tableName VALUES
          (TIMESTAMP '2024-03-01 12:00:00', 'user1'),
          (TIMESTAMP '2024-03-03 12:00:00', 'user2')
      """)

      DeltaLake.statsDateRange(tableName, "created_at", PartitionSpec.daily) shouldBe
        Some(StatsDateRange(start = "2024-03-01", end = "2024-03-03"))
      DeltaLake.virtualPartitions(tableName, "created_at", PartitionSpec.daily) shouldBe
        List("2024-03-01", "2024-03-02")
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(s"DROP DATABASE IF EXISTS $dbName")
    }
  }

  it should "return the inclusive last partition from Delta log stats for a single-day timestamp range" in {
    val range = StatsDateRange(start = "2024-01-01", end = "2024-01-01")

    range.virtualPartitions(PartitionSpec.daily) shouldBe List("2024-01-01")
    range.firstAvailablePartition shouldBe "2024-01-01"
    range.lastAvailablePartition shouldBe "2024-01-01"
  }

  it should "prefer actual partition metadata over Delta log stats" in {
    val dbName = s"delta_partition_metadata_${System.nanoTime()}"
    val tableName = s"$dbName.time_partitioned"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")

    try {
      spark.sql(s"""
        CREATE TABLE $tableName (
          created_at TIMESTAMP,
          user_id STRING,
          ds STRING
        ) USING DELTA
        PARTITIONED BY (ds)
      """)
      spark.sql(s"""
        INSERT INTO $tableName VALUES
          (TIMESTAMP '2024-06-01 12:00:00', 'user1', '2024-06-01'),
          (TIMESTAMP '2024-06-03 12:00:00', 'user2', '2024-06-03')
      """)

      DeltaLake.virtualPartitions(tableName, "ds", PartitionSpec.daily) should contain theSameElementsAs
        List("2024-06-01", "2024-06-03")
      DeltaLake.firstAvailablePartition(tableName, "ds", PartitionSpec.daily) shouldBe Some("2024-06-01")
      DeltaLake.lastAvailablePartition(tableName, "ds", PartitionSpec.daily) shouldBe Some("2024-06-03")
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(s"DROP DATABASE IF EXISTS $dbName")
    }
  }

  it should "return distinct logical partitions from Delta file metadata" in {
    val dbName = s"delta_duplicate_partition_metadata_${System.nanoTime()}"
    val tableName = s"$dbName.duplicate_partition_files"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")

    try {
      spark.sql(s"""
        CREATE TABLE $tableName (
          user_id STRING,
          ds STRING
        ) USING DELTA
        PARTITIONED BY (ds)
      """)
      spark.sql(s"INSERT INTO $tableName VALUES ('user1', '2024-08-01')")
      spark.sql(s"INSERT INTO $tableName VALUES ('user2', '2024-08-01')")

      DeltaLake.partitions(tableName, "") shouldBe List(Map("ds" -> "2024-08-01"))
      DeltaLake.virtualPartitions(tableName, "ds", PartitionSpec.daily) shouldBe List("2024-08-01")
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(s"DROP DATABASE IF EXISTS $dbName")
    }
  }

  it should "derive Delta log stats boundaries for date and date string columns without timestamp casts" in {
    val dbName = s"delta_date_stats_${System.nanoTime()}"
    val tableName = s"$dbName.date_with_stats"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")

    try {
      spark.sql(s"""
        CREATE TABLE $tableName (
          created_date DATE,
          created_day STRING,
          user_id STRING
        ) USING DELTA
      """)
      spark.sql(s"""
        INSERT INTO $tableName VALUES
          (DATE '2024-07-01', '2024-07-01', 'user1'),
          (DATE '2024-07-03', '2024-07-03', 'user2')
      """)

      DeltaLake.statsDateRange(tableName, "created_date", PartitionSpec.daily) shouldBe
        Some(StatsDateRange(start = "2024-07-01", end = "2024-07-03"))
      DeltaLake.statsDateRange(tableName, "created_day", PartitionSpec.daily) shouldBe
        Some(StatsDateRange(start = "2024-07-01", end = "2024-07-03"))
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(s"DROP DATABASE IF EXISTS $dbName")
    }
  }

  it should "fall back to scanning when Delta log stats do not cover the timestamp column" in {
    val dbName = s"delta_stats_fallback_${System.nanoTime()}"
    val tableName = s"$dbName.time_missing_stats"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")

    try {
      spark.sql(s"""
        CREATE TABLE $tableName (
          indexed_id INT,
          created_at TIMESTAMP,
          user_id STRING
        ) USING DELTA
        TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '1')
      """)
      spark.sql(s"""
        INSERT INTO $tableName VALUES
          (1, TIMESTAMP '2024-02-01 12:00:00', 'user1'),
          (2, TIMESTAMP '2024-02-03 12:00:00', 'user2')
      """)

      DeltaLake.statsDateRange(tableName, "created_at", PartitionSpec.daily) shouldBe None
      DeltaLake.virtualPartitions(tableName, "created_at", PartitionSpec.daily) shouldBe
        List("2024-02-01", "2024-02-02")
      DeltaLake.firstAvailablePartition(tableName, "created_at", PartitionSpec.daily) shouldBe Some("2024-02-01")
      DeltaLake.lastAvailablePartition(tableName, "created_at", PartitionSpec.daily) shouldBe Some("2024-02-03")
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(s"DROP DATABASE IF EXISTS $dbName")
    }
  }
}

/** Integration tests for Delta Lake CLUSTER BY tables.
  * Verifies the full partition-detection and write round-trip through TableUtils so that
  * unfilledRanges correctly identifies already-written partitions in clustered tables.
  */
class DeltaLakeClusteringTest extends AnyFlatSpec with BeforeAndAfterAll {

  private implicit lazy val spark: SparkSession =
    SparkSessionBuilder.build(
      "DeltaLakeClusteringTest",
      local = true,
      additionalConfig = Some(
        Map(
          "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
          "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
          "spark.chronon.partition.column" -> "ds",
          "spark.chronon.table_write.format" -> "delta"
        ))
    )

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  private val dbName = s"delta_cluster_test_${System.nanoTime()}"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
  }

  it should "return empty partitionColumnNames for a CLUSTER BY table" in {
    val tableName = s"$dbName.cluster_partcols"
    try {
      spark.sql(s"""
        CREATE TABLE $tableName (id INT, value STRING, ds STRING)
        USING DELTA CLUSTER BY (ds)
      """)
      DeltaLake.partitionColumnNames(tableName) shouldBe empty
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  it should "return empty primaryPartitions for a CLUSTER BY table" in {
    val tableName = s"$dbName.cluster_primary"
    try {
      spark.sql(s"""
        CREATE TABLE $tableName (id INT, ds STRING)
        USING DELTA CLUSTER BY (ds)
      """)
      spark.sql(s"INSERT INTO $tableName VALUES (1, '2024-01-01'), (2, '2024-01-02')")
      DeltaLake.primaryPartitions(tableName, "ds", "") shouldBe empty
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  it should "detect distinct partition values via scanDistinctPartitions for a CLUSTER BY table" in {
    val tableName = s"$dbName.cluster_scan"
    try {
      spark.sql(s"""
        CREATE TABLE $tableName (id INT, ds STRING)
        USING DELTA CLUSTER BY (ds)
      """)
      spark.sql(s"INSERT INTO $tableName VALUES (1, '2024-01-01'), (2, '2024-01-02'), (3, '2024-01-01')")

      val result = DeltaLake.scanDistinctPartitions(tableName, "ds", "")
      result should contain theSameElementsAs List("2024-01-01", "2024-01-02")
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  it should "detect partitions through TableUtils.partitions() for a CLUSTER BY table" in {
    val tableName = s"$dbName.cluster_tu_parts"
    try {
      spark.sql(s"""
        CREATE TABLE $tableName (id INT, ds STRING)
        USING DELTA CLUSTER BY (ds)
      """)
      spark.sql(s"INSERT INTO $tableName VALUES (1, '2024-01-01'), (2, '2024-01-02'), (3, '2024-01-03')")

      val tu = new TableUtils(spark)
      tu.partitions(tableName) should contain theSameElementsAs List("2024-01-01", "2024-01-02", "2024-01-03")
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  it should "return None from unfilledRanges when all partitions exist in a CLUSTER BY table" in {
    val tableName = s"$dbName.cluster_unfilled"
    try {
      spark.sql(s"""
        CREATE TABLE $tableName (id INT, ds STRING)
        USING DELTA CLUSTER BY (ds)
      """)
      spark.sql(s"INSERT INTO $tableName VALUES (1, '2024-01-01'), (2, '2024-01-02'), (3, '2024-01-03')")

      val tu = new TableUtils(spark)
      val range = PartitionRange("2024-01-01", "2024-01-03")(tu.partitionSpec)
      tu.unfilledRanges(tableName, range) shouldBe None
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  it should "identify missing partitions in a CLUSTER BY table via unfilledRanges" in {
    val tableName = s"$dbName.cluster_unfilled_gap"
    try {
      spark.sql(s"""
        CREATE TABLE $tableName (id INT, ds STRING)
        USING DELTA CLUSTER BY (ds)
      """)
      spark.sql(s"INSERT INTO $tableName VALUES (1, '2024-01-01'), (3, '2024-01-03')")

      val tu = new TableUtils(spark)
      val range = PartitionRange("2024-01-01", "2024-01-03")(tu.partitionSpec)
      val unfilled = tu.unfilledRanges(tableName, range, skipFirstHole = false)
      unfilled shouldBe defined
      unfilled.get.flatMap(_.partitions) should contain theSameElementsAs Seq("2024-01-02")
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  it should "preserve prior partitions when writing via insertPartitions with replaceWhere" in {
    val tableName = s"$dbName.cluster_insert_read"
    try {
      val tu = new TableUtils(spark)
      import spark.implicits._

      // First write: days 1 and 2
      val df1 = Seq((1, "val1", "2024-02-01"), (2, "val2", "2024-02-02")).toDF("id", "value", "ds")
      tu.insertPartitions(df1, tableName, partitionColumns = List("ds"), clusterByColumns = List("ds"))

      tu.partitions(tableName) should contain theSameElementsAs List("2024-02-01", "2024-02-02")

      // Second write: day 3 only — days 1 and 2 must survive
      val df2 = Seq((3, "val3", "2024-02-03")).toDF("id", "value", "ds")
      tu.insertPartitions(df2, tableName, partitionColumns = List("ds"), clusterByColumns = List("ds"))

      tu.partitions(tableName) should contain theSameElementsAs List("2024-02-01", "2024-02-02", "2024-02-03")

      val range = PartitionRange("2024-02-01", "2024-02-03")(tu.partitionSpec)
      tu.unfilledRanges(tableName, range) shouldBe None
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  it should "work with a custom partition column name like featureDt" in {
    val tableName = s"$dbName.cluster_custom_col"
    try {
      val customSpec = PartitionSpec("featureDt", "yyyy-MM-dd", 86400000L)
      val tu = new TableUtils(spark, partitionSpecOverride = Some(customSpec))
      import spark.implicits._

      val df = Seq((1, "val1", "2024-03-01"), (2, "val2", "2024-03-02")).toDF("id", "value", "featureDt")
      tu.insertPartitions(df, tableName, partitionColumns = List("featureDt"), clusterByColumns = List("featureDt"))

      val schema = spark.read.table(tableName).schema
      schema("featureDt").dataType shouldBe StringType

      tu.partitions(tableName) should contain theSameElementsAs List("2024-03-01", "2024-03-02")

      val range = PartitionRange("2024-03-01", "2024-03-02")(tu.partitionSpec)
      tu.unfilledRanges(tableName, range) shouldBe None
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  it should "detect partitions when the clustered column is DateType, not StringType" in {
    val tableName = s"$dbName.cluster_date_col"
    try {
      val customSpec = PartitionSpec("featureDt", "yyyy-MM-dd", 86400000L)
      val tu = new TableUtils(spark, partitionSpecOverride = Some(customSpec))

      spark.sql(s"""
        CREATE TABLE $tableName (
          id INT, value STRING, featureDt DATE
        ) USING DELTA CLUSTER BY (featureDt)
      """)
      spark.sql(s"""
        INSERT INTO $tableName VALUES
          (1, 'v1', DATE '2024-03-01'),
          (2, 'v2', DATE '2024-03-02'),
          (3, 'v3', DATE '2024-03-03')
      """)

      spark.read.table(tableName).schema("featureDt").dataType shouldBe org.apache.spark.sql.types.DateType

      val scanned = DeltaLake.scanDistinctPartitions(tableName, "featureDt", "")
      scanned should contain theSameElementsAs List("2024-03-01", "2024-03-02", "2024-03-03")

      tu.partitions(tableName) should contain theSameElementsAs List("2024-03-01", "2024-03-02", "2024-03-03")

      val range = PartitionRange("2024-03-01", "2024-03-03")(tu.partitionSpec)
      tu.unfilledRanges(tableName, range) shouldBe None
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  it should "detect DateType partitions in a multi-column CLUSTER BY table written via insertPartitions" in {
    val tableName = s"$dbName.cluster_multikey_date"
    try {
      val customSpec = PartitionSpec("featureDt", "yyyy-MM-dd", 86400000L)
      val tu = new TableUtils(spark, partitionSpecOverride = Some(customSpec))
      import spark.implicits._

      val df = Seq(
        (1, java.sql.Date.valueOf("2024-05-01"), "acct1", java.sql.Timestamp.valueOf("2024-05-01 08:30:00")),
        (2, java.sql.Date.valueOf("2024-05-01"), "acct2", java.sql.Timestamp.valueOf("2024-05-01 09:00:00")),
        (3, java.sql.Date.valueOf("2024-05-02"), "acct1", java.sql.Timestamp.valueOf("2024-05-02 10:15:00"))
      ).toDF("id", "featureDt", "accountId", "availabilityTs")

      tu.insertPartitions(df, tableName,
        partitionColumns = List("featureDt"),
        clusterByColumns = List("featureDt", "accountId", "availabilityTs"))

      spark.read.table(tableName).schema("featureDt").dataType shouldBe org.apache.spark.sql.types.DateType
      tu.partitions(tableName) should contain theSameElementsAs List("2024-05-01", "2024-05-02")

      // Second write: day 3 — prior data must survive
      val df2 = Seq(
        (4, java.sql.Date.valueOf("2024-05-03"), "acct3", java.sql.Timestamp.valueOf("2024-05-03 11:00:00"))
      ).toDF("id", "featureDt", "accountId", "availabilityTs")
      tu.insertPartitions(df2, tableName,
        partitionColumns = List("featureDt"),
        clusterByColumns = List("featureDt", "accountId", "availabilityTs"))

      tu.partitions(tableName) should contain theSameElementsAs List("2024-05-01", "2024-05-02", "2024-05-03")

      val range = PartitionRange("2024-05-01", "2024-05-03")(tu.partitionSpec)
      tu.unfilledRanges(tableName, range) shouldBe None
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  it should "throw IllegalStateException with clear message when writing empty DataFrame to clustered table" in {
    val tableName = s"$dbName.cluster_empty_df"
    try {
      val customSpec = PartitionSpec("featureDt", "yyyy-MM-dd", 86400000L)
      val tu = new TableUtils(spark, partitionSpecOverride = Some(customSpec))
      import spark.implicits._

      val df1 = Seq((1, java.sql.Date.valueOf("2024-06-01"), "acct1")).toDF("id", "featureDt", "accountId")
      tu.insertPartitions(df1, tableName,
        partitionColumns = List("featureDt"),
        clusterByColumns = List("featureDt", "accountId"))

      val emptyDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], df1.schema)
      val ex = intercept[IllegalStateException] {
        tu.insertPartitions(emptyDf, tableName,
          partitionColumns = List("featureDt"),
          clusterByColumns = List("featureDt", "accountId"))
      }
      ex.getMessage should include("DataFrame has zero rows")
      ex.getMessage should include("featureDt")
      ex.getMessage should include(tableName)
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  it should "preserve existing data when empty DataFrame write is rejected" in {
    val tableName = s"$dbName.cluster_empty_preserve"
    try {
      val customSpec = PartitionSpec("featureDt", "yyyy-MM-dd", 86400000L)
      val tu = new TableUtils(spark, partitionSpecOverride = Some(customSpec))
      import spark.implicits._

      val df1 = Seq(
        (1, java.sql.Date.valueOf("2024-06-01"), "acct1"),
        (2, java.sql.Date.valueOf("2024-06-02"), "acct2")
      ).toDF("id", "featureDt", "accountId")
      tu.insertPartitions(df1, tableName,
        partitionColumns = List("featureDt"),
        clusterByColumns = List("featureDt", "accountId"))

      val emptyDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], df1.schema)
      intercept[IllegalStateException] {
        tu.insertPartitions(emptyDf, tableName,
          partitionColumns = List("featureDt"),
          clusterByColumns = List("featureDt", "accountId"))
      }

      // Original data must survive the failed write attempt
      tu.partitions(tableName) should contain theSameElementsAs List("2024-06-01", "2024-06-02")
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  it should "place clustering columns first in the schema for Delta stats coverage" in {
    val tableName = s"$dbName.cluster_col_order"
    try {
      val customSpec = PartitionSpec("featureDt", "yyyy-MM-dd", 86400000L)
      val tu = new TableUtils(spark, partitionSpecOverride = Some(customSpec))
      import spark.implicits._

      // DataFrame with featureDt buried in the middle — clustering must move it first
      val df = Seq(
        (1, "val1", 100.0, java.sql.Date.valueOf("2024-07-01"), "acct1")
      ).toDF("id", "value", "amount", "featureDt", "accountId")

      tu.insertPartitions(df, tableName,
        partitionColumns = List("featureDt"),
        clusterByColumns = List("featureDt", "accountId"))

      val tableColumns = spark.read.table(tableName).columns.toSeq
      // Clustering columns must be first two, in declared order
      tableColumns.head shouldBe "featureDt"
      tableColumns(1) shouldBe "accountId"
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  it should "generate string-based where clauses for DateType partition columns" in {
    val tableName = s"$dbName.cluster_date_where"
    try {
      spark.sql(s"""
        CREATE TABLE $tableName (id INT, featureDt DATE)
        USING DELTA CLUSTER BY (featureDt)
      """)
      spark.sql(s"INSERT INTO $tableName VALUES (1, DATE '2024-08-15')")

      val customSpec = PartitionSpec("featureDt", "yyyy-MM-dd", 86400000L)
      val tu = new TableUtils(spark, partitionSpecOverride = Some(customSpec))
      val range = PartitionRange("2024-08-01", "2024-08-31")(customSpec)

      // typedWhereClauses must produce string comparisons, not timestamp_millis
      val clauses = tu.typedWhereClauses(range, "featureDt", org.apache.spark.sql.types.DateType)
      clauses.foreach { clause =>
        clause should not include "timestamp_millis"
        clause should (include(">=") or include("<"))
      }

      // Verify the clauses actually filter correctly
      val filtered = spark.read.table(tableName).where(clauses.mkString(" AND "))
      filtered.count() shouldBe 1
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  it should "keep partition columns last for non-clustered tables (Hive convention)" in {
    val tableName = s"$dbName.hive_col_order"
    try {
      val tu = new TableUtils(spark)
      import spark.implicits._

      val df = Seq(("2024-07-01", 1, "val1")).toDF("ds", "id", "value")

      // No clusterByColumns → Hive-style: ds goes last
      tu.insertPartitions(df, tableName, partitionColumns = List("ds"))

      val tableColumns = spark.read.table(tableName).columns.toSeq
      tableColumns.last shouldBe "ds"
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }
}
