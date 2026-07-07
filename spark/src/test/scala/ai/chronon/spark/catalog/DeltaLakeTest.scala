package ai.chronon.spark.catalog

import ai.chronon.api.PartitionSpec
import ai.chronon.spark.submission.SparkSessionBuilder
import org.apache.spark.sql.SparkSession
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
      // readiness reports the partition containing maxTs; virtualPartitions stays conservative
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
      // the single INSERT commit landed long after the 12:00 interval closed — a chunked
      // (backfill-style) write, so the tail interval counts as complete
      DeltaLake.virtualPartitions(tableName, "created_at", threeHourSpec) shouldBe
        List("2024-01-01-09-00", "2024-01-01-12-00")
      DeltaLake.lastAvailablePartition(tableName, "created_at", threeHourSpec) shouldBe Some("2024-01-01-12-00")

      // grid phased by 1h: 01:00, 04:00, 07:00, 10:00, 13:00, ...
      DeltaLake.statsDateRange(tableName, "created_at", offsetSpec) shouldBe
        Some(StatsDateRange(start = "2024-01-01-07-00", end = "2024-01-01-13-00"))
      DeltaLake.virtualPartitions(tableName, "created_at", offsetSpec) shouldBe
        List("2024-01-01-07-00", "2024-01-01-10-00", "2024-01-01-13-00")
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
      // sub-daily: the single commit landed after the 12:00 interval closed (chunked write),
      // so the tail interval counts as complete, same as timestamps
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

  it should "report the tail interval once each chunk lands after its interval closes" in {
    val dbName = s"delta_chunked_write_${System.nanoTime()}"
    val tableName = s"$dbName.chunked_time_with_stats"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")

    val threeHourSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000)

    try {
      spark.sql(s"""
        CREATE TABLE $tableName (
          created_at TIMESTAMP,
          user_id STRING
        ) USING DELTA
      """)
      // two 3h chunks landing as separate commits, both long after their intervals closed —
      // the shape of a chunked batch writer catching up
      spark.sql(s"""
        INSERT INTO $tableName VALUES
          (TIMESTAMP '2024-01-02 00:30:00', 'user1'),
          (TIMESTAMP '2024-01-02 02:45:00', 'user2')
      """)
      spark.sql(s"""
        INSERT INTO $tableName VALUES
          (TIMESTAMP '2024-01-02 03:15:00', 'user3'),
          (TIMESTAMP '2024-01-02 05:59:00', 'user4')
      """)

      DeltaLake.lastAvailablePartition(tableName, "created_at", threeHourSpec) shouldBe Some("2024-01-02-03-00")
      DeltaLake.virtualPartitions(tableName, "created_at", threeHourSpec) shouldBe
        List("2024-01-02-00-00", "2024-01-02-03-00")

      // the sensor watermark credits the full tail interval; time_partitioned specs carry the
      // timestamp column as their partition column
      val watermarkSpec = PartitionSpec("created_at", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000)
      TableUtils(spark).dataWatermarkMillis(tableName, Some(watermarkSpec)) shouldBe
        Some(watermarkSpec.partitionEndMillis("2024-01-02-03-00"))
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(s"DROP DATABASE IF EXISTS $dbName")
    }
  }

  it should "keep interval-end readiness while the tail interval is still open" in {
    val dbName = s"delta_open_interval_${System.nanoTime()}"
    val tableName = s"$dbName.open_interval_time_with_stats"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")

    val threeHourSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000)
    val formatter = java.time.format.DateTimeFormatter
      .ofPattern("yyyy-MM-dd HH:mm:ss")
      .withZone(java.time.ZoneOffset.UTC)

    try {
      spark.sql(s"""
        CREATE TABLE $tableName (
          created_at TIMESTAMP,
          user_id STRING
        ) USING DELTA
      """)
      // data in the next interval: it cannot close during the test, so the commit always
      // lands before the interval end — a stream mid-interval, not a finished chunk
      val openIntervalMillis = System.currentTimeMillis() + threeHourSpec.spanMillis
      val openTs = formatter.format(java.time.Instant.ofEpochMilli(openIntervalMillis))
      spark.sql(s"INSERT INTO $tableName VALUES (TIMESTAMP '$openTs', 'user1')")

      val dataBearing = threeHourSpec.at(openIntervalMillis)
      DeltaLake.lastAvailablePartition(tableName, "created_at", threeHourSpec) shouldBe
        Some(threeHourSpec.before(dataBearing))
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(s"DROP DATABASE IF EXISTS $dbName")
    }
  }

  it should "keep interval-end readiness when commits landed during the interval, even after compaction" in {
    val dbName = s"delta_streamed_history_${System.nanoTime()}"
    val tableName = s"$dbName.streamed_time_with_stats"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")

    val threeHourSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000)

    try {
      spark.sql(s"""
        CREATE TABLE $tableName (
          created_at TIMESTAMP,
          user_id STRING
        ) USING DELTA
      """)
      spark.sql(s"INSERT INTO $tableName VALUES (TIMESTAMP '2024-01-01 06:30:00', 'user1')")
      spark.sql(s"INSERT INTO $tableName VALUES (TIMESTAMP '2024-01-01 08:45:00', 'user2')")

      // Delta commit timestamps are the log files' modification times (in-commit timestamps
      // are off by default): rewrite them so the data commits land DURING the 06:00-09:00
      // interval, i.e. a streaming writer's history
      val location = spark.sql(s"DESCRIBE DETAIL $tableName").select("location").head().getString(0)
      val logDir = java.nio.file.Paths.get(new java.net.URI(location).getPath).resolve("_delta_log")
      def setCommitTime(version: Long, utc: String): Unit = {
        val millis = java.time.Instant.parse(utc).toEpochMilli
        val file = logDir.resolve(f"$version%020d.json").toFile
        assert(file.exists(), s"expected delta commit file $file")
        assert(file.setLastModified(millis), s"could not set mtime on $file")
      }
      setCommitTime(0, "2024-01-01T05:00:00Z")
      setCommitTime(1, "2024-01-01T06:35:00Z")
      setCommitTime(2, "2024-01-01T08:50:00Z")

      DeltaLake.lastAvailablePartition(tableName, "created_at", threeHourSpec) shouldBe Some("2024-01-01-03-00")
      // the only data-bearing interval is the in-flight tail, so no interval is complete yet
      DeltaLake.virtualPartitions(tableName, "created_at", threeHourSpec) shouldBe List.empty

      // compaction rewrites the streamed data into fresh files with current timestamps; the
      // dataChange=false commit must not make the interval look chunk-complete
      spark.sql(s"OPTIMIZE $tableName")
      DeltaLake.lastAvailablePartition(tableName, "created_at", threeHourSpec) shouldBe Some("2024-01-01-03-00")
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
