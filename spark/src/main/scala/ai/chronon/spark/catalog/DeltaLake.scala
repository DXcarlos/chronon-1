package ai.chronon.spark.catalog

import ai.chronon.api.PartitionSpec
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.{
  coalesce,
  col,
  count,
  from_json,
  lit,
  min,
  max,
  to_date,
  to_timestamp,
  when
}
import org.apache.spark.sql.types.{DataType, DateType, MapType, StringType, StructField, StructType, TimestampType}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

// Compiled against delta-spark 3.3.2 to match EMR 7.12.0. DeltaLog.update() signature changes
// across Delta versions (e.g. 2 params in 3.2, 3 params in 3.3), so compiling against an older
// version will cause NoSuchMethodError at runtime if the EMR-bundled Delta jar has a newer signature.
case object DeltaLake extends Format {

  override def tableTypeString: String = "delta"

  override def primaryPartitions(tableName: String,
                                 partitionColumn: String,
                                 partitionFilters: String,
                                 subPartitionsFilter: Map[String, String])(implicit
      sparkSession: SparkSession): List[String] =
    super.primaryPartitions(tableName, partitionColumn, partitionFilters, subPartitionsFilter)

  override def partitions(tableName: String, partitionFilters: String)(implicit
      sparkSession: SparkSession): List[Map[String, String]] = {

    // delta lake doesn't support the `SHOW PARTITIONS <tableName>` syntax - https://github.com/delta-io/delta/issues/996
    // there's alternative ways to retrieve partitions using the DeltaLog abstraction which is what we have to lean into
    // below first pull table location as that is what we need to pass to the delta log
    val describeResult = sparkSession.sql(s"DESCRIBE DETAIL $tableName")
    val tablePath = describeResult.select("location").head().getString(0)

    val snapshot = DeltaLog.forTable(sparkSession, tablePath).update()
    val snapshotPartitionsDf = snapshot.allFiles.toDF().select("partitionValues")

    val partitions = snapshotPartitionsDf.collect().map(r => r.getAs[Map[String, String]](0))
    partitions.toList.distinct

  }

  override def virtualPartitions(tableName: String, timestampColumn: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): List[String] = {
    metadataPartitions(tableName, timestampColumn)
      .filter(_.nonEmpty)
      .orElse(statsDateRange(tableName, timestampColumn, partitionSpec)
        .map(_.virtualPartitions(partitionSpec)))
      .getOrElse(super.virtualPartitions(tableName, timestampColumn, partitionSpec))
  }

  override def firstAvailablePartition(tableName: String, partitionColumn: String, partitionSpec: PartitionSpec)(
      implicit sparkSession: SparkSession): Option[String] =
    metadataFirstAvailablePartition(tableName, partitionColumn)
      .orElse(statsDateRange(tableName, partitionColumn, partitionSpec).map(_.firstAvailablePartition))
      .orElse(scanFirstAvailablePartition(tableName, partitionColumn, partitionSpec))

  override def lastAvailablePartition(tableName: String, partitionColumn: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[String] =
    metadataLastAvailablePartition(tableName, partitionColumn)
      .orElse(statsLastAvailablePartition(tableName, partitionColumn, partitionSpec))
      .orElse(scanLastAvailablePartition(tableName, partitionColumn, partitionSpec))

  override def maxTimestampMillisFromStats(tableName: String, timestampColumn: String)(implicit
      sparkSession: SparkSession): Option[Long] =
    DeltaStats.maxTimestampMillis(tableName, timestampColumn)

  private def statsLastAvailablePartition(tableName: String, columnName: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[String] =
    statsDateRange(tableName, columnName, partitionSpec).map { range =>
      sparkSession.read.table(tableName).schema(columnName).dataType match {
        case TimestampType => partitionSpec.before(range.lastAvailablePartition)
        case _             => range.lastAvailablePartition
      }
    }

  private[catalog] def statsDateRange(tableName: String, columnName: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[StatsDateRange] =
    DeltaStats.millisRange(tableName, columnName, partitionSpec).map(_.toDateRange(partitionSpec))

  override def supportSubPartitionsFilter: Boolean = true
}

private[catalog] object DeltaStats {
  @transient private lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private val DayMillis = 24L * 60L * 60L * 1000L

  def millisRange(tableName: String, columnName: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[StatsMillisRange] = {
    import sparkSession.implicits._

    Try {
      val activeFiles = deltaActiveFiles(tableName)
      val columnType = sparkSession.read.table(tableName).schema(columnName).dataType

      val statsSchema = StructType(
        Seq(
          StructField("minValues", MapType(StringType, StringType), nullable = true),
          StructField("maxValues", MapType(StringType, StringType), nullable = true)
        ))

      val stats = activeFiles
        .select(from_json(col("stats"), statsSchema).as("stats"))
        .select(
          col("stats.minValues").getItem(columnName).as("min_value"),
          col("stats.maxValues").getItem(columnName).as("max_value")
        )

      val boundaries = stats
        .agg(
          count(lit(1)).as("fileCount"),
          count(when(col("min_value").isNull || col("max_value").isNull, lit(1))).as("missingCount"),
          min(statsBoundaryTimestamp("min_value", columnType, partitionSpec)).as("startTimestamp"),
          max(statsBoundaryTimestamp("max_value", columnType, partitionSpec)).as("endTimestamp")
        )
        .as[(Long, Long, java.sql.Timestamp, java.sql.Timestamp)]
        .collect()
        .headOption

      boundaries.flatMap { case (fileCount, missingCount, startTs, endTs) =>
        if (fileCount > 0 && missingCount == 0 && startTs != null && endTs != null) {
          Some(StatsMillisRange(startMillis = startTs.getTime, endMillis = endTs.getTime))
        } else {
          None
        }
      }
    } match {
      case Success(result) =>
        if (result.isDefined) {
          logger.info(s"Resolved Delta log stats millis boundaries for $tableName.$columnName: ${result.get}")
        } else {
          logger.info(s"Delta log stats were incomplete for $tableName.$columnName")
        }
        result
      case Failure(e) =>
        logger.warn(
          s"Failed to resolve Delta log stats boundaries for $tableName.$columnName: ${Option(e.getMessage).getOrElse("(no message)")}")
        None
    }
  }

  def maxTimestampMillis(tableName: String, timestampColumn: String)(implicit sparkSession: SparkSession): Option[Long] = {
    import sparkSession.implicits._

    Try {
      val columnType = sparkSession.read.table(tableName).schema(timestampColumn).dataType
      if (columnType != TimestampType) {
        None
      } else {
        val statsSchema = StructType(
          Seq(
            StructField("maxValues", MapType(StringType, StringType), nullable = true)
          ))

        val maxTimestamp = deltaActiveFiles(tableName)
          .select(from_json(col("stats"), statsSchema).as("stats"))
          .select(col("stats.maxValues").getItem(timestampColumn).as("max_value"))
          .agg(
            count(lit(1)).as("fileCount"),
            count(when(col("max_value").isNull, lit(1))).as("missingCount"),
            max(statsBoundaryTimestamp("max_value", columnType, PartitionSpec.daily)).as("maxTimestamp")
          )
          .as[(Long, Long, java.sql.Timestamp)]
          .collect()
          .headOption

        maxTimestamp.flatMap { case (fileCount, missingCount, maxTs) =>
          if (fileCount > 0 && missingCount == 0 && maxTs != null) Some(maxTs.getTime) else None
        }
      }
    } match {
      case Success(result) =>
        if (result.isDefined) {
          logger.info(s"Resolved Delta log stats max timestamp for $tableName.$timestampColumn: ${result.get}")
        } else {
          logger.warn(s"Delta log stats max timestamp unavailable for $tableName.$timestampColumn")
        }
        result
      case Failure(e) =>
        logger.warn(
          s"Failed to resolve Delta log stats max timestamp for $tableName.$timestampColumn: ${Option(e.getMessage).getOrElse("(no message)")}")
        None
    }
  }

  private def deltaActiveFiles(tableName: String)(implicit sparkSession: SparkSession) = {
    val describeResult = sparkSession.sql(s"DESCRIBE DETAIL $tableName")
    val tablePath = describeResult.select("location").head().getString(0)
    DeltaLog.forTable(sparkSession, tablePath).update().allFiles.toDF()
  }

  private def statsBoundaryTimestamp(boundaryColumn: String, columnType: DataType, partitionSpec: PartitionSpec): Column =
    columnType match {
      case DateType =>
        col(boundaryColumn).cast(DateType).cast("timestamp")
      case StringType if partitionSpec.spanMillis >= DayMillis =>
        coalesce(to_date(col(boundaryColumn), partitionSpec.format), col(boundaryColumn).cast(DateType)).cast("timestamp")
      case StringType =>
        coalesce(to_timestamp(col(boundaryColumn), partitionSpec.format), col(boundaryColumn).cast("timestamp"))
      case _ =>
        col(boundaryColumn).cast("timestamp")
    }
}
