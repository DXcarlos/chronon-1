package ai.chronon.spark.catalog

import ai.chronon.api.PartitionSpec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, min, max}
import org.apache.spark.sql.types.StringType
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

/** Delta Lake format for Databricks Runtime environments.
  *
  * Unlike the OSS [[DeltaLake]] object (which imports `org.apache.spark.sql.delta.DeltaLog`),
  * this implementation uses only standard Spark SQL APIs that are available on both
  * OSS Spark+Delta and Databricks Runtime (which ships its own internal Delta under
  * `com.databricks.sql.transaction.tahoe.*`).
  *
  * Trade-off: loses the optimization of reading file-level stats from the Delta transaction
  * log. In practice, Databricks' query optimizer handles `MIN`/`MAX` on partitioned columns
  * efficiently via data skipping, so the difference is negligible for partition boundary queries.
  *
  * @see [[DatabricksFormatProvider]] for the companion FormatProvider that routes Delta
  *      tables to this object instead of the OSS [[DeltaLake]].
  */
case object DatabricksDeltaLake extends Format {

  @transient override protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  override def tableTypeString: String = "delta"

  override def supportSubPartitionsFilter: Boolean = true

  // ─── Partition column discovery ────────────────────────────────────────────────
  // DESCRIBE DETAIL works identically on both OSS Delta and Databricks Runtime.
  override def partitionColumnNames(tableName: String)(implicit sparkSession: SparkSession): Seq[String] =
    Try {
      sparkSession
        .sql(s"DESCRIBE DETAIL $tableName")
        .select("partitionColumns")
        .head()
        .getSeq[String](0)
        .toList
    }.getOrElse(Seq.empty)

  // ─── Partition listing ─────────────────────────────────────────────────────────
  // `SHOW PARTITIONS` does NOT work on Delta tables (neither OSS nor Databricks):
  //   https://github.com/delta-io/delta/issues/996
  //
  // The OSS DeltaLake object reads DeltaLog.allFiles for this; we instead do a
  // `SELECT DISTINCT <partition_cols>` scan. For tables with a string partition column
  // (the typical Chronon `ds` column), Databricks' data skipping makes this efficient.
  override def partitions(tableName: String, partitionFilters: String)(implicit
      sparkSession: SparkSession): List[Map[String, String]] = {
    val partCols = partitionColumnNames(tableName)
    if (partCols.isEmpty) return List.empty

    Try {
      val df = sparkSession.read.table(tableName)
      val filtered = if (partitionFilters.isEmpty) df else df.where(partitionFilters)
      filtered
        .select(partCols.map(col): _*)
        .distinct()
        .collect()
        .map { row =>
          partCols.zipWithIndex.map { case (colName, idx) =>
            colName -> Option(row.get(idx)).map(_.toString).orNull
          }.toMap
        }
        .toList
    } match {
      case Success(result) => result
      case Failure(e) if Option(e.getMessage).exists(_.contains("TABLE_OR_VIEW_NOT_FOUND")) =>
        logger.warn(s"Table not found during partition scan: $tableName")
        List.empty
      case Failure(e) =>
        logger.warn(
          s"Failed to scan partitions for $tableName: ${e.getClass.getSimpleName}: " +
            s"${Option(e.getMessage).getOrElse("(no message)")}")
        List.empty
    }
  }

  override def primaryPartitions(tableName: String,
                                 partitionColumn: String,
                                 partitionFilters: String,
                                 subPartitionsFilter: Map[String, String])(implicit
      sparkSession: SparkSession): List[String] =
    super.primaryPartitions(tableName, partitionColumn, partitionFilters, subPartitionsFilter)

  // ─── Partition boundaries ──────────────────────────────────────────────────────
  // Delegate to the base trait's scan-based implementations (SELECT MIN/MAX).
  // The OSS DeltaLake object optimizes these with DeltaLog file-level stats;
  // on Databricks, the query optimizer's data skipping achieves similar performance.
  override def firstAvailablePartition(tableName: String, partitionColumn: String, partitionSpec: PartitionSpec)(
      implicit sparkSession: SparkSession): Option[String] =
    metadataFirstAvailablePartition(tableName, partitionColumn)
      .orElse(scanFirstAvailablePartition(tableName, partitionColumn, partitionSpec))

  override def lastAvailablePartition(tableName: String, partitionColumn: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[String] =
    metadataLastAvailablePartition(tableName, partitionColumn)
      .orElse(scanLastAvailablePartition(tableName, partitionColumn, partitionSpec))

  // ─── Virtual partitions (time-partitioned tables) ──────────────────────────────
  override def virtualPartitions(tableName: String, timestampColumn: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): List[String] = {
    metadataPartitions(tableName, timestampColumn)
      .filter(_.nonEmpty)
      .getOrElse(super.virtualPartitions(tableName, timestampColumn, partitionSpec))
  }
}
