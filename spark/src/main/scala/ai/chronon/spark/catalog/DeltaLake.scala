package ai.chronon.spark.catalog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

import scala.jdk.CollectionConverters._

// The Delta Lake format is compatible with the Delta lake and Spark versions currently supported by the project.
// Attempting to use newer Delta lake library versions (e.g. 3.2 which works with Spark 3.5) results in errors:
// java.lang.NoSuchMethodError: 'org.apache.spark.sql.delta.Snapshot org.apache.spark.sql.delta.DeltaLog.update(boolean)'
// In such cases, you should implement your own FormatProvider built on the newer Delta lake version
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
    val describeResult = sparkSession.sql(s"DESCRIBE DETAIL $tableName")
    val partitionColumns = describeResult.select("partitionColumns").head().getList[String](0).asScala.toList

    if (partitionColumns.nonEmpty) partitionsFromDeltaLog(tableName, describeResult)
    else queryDistinctPartitions(tableName, partitionFilters)
  }

  // delta lake doesn't support `SHOW PARTITIONS` - https://github.com/delta-io/delta/issues/996
  // use DeltaLog to read partition values from the table's file manifest
  private def partitionsFromDeltaLog(tableName: String, describeResult: org.apache.spark.sql.DataFrame)(implicit
      sparkSession: SparkSession): List[Map[String, String]] = {
    val tablePath = describeResult.select("location").head().getString(0)
    val snapshot = DeltaLog.forTable(sparkSession, tablePath).update()
    snapshot.allFiles.toDF().select("partitionValues").collect().map(r => r.getAs[Map[String, String]](0)).toList
  }

  // For unpartitioned tables, derive partitions via SQL query.
  // Supports spark.chronon.partition.expression for tables without a date column
  // (e.g. "CAST(event_timestamp AS DATE)")
  private def queryDistinctPartitions(tableName: String, partitionFilters: String)(implicit
      sparkSession: SparkSession): List[Map[String, String]] = {
    val partitionColumn = sparkSession.conf.get("spark.chronon.partition.column", "ds")
    val partitionFormat = sparkSession.conf.get("spark.chronon.partition.format", "yyyy-MM-dd")
    val partitionExpr = sparkSession.conf.get("spark.chronon.partition.expression", partitionColumn)

    val query = buildPartitionQuery(tableName, partitionColumn, partitionExpr, partitionFormat, partitionFilters)
    logger.info(s"Querying unpartitioned Delta table: $query")

    sparkSession.sql(query).collect().flatMap { row =>
      Option(row.getString(0)).map(v => Map(partitionColumn -> v))
    }.toList
  }

  private def buildPartitionQuery(tableName: String,
                                  partitionColumn: String,
                                  partitionExpr: String,
                                  partitionFormat: String,
                                  partitionFilters: String): String = {
    val baseQuery = s"SELECT DISTINCT date_format(CAST($partitionExpr AS DATE), '$partitionFormat') AS $partitionColumn FROM $tableName"
    if (partitionFilters.nonEmpty) s"$baseQuery WHERE $partitionFilters" else baseQuery
  }

  override def supportSubPartitionsFilter: Boolean = true
}
