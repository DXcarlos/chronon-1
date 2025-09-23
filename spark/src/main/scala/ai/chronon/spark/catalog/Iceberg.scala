package ai.chronon.spark.catalog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format}
import org.apache.spark.sql.types.StructType

case object Iceberg extends Format {

  override def primaryPartitions(tableName: String,
                                 partitionColumn: String,
                                 partitionFilters: String,
                                 subPartitionsFilter: Map[String, String])(implicit
      sparkSession: SparkSession): List[String] = {

    if (!supportSubPartitionsFilter && subPartitionsFilter.nonEmpty) {
      throw new NotImplementedError("subPartitionsFilter is not supported on this format")
    }

    getIcebergPartitions(tableName, partitionColumn)
  }

  override def partitions(tableName: String, partitionFilters: String)(implicit
      sparkSession: SparkSession): List[Map[String, String]] = {
    throw new NotImplementedError(
      "Multi-partitions retrieval is not supported on Iceberg tables yet." +
        "For single partition retrieval, please use 'partition' method.")
  }

  private def getIcebergPartitions(tableName: String, partitionColumn: String)(implicit
      sparkSession: SparkSession): List[String] = {

    val partitionsDf = sparkSession.read
      .format("iceberg")
      .load(s"${tableName}.partitions")

    val index = partitionsDf.schema.fieldIndex("partition")
    val tableUtils = TableUtils(sparkSession)
    val partitionFmt = tableUtils.partitionFormat
    if (partitionsDf.schema(index).dataType.asInstanceOf[StructType].fieldNames.contains("hr")) {
      // Hour filter is currently buggy in iceberg. https://github.com/apache/iceberg/issues/4718
      // so we collect and then filter.
      partitionsDf
        .select(date_format(col(s"partition.$partitionColumn"), partitionFmt), col("partition.hr"))
        .collect()
        .filter(_.get(1) == null)
        .map(_.getString(0))
        .toList

    } else {

      partitionsDf
        .select(date_format(col(s"partition.$partitionColumn"), partitionFmt))
        .collect()
        .map(_.getString(0))
        .toList
    }
  }

  override def supportSubPartitionsFilter: Boolean = false
}
