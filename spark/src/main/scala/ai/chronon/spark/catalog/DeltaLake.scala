package ai.chronon.spark.catalog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

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

    // delta lake doesn't support the `SHOW PARTITIONS <tableName>` syntax - https://github.com/delta-io/delta/issues/996
    // there's alternative ways to retrieve partitions using the DeltaLog abstraction which is what we have to lean into
    // below
    // first pull table location as that is what we need to pass to the delta log
    val describeResult = sparkSession.sql(s"DESCRIBE DETAIL $tableName")
    val tablePath = describeResult.select("location").head().getString(0)

    val snapshot = DeltaLog.forTable(sparkSession, tablePath).update()
    val snapshotPartitionsDf = snapshot.allFiles.toDF().select("partitionValues")

    val partitions = snapshotPartitionsDf.collect().map(r => r.getAs[Map[String, String]](0))
    partitions.toList

  }

  override def supportSubPartitionsFilter: Boolean = true
}
