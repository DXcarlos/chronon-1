package ai.chronon.spark.catalog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

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
    partitions.toList

  }

  override def supportSubPartitionsFilter: Boolean = true
}
