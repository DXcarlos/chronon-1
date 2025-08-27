package ai.chronon.spark.test.utils

import ai.chronon.spark.test.utils.TableTestUtils
import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.SparkSession


case class TableTestUtils(override val sparkSession: SparkSession) extends TableUtils(sparkSession: SparkSession) {

  def dropPartitions(tableName: String,
                     partitions: Seq[String],
                     partitionColumn: String = partitionColumn,
                     subPartitionFilters: Map[String, String] = Map.empty): Unit = {
    if (partitions.nonEmpty && tableReachable(tableName)) {
      val partitionSpecs = partitions
        .map { partition =>
          val mainSpec = s"$partitionColumn='$partition'"
          val specs = mainSpec +: subPartitionFilters.map { case (key, value) =>
            s"$key='$value'"
          }.toSeq
          specs.mkString("PARTITION (", ",", ")")
        }
        .mkString(",")
      val dropSql = s"ALTER TABLE $tableName DROP IF EXISTS $partitionSpecs"
      sql(dropSql)
    } else {
      logger.info(s"$tableName doesn't exist, please double check before drop partitions")
    }
  }

  def dropPartitionRange(tableName: String,
                         startDate: String,
                         endDate: String,
                         subPartitionFilters: Map[String, String] = Map.empty): Unit = {
    if (tableReachable(tableName)) {
      val toDrop = Stream.iterate(startDate)(partitionSpec.after).takeWhile(_ <= endDate)
      dropPartitions(tableName, toDrop, partitionColumn, subPartitionFilters)
    } else {
      logger.info(s"$tableName doesn't exist, please double check before drop partitions")
    }
  }

}
