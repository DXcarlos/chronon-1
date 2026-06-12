package ai.chronon.spark

import ai.chronon.api.{MetaData, PartitionSpec}
import ai.chronon.api.planner.MetaDataUtils
import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.SparkSession

object RunnerUtils {

  def outputPartitionSpec(metadata: MetaData, defaultSpec: PartitionSpec = PartitionSpec.daily): PartitionSpec =
    MetaDataUtils.outputPartitionSpec(metadata, defaultSpec)

  def outputPartitionSpec(metadata: MetaData, sparkSession: SparkSession): PartitionSpec =
    outputPartitionSpec(metadata, TableUtils(sparkSession).partitionSpec)

  def tableUtilsForMetadata(sparkSession: SparkSession, metadata: MetaData): TableUtils =
    TableUtils(sparkSession, outputPartitionSpec(metadata, sparkSession))
}
