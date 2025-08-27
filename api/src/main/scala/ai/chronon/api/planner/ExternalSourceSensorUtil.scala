package ai.chronon.api.planner

import ai.chronon.api.Extensions._
import ai.chronon.api.{MetaData, PartitionSpec}
import ai.chronon.planner.ExternalSourceSensorNode

import scala.collection.JavaConverters._

object ExternalSourceSensorUtil {

  def semanticExternalSourceSensor(sensorNode: ExternalSourceSensorNode): ExternalSourceSensorNode = {
    val semanticSensor = sensorNode.deepCopy()
    semanticSensor.unsetMetaData()
    semanticSensor
  }

  def sensorNodes(metaData: MetaData)(implicit spec: PartitionSpec): Seq[ExternalSourceSensorNode] = {

    metaData.executionInfo.tableDependencies.asScala
      .map((td) => {
        val tdSpec = td.tableInfo.partitionSpec(spec)
        val sensorMd = MetaDataUtils.layer(
          metaData,
          "backfill",
          f"wait_for_sensor__${td.tableInfo.table}__backfill",
          Seq(), // No table dependencies for sensors
          outputTableOverride =
            Option(td.tableInfo.table) // The input table and the output table are the same for sensors.
        )(tdSpec)
        val retryInterval = 15 // minutes
        val retryCount = 96
        new ExternalSourceSensorNode()
          .setSourceTableDependency(td)
          .setMetaData(sensorMd)
          .setRetryCount(retryCount)
          .setRetryIntervalMin(retryInterval)
      })
      .toList
  }

}
