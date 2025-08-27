package ai.chronon.api.planner

import ai.chronon.api.Extensions._
import ai.chronon.api.{PartitionSpec, StagingQuery}
import ai.chronon.planner.{ConfPlan, StagingQueryNode}

import scala.collection.JavaConverters._

case class StagingQueryPlanner(stagingQuery: StagingQuery)(implicit outputPartitionSpec: PartitionSpec)
    extends ConfPlanner[StagingQuery](stagingQuery)(outputPartitionSpec) {

  private def semanticStagingQuery(stagingQuery: StagingQuery): StagingQuery = {
    val semanticStagingQuery = stagingQuery.deepCopy()
    semanticStagingQuery.unsetMetaData()
    semanticStagingQuery
  }

  override def buildPlan: ConfPlan = {
    val tableDependencies = TableDependencies.fromStagingQuery(stagingQuery)

    val metaData = MetaDataUtils.layer(
      stagingQuery.metaData,
      "backfill",
      stagingQuery.metaData.name + "__backfill",
      tableDependencies,
      Some(1), // Default step days for staging queries
      Some(stagingQuery.metaData.outputTable)
    )

    val node = new StagingQueryNode().setStagingQuery(stagingQuery)
    val finalNode = toNode(metaData, _.setStagingQuery(node), semanticStagingQuery(stagingQuery))
    val externalSensorNodes = ExternalSourceSensorUtil
      .sensorNodes(finalNode.metaData)
      .map((es) =>
        toNode(es.metaData, _.setExternalSourceSensor(es), ExternalSourceSensorUtil.semanticExternalSourceSensor(es)))

    val terminalNodeNames = Map(
      ai.chronon.planner.Mode.BACKFILL -> finalNode.metaData.name
    )

    new ConfPlan()
      .setNodes((Seq(finalNode) ++ externalSensorNodes).asJava)
      .setTerminalNodeNames(terminalNodeNames.asJava)
  }
}
