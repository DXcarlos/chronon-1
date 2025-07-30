package ai.chronon.api.planner

import ai.chronon.api.{StagingQuery, PartitionSpec}
import ai.chronon.api.Extensions._
import ai.chronon.planner.ConfPlan
import scala.collection.JavaConverters._
import ai.chronon.planner.StagingQueryNode

case class StagingQueryPlanner(stagingQuery: StagingQuery)(implicit outputPartitionSpec: PartitionSpec)
    extends ConfPlanner[StagingQuery](stagingQuery)(outputPartitionSpec) {

  private def semanticStagingQuery(stagingQuery: StagingQuery): StagingQuery = {
    val semanticStagingQuery = stagingQuery.deepCopy()
    semanticStagingQuery.unsetMetaData()
    semanticStagingQuery
  }

  override def buildPlan: ConfPlan = {

    val metaData = MetaDataUtils.layer(
      stagingQuery.metaData,
      "backfill",
      stagingQuery.metaData.name + "__backfill",
      TableDependencies.fromStagingQuery(stagingQuery),
      Some(1), // Default step days for staging queries
      Some(stagingQuery.metaData.outputTable)
    )

    val node = new StagingQueryNode().setStagingQuery(stagingQuery)
    val finalNode = toNode(metaData, _.setStagingQuery(node), semanticStagingQuery(stagingQuery))

    val terminalNodeNames = Map(
      ai.chronon.planner.Mode.BACKFILL -> finalNode.metaData.name
    )

    new ConfPlan()
      .setNodes(List(finalNode).asJava)
      .setTerminalNodeNames(terminalNodeNames.asJava)
  }
}
