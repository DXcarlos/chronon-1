package ai.chronon.api.planner

import ai.chronon.api.Extensions._
import ai.chronon.api.{PartitionSpec, StagingQuery, TableDependency, TableInfo}
import ai.chronon.planner
import ai.chronon.planner.{ConfPlan, Node, StagingQueryNode, StagingQueryStatsComputeNode}

import scala.collection.JavaConverters._

case class StagingQueryPlanner(stagingQuery: StagingQuery)(implicit outputPartitionSpec: PartitionSpec)
    extends ConfPlanner[StagingQuery](stagingQuery)(outputPartitionSpec) {

  private def semanticStagingQuery(stagingQuery: StagingQuery): StagingQuery = {
    val semanticStagingQuery = stagingQuery.deepCopy()
    semanticStagingQuery.unsetMetaData()
    semanticStagingQuery
  }

  private def stagingNode: Node = {
    val tableDependencies = TableDependencies.fromStagingQuery(stagingQuery)

    val metaData = MetaDataUtils.layer(
      stagingQuery.metaData,
      "staging",
      stagingQuery.metaData.name + "__staging",
      tableDependencies,
      outputTableOverride = Some(stagingQuery.metaData.outputTable)
    )

    val node = new StagingQueryNode().setStagingQuery(stagingQuery)
    toNode(metaData, _.setStagingQuery(node), semanticStagingQuery(stagingQuery))
  }

  // Stats compute depends on the staging query output table — same shape as MonolithJoinPlanner.statsComputeNode.
  private def statsComputeNode(stagingNodeOutputTable: String): Node = {
    val defaultStepDays = 1
    val effectiveStepDays = Option(stagingQuery.metaData.executionInfo)
      .filter(_.isSetStepDays)
      .map(_.stepDays)
      .getOrElse(defaultStepDays)

    val tableDep = new TableDependency()
      .setTableInfo(
        new TableInfo()
          .setTable(stagingNodeOutputTable)
          .setPartitionColumn(outputPartitionSpec.column)
          .setPartitionFormat(outputPartitionSpec.format)
          .setPartitionInterval(WindowUtils.hours(outputPartitionSpec.spanMillis))
      )
      .setStartOffset(WindowUtils.zero())
      .setEndOffset(WindowUtils.zero())

    val metaData =
      MetaDataUtils.layer(stagingQuery.metaData,
                          "stats_compute",
                          stagingQuery.metaData.name + "__stats_compute",
                          Seq(tableDep),
                          Some(effectiveStepDays))

    val node = new StagingQueryStatsComputeNode().setStagingQuery(stagingQuery)
    toNode(metaData, _.setStagingQueryStatsCompute(node), semanticStagingQuery(stagingQuery))
  }

  override def buildPlan: ConfPlan = {
    val backfill = stagingNode

    val enableStatsCompute = Option(stagingQuery.metaData.executionInfo)
      .flatMap(ei => Option(ei.enableStatsCompute))
      .exists(_.booleanValue())

    val externalSensorNodes = ExternalSourceSensorUtil
      .sensorNodes(backfill.metaData)
      .map((es) =>
        toNode(es.metaData, _.setExternalSourceSensor(es), ExternalSourceSensorUtil.semanticExternalSourceSensor(es)))

    val (allNodes, terminalNodeNames) = if (enableStatsCompute) {
      val statsCompute = statsComputeNode(backfill.metaData.outputTable)
      val terminals = Map(planner.Mode.BACKFILL -> statsCompute.metaData.name)
      (Seq(backfill, statsCompute) ++ externalSensorNodes, terminals)
    } else {
      val terminals = Map(planner.Mode.BACKFILL -> backfill.metaData.name)
      (Seq(backfill) ++ externalSensorNodes, terminals)
    }

    new ConfPlan()
      .setNodes(allNodes.asJava)
      .setTerminalNodeNames(terminalNodeNames.asJava)
  }
}
