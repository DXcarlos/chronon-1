package ai.chronon.api.planner

import ai.chronon.api.{DataModel, GroupBy, PartitionSpec, TableDependency, TableInfo}
import ai.chronon.api.Extensions.{GroupByOps, MetadataOps, WindowUtils}
import ai.chronon.planner.{
  ConfPlan,
  GroupByBackfillNode,
  GroupByUploadNode,
  GroupByUploadToKVNode,
  GroupByStreamingNode,
  Node
}
import scala.collection.JavaConverters._

case class GroupByPlanner(groupBy: GroupBy)(implicit outputPartitionSpec: PartitionSpec)
    extends ConfPlanner[GroupBy](groupBy)(outputPartitionSpec) {

  // execInfo can be heavy - and we don't want to duplicate it
  private def eraseExecutionInfo: GroupBy = {
    val result = groupBy.deepCopy()
    result.metaData.unsetExecutionInfo()
    result
  }

  private val groupByTableDeps: Seq[TableDependency] = TableDependencies.fromGroupBy(groupBy)

  def backfillNode: Node = {
    val defaultStepDays = if (groupBy.dataModel == DataModel.EVENTS) 15 else 1
    val effectiveStepDays =
      Option(groupBy.metaData.executionInfo).filter(_.isSetStepDays).map(_.stepDays).getOrElse(defaultStepDays)

    val metaData = MetaDataUtils.layer(groupBy.metaData,
                                       "backfill",
                                       groupBy.metaData.name + "__backfill",
                                       groupByTableDeps,
                                       Option(effectiveStepDays))

    val node = new GroupByBackfillNode().setGroupBy(eraseExecutionInfo)

    toNode(metaData, _.setGroupByBackfill(node), semanticGroupBy(groupBy))
  }

  private def semanticGroupBy(groupBy: GroupBy): GroupBy = {
    val semanticGroupBy = groupBy.deepCopy()
    semanticGroupBy.unsetMetaData()
    semanticGroupBy
  }

  def uploadNode: Node = {
    val stepDays = 1 // GBUs write out data per day
    val metaData =
      MetaDataUtils.layer(groupBy.metaData,
                          "upload",
                          groupBy.metaData.name + "__upload",
                          groupByTableDeps,
                          Some(stepDays),
                          Some(groupBy.metaData.uploadTable))(PartitionSpec.daily)

    val node = new GroupByUploadNode().setGroupBy(eraseExecutionInfo)
    toNode(metaData, _.setGroupByUpload(node), semanticGroupBy(groupBy))
  }

  def uploadToKVNode: Node = {
    val tableDep = new TableDependency()
      .setTableInfo(
        new TableInfo()
          .setTable(groupBy.metaData.uploadTable)
          .setPartitionColumn(outputPartitionSpec.column)
          .setPartitionFormat(outputPartitionSpec.format)
          .setPartitionInterval(WindowUtils.hours(outputPartitionSpec.spanMillis))
      )
      .setStartOffset(WindowUtils.zero())
      .setEndOffset(WindowUtils.zero())
    val uploadToKVTableDeps = Seq(tableDep)

    val metaData =
      MetaDataUtils.layer(
        groupBy.metaData,
        GroupByPlanner.UploadToKV,
        groupBy.metaData.name + s"__${GroupByPlanner.UploadToKV}",
        uploadToKVTableDeps,
        None,
        Some(groupBy.metaData.name + s"__${GroupByPlanner.UploadToKV}")
      )

    val node = new GroupByUploadToKVNode().setGroupBy(eraseExecutionInfo)
    toNode(metaData, _.setGroupByUploadToKV(node), semanticGroupBy(groupBy))
  }

  def streamingNode: Option[Node] = {
    groupBy.streamingSource.map { _ =>
      // Streaming node has table dependency on the upload to KV
      val tableDep = new TableDependency()
        .setTableInfo(
          new TableInfo()
            .setTable(groupBy.metaData.name + s"__${GroupByPlanner.UploadToKV}")
        )
        .setStartOffset(WindowUtils.zero())
        .setEndOffset(WindowUtils.zero())
      val streamingTableDeps = Seq(tableDep)

      val metaData =
        MetaDataUtils.layer(
          groupBy.metaData,
          GroupByPlanner.Streaming,
          groupBy.metaData.name + s"__${GroupByPlanner.Streaming}",
          streamingTableDeps,
          None,
          Some(groupBy.metaData.name + s"__${GroupByPlanner.Streaming}")
        )

      val node = new GroupByStreamingNode().setGroupBy(eraseExecutionInfo)
      toNode(metaData, _.setGroupByStreaming(node), semanticGroupBy(groupBy))
    }
  }

  override def buildPlan: ConfPlan = {
    val allNodes = Seq(backfillNode, uploadNode, uploadToKVNode) ++ streamingNode.toSeq

    val deployTerminalNode = streamingNode.map(_.metaData.name).getOrElse(uploadToKVNode.metaData.name)

    val terminalNodeNames = Map(
      ai.chronon.planner.Mode.BACKFILL -> backfillNode.metaData.name,
      ai.chronon.planner.Mode.DEPLOY -> deployTerminalNode
    )

    val confPlan = new ConfPlan()
      .setNodes(allNodes.asJava)
      .setTerminalNodeNames(terminalNodeNames.asJava)
    confPlan
  }
}

object GroupByPlanner {
  val Streaming = "streaming"
  val UploadToKV = "uploadToKV"
}
