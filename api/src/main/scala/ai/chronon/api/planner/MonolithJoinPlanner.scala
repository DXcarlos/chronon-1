package ai.chronon.api.planner

import ai.chronon.api.Extensions.{GroupByOps, WindowUtils}
import ai.chronon.api.Extensions._
import ai.chronon.api.{Join, PartitionSpec, TableDependency, TableInfo}
import ai.chronon.planner
import ai.chronon.planner.Node

import scala.collection.JavaConverters._

case class MonolithJoinPlanner(join: Join)(implicit outputPartitionSpec: PartitionSpec)
    extends ConfPlanner[Join](join)(outputPartitionSpec) {

  private def semanticMonolithJoin(join: Join): Join = {
    val semanticJoin = join.deepCopy()
    semanticJoin.unsetMetaData()
    Option(semanticJoin.joinParts).map(_.asScala).foreach { parts =>
      parts.foreach(joinPart => joinPart.groupBy.unsetMetaData())
    }
    Option(semanticJoin.bootstrapParts).map(_.asScala).foreach { bootstrapParts =>
      bootstrapParts.foreach(bootstrapPart => bootstrapPart.unsetMetaData())
    }
    Option(semanticJoin.labelParts).map(_.labels).map(_.asScala).foreach { labelPart =>
      labelPart.foreach { joinPart => joinPart.groupBy.unsetMetaData() }
    }
    semanticJoin.unsetOnlineExternalParts()
    semanticJoin

  }

  def backfillNode: Node = {
    val tableDeps = TableDependencies.fromJoin(join)

    val metaData =
      MetaDataUtils.layer(join.metaData,
                          "backfill",
                          join.metaData.name + "__backfill",
                          tableDeps,
                          None,
                          Some(join.metaData.outputTable))
    val node = new planner.MonolithJoinNode().setJoin(join)
    toNode(metaData, _.setMonolithJoin(node), semanticMonolithJoin(join))
  }

  def metadataUploadNode: Node = {
    val stepDays = 1 // Default step days for metadata upload

    // Create table dependencies to GroupBy nodes (either uploadToKV or streaming)
    val joinPartDeps = Option(join.joinParts).map(_.asScala).getOrElse(Seq.empty).flatMap { joinPart =>
      val groupBy = joinPart.groupBy
      val hasStreamingSource = groupBy.streamingSource.isDefined

      val tableName = if (hasStreamingSource) {
        groupBy.metaData.outputTable + s"__${GroupByPlanner.Streaming}"
      } else {
        groupBy.metaData.outputTable + s"__${GroupByPlanner.UploadToKV}"
      }

      val tableDep = new TableDependency()
        .setTableInfo(
          new TableInfo()
            .setTable(tableName)
        )
        .setStartOffset(WindowUtils.zero())
        .setEndOffset(WindowUtils.zero())

      Some(tableDep)
    }

    val metaData =
      MetaDataUtils.layer(join.metaData,
                          "metadata_upload",
                          join.metaData.name + "__metadata_upload",
                          joinPartDeps,
                          Some(stepDays))
    val node = new planner.JoinMetadataUpload().setJoin(join)
    toNode(metaData, _.setJoinMetadataUpload(node), semanticMonolithJoin(join))
  }

  override def buildPlan: planner.ConfPlan = {
    val confPlan = new planner.ConfPlan()

    val backfill = backfillNode
    val sensorNodes = ExternalSourceSensorUtil
      .sensorNodes(backfill.metaData)
      .map((es) =>
        toNode(es.metaData, _.setExternalSourceSensor(es), ExternalSourceSensorUtil.semanticExternalSourceSensor(es)))

    val terminalNodeNames = Map(
      planner.Mode.BACKFILL -> backfill.metaData.name,
      planner.Mode.DEPLOY -> metadataUploadNode.metaData.name
    )

    confPlan
      .setNodes((List(backfill, metadataUploadNode) ++ sensorNodes).asJava)
      .setTerminalNodeNames(terminalNodeNames.asJava)
  }
}
