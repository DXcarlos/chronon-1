package ai.chronon.api.planner

import ai.chronon.api.{ModelTransforms, PartitionSpec, TableDependency, TableInfo}
import ai.chronon.api.Extensions.{MetadataOps, WindowUtils}
import ai.chronon.planner.{ConfPlan, ModelTransformsUploadNode, Node}
import ai.chronon.planner

import scala.collection.JavaConverters._

class ModelTransformsPlanner(modelTransforms: ModelTransforms)(implicit outputPartitionSpec: PartitionSpec)
    extends ConfPlanner[ModelTransforms](modelTransforms)(outputPartitionSpec) {

  private def eraseExecutionInfo: ModelTransforms = {
    val result = modelTransforms.deepCopy()
    result.metaData.unsetExecutionInfo()
    result
  }

  private def semanticModelTransforms(modelTransforms: ModelTransforms): ModelTransforms = {
    val semantic = modelTransforms.deepCopy()
    semantic.unsetMetaData()
    semantic
  }

  def uploadNode: Node = {
    val stepDays = 1 // Default step days for metadata upload

    // Create table dependencies only for JoinSource sources - we ensure join metadata is uploaded before proceeding
    val allDeps = TableDependencies.fromJoinSources(modelTransforms.sources)

    val metaData =
      MetaDataUtils.layer(
        modelTransforms.metaData,
        "upload",
        modelTransforms.metaData.name + "__upload",
        allDeps,
        Some(stepDays)
      )

    val node = new ModelTransformsUploadNode().setModelTransforms(eraseExecutionInfo)

    val copy = semanticModelTransforms(modelTransforms)

    toNode(metaData, _.setModelTransformsUpload(node), copy)
  }

  override def buildPlan: ConfPlan = {
    val upload = uploadNode

    // Only DEPLOY for now, we'll add backfill later
    val terminalNodeNames = Map(
      planner.Mode.DEPLOY -> upload.metaData.name
    )

    new ConfPlan()
      .setNodes(Seq(upload).asJava)
      .setTerminalNodeNames(terminalNodeNames.asJava)
  }
}

object ModelTransformsPlanner {
  def apply(modelTransforms: ModelTransforms)(implicit outputPartitionSpec: PartitionSpec): ModelTransformsPlanner =
    new ModelTransformsPlanner(modelTransforms)(outputPartitionSpec)
}
