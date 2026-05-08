package ai.chronon.api.planner

import ai.chronon.api.{Inference, PartitionSpec, TableDependency, TableInfo}
import ai.chronon.api.Extensions.{MetadataOps, WindowUtils}
import ai.chronon.api.ScalaJavaConversions.IterableOps
import ai.chronon.api.planner.TableDependencies.{fromSource, fromTable}
import ai.chronon.planner.{ConfPlan, InferenceBackfillNode, InferenceUploadNode, Node}
import ai.chronon.planner

import scala.collection.JavaConverters._

class InferencePlanner(inference: Inference)(implicit outputPartitionSpec: PartitionSpec)
    extends ConfPlanner[Inference](inference)(outputPartitionSpec) {

  private def eraseExecutionInfo: Inference = {
    val result = inference.deepCopy()
    result.metaData.unsetExecutionInfo()
    result
  }

  private def semanticInference(inference: Inference): Inference = {
    val semantic = inference.deepCopy()
    semantic.unsetMetaData()
    semantic
  }

  def backfillNode: Node = {
    val sourceDeps =
      Option(inference.features)
        .map(_.toScala.toSeq)
        .getOrElse(Seq.empty)
        .flatMap { source =>
          if (source.isSetJoinSource) {
            // For join sources, depend on the join's output table
            val upstreamJoin = source.getJoinSource.getJoin

            // check if derivations
            if (upstreamJoin.isSetDerivations && !upstreamJoin.getDerivations.isEmpty) {
              val derivationOutputTable = upstreamJoin.metaData.outputTable + "__derived"
              Some(fromTable(derivationOutputTable, source.getJoinSource.query))
            } else {
              val upstreamJoinOutputTable = upstreamJoin.metaData.outputTable
              Some(fromTable(upstreamJoinOutputTable, source.getJoinSource.query))
            }
          } else {
            fromSource(source)
          }
        }

    // add model dependencies - we depend on the deployed model endpoint for models that are custom and trained by us
    val modelDeps = Option(inference.models)
      .map(_.toScala.toSeq)
      .getOrElse(Seq.empty)
      .flatMap { model =>
        if (model.isSetTrain) {
          val deployNodeName = model.metaData.outputTable + "__model_deploy"
          val deployNodeTableDep = new TableDependency()
            .setTableInfo(
              new TableInfo()
                .setTable(deployNodeName)
            )
            .setStartOffset(WindowUtils.zero())
            .setEndOffset(WindowUtils.zero())
            .setIsSoftNodeDependency(true)
          Some(deployNodeTableDep)
        } else {
          None
        }
      }

    val tableDeps = sourceDeps ++ modelDeps

    val metaData =
      MetaDataUtils.layer(
        inference.metaData,
        "inference_backfill",
        inference.metaData.name + "__inference_backfill",
        tableDeps,
        outputTableOverride = Some(inference.metaData.outputTable)
      )

    val node = new InferenceBackfillNode().setInference(inference)

    val copy = semanticInference(inference)

    toNode(metaData, _.setInferenceBackfill(node), copy)
  }

  def uploadNode: Node = {
    val stepDays = 1 // Default step days for metadata upload

    // Create table dependencies only for JoinSource sources - we ensure join metadata is uploaded before proceeding
    val allDeps = TableDependencies.fromJoinSources(inference.features)

    val metaData =
      MetaDataUtils.layer(
        inference.metaData,
        "inference_upload",
        inference.metaData.name + "__inference_upload",
        allDeps,
        Some(stepDays)
      )

    val node = new InferenceUploadNode().setInference(eraseExecutionInfo)

    val copy = semanticInference(inference)

    toNode(metaData, _.setInferenceUpload(node), copy)
  }

  override def buildPlan: ConfPlan = {
    val upload = uploadNode
    val backfill = backfillNode

    val terminalNodeNames = Map(
      planner.Mode.BACKFILL -> backfill.metaData.name,
      planner.Mode.DEPLOY -> upload.metaData.name
    )

    new ConfPlan()
      .setNodes(Seq(upload, backfill).asJava)
      .setTerminalNodeNames(terminalNodeNames.asJava)
  }
}

object InferencePlanner {
  def apply(inference: Inference)(implicit outputPartitionSpec: PartitionSpec): InferencePlanner =
    new InferencePlanner(inference)(outputPartitionSpec)
}
