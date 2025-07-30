package ai.chronon.api.planner

import ai.chronon.api.Extensions.{GroupByOps, MetadataOps, SourceOps, StringOps}
import ai.chronon.api.ScalaJavaConversions.{IterableOps, IteratorOps}
import ai.chronon.api._
import ai.chronon.planner._

import scala.collection.Seq
import scala.language.{implicitConversions, reflectiveCalls}

class JoinPlanner(join: Join)(implicit outputPartitionSpec: PartitionSpec)
    extends ConfPlanner[Join](join)(outputPartitionSpec) {

  // will mutate the join in place - use on deepCopy-ied objects only
  private def unsetNestedMetadata(join: Join): Unit = {
    join.unsetMetaData()
    Option(join.joinParts).foreach(_.iterator().toScala.foreach(_.groupBy.unsetMetaData()))
    Option(join.labelParts).foreach(_.labels.iterator().toScala.foreach(_.groupBy.unsetMetaData()))
    join.unsetOnlineExternalParts()
  }

  private def joinWithoutExecutionInfo: Join = {
    val copied = join.deepCopy()
    copied.metaData.unsetExecutionInfo()
    Option(copied.joinParts).foreach(_.iterator().toScala.foreach(_.groupBy.metaData.unsetExecutionInfo()))
    Option(copied.labelParts).foreach(_.labels.iterator().toScala.foreach(_.groupBy.metaData.unsetExecutionInfo()))
    copied.unsetOnlineExternalParts()
    copied
  }

  val leftSourceNode: Node = {

    val left = join.left
    val result = new SourceWithFilterNode()
      .setSource(left)
      .setExcludeKeys(join.skewKeys)

    val leftSourceHash = ThriftJsonCodec.hexDigest(result)
    val leftSourceTable = left.table.replace(".", "__").sanitize // source_namespace.table -> source_namespace__table
    val outputTableName =
      leftSourceTable + "__" + leftSourceHash + "/source_cache" // source__<source_namespace>__<table>__<hash>

    // at this point metaData.outputTable = join_namespace.source__<source_namespace>__<table>__<hash>
    val metaData = MetaDataUtils.layer(
      join.metaData,
      "left_source",
      outputTableName,
      TableDependencies.fromSource(join.left).toSeq,
      stepDays = Some(1)
    )

    toNode(metaData, _.setSourceWithFilter(result), result)
  }

  private val bootstrapNodeOpt: Option[Node] = Option(join.bootstrapParts).map { bootstrapParts =>
    val result = new JoinBootstrapNode()
      .setJoin(joinWithoutExecutionInfo)

    // bootstrap tables are unfortunately unique to the join - can't be re-used if a new join part is added
    val bootstrapNodeName = join.metaData.name + "__bootstrap"

    val tableDeps = bootstrapParts.toScala.map { bp =>
      TableDependencies.fromTable(bp.table, bp.query)
    }.toSeq :+ TableDependencies.fromTable(leftSourceNode.metaData.outputTable)

    val metaData = MetaDataUtils.layer(
      join.metaData,
      "bootstrap",
      bootstrapNodeName,
      tableDeps,
      stepDays = Some(1)
    )

    val content = new NodeContent()
    content.setJoinBootstrap(result)

    val copy = result.deepCopy()
    unsetNestedMetadata(copy.join)

    toNode(metaData, _.setJoinBootstrap(result), copy)
  }

  private def copyAndEraseExecutionInfo(joinPart: JoinPart): JoinPart = {
    val copy = joinPart.deepCopy()
    copy.groupBy.metaData.unsetExecutionInfo()
    copy
  }

  private def buildJoinPartNode(joinPart: JoinPart): Node = {

    val result = new JoinPartNode()
      .setJoinPart(copyAndEraseExecutionInfo(joinPart))
      .setLeftDataModel(join.left.dataModel)
      .setLeftSourceTable(leftSourceNode.metaData.outputTable)

    val partTable = RelevantLeftForJoinPart.partTableName(join, joinPart)

    val deps = TableDependencies.fromGroupBy(joinPart.groupBy, Option(join.left.dataModel)) :+
      TableDependencies.fromTable(leftSourceNode.metaData.outputTable)

    // use step days from group_by if set, otherwise default to 15d for events and 1 for entities
    val stepDays = Option(joinPart.groupBy.metaData.executionInfo)
      .filter(_.isSetStepDays)
      .map(_.stepDays)
      .getOrElse(joinPart.groupBy.dataModel match {
        case DataModel.ENTITIES => 1
        case DataModel.EVENTS   => 15
      })

    // pull conf params from the groupBy metadata, but use the join namespace to write to.
    val metaData = MetaDataUtils
      .layer(
        joinPart.groupBy.metaData,
        "right_part",
        partTable,
        deps,
        stepDays = Some(stepDays)
      )
      .setOutputNamespace(join.metaData.outputNamespace)

    val copy = result.deepCopy()
    copy.joinPart.groupBy.unsetMetaData()

    toNode(metaData, _.setJoinPart(result), copy)
  }

  private val joinPartNodes: Seq[Node] = join.joinParts.toScala.map { buildJoinPartNode }.toSeq

  val mergeNode: Node = {
    val result = new JoinMergeNode()
      .setJoin(join)

    // sometimes the keys get bootstrapped. so we need to pick bootstraps if present for left side
    val leftTable = bootstrapNodeOpt
      .map(_.metaData.outputTable)
      .getOrElse(leftSourceNode.metaData.outputTable)

    // TODO: we might need to shift back 1 day for snapshot events case while partition sensing
    //
    // currently it works out fine, because we shift forward and back in the engine cancelling out the
    // date ranges that need to be scheduled
    val deps = joinPartNodes.map { jpNode =>
      TableDependencies.fromTable(jpNode.metaData.outputTable)
    } :+
      TableDependencies.fromTable(leftTable)

    val mergeNodeName = join.metaData.name + "/merged"

    val metaData = MetaDataUtils
      .layer(
        join.metaData,
        "merge",
        mergeNodeName,
        deps,
        stepDays = Some(1)
      )

    val copy = result.deepCopy()
    unsetNestedMetadata(copy.join)
    copy.join.unsetDerivations()
    copy.join.unsetLabelParts()

    toNode(metaData, _.setJoinMerge(result), copy)
  }

  private val derivationNodeOpt: Option[Node] = Option(join.derivations).map { _ =>
    val result = new JoinDerivationNode()
      .setJoin(join)

    val derivationNodeName = join.metaData.name + "/derived"

    val metaData = MetaDataUtils
      .layer(
        join.metaData,
        "derive",
        derivationNodeName,
        Seq(TableDependencies.fromTable(mergeNode.metaData.outputTable)),
        stepDays = Some(1)
      )

    val copy = result.deepCopy()
    unsetNestedMetadata(copy.join)
    copy.join.unsetLabelParts()

    toNode(metaData, _.setJoinDerivation(result), copy)
  }

  // these need us to additionally (groupBy backfill) generate the snapshot tables
  private val snapshotLabelParts: Array[JoinPart] = Option(join.labelParts)
    .map(
      _.labels
        .iterator()
        .toScala
        .filter { jp => jp.groupBy.inferredAccuracy == Accuracy.SNAPSHOT }
        .toArray
    )
    .getOrElse(Array.empty)

  private val labelJoinNodeOpt: Option[Node] = Option(join.labelParts).map { labelParts =>
    val result = new LabelJoinNode()
      .setJoin(join)

    val labelNodeName = join.metaData.name + "/labeled"

    val inputTable = derivationNodeOpt
      .map(_.metaData.outputTable)
      .getOrElse(mergeNode.metaData.outputTable)

    val labelPartDeps = TableDependencies.fromJoin(join) :+ TableDependencies.fromTable(inputTable)

    val metaData = MetaDataUtils
      .layer(
        join.metaData,
        "label_join",
        labelNodeName,
        labelPartDeps,
        stepDays = Some(1)
      )

    val copy = result.deepCopy()
    unsetNestedMetadata(copy.join)

    toNode(metaData, _.setLabelJoin(result), copy)
  }

  def offlineNodes: Seq[Node] = {

    Seq(leftSourceNode) ++
      bootstrapNodeOpt ++
      joinPartNodes ++
      Seq(mergeNode) ++
      derivationNodeOpt ++
      snapshotLabelParts.map(_.groupBy).map { gb =>
        new GroupByPlanner(gb).backfillNode
      } ++
      labelJoinNodeOpt
  }

  override def buildPlan: ConfPlan = ???
}

object JoinPlanner {

  // will mutate the join in place - use on deepCopy-ied objects only
  private def unsetNestedMetadata(join: Join): Unit = {
    join.unsetMetaData()
    Option(join.joinParts).foreach(_.iterator().toScala.foreach(_.groupBy.unsetMetaData()))
    Option(join.labelParts).foreach(_.labels.iterator().toScala.foreach(_.groupBy.unsetMetaData()))
    join.unsetOnlineExternalParts()
  }

}
