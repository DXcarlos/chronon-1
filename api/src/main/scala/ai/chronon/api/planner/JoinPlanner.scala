package ai.chronon.api.planner

import ai.chronon.api.Extensions.{GroupByOps, MetadataOps, SourceOps, StringOps, TableInfoOps, WindowUtils}
import ai.chronon.api.ScalaJavaConversions.{IterableOps, IteratorOps}
import ai.chronon.api._
import ai.chronon.planner
import ai.chronon.planner._

import scala.collection.JavaConverters._
import scala.language.{implicitConversions, reflectiveCalls}

class JoinPlanner(join: Join)(implicit outputPartitionSpec: PartitionSpec)
    extends ConfPlanner[Join](join)(outputPartitionSpec) {

  private val confOutputPartitionSpec: PartitionSpec =
    MetaDataUtils.outputPartitionSpec(join.metaData, outputPartitionSpec)

  private def validatePartitionIntervals(): Unit =
    JoinPlanner.validateJoinPartGrids(join, confOutputPartitionSpec)

  // will mutate the join in place - use on deepCopy-ied objects only
  private def joinWithoutMetadata(join: Join): Unit = {
    join.unsetMetaData()
    Option(join.joinParts).foreach(_.iterator().toScala.foreach(_.groupBy.unsetMetaData()))
  }

  private def joinWithoutExecutionInfo: Join = {
    val copied = join.deepCopy()
    copied.metaData.unsetExecutionInfo()
    Option(copied.joinParts).foreach(_.iterator().toScala.foreach(_.groupBy.metaData.unsetExecutionInfo()))
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
      leftSourceTable + "__" + leftSourceHash + "__source" // source__<source_namespace>__<table>__<hash>

    // at this point metaData.outputTable = join_namespace.source__<source_namespace>__<table>__<hash>
    val metaData = MetaDataUtils.layer(
      join.metaData,
      "source",
      outputTableName,
      TableDependencies.fromSource(join.left, maxWindowOpt = Some(WindowUtils.zero())).toSeq
    )(confOutputPartitionSpec)

    toNode(metaData, _.setSourceWithFilter(result), result)
  }

  private val bootstrapNodeOpt: Option[Node] = Option(join.bootstrapParts).map { bootstrapParts =>
    val result = new JoinBootstrapNode()
      .setJoin(joinWithoutExecutionInfo)

    // bootstrap tables follow the standard naming convention: outputTable + "_bootstrap"
    val bootstrapNodeName = join.metaData.name + "_bootstrap"

    val tableDeps = bootstrapParts.toScala.map { bp =>
      TableDependencies.fromTable(bp.table, bp.query)
    }.toSeq :+ TableDependencies.fromTableInfo(leftSourceNode.metaData.executionInfo.outputTableInfo)

    val metaData = MetaDataUtils.layer(
      join.metaData,
      "bootstrap",
      bootstrapNodeName,
      tableDeps
    )(confOutputPartitionSpec)

    val content = new NodeContent()
    content.setJoinBootstrap(result)

    val copy = result.deepCopy()
    joinWithoutMetadata(copy.join)

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
      TableDependencies.fromTableInfo(leftSourceNode.metaData.executionInfo.outputTableInfo)

    // use step days from group_by if set, otherwise default to 15d for events and 1 for entities
    val stepDays = Option(joinPart.groupBy.metaData.executionInfo)
      .filter(_.isSetStepDays)
      .map(_.stepDays)
      .getOrElse(joinPart.groupBy.dataModel match {
        case DataModel.ENTITIES => 1
        case DataModel.EVENTS   => 15
      })

    val metaData = MetaDataUtils
      .layer(
        joinPart.groupBy.metaData,
        "join_part",
        partTable,
        deps,
        stepDays = Some(stepDays)
      )(confOutputPartitionSpec)
      .setOutputNamespace(join.metaData.outputNamespace)

    // Join part tables are join intermediates partitioned by the join's partition column, but
    // snapshot-accuracy parts under an EVENTS left live at the RHS groupBy's declared snapshot
    // grid (per-row as-of binding) - declare the true physical grid so coverage checks,
    // watermarks and sensors read the table correctly. Everything else stays in the join/left
    // domain.
    val partTableSpec =
      if (join.left.dataModel == DataModel.EVENTS && joinPart.groupBy.inferredAccuracy == Accuracy.SNAPSHOT)
        MetaDataUtils.partSnapshotSpec(joinPart, confOutputPartitionSpec)
      else confOutputPartitionSpec
    MetaDataUtils.applyPartitionSpec(metaData.executionInfo.outputTableInfo, partTableSpec)

    val copy = result.deepCopy()
    copy.joinPart.groupBy.unsetMetaData()

    toNode(metaData, _.setJoinPart(result), copy)
  }

  private val joinPartNodes: Seq[Node] = join.joinParts.toScala.map { buildJoinPartNode }.toSeq

  val mergeNode: Node = {
    val result = new JoinMergeNode()
      .setJoin(join)

    val deps = joinPartNodes.map { jpNode =>
      val shouldShift = join.left.dataModel == DataModel.EVENTS &&
        jpNode.content.getJoinPart.joinPart.groupBy.inferredAccuracy == Accuracy.SNAPSHOT

      // The engine (MergeJob) reads snapshot-part tables one RHS snapshot span back
      // (JoinUtils.snapshotLookbackRange), so sensors must require the same span-based
      // shift - a hardcoded daily shift would mis-require for sub-daily grids. The part
      // node's outputTableInfo carries the RHS snapshot grid for snapshot parts.
      val shiftAmount =
        if (shouldShift)
          Some(
            jpNode.metaData.executionInfo.outputTableInfo
              .partitionSpec(confOutputPartitionSpec)
              .intervalWindow)
        else None
      TableDependencies.fromTableInfo(jpNode.metaData.executionInfo.outputTableInfo, shift = shiftAmount)
    } :+
      TableDependencies.fromTableInfo(
        bootstrapNodeOpt
          .map(_.metaData.executionInfo.outputTableInfo)
          .getOrElse(leftSourceNode.metaData.executionInfo.outputTableInfo)
      )

    val mergeNodeName = join.metaData.name + "__merged"

    val metaData = MetaDataUtils
      .layer(
        join.metaData,
        "merge",
        mergeNodeName,
        deps,
        outputTableOverride = Some(join.metaData.outputTable)
      )(confOutputPartitionSpec)

    val copy = result.deepCopy()
    joinWithoutMetadata(copy.join)
    copy.join.unsetDerivations()

    toNode(metaData, _.setJoinMerge(result), copy)
  }

  private val derivationNodeOpt: Option[Node] = Option(join.derivations).map { _ =>
    val result = new JoinDerivationNode()
      .setJoin(join)

    val derivationNodeName = join.metaData.name + "__derived"
    val derivationOutputTable = join.metaData.outputTable + "__derived"

    val metaData = MetaDataUtils
      .layer(
        join.metaData,
        "derive",
        derivationNodeName,
        Seq(TableDependencies.fromTableInfo(mergeNode.metaData.executionInfo.outputTableInfo)),
        outputTableOverride = Some(derivationOutputTable)
      )(confOutputPartitionSpec)

    val copy = result.deepCopy()
    joinWithoutMetadata(copy.join)

    toNode(metaData, _.setJoinDerivation(result), copy)
  }

  private val statsComputeNodeOpt: Option[Node] = {
    val enableStatsCompute = Option(join.metaData.executionInfo)
      .flatMap(ei => Option(ei.enableStatsCompute))
      .exists(_.booleanValue())

    if (enableStatsCompute) {
      Some {
        val result = new JoinStatsComputeNode()
          .setJoin(join)

        val statsComputeNodeName = join.metaData.name + "__stats_compute"

        // Stats compute depends on the final output (derivation if present, otherwise merge)
        val inputTableInfo = derivationNodeOpt
          .map(_.metaData.executionInfo.outputTableInfo)
          .getOrElse(mergeNode.metaData.executionInfo.outputTableInfo)

        val stepDays = 1 // Stats computed daily

        val tableDep = TableDependencies.fromTableInfo(inputTableInfo)

        val metaData = MetaDataUtils
          .layer(
            join.metaData,
            "stats_compute",
            statsComputeNodeName,
            Seq(tableDep),
            Some(stepDays)
          )(confOutputPartitionSpec)

        val copy = result.deepCopy()
        joinWithoutMetadata(copy.join)

        toNode(metaData, _.setJoinStatsCompute(result), copy)
      }
    } else {
      None
    }
  }

  def offlineNodes: Seq[Node] = {

    Seq(leftSourceNode) ++
      bootstrapNodeOpt ++
      joinPartNodes ++
      Seq(mergeNode) ++
      derivationNodeOpt ++
      statsComputeNodeOpt
  }

  def metadataUploadNode: Node = {
    val stepDays = 1 // Default step days for metadata upload

    // Create table dependencies for all GroupBy parts (both direct GroupBy deps and upstream join deps)
    val allDeps = Option(join.joinParts).map(_.toScala).getOrElse(Seq.empty).flatMap { joinPart =>
      val groupBy = joinPart.groupBy
      val hasStreamingSource = groupBy.streamingSource.isDefined

      // Add dependency on the GroupBy node (either uploadToKV or streaming)
      val groupByTableName = if (hasStreamingSource) {
        groupBy.metaData.outputTable + s"__${GroupByPlanner.Streaming}"
      } else {
        groupBy.metaData.outputTable + s"__${GroupByPlanner.UploadToKV}"
      }
      val groupByOutputSpec = MetaDataUtils.outputPartitionSpec(groupBy.metaData, confOutputPartitionSpec)

      val groupByDep = new TableDependency()
        .setTableInfo(
          MetaDataUtils.tableInfo(groupByTableName, groupByOutputSpec)
        )
        .setStartOffset(WindowUtils.zero())
        .setEndOffset(WindowUtils.zero())

      // Add dependencies on upstream join metadata uploads if GroupBy has JoinSource
      val upstreamJoinDeps = if (hasStreamingSource) {
        // Skip this for streaming GroupBys since the streaming node will handle this dependency
        Seq.empty
      } else {
        TableDependencies.fromJoinSources(groupBy.sources)
      }

      // Return both the GroupBy dependency and any upstream join dependencies
      Seq(groupByDep) ++ upstreamJoinDeps
    }
    val metadataUploadDeps = allDeps

    val metaData =
      MetaDataUtils.layer(join.metaData,
                          "metadata_upload",
                          join.metaData.name + "__metadata_upload",
                          metadataUploadDeps.toSeq,
                          Some(stepDays))(confOutputPartitionSpec)
    val node = new JoinMetadataUpload().setJoin(joinWithoutExecutionInfo)

    val copy = joinWithoutExecutionInfo.deepCopy()
    joinWithoutMetadata(copy)

    toNode(metaData, _.setJoinMetadataUpload(node), copy)
  }

  def unionJoinNode: Node = {
    val result = new planner.UnionJoinNode()
      .setJoin(joinWithoutExecutionInfo)

    val metaData = MetaDataUtils.layer(
      join.metaData,
      "union_join",
      join.metaData.name,
      TableDependencies.fromJoin(join).toSeq,
      outputTableOverride = Some(join.metaData.outputTable)
    )(confOutputPartitionSpec)

    val copy = result.deepCopy()
    joinWithoutMetadata(copy.join)

    toNode(metaData, _.setUnionJoin(result), copy)
  }

  override def buildPlan: ConfPlan = {
    validatePartitionIntervals()
    // Check if this join is eligible for UnionJoin
    // Conditions: left is events, 1 join part, TEMPORAL accuracy, no bootstrap parts
    val isUnionJoinEligible = join.left.isSetEvents &&
      join.getJoinParts.size() == 1 &&
      join.getJoinParts.get(0).groupBy.inferredAccuracy == Accuracy.TEMPORAL &&
      !join.isSetBootstrapParts

    if (isUnionJoinEligible) {
      // Use UnionJoin path
      val unionNode = unionJoinNode
      val sensorNodes = ExternalSourceSensorUtil
        .sensorNodes(unionNode.metaData)
        .map((es) =>
          toNode(es.metaData, _.setExternalSourceSensor(es), ExternalSourceSensorUtil.semanticExternalSourceSensor(es)))

      val metadataUpload = metadataUploadNode

      val terminalNodeNames = Map(
        planner.Mode.BACKFILL -> unionNode.metaData.name,
        planner.Mode.DEPLOY -> metadataUpload.metaData.name
      )

      new ConfPlan()
        .setNodes((Seq(unionNode, metadataUpload) ++ sensorNodes).asJava)
        .setTerminalNodeNames(terminalNodeNames.asJava)
    } else {
      // Use standard modular path
      val allOfflineNodes = offlineNodes

      // The final offline node is the backfill terminal
      val backfillTerminalNode = allOfflineNodes.last

      // Get sensor nodes for the backfill terminal node
      val sensorNodes = ExternalSourceSensorUtil
        .sensorNodes(backfillTerminalNode.metaData)
        .map((es) =>
          toNode(es.metaData, _.setExternalSourceSensor(es), ExternalSourceSensorUtil.semanticExternalSourceSensor(es)))

      val metadataUpload = metadataUploadNode

      val terminalNodeNames = Map(
        planner.Mode.BACKFILL -> backfillTerminalNode.metaData.name,
        planner.Mode.DEPLOY -> metadataUpload.metaData.name
      )

      new ConfPlan()
        .setNodes((allOfflineNodes ++ Seq(metadataUpload) ++ sensorNodes).asJava)
        .setTerminalNodeNames(terminalNodeNames.asJava)
    }
  }
}

object JoinPlanner {

  // Join parts are bound by left row time, not by partition-label covering:
  //   - snapshot parts: MergeJob binds each left row to the latest RHS snapshot whose as-of
  //     boundary is <= the row's ts, ON THE RHS GROUPBY'S DECLARED GRID — binding is
  //     grid-independent, so snapshot-shaped narrowing and widening are both legal (bounded
  //     staleness rather than missing data); validated through the snapshot as-of seam so
  //     future policy changes stay localized,
  //   - temporal parts: the engine recomputes from raw events with temporal accuracy, so the
  //     groupBy output grid does not constrain the join grid at all.
  def validateJoinPartGrids(join: Join, joinSpec: PartitionSpec): Unit = {
    Option(join.joinParts).foreach { joinParts =>
      joinParts.asScala.foreach { joinPart =>
        if (joinPart.groupBy.inferredAccuracy == Accuracy.SNAPSHOT) {
          val groupBySpec = MetaDataUtils.outputPartitionSpec(joinPart.groupBy.metaData, joinSpec)
          if (joinPart.groupBy.dataModel == DataModel.ENTITIES) {
            MetaDataUtils.warnSubDailyEntitySnapshot(
              s"join ${join.metaData.name}, part ${joinPart.groupBy.metaData.name}",
              groupBySpec)
          }
          MetaDataUtils.validateEdgeGrids(
            join.metaData.name,
            joinSpec,
            groupBySpec,
            s"groupBy ${joinPart.groupBy.metaData.name}",
            MetaDataUtils.EdgeShape.Snapshot,
            snapshotAsOf = true
          )
        }
      }
    }
  }

  // will mutate the join in place - use on deepCopy-ied objects only
  private def unsetNestedMetadata(join: Join): Unit = {
    join.unsetMetaData()
    Option(join.joinParts).foreach(_.iterator().toScala.foreach(_.groupBy.unsetMetaData()))
    // Keep onlineExternalParts as they affect output schema and are needed for bootstrap/merge/derivation
    // join.unsetOnlineExternalParts()
  }

}
