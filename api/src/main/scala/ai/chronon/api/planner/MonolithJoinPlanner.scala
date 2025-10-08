package ai.chronon.api.planner

import ai.chronon.api.Extensions.{GroupByOps, WindowUtils}
import ai.chronon.api.Extensions._
import ai.chronon.api.Constants
import ai.chronon.api.{Join, PartitionSpec, TableDependency, TableInfo}
import ai.chronon.planner
import ai.chronon.planner.{JoinConsistencyComputeNode, JoinLogFlatteningNode, JoinMetadataUpload, Node}

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
                          outputTableOverride = Some(join.metaData.outputTable))
    val node = new planner.MonolithJoinNode().setJoin(join)
    toNode(metaData, _.setMonolithJoin(node), semanticMonolithJoin(join))
  }

  private def consistencyComputeNode: Node = {
    val mode = "consistency-metrics-compute"
    val tableDeps = Seq(
      TableDependencies.fromTable(logFlatteningNode.metaData.outputTable)
    )
    val metaData =
      MetaDataUtils.layer(
        join.metaData,
        mode,
        join.metaData.name + s"__${mode.replace('-', '_')}",
        tableDeps,
        outputTableOverride = Some(join.metaData.consistencyTable)
      )
    val node = new JoinConsistencyComputeNode().setJoin(join)
    toNode(metaData, _.setJoinConsistencyComputeNode(node), semanticMonolithJoin(join))
  }

  private def logFlatteningNode: Node = {
    val mode = "log-flattener"
    // Parse Metadata to generate relevant execution info we need to generate tableDeps
    val preParseMetaData =
      MetaDataUtils.layer(join.metaData, mode, "", Seq.empty)
    val tableDeps = Seq(
      TableDependencies.fromTable(preParseMetaData.executionInfo.conf.common.get(Constants.LoggingSchemaTableConf)),
      TableDependencies.fromTable(preParseMetaData.executionInfo.conf.common.get(Constants.LoggingEventsTableConf))
    )
    val metaData =
      MetaDataUtils.layer(join.metaData,
                          mode,
                          join.metaData.name + s"__${mode.replace('-', '_')}",
                          tableDeps,
                          outputTableOverride = Some(join.metaData.loggedTable))
    val node = new JoinLogFlatteningNode().setJoin(join)
    toNode(metaData, _.setJoinLogFlatteningNode(node), semanticMonolithJoin(join))
  }

  private def hasLoggingTablesConfigured: Boolean = {
    // It's possible the tables are defined in common or mode specific conf.
    // So we check the layered metadata for the setting.
    val preParseMetaData =
      MetaDataUtils.layer(join.metaData, "log-flattener", "", Seq.empty)
    Option(preParseMetaData.executionInfo)
      .flatMap(ei => Option(ei.conf))
      .flatMap(conf => Option(conf.common))
      .exists { common =>
        Option(common.get(Constants.LoggingSchemaTableConf)).isDefined &&
        Option(common.get(Constants.LoggingEventsTableConf)).isDefined
      }
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

    // Only create log flattening node if samplePercent is set AND logging tables are configured
    val loggingEnabled = join.metaData.isSetSamplePercent && hasLoggingTablesConfigured

    // Only create consistency compute node if consistencySamplePercent is set and non-zero
    // Consistency requires logging to be enabled first since it depends on the logged table
    val consistencyEnabled = loggingEnabled && join.metaData.consistencyCheck

    val loggingNodes = if (loggingEnabled) {
      List(logFlatteningNode)
    } else {
      List.empty
    }

    val consistencyNodes = if (consistencyEnabled) {
      List(consistencyComputeNode)
    } else {
      List.empty
    }

    // Determine MONITOR terminal node:
    // - If consistency is enabled, point to consistency compute node
    // - Else if logging is enabled, but no consistency point to log flattening node
    // - Else don't include MONITOR mode
    val terminalNodeNames = if (consistencyEnabled) {
      Map(
        planner.Mode.BACKFILL -> backfill.metaData.name,
        planner.Mode.DEPLOY -> metadataUploadNode.metaData.name,
        planner.Mode.MONITOR -> consistencyComputeNode.metaData.name
      )
    } else if (loggingEnabled) {
      Map(
        planner.Mode.BACKFILL -> backfill.metaData.name,
        planner.Mode.DEPLOY -> metadataUploadNode.metaData.name,
        planner.Mode.MONITOR -> logFlatteningNode.metaData.name
      )
    } else {
      Map(
        planner.Mode.BACKFILL -> backfill.metaData.name,
        planner.Mode.DEPLOY -> metadataUploadNode.metaData.name
      )
    }

    confPlan
      .setNodes((List(backfill, metadataUploadNode) ++ loggingNodes ++ consistencyNodes ++ sensorNodes).asJava)
      .setTerminalNodeNames(terminalNodeNames.asJava)
  }
}
