package ai.chronon.api.planner
import ai.chronon.api.{ExecutionInfo, MetaData, PartitionSpec, TableDependency, TableInfo}
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions.JListOps

import java.util

object MetaDataUtils {

  def outputPartitionSpec(baseMetadata: MetaData, defaultSpec: PartitionSpec): PartitionSpec =
    (for {
      metadata <- Option(baseMetadata)
      executionInfo <- Option(metadata.executionInfo)
      outputTableInfo <- Option(executionInfo.outputTableInfo)
    } yield outputTableInfo.partitionSpec(defaultSpec)).getOrElse(defaultSpec)

  def tableInfo(table: String, partitionSpec: PartitionSpec): TableInfo =
    new TableInfo()
      .setTable(table)
      .setPartitionColumn(partitionSpec.column)
      .setPartitionFormat(partitionSpec.format)
      .setPartitionInterval(WindowUtils.fromMillis(partitionSpec.spanMillis))

  def validateWideningOrEqualConsumer(nodeName: String,
                                      consumerPartitionSpec: PartitionSpec,
                                      producerPartitionSpec: PartitionSpec,
                                      producerDescription: String): Unit = {
    val consumerMillis = consumerPartitionSpec.spanMillis
    val producerMillis = producerPartitionSpec.spanMillis
    require(
      consumerMillis >= producerMillis && consumerMillis % producerMillis == 0,
      s"Invalid partition interval for $nodeName: consumer interval ${WindowUtils.millisToString(consumerMillis)} " +
        s"must be equal to or a multiple of producer interval ${WindowUtils.millisToString(producerMillis)} " +
        s"($producerDescription)."
    )
  }

  def layer(baseMetadata: MetaData,
            modeName: String,
            nodeName: String,
            tableDependencies: Seq[TableDependency],
            stepDays: Option[Int] = None,
            outputTableOverride: Option[String] = None)(implicit partitionSpec: PartitionSpec): MetaData = {

    val copy = baseMetadata.deepCopy()
    val effectivePartitionSpec = outputPartitionSpec(copy, partitionSpec)
    val newName = nodeName
    copy.setName(newName)

    val baseExecutionInfo = Option(copy.executionInfo).getOrElse(new ExecutionInfo())
    val mergedExecutionInfo = mergeModeConfAndEnv(baseExecutionInfo, modeName)
    copy.setExecutionInfo(mergedExecutionInfo)

    // if stepDays is passed in respect it, otherwise use what's already there, otherwise set it to 1.
    if (stepDays.nonEmpty) {
      copy.executionInfo.setStepDays(stepDays.get)
    } else if (!copy.executionInfo.isSetStepDays) {
      copy.executionInfo.setStepDays(1)
    }

    // legacy output table and new style should match:
    // align metadata.outputTable == metadata.executionInfo.outputTableInfo.table
    if (copy.executionInfo.outputTableInfo == null) {
      copy.executionInfo.setOutputTableInfo(new TableInfo())
    }

    val tableInfo =
      if (outputTableOverride.isDefined) {
        copy.executionInfo.outputTableInfo.setTable(outputTableOverride.get)
      } else {
        // if output table is not set, use the base metadata's output table
        // fully qualified: namespace + outputTable
        copy.executionInfo.outputTableInfo.setTable(copy.outputTable)
      }

    tableInfo
      .setPartitionColumn(effectivePartitionSpec.column)
      .setPartitionFormat(effectivePartitionSpec.format)
      .setPartitionInterval(WindowUtils.fromMillis(effectivePartitionSpec.spanMillis))

    // set table dependencies
    copy.executionInfo.setTableDependencies(tableDependencies.toJava)

    copy
  }

  // merge common + mode confs and envs, discard others and return a simpler / leaner execution info
  private def mergeModeConfAndEnv(executionInfo: ExecutionInfo, mode: String): ExecutionInfo = {

    val result = executionInfo.deepCopy()

    if (executionInfo.conf != null) {
      val merged = new util.HashMap[String, String]()

      if (executionInfo.conf.common != null) merged.putAll(executionInfo.conf.common)

      if (executionInfo.conf.modeConfigs != null) {
        val modeConf = executionInfo.conf.modeConfigs.get(mode)
        if (modeConf != null) merged.putAll(modeConf)
      }

      result.conf.setCommon(merged)
      result.conf.unsetModeConfigs()
    }

    if (executionInfo.clusterConf != null) {
      val clusterMerged = new util.HashMap[String, String]()
      if (executionInfo.clusterConf.common != null) clusterMerged.putAll(executionInfo.clusterConf.common)
      if (executionInfo.clusterConf.modeClusterConfigs != null) {
        val modeConf = executionInfo.clusterConf.modeClusterConfigs.get(mode)
        if (modeConf != null) clusterMerged.putAll(modeConf)
      }

      result.clusterConf.setCommon(clusterMerged)
      result.clusterConf.unsetModeClusterConfigs()

    }

    if (executionInfo.env != null) {
      val merged = new util.HashMap[String, String]()

      if (executionInfo.env.common != null) merged.putAll(executionInfo.env.common)

      if (executionInfo.env.modeEnvironments != null) {
        val modeEnv = executionInfo.env.modeEnvironments.get(mode)
        if (modeEnv != null) merged.putAll(modeEnv)
      }

      result.env.setCommon(merged)
      result.env.unsetModeEnvironments()
    }

    result
  }

}
