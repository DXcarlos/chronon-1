package ai.chronon.api.planner
import ai.chronon.api.{DataModel, ExecutionInfo, MetaData, PartitionSpec, TableDependency, TableInfo}
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

  def applyPartitionSpec(tableInfo: TableInfo, partitionSpec: PartitionSpec): TableInfo =
    tableInfo
      .setPartitionColumn(partitionSpec.column)
      .setPartitionFormat(partitionSpec.format)
      .setPartitionInterval(WindowUtils.fromMillis(partitionSpec.spanMillis))
      .setPartitionOffset(WindowUtils.fromMillis(partitionSpec.offsetMillis))

  def tableInfo(table: String, partitionSpec: PartitionSpec): TableInfo =
    applyPartitionSpec(new TableInfo().setTable(table), partitionSpec)

  /** Shape of the producer side of a partition-grid edge. The narrowing-rejection policy is
    * event-shaped: a finer consumer over a coarser event producer is missing data. Snapshot
    * producers (entity snapshots, and later model versions) admit valid narrowing semantics —
    * binding the latest producer partition at or before the consumer boundary yields staleness,
    * not missing data — so their policy lives on a separate path that the as-of follow-up can
    * relax without touching the event policy.
    */
  sealed trait EdgeShape
  object EdgeShape {
    case object Events extends EdgeShape
    case object Snapshot extends EdgeShape

    def of(dataModel: DataModel): EdgeShape = dataModel match {
      case DataModel.EVENTS   => Events
      case DataModel.ENTITIES => Snapshot
    }
  }

  private def gridString(spec: PartitionSpec): String =
    s"interval ${WindowUtils.millisToString(spec.spanMillis)} @ offset ${WindowUtils.millisToString(spec.offsetMillis)}"

  /** Validates that a consumer can cleanly cover its producer's partition grid: the consumer
    * interval must be an equal-or-coarser multiple of the producer interval AND the two grids
    * must be congruent (offsets differ by a whole number of producer intervals).
    *
    * @param snapshotAsOf snapshot-shaped edges where the engine binds the producer as-of the
    *                     consumer boundary (e.g. join snapshot parts recomputed per left row
    *                     time). Narrowing and grid misalignment are legal there — staleness,
    *                     not missing data — so covering validation is skipped for them.
    */
  def validateEdgeGrids(nodeName: String,
                        consumerSpec: PartitionSpec,
                        producerSpec: PartitionSpec,
                        producerDescription: String,
                        shape: EdgeShape,
                        snapshotAsOf: Boolean = false): Unit = {
    val consumerMillis = consumerSpec.spanMillis
    val producerMillis = producerSpec.spanMillis
    val covering = consumerMillis >= producerMillis && consumerMillis % producerMillis == 0
    val congruent = Math.floorMod(consumerSpec.offsetMillis - producerSpec.offsetMillis, producerMillis) == 0L

    shape match {
      case EdgeShape.Snapshot if snapshotAsOf =>
      // as-of binding: alignment is irrelevant, nothing to validate

      case EdgeShape.Snapshot =>
        // First-cut seam: snapshot edges keep the covering rejection here until as-of binding
        // is supported on this edge. Relaxing this branch later is additive.
        require(
          covering,
          s"Invalid partition interval for $nodeName: consumer interval ${WindowUtils.millisToString(consumerMillis)} " +
            s"must be equal to or a multiple of snapshot producer interval ${WindowUtils.millisToString(producerMillis)} " +
            s"($producerDescription); as-of consumption of finer snapshot grids is not supported on this edge yet."
        )
        requireCongruent(nodeName, consumerSpec, producerSpec, producerDescription, congruent)

      case EdgeShape.Events =>
        require(
          covering,
          s"Invalid partition interval for $nodeName: consumer interval ${WindowUtils.millisToString(consumerMillis)} " +
            s"must be equal to or a multiple of event producer interval ${WindowUtils.millisToString(producerMillis)} " +
            s"($producerDescription)."
        )
        requireCongruent(nodeName, consumerSpec, producerSpec, producerDescription, congruent)
    }
  }

  private def requireCongruent(nodeName: String,
                               consumerSpec: PartitionSpec,
                               producerSpec: PartitionSpec,
                               producerDescription: String,
                               congruent: Boolean): Unit =
    require(
      congruent,
      s"Incompatible partition grids for $nodeName: consumer grid (${gridString(consumerSpec)}) is not congruent " +
        s"with producer grid (${gridString(producerSpec)}) ($producerDescription); " +
        "grid offsets must differ by a whole number of producer intervals."
    )

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

    applyPartitionSpec(tableInfo, effectivePartitionSpec)

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
