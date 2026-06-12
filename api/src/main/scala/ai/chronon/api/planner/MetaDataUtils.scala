package ai.chronon.api.planner
import ai.chronon.api.{DataModel, ExecutionInfo, MetaData, PartitionSpec, TableDependency, TableInfo}
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions.JListOps

import java.util

object MetaDataUtils {

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  /** Sub-daily entity snapshots are supported but storage-expensive: every snapshot partition
    * is a FULL copy of dimensional state, so an N-per-day grid multiplies storage and the
    * partitions scanned by windowed aggregations by N - mostly for features that did not
    * change between snapshots. If intraday entity state matters, prefer declaring mutations
    * and TEMPORAL accuracy, which gives row-granularity freshness without re-materializing
    * the dimension table N times a day.
    */
  def warnSubDailyEntitySnapshot(nodeName: String, spec: PartitionSpec): Unit =
    if (!spec.isDaily) {
      val perDay = spec.grid.partitionsPerDay
      logger.warn(
        s"$nodeName: sub-daily ENTITIES snapshots (${WindowUtils.millisToString(spec.spanMillis)} grid) " +
          s"re-materialize the full dimensional state ${perDay}x per day, multiplying storage and " +
          s"windowed-aggregation scan cost ${perDay}x. If intraday entity state matters, consider " +
          "declaring a mutation stream and TEMPORAL accuracy instead, which tracks entity state at " +
          "row granularity without re-materializing snapshots."
      )
    }

  /** The snapshot grid a snapshot-accuracy join part lives on: the RHS groupBy's declared
    * output grid (partition_interval/partition_offset), falling back to the coarsest grid the
    * groupBy's sources declare (planner nodes strip executionInfo from embedded groupBys, and
    * an entity source's table grid IS its snapshot grain), then to the join's grid when
    * nothing is declared anywhere. Declaring the join's own grid is a no-op, so daily-RHS-
    * under-daily-join reproduces the historical behavior exactly. Part tables are always
    * partitioned by the join's partition column; only the RHS span/offset/format carry over.
    */
  def partSnapshotSpec(joinPart: ai.chronon.api.JoinPart, joinSpec: PartitionSpec): PartitionSpec = {
    PartitionSpecResolver.snapshotSpec(joinPart, joinSpec)
  }

  def outputPartitionSpec(baseMetadata: MetaData, defaultSpec: PartitionSpec): PartitionSpec =
    PartitionSpecResolver.outputSpec(baseMetadata, defaultSpec)

  /** Stamps a table's partition spec; the offset is emitted only when nonzero so existing
    * daily confs serialize byte-identically (absent offset means midnight-anchored).
    */
  def applyPartitionSpec(tableInfo: TableInfo, partitionSpec: PartitionSpec): TableInfo = {
    tableInfo
      .setPartitionColumn(partitionSpec.column)
      .setPartitionFormat(partitionSpec.format)
      .setPartitionInterval(WindowUtils.fromMillis(partitionSpec.spanMillis))
    if (partitionSpec.offsetMillis != 0)
      tableInfo.setPartitionOffset(WindowUtils.fromMillis(partitionSpec.offsetMillis))
    else
      tableInfo.unsetPartitionOffset()
    tableInfo
  }

  def tableInfo(table: String, partitionSpec: PartitionSpec): TableInfo =
    PartitionSpecResolver.tableInfo(table, partitionSpec)

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

  /** Validates that a consumer can cleanly cover its producer's partition grid: the consumer
    * interval must be an exact multiple of the producer interval AND the consumer
    * must be aligned to the producer grid.
    *
    * @param snapshotAsOf snapshot-shaped edges where the engine binds the producer per row,
    *                     as-of the row's time on the producer's declared grid (join snapshot
    *                     parts: each left row binds the latest RHS snapshot with as-of
    *                     boundary <= row ts). Binding is grid-independent there - snapshot-
    *                     shaped narrowing AND widening are both fine, yielding bounded
    *                     staleness rather than missing data - so neither covering nor
    *                     congruence is validated for them.
    */
  def validateEdgeGrids(nodeName: String,
                        consumerSpec: PartitionSpec,
                        producerSpec: PartitionSpec,
                        producerDescription: String,
                        shape: EdgeShape,
                        snapshotAsOf: Boolean = false): Unit = {
    val consumerGrid = consumerSpec.grid
    val producerGrid = producerSpec.grid

    shape match {
      case EdgeShape.Snapshot if snapshotAsOf =>
      // per-row as-of binding on the producer's declared grid: alignment is irrelevant,
      // nothing to validate

      case EdgeShape.Snapshot =>
        // Non-as-of snapshot edges (e.g. a groupBy reading an entity snapshot source passes
        // the producer's partitions through unchanged) keep the covering rejection.
        require(
          consumerGrid.isExactMultipleOf(producerGrid),
          s"Invalid partition interval for $nodeName: ${consumerGrid.exactMultipleRequirement(producerGrid)} " +
            s"($producerDescription); as-of consumption of finer snapshot grids is not supported on this edge yet."
        )
        requireAligned(nodeName,
                       consumerSpec,
                       producerSpec,
                       producerDescription,
                       consumerGrid.isAlignedTo(producerGrid))

      case EdgeShape.Events =>
        require(
          consumerGrid.isExactMultipleOf(producerGrid),
          s"Invalid partition interval for $nodeName: ${consumerGrid.exactMultipleRequirement(producerGrid)} " +
            s"($producerDescription)."
        )
        requireAligned(nodeName,
                       consumerSpec,
                       producerSpec,
                       producerDescription,
                       consumerGrid.isAlignedTo(producerGrid))
    }
  }

  private def requireAligned(nodeName: String,
                             consumerSpec: PartitionSpec,
                             producerSpec: PartitionSpec,
                             producerDescription: String,
                             aligned: Boolean): Unit =
    require(
      aligned,
      s"Incompatible partition grids for $nodeName: ${consumerSpec.grid.alignmentRequirement(producerSpec.grid)} " +
        s"($producerDescription)"
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

    outputTableOverride match {
      case Some(outputTable) =>
        // Changing table identity also changes ownership of the partition metadata: rewrite all
        // partition fields so layered sensors cannot keep stale downstream grids.
        applyPartitionSpec(copy.executionInfo.outputTableInfo.setTable(outputTable), partitionSpec)
      case None =>
        // if output table is not set, use the base metadata's output table
        // fully qualified: namespace + outputTable
        val tableInfo = copy.executionInfo.outputTableInfo.setTable(copy.outputTable)
        // effectivePartitionSpec already preserves author-declared output fields and fills
        // missing fields from the default spec.
        applyPartitionSpec(tableInfo, effectivePartitionSpec)
    }

    val resolvedTableDependencies = PartitionSpecResolver.resolveDependencies(tableDependencies, effectivePartitionSpec)

    // time-partitioned dependencies have no physical grid - their column is a real timestamp -
    // so apply the consumer's grid to them: range math, sensing, and orchestration then
    // quantize intraday requirements on the node's own grain instead of assuming daily (which
    // would stall sub-daily readiness until the upstream day closes)
    resolvedTableDependencies.foreach { dep =>
      Option(dep.tableInfo).filter(ti => ti.isSetTimePartitioned && ti.timePartitioned).foreach { ti =>
        if (!ti.isSetPartitionInterval)
          ti.setPartitionInterval(WindowUtils.fromMillis(effectivePartitionSpec.spanMillis))
        if (!ti.isSetPartitionOffset && effectivePartitionSpec.offsetMillis != 0)
          ti.setPartitionOffset(WindowUtils.fromMillis(effectivePartitionSpec.offsetMillis))
        if (!ti.isSetPartitionFormat) ti.setPartitionFormat(effectivePartitionSpec.format)
      }
    }

    // set table dependencies
    copy.executionInfo.setTableDependencies(resolvedTableDependencies.toJava)

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
