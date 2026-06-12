package ai.chronon.api.planner
import ai.chronon.api.{DataModel, ExecutionInfo, MetaData, PartitionSpec, TableDependency, TableInfo}
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions.{JListOps, ListOps}

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
    val declaredOutput = for {
      md <- Option(joinPart.groupBy.metaData)
      ei <- Option(md.executionInfo)
      oti <- Option(ei.outputTableInfo)
      _ <- Option(oti.partitionInterval) // only an explicit declaration counts
    } yield oti.partitionSpec(joinSpec)

    lazy val declaredSource = Option(joinPart.groupBy.sources)
      .map(_.toScala.toSeq)
      .getOrElse(Seq.empty)
      .flatMap { s =>
        Option(s.query)
          .flatMap(q => Option(q.partitionInterval))
          .map(_ => s.query.partitionSpec(joinSpec))
      }
      .sortBy(-_.spanMillis)
      .headOption

    val declared = declaredOutput.orElse(declaredSource).getOrElse(joinSpec)
    if (declared.hasSameGrid(joinSpec)) joinSpec else declared.copy(column = joinSpec.column)
  }

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
    val consumerMillis = consumerSpec.spanMillis
    val producerMillis = producerSpec.spanMillis
    val covering = consumerMillis >= producerMillis && consumerMillis % producerMillis == 0
    val congruent = Math.floorMod(consumerSpec.offsetMillis - producerSpec.offsetMillis, producerMillis) == 0L

    shape match {
      case EdgeShape.Snapshot if snapshotAsOf =>
      // per-row as-of binding on the producer's declared grid: alignment is irrelevant,
      // nothing to validate

      case EdgeShape.Snapshot =>
        // Non-as-of snapshot edges (e.g. a groupBy reading an entity snapshot source passes
        // the producer's partitions through unchanged) keep the covering rejection.
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
