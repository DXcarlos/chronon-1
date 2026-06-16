package ai.chronon.api.planner

import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions.ListOps
import ai.chronon.api.{
  Accuracy,
  DataModel,
  ExecutionInfo,
  JoinPart,
  MetaData,
  PartitionSpec,
  Query,
  Source,
  TableDependency,
  TableInfo,
  Window
}

/** Planner-side partition resolution and grid validation. This object owns fallback policy and
  * writes the resolved specs into existing thrift fields so runtime code can read them without
  * re-deriving policy.
  */
object PartitionSpecResolver {

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  /** Resolves a node output spec from metadata, falling back to the supplied planner default. */
  def outputSpec(metadata: MetaData, defaultSpec: PartitionSpec): PartitionSpec =
    (for {
      md <- Option(metadata)
      executionInfo <- Option(md.executionInfo)
      outputTableInfo <- Option(executionInfo.outputTableInfo)
    } yield outputTableInfo.partitionSpec(defaultSpec)).getOrElse(defaultSpec)

  /** The snapshot grid a snapshot-accuracy join part lives on: the RHS groupBy's declared
    * output grid (partition_interval/partition_offset), falling back to the coarsest grid the
    * groupBy's sources declare (planner nodes strip executionInfo from embedded groupBys, and
    * an entity source's table grid IS its snapshot cadence), then to the join's grid when
    * nothing is declared anywhere. Declaring the join's own grid is a no-op, so daily-RHS-
    * under-daily-join reproduces the historical behavior exactly. Part tables are always
    * partitioned by the join's partition column; only the RHS interval/offset/format carry over.
    * Deliberate relaxation: sources are validated independently against the downstream node;
    * sources need not be multiples of each other.
    */
  def snapshotSpec(joinPart: JoinPart, joinSpec: PartitionSpec): PartitionSpec = {
    val declaredOutput = for {
      md <- Option(joinPart.groupBy.metaData)
      ei <- Option(md.executionInfo)
      oti <- Option(ei.outputTableInfo)
      _ <- Option(oti.partitionInterval)
    } yield oti.partitionSpec(joinSpec)

    lazy val declaredSource = Option(joinPart.groupBy.sources)
      .map(_.toScala.toSeq)
      .getOrElse(Seq.empty)
      .flatMap(sourceSpec(_, joinSpec))
      .sortBy(-_.spanMillis)
      .headOption

    val declared = declaredOutput.orElse(declaredSource).getOrElse(joinSpec)
    if (declared.hasSameGrid(joinSpec)) joinSpec else declared.copy(column = joinSpec.column)
  }

  /** Resolves an explicitly declared source spec, if the source query declares one. */
  def sourceSpec(source: Source, defaultSpec: PartitionSpec): Option[PartitionSpec] =
    Option(source.query).flatMap(querySpec(_, defaultSpec))

  /** Resolves an explicitly declared query spec, if partition_interval is present. */
  def querySpec(query: Query, defaultSpec: PartitionSpec): Option[PartitionSpec] =
    Option(query.partitionInterval).map(_ => query.partitionSpec(defaultSpec))

  /** Returns a copy of a dependency whose tableInfo carries a fully resolved partition spec. */
  def resolveDependency(tableDependency: TableDependency, defaultSpec: PartitionSpec): TableDependency = {
    val result = tableDependency.deepCopy()
    Option(result.tableInfo).foreach { tableInfo =>
      result.setTableInfo(tableInfo.withSpec(tableInfo.partitionSpec(defaultFor(tableInfo, defaultSpec))))
    }
    result
  }

  /** Resolves partition specs for every dependency using the supplied node/default spec. */
  def resolveDependencies(tableDependencies: Seq[TableDependency], defaultSpec: PartitionSpec): Seq[TableDependency] =
    tableDependencies.map(resolveDependency(_, defaultSpec))

  /** Resolves an upstream node's output spec using the global planner default, not a downstream spec. */
  def upstreamOutputSpec(metadata: MetaData, globalDefaultSpec: PartitionSpec): PartitionSpec =
    outputSpec(metadata, globalDefaultSpec)

  /** Computes the source-dependency lookback needed by events-left snapshot join parts. */
  def snapshotSourceShift(joinPart: JoinPart,
                          leftDataModel: Option[DataModel],
                          joinSpec: PartitionSpec): Option[Window] = {
    val spec = snapshotSpec(joinPart, joinSpec)
    if (
      leftDataModel.contains(DataModel.EVENTS) &&
      joinPart.groupBy.inferredAccuracy == Accuracy.SNAPSHOT &&
      !spec.hasSameGrid(joinSpec)
    )
      Some(WindowUtils.fromMillis(spec.spanMillis))
    else None
  }

  /** Applies the resolved snapshot grid to the embedded groupBy metadata on an executable join part. */
  def applyJoinPartSnapshotSpec(joinPart: JoinPart, joinSpec: PartitionSpec): JoinPart = {
    if (joinPart.groupBy.inferredAccuracy != Accuracy.SNAPSHOT) return joinPart

    val spec = snapshotSpec(joinPart, joinSpec)
    val groupBy = joinPart.groupBy
    val metaData = Option(groupBy.metaData).getOrElse(new MetaData())
    groupBy.setMetaData(metaData)

    val executionInfo = Option(metaData.executionInfo).getOrElse(new ExecutionInfo())
    metaData.setExecutionInfo(executionInfo)

    val tableInfo = Option(executionInfo.outputTableInfo).getOrElse(new TableInfo().setTable(metaData.outputTable))
    executionInfo.setOutputTableInfo(tableInfo.withSpec(spec))
    joinPart
  }

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

  /** A downstream table can only read whole partitions of its upstream, so every downstream
    * boundary must also be an upstream boundary: the downstream partitionInterval is a multiple
    * of the upstream's, and the boundaries line up. Example: 6h@4h over 3h@1h is fine - 04:00,
    * 10:00, 16:00, 22:00 all sit on the 3h@1h grid. 6h@2h over 3h@1h is rejected - 02:00 doesn't.
    *
    * Snapshot join parts never reach this check: they pick the latest snapshot at or before each
    * row's ts on the RHS's own grid, so any RHS grid is fine there (see
    * JoinPlanner.validateJoinPartGrids). ENTITIES upstreams that do reach it (a groupBy reading
    * an entity snapshot source passes the upstream's partitions through unchanged) get the same
    * boundary-subset requirement, with a note that the per-row relaxation does not apply yet.
    */
  def validateUpstreamGrid(nodeName: String,
                           downstreamSpec: PartitionSpec,
                           upstreamSpec: PartitionSpec,
                           upstreamDescription: String,
                           dataModel: DataModel,
                           requireBoundaryAlignment: Boolean = true): Unit = {
    val downstreamGrid = downstreamSpec.grid
    val upstreamGrid = upstreamSpec.grid

    val snapshotNote = dataModel match {
      case DataModel.ENTITIES =>
        " Reading a finer snapshot grid by picking the latest snapshot at or before each boundary" +
          " is not supported here yet."
      case DataModel.EVENTS => ""
    }

    require(
      downstreamGrid.isExactMultipleOf(upstreamGrid),
      s"Invalid partition interval for $nodeName: its partitionInterval " +
        s"(${WindowUtils.millisToString(downstreamGrid.spanMillis)}) must be equal to or an exact multiple of " +
        s"$upstreamDescription's (${WindowUtils.millisToString(upstreamGrid.spanMillis)}).$snapshotNote"
    )
    if (requireBoundaryAlignment) {
      require(
        downstreamGrid.linesUpWith(upstreamGrid),
        s"Incompatible partition grids for $nodeName: its partitions (${downstreamGrid.show}) " +
          s"don't line up on $upstreamDescription's boundaries (${upstreamGrid.show})."
      )
    }
  }

  /** Validates an authored query against the downstream node's output grid. */
  def validateQueryGrid(nodeName: String,
                        downstreamSpec: PartitionSpec,
                        query: Query,
                        sourceDescription: String,
                        dataModel: DataModel): Unit = {
    if (Option(query.partitionInterval).isDefined) {
      validateUpstreamGrid(nodeName, downstreamSpec, query.partitionSpec(downstreamSpec), sourceDescription, dataModel)
    } else if (
      downstreamSpec.spanMillis < WindowUtils.Day.millis && !(query.isSetTimePartitioned && query.timePartitioned)
    ) {
      // an undeclared upstream must not silently inherit a sub-daily downstream grid
      throw undeclaredPartitionInterval(nodeName, downstreamSpec, sourceDescription)
    }
  }

  /** Temporal join parts filter by left-row time and DependencyResolver expands source reads
    * by interval intersection. That makes a coarser join grid over a finer, non-aligned event
    * source safe: it may read boundary-crossing source partitions, but it does not skip time.
    */
  def validateTemporalJoinEventSourceGrid(nodeName: String,
                                          downstreamSpec: PartitionSpec,
                                          query: Query,
                                          sourceDescription: String): Unit = {
    if (Option(query.partitionInterval).isDefined) {
      validateUpstreamGrid(nodeName,
                           downstreamSpec,
                           query.partitionSpec(downstreamSpec),
                           sourceDescription,
                           DataModel.EVENTS,
                           requireBoundaryAlignment = false)
    } else if (
      downstreamSpec.spanMillis < WindowUtils.Day.millis && !(query.isSetTimePartitioned && query.timePartitioned)
    ) {
      throw undeclaredPartitionInterval(nodeName, downstreamSpec, sourceDescription)
    }
  }

  /** Validates a table dependency when it declares a physical partition grid. */
  def validateTableInfoGrid(nodeName: String,
                            downstreamSpec: PartitionSpec,
                            tableInfo: TableInfo,
                            sourceDescription: String,
                            dataModel: DataModel): Unit = {
    val hasPartialPartitionFields = Option(tableInfo).exists { ti =>
      ti.isSetPartitionColumn || ti.isSetPartitionFormat || ti.isSetPartitionOffset
    }
    if (Option(tableInfo).exists(_.isSetPartitionInterval)) {
      validateUpstreamGrid(nodeName,
                           downstreamSpec,
                           tableInfo.partitionSpec(downstreamSpec),
                           sourceDescription,
                           dataModel)
    } else if (
      downstreamSpec.spanMillis < WindowUtils.Day.millis &&
      hasPartialPartitionFields &&
      !Option(tableInfo).exists(ti => ti.isSetTimePartitioned && ti.timePartitioned)
    ) {
      throw undeclaredPartitionInterval(nodeName, downstreamSpec, sourceDescription)
    }
  }

  /** Validates and resolves table dependencies for planner nodes that read whole upstream partitions. */
  def validateAndResolveDependencies(nodeName: String,
                                     downstreamSpec: PartitionSpec,
                                     tableDependencies: Seq[TableDependency],
                                     sourceDescription: TableDependency => String,
                                     dataModel: DataModel): Seq[TableDependency] =
    tableDependencies.map { tableDependency =>
      require(tableDependency.tableInfo != null, s"$nodeName has a table dependency without tableInfo")
      validateTableInfoGrid(nodeName,
                            downstreamSpec,
                            tableDependency.tableInfo,
                            sourceDescription(tableDependency),
                            dataModel)
      resolveDependency(tableDependency, downstreamSpec)
    }

  /** Builds the shared error for a sub-daily node over an implicitly daily dependency. */
  private def undeclaredPartitionInterval(nodeName: String,
                                          downstreamSpec: PartitionSpec,
                                          sourceDescription: String): IllegalArgumentException =
    new IllegalArgumentException(
      s"$nodeName has a sub-daily output grid (${downstreamSpec.grid.show}) over $sourceDescription " +
        "with no declared partition_interval - implicitly daily. Every intraday run would wait for the " +
        "full day's partition and land a day late. Declare the source's partition_interval, or mark it " +
        "time_partitioned if data lands continuously and readiness can be sensed from timestamps."
    )

  /** Chooses the default spec to use when resolving missing fields on table metadata. */
  private def defaultFor(tableInfo: TableInfo, defaultSpec: PartitionSpec): PartitionSpec = {
    val declaresIntervalOrTimestamp = Option(tableInfo).exists { ti =>
      ti.isSetPartitionInterval || (ti.isSetTimePartitioned && ti.timePartitioned)
    }
    if (declaresIntervalOrTimestamp || defaultSpec.isDaily) defaultSpec
    else PartitionSpec(defaultSpec.column, PartitionSpec.daily.format, WindowUtils.Day.millis)
  }
}
