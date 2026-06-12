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

/** Planner-side partition resolution. This object owns fallback policy and writes the resolved
  * specs into existing thrift fields so runtime code can read them without re-deriving policy.
  */
object PartitionSpecResolver {

  /** Resolves a node output spec from metadata, falling back to the supplied planner default. */
  def outputSpec(metadata: MetaData, defaultSpec: PartitionSpec): PartitionSpec =
    (for {
      md <- Option(metadata)
      executionInfo <- Option(md.executionInfo)
      outputTableInfo <- Option(executionInfo.outputTableInfo)
    } yield outputTableInfo.partitionSpec(defaultSpec)).getOrElse(defaultSpec)

  /** Resolves the logical RHS snapshot grid for an events-left snapshot join part. */
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

  /** Returns a copy of a TableInfo with all partition fields set to the supplied spec. */
  def tableInfoWithSpec(tableInfo: TableInfo, partitionSpec: PartitionSpec): TableInfo = {
    val result = Option(tableInfo).map(_.deepCopy()).getOrElse(new TableInfo())
    result
      .setPartitionColumn(partitionSpec.column)
      .setPartitionFormat(partitionSpec.format)
      .setPartitionInterval(WindowUtils.fromMillis(partitionSpec.spanMillis))
    if (partitionSpec.offsetMillis != 0)
      result.setPartitionOffset(WindowUtils.fromMillis(partitionSpec.offsetMillis))
    else
      result.unsetPartitionOffset()
    result
  }

  /** Builds a TableInfo for a table with all partition fields set to the supplied spec. */
  def tableInfo(table: String, partitionSpec: PartitionSpec): TableInfo =
    tableInfoWithSpec(new TableInfo().setTable(table), partitionSpec)

  /** Returns a copy of a TableInfo with missing partition fields resolved from a default spec. */
  def resolveTableInfo(tableInfo: TableInfo, defaultSpec: PartitionSpec): TableInfo =
    tableInfoWithSpec(tableInfo, tableInfo.partitionSpec(defaultFor(tableInfo, defaultSpec)))

  /** Returns a copy of a dependency whose tableInfo carries a fully resolved partition spec. */
  def resolveDependency(tableDependency: TableDependency, defaultSpec: PartitionSpec): TableDependency = {
    val result = tableDependency.deepCopy()
    if (result.tableInfo != null) {
      result.setTableInfo(resolveTableInfo(result.tableInfo, defaultSpec))
    }
    result
  }

  /** Resolves partition specs for every dependency using the supplied node/default spec. */
  def resolveDependencies(tableDependencies: Seq[TableDependency], defaultSpec: PartitionSpec): Seq[TableDependency] =
    tableDependencies.map(resolveDependency(_, defaultSpec))

  /** Resolves a producer node's output spec using the global planner default, not a consumer spec. */
  def producerOutputSpec(metadata: MetaData, globalDefaultSpec: PartitionSpec): PartitionSpec =
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
    executionInfo.setOutputTableInfo(tableInfoWithSpec(tableInfo, spec))
    joinPart
  }

  /** Validates that an authored query can cover a consumer output grid. */
  def validateCoverageQuery(nodeName: String,
                            consumerSpec: PartitionSpec,
                            query: Query,
                            sourceDescription: String,
                            shape: MetaDataUtils.EdgeShape): Unit = {
    if (Option(query.partitionInterval).isDefined) {
      MetaDataUtils.validateEdgeGrids(nodeName,
                                      consumerSpec,
                                      query.partitionSpec(consumerSpec),
                                      sourceDescription,
                                      shape)
    } else {
      validateDeclaredPartitionInterval(nodeName, consumerSpec, query, sourceDescription)
    }
  }

  /** Validates that an edge does not silently inherit a sub-daily consumer grid. */
  private def validateDeclaredPartitionInterval(nodeName: String,
                                                consumerSpec: PartitionSpec,
                                                query: Query,
                                                sourceDescription: String): Unit = {
    if (consumerSpec.spanMillis < WindowUtils.Day.millis && !(query.isSetTimePartitioned && query.timePartitioned)) {
      throw undeclaredPartitionInterval(nodeName, consumerSpec, sourceDescription)
    }
  }

  /** Validates a table dependency when it declares a physical partition grid. */
  def validateCoverageTableInfo(nodeName: String,
                                consumerSpec: PartitionSpec,
                                tableInfo: TableInfo,
                                sourceDescription: String,
                                shape: MetaDataUtils.EdgeShape): Unit = {
    val hasPartialPartitionFields = Option(tableInfo).exists { ti =>
      ti.isSetPartitionColumn || ti.isSetPartitionFormat || ti.isSetPartitionOffset
    }
    if (Option(tableInfo).exists(_.isSetPartitionInterval)) {
      MetaDataUtils.validateEdgeGrids(nodeName,
                                      consumerSpec,
                                      tableInfo.partitionSpec(consumerSpec),
                                      sourceDescription,
                                      shape)
    } else if (
      consumerSpec.spanMillis < WindowUtils.Day.millis &&
      hasPartialPartitionFields &&
      !Option(tableInfo).exists(ti => ti.isSetTimePartitioned && ti.timePartitioned)
    ) {
      throw undeclaredPartitionInterval(nodeName, consumerSpec, sourceDescription)
    }
  }

  /** Validates and resolves table dependencies for planner nodes with coverage requirements. */
  def resolveCoverageDependencies(nodeName: String,
                                  consumerSpec: PartitionSpec,
                                  tableDependencies: Seq[TableDependency],
                                  sourceDescription: TableDependency => String,
                                  shape: MetaDataUtils.EdgeShape): Seq[TableDependency] =
    tableDependencies.map { tableDependency =>
      require(tableDependency.tableInfo != null, s"$nodeName has a table dependency without tableInfo")
      validateCoverageTableInfo(nodeName,
                                consumerSpec,
                                tableDependency.tableInfo,
                                sourceDescription(tableDependency),
                                shape)
      resolveDependency(tableDependency, consumerSpec)
    }

  /** Builds the shared error for sub-daily coverage over an implicitly daily dependency. */
  private def undeclaredPartitionInterval(nodeName: String,
                                          consumerSpec: PartitionSpec,
                                          sourceDescription: String): IllegalArgumentException =
    new IllegalArgumentException(
      s"$nodeName has a sub-daily output grid (${consumerSpec.grid.show}) over $sourceDescription " +
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
