package ai.chronon.api.test.planner

import ai.chronon.api.Builders.{Join, MetaData}
import ai.chronon.api.Extensions.WindowUtils
import ai.chronon.api.Extensions._
import ai.chronon.api.planner.JoinPlanner
import ai.chronon.api.{Accuracy, Builders, ConfigProperties, ExecutionInfo, Operation, PartitionSpec, TableInfo}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class JoinPlannerTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec: PartitionSpec = PartitionSpec.daily
  private val threeHourSpec = PartitionSpec("ds", "yyyy-MM-dd HH:mm", 3 * 60 * 60 * 1000)

  private def outputTableInfo(table: String, spec: PartitionSpec): TableInfo =
    new TableInfo()
      .setTable(table)
      .setPartitionColumn(spec.column)
      .setPartitionFormat(spec.format)
      .setPartitionInterval(WindowUtils.fromMillis(spec.spanMillis))

  private def executionInfoFor(table: String, spec: PartitionSpec): ExecutionInfo =
    new ExecutionInfo().setOutputTableInfo(outputTableInfo(table, spec))

  private def groupByWithOutputSpec(name: String,
                                    spec: PartitionSpec,
                                    accuracy: Accuracy = Accuracy.TEMPORAL): ai.chronon.api.GroupBy =
    Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = s"test.$name")),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(Builders.Aggregation(Operation.COUNT, "event_count", Seq(WindowUtils.Unbounded))),
      accuracy = accuracy,
      metaData = Builders.MetaData(
        namespace = "test_namespace",
        name = name,
        executionInfo = executionInfoFor(s"test_namespace.$name", spec)
      )
    )

  private def modularExecutionInfo: ExecutionInfo =
    new ExecutionInfo().setConf(
      new ConfigProperties().setCommon(Map("modular_execution" -> "true").asJava)
    )

  private def temporalEntityGroupBy(name: String): ai.chronon.api.GroupBy = {
    val entityQuery = Builders.Query(
      startPartition = "2025-01-01",
      partitionColumn = "ds"
    )
    entityQuery.setPartitionInterval(WindowUtils.Day)

    Builders.GroupBy(
      sources = Seq(Builders.Source.entities(
        query = entityQuery,
        snapshotTable = "test.dim_snapshot",
        mutationTable = "test.dim_mutations"
      )),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(Builders.Aggregation(Operation.LAST, "headline", Seq(WindowUtils.Unbounded))),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(namespace = "test_namespace", name = name)
    )
  }

  it should "include mutation table dependencies and sensors on the UnionJoin path" in {
    val join = Join(
      metaData = MetaData(
        name = "modular_union_join",
        namespace = "test_namespace",
        executionInfo = modularExecutionInfo
      ),
      left = Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.left_events"),
      joinParts = Seq(Builders.JoinPart(groupBy = temporalEntityGroupBy("union_temporal_entity_gb")))
    )

    val plan = new JoinPlanner(join).buildPlan

    val unionNode = plan.nodes.asScala.find(_.content.isSetUnionJoin).get
    val tableDeps = unionNode.metaData.executionInfo.tableDependencies.asScala

    tableDeps.map(_.tableInfo.table) should equal(
      Seq("test.left_events", "test.dim_snapshot", "test.dim_mutations")
    )
    tableDeps(1).startOffset should equal(WindowUtils.Day)
    tableDeps(1).endOffset should equal(WindowUtils.Day)
    tableDeps(2).startOffset should equal(WindowUtils.zero())
    tableDeps(2).endOffset should equal(WindowUtils.zero())

    val sensorOutputTables = plan.nodes.asScala
      .filter(_.content.isSetExternalSourceSensor)
      .map(_.content.getExternalSourceSensor.metaData.executionInfo.outputTableInfo.table)
      .toSet

    sensorOutputTables should equal(
      Set("test.left_events", "test.dim_snapshot", "test.dim_mutations")
    )
  }

  it should "allow daily joins over sub-daily groupBy outputs and preserve dependency partition specs" in {
    val hourlyGroupBy = groupByWithOutputSpec("three_hour_gb", threeHourSpec)
    val join = Join(
      metaData = MetaData(name = "daily_join", namespace = "test_namespace"),
      left = Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.left_events"),
      joinParts = Seq(Builders.JoinPart(groupBy = hourlyGroupBy))
    )

    val plan = new JoinPlanner(join).buildPlan

    val metadataUploadNode = plan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get
    val groupByDep = metadataUploadNode.metaData.executionInfo.tableDependencies.asScala
      .find(_.tableInfo.table == hourlyGroupBy.metaData.outputTable + "__uploadToKV")
      .get
    groupByDep.tableInfo.partitionFormat should equal(threeHourSpec.format)
    groupByDep.tableInfo.partitionInterval should equal(WindowUtils.fromMillis(threeHourSpec.spanMillis))

    val backfillNode = plan.nodes.asScala.find(_.content.isSetUnionJoin).get
    backfillNode.metaData.executionInfo.outputTableInfo.partitionFormat should equal(PartitionSpec.daily.format)
  }

  it should "allow sub-daily joins over coarser groupBy outputs - parts bind by left row time" in {
    // snapshot parts are bound as-of the left row time (floor to grid + one-span shift) and
    // temporal parts recompute from raw events, so a finer join over a coarser groupBy output
    // grid is staleness, not missing data
    val dailySnapshotGroupBy = groupByWithOutputSpec("daily_snapshot_gb", PartitionSpec.daily, Accuracy.SNAPSHOT)
    val dailyTemporalGroupBy = groupByWithOutputSpec("daily_temporal_gb", PartitionSpec.daily)
    val subDailyJoin = Join(
      metaData = MetaData(
        name = "three_hour_join",
        namespace = "test_namespace",
        executionInfo = executionInfoFor("test_namespace.three_hour_join", threeHourSpec)
      ),
      left = Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.left_events"),
      joinParts =
        Seq(Builders.JoinPart(groupBy = dailySnapshotGroupBy), Builders.JoinPart(groupBy = dailyTemporalGroupBy))
    )

    noException should be thrownBy new JoinPlanner(subDailyJoin).buildPlan
  }

  it should "shift snapshot part merge dependencies by one join span instead of one day" in {
    def mergeSnapshotDepOffset(joinSpec: PartitionSpec): ai.chronon.api.Window = {
      val snapshotGroupBy =
        groupByWithOutputSpec(s"snapshot_gb_${joinSpec.spanMillis}", joinSpec, Accuracy.SNAPSHOT)
      val join = Join(
        metaData = MetaData(
          name = s"merge_shift_join_${joinSpec.spanMillis}",
          namespace = "test_namespace",
          executionInfo = executionInfoFor(s"test_namespace.merge_shift_join_${joinSpec.spanMillis}", joinSpec)
        ),
        left = Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.left_events"),
        joinParts = Seq(Builders.JoinPart(groupBy = snapshotGroupBy))
      )
      val plan = new JoinPlanner(join).buildPlan
      val mergeNode = plan.nodes.asScala.find(_.content.isSetJoinMerge).get
      val partDep = mergeNode.metaData.executionInfo.tableDependencies.asScala
        .find(_.tableInfo.table.contains("snapshot_gb"))
        .get
      partDep.startOffset
    }

    // the engine (MergeJob) shifts snapshot part reads back one join output interval, so the
    // sensor-facing dependency must require the same span - daily behavior stays one day
    mergeSnapshotDepOffset(PartitionSpec.daily).millis should equal(WindowUtils.Day.millis)
    mergeSnapshotDepOffset(threeHourSpec).millis should equal(threeHourSpec.spanMillis)
  }

  it should "partition modular join part intermediates in the join output domain" in {
    val hourlyGroupBy = groupByWithOutputSpec("modular_three_hour_gb", threeHourSpec)
    val dailyGroupBy = groupByWithOutputSpec("modular_daily_gb", PartitionSpec.daily)
    val join = Join(
      metaData = MetaData(
        name = "daily_modular_join",
        namespace = "test_namespace",
        executionInfo = modularExecutionInfo
      ),
      left = Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.left_events"),
      joinParts = Seq(Builders.JoinPart(groupBy = hourlyGroupBy), Builders.JoinPart(groupBy = dailyGroupBy))
    )

    val plan = new JoinPlanner(join).buildPlan

    val hourlyJoinPartNode = plan.nodes.asScala
      .find(node =>
        node.content.isSetJoinPart &&
          node.content.getJoinPart.joinPart.groupBy.metaData.name == hourlyGroupBy.metaData.name)
      .get
    val joinPartOutputTableInfo = hourlyJoinPartNode.metaData.executionInfo.outputTableInfo

    joinPartOutputTableInfo.partitionFormat should equal(PartitionSpec.daily.format)
    joinPartOutputTableInfo.partitionInterval should equal(WindowUtils.Day)
  }

  it should "include mutation table dependencies on the join part for the standard modular path" in {
    val standardGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.other_events")),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(Builders.Aggregation(Operation.COUNT, "event_count", Seq(WindowUtils.Unbounded))),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(namespace = "test_namespace", name = "standard_events_gb")
    )

    val join = Join(
      metaData = MetaData(
        name = "modular_standard_join",
        namespace = "test_namespace",
        executionInfo = modularExecutionInfo
      ),
      left = Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.left_events"),
      joinParts = Seq(
        Builders.JoinPart(groupBy = temporalEntityGroupBy("standard_temporal_entity_gb")),
        Builders.JoinPart(groupBy = standardGroupBy)
      ),
      bootstrapParts = Seq.empty
    )

    val plan = new JoinPlanner(join).buildPlan

    val temporalJoinPartNode = plan.nodes.asScala
      .find(node =>
        node.content.isSetJoinPart &&
          node.content.getJoinPart.joinPart.groupBy.metaData.name == "standard_temporal_entity_gb")
      .get

    val tableDeps = temporalJoinPartNode.metaData.executionInfo.tableDependencies.asScala
    val depTables = tableDeps.map(_.tableInfo.table)

    depTables should contain("test.dim_snapshot")
    depTables should contain("test.dim_mutations")
    depTables should contain(temporalJoinPartNode.content.getJoinPart.leftSourceTable)

    val snapshotDep = tableDeps.find(_.tableInfo.table == "test.dim_snapshot").get
    snapshotDep.startOffset should equal(WindowUtils.Day)
    snapshotDep.endOffset should equal(WindowUtils.Day)

    val mutationDep = tableDeps.find(_.tableInfo.table == "test.dim_mutations").get
    mutationDep.startOffset should equal(WindowUtils.zero())
    mutationDep.endOffset should equal(WindowUtils.zero())
  }

  it should "depend on upstream join output when the left source is a join source" in {
    val upstreamListingLookup = Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.user_listings")),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Builders.Aggregation(Operation.LAST, "listing_id", Seq(WindowUtils.Unbounded))),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(namespace = "test_namespace", name = "upstream_listing_lookup")
    )

    val upstreamJoin = Join(
      metaData = MetaData(
        name = "upstream_join",
        namespace = "test_namespace",
        executionInfo = modularExecutionInfo
      ),
      left = Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.left_events"),
      joinParts = Seq(Builders.JoinPart(groupBy = upstreamListingLookup))
    )

    val downstreamListingFeatures = Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.listing_features")),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(Builders.Aggregation(Operation.LAST, "price", Seq(WindowUtils.Unbounded))),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(namespace = "test_namespace", name = "downstream_listing_features")
    )

    val listingIdColumn = upstreamListingLookup.valueColumns.head
    val leftJoinSourceQuery = Builders.Query(
      selects = Builders.Selects.exprs(
        "user_id" -> "user_id",
        "listing_id" -> listingIdColumn,
        "ts" -> "ts"
      ),
      partitionColumn = "ds"
    )

    val downstreamJoin = Join(
      metaData = MetaData(
        name = "downstream_join",
        namespace = "test_namespace",
        executionInfo = modularExecutionInfo
      ),
      left = Builders.Source.joinSource(upstreamJoin, leftJoinSourceQuery),
      joinParts = Seq(Builders.JoinPart(groupBy = downstreamListingFeatures))
    )

    val plan = new JoinPlanner(downstreamJoin).buildPlan

    val sourceNode = plan.nodes.asScala.find(_.content.isSetSourceWithFilter).get
    val sourceDeps = sourceNode.metaData.executionInfo.tableDependencies.asScala.map(_.tableInfo.table)
    sourceDeps should contain(upstreamJoin.metaData.outputTable)

    val metadataUploadNode = plan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get
    val metadataDeps = metadataUploadNode.metaData.executionInfo.tableDependencies.asScala.map(_.tableInfo.table)
    metadataDeps should contain(downstreamListingFeatures.metaData.outputTable + "__uploadToKV")
    metadataDeps should not contain (upstreamJoin.metaData.outputTable + "__metadata_upload")
  }
}
