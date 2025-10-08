package ai.chronon.api.test.planner

import ai.chronon.api
import ai.chronon.api.Builders.{Join, MetaData}
import ai.chronon.api.planner.{LocalRunner, MonolithJoinPlanner}
import ai.chronon.api.{Builders, ConfigProperties, Constants, ExecutionInfo, PartitionSpec}
import ai.chronon.planner.{ConfPlan, Mode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ai.chronon.api.Extensions._

import java.nio.file.Paths
import scala.jdk.CollectionConverters._

class MonolithJoinPlannerTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec: PartitionSpec = PartitionSpec.daily

  private def validateJoinPlan(plan: ConfPlan): Unit = {
    // Should create plan successfully with both backfill and metadata upload nodes

    // Find the backfill node and metadata upload node
    val backfillNode = plan.nodes.asScala.find(_.content.isSetMonolithJoin)
    val metadataUploadNode = plan.nodes.asScala.find(_.content.isSetJoinMetadataUpload)

    backfillNode should be(defined)
    metadataUploadNode should be(defined)

    // Backfill node should have content
    backfillNode.get.content should not be null
    backfillNode.get.content.getMonolithJoin should not be null
    backfillNode.get.content.getMonolithJoin.join should not be null

    // Metadata upload node should have content
    metadataUploadNode.get.content should not be null
    metadataUploadNode.get.content.getJoinMetadataUpload should not be null
    metadataUploadNode.get.content.getJoinMetadataUpload.join should not be null

    plan.terminalNodeNames.asScala.size shouldBe 2
    plan.terminalNodeNames.containsKey(Mode.DEPLOY) shouldBe true
    plan.terminalNodeNames.containsKey(Mode.BACKFILL) shouldBe true

    // Validate that no node names contain forward slashes
    plan.nodes.asScala.foreach { node =>
      val nodeName = node.metaData.name
      withClue(s"Node name '$nodeName' contains forward slash") {
        nodeName should not contain "/"
      }
    }
  }

  it should "monolith join planner plans valid confs without exceptions" in {

    val rootDir = Paths.get(getClass.getClassLoader.getResource("canary/compiled/joins").getPath)

    val joinConfs = LocalRunner.parseConfs[api.Join](rootDir.toString)

    val joinPlanners = joinConfs.map(MonolithJoinPlanner(_))

    joinPlanners
      .foreach { planner =>
        noException should be thrownBy {
          val plan = planner.buildPlan
          validateJoinPlan(plan)
        }
      }
  }

  it should "monolith join should avoid metadata when computing semantic hash" in {
    val firstJoin = Join(
      metaData = MetaData(name = "firstJoin", executionInfo = new ExecutionInfo().setStepDays(2)),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.join_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val secondJoin = Join(
      metaData = MetaData(name = "secondJoin", executionInfo = new ExecutionInfo().setStepDays(1)),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.join_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )
    val firstPlan = MonolithJoinPlanner(firstJoin).buildPlan
    val secondPlan = MonolithJoinPlanner(secondJoin).buildPlan

    // Semantic hashes should be identical since metadata is excluded
    val firstSemanticHashes = firstPlan.nodes.asScala.map(_.semanticHash)
    val secondSemanticHashes = secondPlan.nodes.asScala.map(_.semanticHash)
    firstSemanticHashes should equal(secondSemanticHashes)
  }

  it should "monolith join planner should create valid terminal node names" in {
    val join = Join(
      metaData = MetaData(name = "testJoin"),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.test_join_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(join)
    val plan = planner.buildPlan

    plan.terminalNodeNames.asScala should contain key ai.chronon.planner.Mode.BACKFILL
    plan.terminalNodeNames.asScala should contain key ai.chronon.planner.Mode.DEPLOY
    plan.terminalNodeNames.asScala(ai.chronon.planner.Mode.BACKFILL) should equal("testJoin__backfill")
    plan.terminalNodeNames.asScala(ai.chronon.planner.Mode.DEPLOY) should equal("testJoin__metadata_upload")
  }

  it should "monolith join planner should respect step days from execution info" in {
    val joinWithStepDays = Join(
      metaData = MetaData(name = "testJoin", executionInfo = new ExecutionInfo().setStepDays(5)),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.test_join_with_step_days_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val joinWithoutStepDays = Join(
      metaData = MetaData(name = "testJoin2"),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.test_join_without_step_days_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val plannerWithStepDays = MonolithJoinPlanner(joinWithStepDays)
    val plannerWithoutStepDays = MonolithJoinPlanner(joinWithoutStepDays)

    noException should be thrownBy {
      plannerWithStepDays.buildPlan
      plannerWithoutStepDays.buildPlan
    }
  }

  it should "set nonzero step days" in {
    val joinWithNonZeroStepDays = Join(
      metaData = MetaData(name = "testJoin", executionInfo = new ExecutionInfo()),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.test_join_non_zero_step_days_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val plannerWithNonZeroStepDays = MonolithJoinPlanner(joinWithNonZeroStepDays)
    val plan = plannerWithNonZeroStepDays.buildPlan
    plan.nodes.asScala.foreach((node) => node.metaData.executionInfo.stepDays should equal(1))
  }

  it should "monolith join planner should produce same semantic hash with different executionInfo" in {
    val joinWithExecutionInfo1 = Join(
      metaData = MetaData(
        name = "testJoin1",
        executionInfo = new ExecutionInfo().setStepDays(3)
      ),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.test_join_execution_info_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val joinWithExecutionInfo2 = Join(
      metaData = MetaData(
        name = "testJoin2",
        executionInfo = new ExecutionInfo().setStepDays(7)
      ),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.test_join_execution_info_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val firstPlan = MonolithJoinPlanner(joinWithExecutionInfo1).buildPlan
    val secondPlan = MonolithJoinPlanner(joinWithExecutionInfo2).buildPlan

    // Semantic hashes should be identical since executionInfo is part of metadata and excluded
    val firstSemanticHashes = firstPlan.nodes.asScala.map(_.semanticHash)
    val secondSemanticHashes = secondPlan.nodes.asScala.map(_.semanticHash)
    firstSemanticHashes should equal(secondSemanticHashes)
  }

  it should "monolith join planner should produce exactly two nodes (backfill and metadata upload) for canary confs" in {
    val rootDir = Paths.get(getClass.getClassLoader.getResource("canary/compiled/joins").getPath)

    val joinConfs = LocalRunner.parseConfs[api.Join](rootDir.toString)

    joinConfs.foreach { joinConf =>
      val planner = MonolithJoinPlanner(joinConf)
      val plan = planner.buildPlan

      validateJoinPlan(plan)
    }
  }

  it should "monolith join planner should create metadata upload node with correct properties" in {
    val join = Join(
      metaData = MetaData(name = "testJoin"),
      left = Builders.Source.events(Builders.Query(), table = s"test_namespace.test_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(join)
    val plan = planner.buildPlan

    validateJoinPlan(plan)

    val metadataUploadNode = plan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get

    // Verify metadata upload node name
    metadataUploadNode.metaData.name should equal("testJoin__metadata_upload")

    // Verify the wrapped join is correct
    metadataUploadNode.content.getJoinMetadataUpload.join should equal(join)
  }

  it should "monolith join planner should skip metadata in semantic hash for both nodes" in {
    val firstJoin = Join(
      metaData = MetaData(name = "firstJoin", executionInfo = new ExecutionInfo().setStepDays(2)),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.join_semantic_hash_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val secondJoin = Join(
      metaData = MetaData(name = "secondJoin", executionInfo = new ExecutionInfo().setStepDays(1)),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.join_semantic_hash_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val firstPlan = MonolithJoinPlanner(firstJoin).buildPlan
    val secondPlan = MonolithJoinPlanner(secondJoin).buildPlan

    // Semantic hashes should be identical for both backfill and metadata upload nodes
    val firstBackfillHash = firstPlan.nodes.asScala.find(_.content.isSetMonolithJoin).get.semanticHash
    val secondBackfillHash = secondPlan.nodes.asScala.find(_.content.isSetMonolithJoin).get.semanticHash
    firstBackfillHash should equal(secondBackfillHash)

    val firstMetadataUploadHash = firstPlan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get.semanticHash
    val secondMetadataUploadHash = secondPlan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get.semanticHash
    firstMetadataUploadHash should equal(secondMetadataUploadHash)
  }

  it should "metadata upload node should depend on streaming GroupBy nodes when join parts have streaming sources" in {
    import ai.chronon.api.Builders._

    // Create a streaming GroupBy (has topic)
    val streamingGroupBy = GroupBy(
      sources = Seq(Source.events(Query(), table = "test_namespace.events_table", topic = "events_topic")),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Aggregation(ai.chronon.api.Operation.COUNT, "event_count")),
      metaData = MetaData(namespace = "test_namespace", name = "streaming_gb")
    )

    val joinPart = new ai.chronon.api.JoinPart()
      .setGroupBy(streamingGroupBy)

    val join = Join(
      metaData = MetaData(name = "testJoinWithStreaming"),
      left = Source.events(Query(), table = "test_namespace.left_table"),
      joinParts = Seq(joinPart),
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(join)
    val plan = planner.buildPlan

    validateJoinPlan(plan)

    val metadataUploadNode = plan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get

    // Should have dependencies on streaming GroupBy node
    val tableDeps = metadataUploadNode.metaData.executionInfo.tableDependencies.asScala
    tableDeps should not be empty
    val streamingDep = tableDeps.head
    streamingDep.tableInfo.table should equal(streamingGroupBy.metaData.outputTable + "__streaming")
  }

  it should "metadata upload node should depend on uploadToKV GroupBy nodes when join parts have non-streaming sources" in {
    import ai.chronon.api.Builders._

    // Create a non-streaming GroupBy (no topic)
    val nonStreamingGroupBy = GroupBy(
      sources = Seq(Source.events(Query(), table = "test_namespace.events_table")),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Aggregation(ai.chronon.api.Operation.COUNT, "event_count")),
      metaData = MetaData(namespace = "test_namespace", name = "non_streaming_gb")
    )

    val joinPart = new ai.chronon.api.JoinPart()
      .setGroupBy(nonStreamingGroupBy)

    val join = Join(
      metaData = MetaData(name = "testJoinWithNonStreaming"),
      left = Source.events(Query(), table = "test_namespace.left_table"),
      joinParts = Seq(joinPart),
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(join)
    val plan = planner.buildPlan

    validateJoinPlan(plan)

    val metadataUploadNode = plan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get

    // Should have dependencies on uploadToKV GroupBy node
    val tableDeps = metadataUploadNode.metaData.executionInfo.tableDependencies.asScala
    tableDeps should not be empty
    val uploadToKVDep = tableDeps.head
    uploadToKVDep.tableInfo.table should equal(nonStreamingGroupBy.metaData.outputTable + "__uploadToKV")
  }

  it should "metadata upload node should handle mixed streaming and non-streaming GroupBy dependencies" in {
    import ai.chronon.api.Builders._

    // Create a streaming GroupBy
    val streamingGroupBy = GroupBy(
      sources = Seq(Source.events(Query(), table = "test_namespace.streaming_events", topic = "streaming_topic")),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Aggregation(ai.chronon.api.Operation.COUNT, "stream_count")),
      metaData = MetaData(namespace = "test_namespace", name = "mixed_streaming_gb")
    )

    // Create a non-streaming GroupBy
    val nonStreamingGroupBy = GroupBy(
      sources = Seq(Source.events(Query(), table = "test_namespace.batch_events")),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Aggregation(ai.chronon.api.Operation.SUM, "batch_sum")),
      metaData = MetaData(namespace = "test_namespace", name = "mixed_batch_gb")
    )

    val streamingJoinPart = new ai.chronon.api.JoinPart()
      .setGroupBy(streamingGroupBy)

    val nonStreamingJoinPart = new ai.chronon.api.JoinPart()
      .setGroupBy(nonStreamingGroupBy)

    val join = Join(
      metaData = MetaData(name = "testJoinMixed"),
      left = Source.events(Query(), table = "test_namespace.left_table"),
      joinParts = Seq(streamingJoinPart, nonStreamingJoinPart),
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(join)
    val plan = planner.buildPlan

    validateJoinPlan(plan)

    val metadataUploadNode = plan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get

    // Should have dependencies on both streaming and uploadToKV nodes
    val tableDeps = metadataUploadNode.metaData.executionInfo.tableDependencies.asScala
    tableDeps should not be empty
    tableDeps should have size 2

    val depTables = tableDeps.map(_.tableInfo.table).toSet
    depTables should contain(streamingGroupBy.metaData.outputTable + "__streaming")
    depTables should contain(nonStreamingGroupBy.metaData.outputTable + "__uploadToKV")
  }

  it should "metadata upload node should have no GroupBy dependencies when join has no join parts" in {
    val join = Join(
      metaData = MetaData(name = "testJoinNoJoinParts"),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.left_only_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(join)
    val plan = planner.buildPlan

    validateJoinPlan(plan)

    val metadataUploadNode = plan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get

    // Should have no GroupBy dependencies
    val tableDeps = metadataUploadNode.metaData.executionInfo.tableDependencies.asScala
    tableDeps.size should be(0)
  }

  it should "not create logging nodes when samplePercent is not set" in {
    val join = Join(
      metaData = MetaData(name = "testJoinNoLogging"),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.test_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(join)
    val plan = planner.buildPlan

    // Should have backfill and metadata upload nodes
    plan.nodes.asScala.find(_.content.isSetMonolithJoin) should be(defined)
    plan.nodes.asScala.find(_.content.isSetJoinMetadataUpload) should be(defined)
    // Should NOT have logging or consistency nodes
    plan.nodes.asScala.find(_.content.isSetJoinLogFlatteningNode) should be(empty)
    plan.nodes.asScala.find(_.content.isSetJoinConsistencyComputeNode) should be(empty)

    // Should not have MONITOR terminal node
    plan.terminalNodeNames.asScala.size should be(2)
    plan.terminalNodeNames.containsKey(Mode.MONITOR) should be(false)
  }

  it should "not create logging nodes when logging tables are not configured" in {
    val join = Join(
      metaData = MetaData(
        name = "testJoinNoLoggingTables",
        samplePercent = 10.0
      ),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.test_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(join)
    val plan = planner.buildPlan

    // Should NOT have logging or consistency nodes (no logging tables configured)
    plan.nodes.asScala.find(_.content.isSetJoinLogFlatteningNode) should be(empty)
    plan.nodes.asScala.find(_.content.isSetJoinConsistencyComputeNode) should be(empty)

    // Should not have MONITOR terminal node
    plan.terminalNodeNames.containsKey(Mode.MONITOR) should be(false)
  }

  it should "create log flattening node when samplePercent is set and logging tables are configured" in {
    val join = Join(
      metaData = MetaData(
        name = "testJoinWithLogging",
        samplePercent = 10.0,
        executionInfo = new ExecutionInfo()
          .setConf(
            new ConfigProperties()
              .setCommon(Map(
                Constants.LoggingSchemaTableConf -> "test_namespace.schema_table",
                Constants.LoggingEventsTableConf -> "test_namespace.events_table"
              ).asJava)
          )
      ),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.test_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(join)
    val plan = planner.buildPlan

    // Should have log flattening node
    val logFlatteningNode = plan.nodes.asScala.find(_.content.isSetJoinLogFlatteningNode)
    logFlatteningNode should be(defined)
    logFlatteningNode.get.metaData.name should equal("testJoinWithLogging__log_flattener")

    // Should not have consistency compute node
    plan.nodes.asScala.find(_.content.isSetJoinConsistencyComputeNode) should be(empty)

    // MONITOR terminal node should point to log flattening node
    plan.terminalNodeNames.asScala.size should be(3)
    plan.terminalNodeNames.containsKey(Mode.MONITOR) should be(true)
    plan.terminalNodeNames.asScala(Mode.MONITOR) should equal("testJoinWithLogging__log_flattener")
  }

  it should "create both logging and consistency nodes when both are enabled" in {
    val metaData = MetaData(
      name = "testJoinWithConsistency",
      samplePercent = 10.0,
      executionInfo = new ExecutionInfo()
        .setConf(
          new ConfigProperties()
            .setCommon(Map(
              Constants.LoggingSchemaTableConf -> "test_namespace.schema_table",
              Constants.LoggingEventsTableConf -> "test_namespace.events_table"
            ).asJava)
        )
    )
    metaData.setConsistencyCheck(true)

    val join = Join(
      metaData = metaData,
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.test_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(join)
    val plan = planner.buildPlan

    // Should have both log flattening and consistency compute nodes
    val logFlatteningNode = plan.nodes.asScala.find(_.content.isSetJoinLogFlatteningNode)
    val consistencyNode = plan.nodes.asScala.find(_.content.isSetJoinConsistencyComputeNode)

    logFlatteningNode should be(defined)
    consistencyNode should be(defined)

    logFlatteningNode.get.metaData.name should equal("testJoinWithConsistency__log_flattener")
    consistencyNode.get.metaData.name should equal("testJoinWithConsistency__consistency_metrics_compute")

    // MONITOR terminal node should point to consistency compute node
    plan.terminalNodeNames.asScala.size should be(3)
    plan.terminalNodeNames.containsKey(Mode.MONITOR) should be(true)
    plan.terminalNodeNames.asScala(Mode.MONITOR) should equal("testJoinWithConsistency__consistency_metrics_compute")
  }

  it should "consistency compute node should depend on log flattening output when both enabled" in {
    val metaData = MetaData(
      name = "testJoinDeps",
      samplePercent = 10.0,
      executionInfo = new ExecutionInfo()
        .setConf(
          new ConfigProperties()
            .setCommon(Map(
              Constants.LoggingSchemaTableConf -> "test_namespace.schema_table",
              Constants.LoggingEventsTableConf -> "test_namespace.events_table"
            ).asJava)
        )
    )
    metaData.setConsistencyCheck(true)

    val join = Join(
      metaData = metaData,
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.test_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(join)
    val plan = planner.buildPlan

    val consistencyNode = plan.nodes.asScala.find(_.content.isSetJoinConsistencyComputeNode).get
    val tableDeps = consistencyNode.metaData.executionInfo.tableDependencies.asScala

    // Consistency node should depend on logged table (output of log flattening)
    tableDeps should not be empty
    tableDeps.head.tableInfo.table should equal(join.metaData.loggedTable)
  }

  it should "log flattening node should have table dependencies from config" in {
    val join = Join(
      metaData = MetaData(
        name = "testJoinLogDeps",
        samplePercent = 10.0,
        executionInfo = new ExecutionInfo()
          .setConf(
            new ConfigProperties()
              .setCommon(Map(
                Constants.LoggingSchemaTableConf -> "test_namespace.schema_table",
                Constants.LoggingEventsTableConf -> "test_namespace.events_table"
              ).asJava)
          )
      ),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.test_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(join)
    val plan = planner.buildPlan

    val logFlatteningNode = plan.nodes.asScala.find(_.content.isSetJoinLogFlatteningNode).get
    val tableDeps = logFlatteningNode.metaData.executionInfo.tableDependencies.asScala

    tableDeps.size should be(2)
    val depTables = tableDeps.map(_.tableInfo.table).toSet
    depTables should contain("test_namespace.schema_table")
    depTables should contain("test_namespace.events_table")
  }

  it should "not create consistency node when consistencyCheck is false" in {
    val metaData = MetaData(
      name = "testJoinNoConsistency",
      samplePercent = 10.0,
      executionInfo = new ExecutionInfo()
        .setConf(
          new ConfigProperties()
            .setCommon(Map(
              Constants.LoggingSchemaTableConf -> "test_namespace.schema_table",
              Constants.LoggingEventsTableConf -> "test_namespace.events_table"
            ).asJava)
        )
    )
    metaData.setConsistencyCheck(false)

    val join = Join(
      metaData = metaData,
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.test_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(join)
    val plan = planner.buildPlan

    // Should have log flattening but NOT consistency compute node
    plan.nodes.asScala.find(_.content.isSetJoinLogFlatteningNode) should be(defined)
    plan.nodes.asScala.find(_.content.isSetJoinConsistencyComputeNode) should be(empty)

    // MONITOR should point to log flattening node, not consistency
    plan.terminalNodeNames.asScala(Mode.MONITOR) should equal("testJoinNoConsistency__log_flattener")
  }
}
