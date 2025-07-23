package ai.chronon.api.test.planner

import ai.chronon.api.{Accuracy, Aggregation, Builders, EnvironmentVariables, GroupBy, Operation, PartitionSpec}
import ai.chronon.api.Extensions.{GroupByOps, MetadataOps, WindowUtils}
import ai.chronon.api.planner.{GroupByPlanner, LocalRunner}
import ai.chronon.api.test.planner.GroupByPlannerTest.buildGroupBy
import ai.chronon.planner.{ConfPlan, Mode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths
import scala.jdk.CollectionConverters._

class GroupByPlannerTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec: PartitionSpec = PartitionSpec.daily

  private def validateGBPlan(groupBy: GroupBy, plan: ConfPlan): Unit = {
    // Should create plan successfully with expected number of nodes
    val hasStreaming = groupBy.streamingSource.isDefined
    val expectedNodeCount = if (hasStreaming) 4 else 3
    plan.nodes.asScala should have size expectedNodeCount

    // Find the nodes
    val uploadNode = plan.nodes.asScala.find(_.content.isSetGroupByUpload)
    val backfillNode = plan.nodes.asScala.find(_.content.isSetGroupByBackfill)
    val uploadToKVNode = plan.nodes.asScala.find(_.content.isSetGroupByUploadToKV)
    val streamingNode = plan.nodes.asScala.find(_.content.isSetGroupByStreaming)

    uploadNode should be(defined)
    backfillNode should be(defined)
    uploadToKVNode should be(defined)

    if (hasStreaming) {
      streamingNode should be(defined)
    } else {
      streamingNode should not be defined
    }

    // Upload node should have content
    uploadNode.get.content should not be null
    uploadNode.get.content.getGroupByUpload should not be null
    uploadNode.get.content.getGroupByUpload.groupBy should not be null
    // upload node should use the daily partition spec
    uploadNode.get.metaData.executionInfo.outputTableInfo.partitionColumn should equal(PartitionSpec.daily.column)
    uploadNode.get.metaData.executionInfo.outputTableInfo.partitionFormat should equal(PartitionSpec.daily.format)

    // Backfill node should have content
    backfillNode.get.content should not be null
    backfillNode.get.content.getGroupByBackfill should not be null
    backfillNode.get.content.getGroupByBackfill.groupBy should not be null

    // UploadToKV node should have content
    uploadToKVNode.get.content should not be null
    uploadToKVNode.get.content.getGroupByUploadToKV should not be null
    uploadToKVNode.get.content.getGroupByUploadToKV.groupBy should not be null

    // Streaming node should have content if present
    if (hasStreaming) {
      streamingNode.get.content should not be null
      streamingNode.get.content.getGroupByStreaming should not be null
      streamingNode.get.content.getGroupByStreaming.groupBy should not be null
    }

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

  it should "always plan nonzero step days" in {
    val groupBy = buildGroupBy()
    val plannerWithNonZeroStepDays = GroupByPlanner(groupBy)
    val plan = plannerWithNonZeroStepDays.buildPlan
    plan.nodes.asScala.foreach((node) => node.metaData.executionInfo.stepDays shouldNot be(0))
  }

  it should "GB planner handles valid confs" in {

    val runfilesDir = System.getenv("RUNFILES_DIR")
    val gbRootDir = Paths.get(runfilesDir, "chronon/spark/src/test/resources/canary/compiled/group_bys")

    val gbConfs = LocalRunner.parseConfs[ai.chronon.api.GroupBy](gbRootDir.toString)

    val planners = gbConfs.map(new GroupByPlanner(_))

    planners
      .foreach { planner =>
        noException should be thrownBy {
          val plan = planner.buildPlan
          validateGBPlan(planner.groupBy, plan)
        }
      }
  }

  it should "GB planner should create valid plans" in {
    val gb = buildGroupBy()

    val planner = new GroupByPlanner(gb)

    noException should be thrownBy {
      val plan = planner.buildPlan
      validateGBPlan(planner.groupBy, plan)
      plan.terminalNodeNames.asScala(Mode.DEPLOY) should equal("user_charges__uploadToKV")
      plan.terminalNodeNames.asScala(Mode.BACKFILL) should equal("user_charges__backfill")
    }
  }

  it should "GB planner should skip metadata in sem hash" in {
    val firstGb = buildGroupBy()

    // Create a second GroupBy with different metadata
    val secondGb = buildGroupBy()
    secondGb.metaData = firstGb.metaData.deepCopy()
    secondGb.metaData.name = "secondGroupBy"
    secondGb.metaData.samplePercent = 0.5 // Different sample percent

    val firstPlanner = new GroupByPlanner(firstGb)
    val secondPlanner = new GroupByPlanner(secondGb)

    val firstPlan = firstPlanner.buildPlan
    val secondPlan = secondPlanner.buildPlan

    // Semantic hashes should be identical since metadata is excluded (for upload nodes)
    val firstUploadHash = firstPlan.nodes.asScala.find(_.content.isSetGroupByUpload).get.semanticHash
    val secondUploadHash = secondPlan.nodes.asScala.find(_.content.isSetGroupByUpload).get.semanticHash
    firstUploadHash should equal(secondUploadHash)
  }

  it should "GB planner should produce same semantic hash with different executionInfo" in {
    val firstGB = buildGroupBy()

    val secondGB = buildGroupBy()
    secondGB.metaData = firstGB.metaData.deepCopy()
    secondGB.metaData.executionInfo = firstGB.metaData.executionInfo.deepCopy()
    secondGB.metaData.executionInfo.env =
      new EnvironmentVariables().setCommon(Map("foo" -> "bar", "baz" -> "qux").asJava)

    val firstPlanner = new GroupByPlanner(firstGB)
    val secondPlanner = new GroupByPlanner(secondGB)

    val firstPlan = firstPlanner.buildPlan
    val secondPlan = secondPlanner.buildPlan

    // Semantic hashes should be identical since metadata (including executionInfo) is excluded
    val firstSemanticHashes = firstPlan.nodes.asScala.map(_.semanticHash)
    val secondSemanticHashes = secondPlan.nodes.asScala.map(_.semanticHash)
    firstSemanticHashes should equal(secondSemanticHashes)
  }

  it should "GB planner uploadToKV node should have correct table dependencies" in {
    val gb = buildGroupBy()
    val planner = new GroupByPlanner(gb)
    val plan = planner.buildPlan
    validateGBPlan(gb, plan)

    val uploadToKVNode = plan.nodes.asScala.find(_.content.isSetGroupByUploadToKV).get
    val executionInfo = uploadToKVNode.metaData.executionInfo
    val tableDeps = executionInfo.tableDependencies.asScala

    // Should have exactly one table dependency
    tableDeps should have size 1

    val tableDep = tableDeps.head
    // Validate table dependency references the upload table
    tableDep.tableInfo.table should equal(gb.metaData.uploadTable)

    // Validate partition specifications match the test partition spec
    tableDep.tableInfo.partitionColumn should equal(testPartitionSpec.column)
    tableDep.tableInfo.partitionFormat should equal(testPartitionSpec.format)

    // Validate offsets are set to zero for upload scenarios
    tableDep.startOffset should not be null
    tableDep.endOffset should not be null

    // Validate output table info is properly set
    val outputTableInfo = uploadToKVNode.metaData.executionInfo.outputTableInfo
    outputTableInfo should not be null
    outputTableInfo.table should equal(gb.metaData.name + "__uploadToKV")
    outputTableInfo.partitionColumn should equal(testPartitionSpec.column)
    outputTableInfo.partitionFormat should equal(testPartitionSpec.format)
    outputTableInfo.partitionInterval should not be null
  }

  it should "GB planner should create streaming node when streamingSource is present" in {
    val gb = buildGroupBy(includeTopic = true)
    val planner = GroupByPlanner(gb)
    val plan = planner.buildPlan

    validateGBPlan(gb, plan)

    // DEPLOY mode should now point to streaming node
    plan.terminalNodeNames.asScala(Mode.DEPLOY) should equal("user_charges__streaming")
    plan.terminalNodeNames.asScala(Mode.BACKFILL) should equal("user_charges__backfill")

    // Verify streaming node has correct table dependencies (same as uploadToKV)
    val streamingNode = plan.nodes.asScala.find(_.content.isSetGroupByStreaming).get
    val executionInfo = streamingNode.metaData.executionInfo
    val tableDeps = executionInfo.tableDependencies.asScala

    tableDeps should have size 1
    val tableDep = tableDeps.head
    tableDep.tableInfo.table should equal(gb.metaData.name + "__uploadToKV")

    // Verify streaming node has correct output table info
    val streamingOutputTableInfo = streamingNode.metaData.executionInfo.outputTableInfo
    streamingOutputTableInfo should not be null
    streamingOutputTableInfo.table should equal(gb.metaData.name + "__streaming")
    streamingOutputTableInfo.partitionColumn should equal(testPartitionSpec.column)
    streamingOutputTableInfo.partitionFormat should equal(testPartitionSpec.format)
    streamingOutputTableInfo.partitionInterval should not be null
  }
}

object GroupByPlannerTest {
  def buildGroupBy(includeTopic: Boolean = false): GroupBy = {
    val eventsTable = "my_user_events"
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.COUNT, "charges", Seq(WindowUtils.Unbounded))
    )
    val source = Builders.Source.events(Builders.Query(), table = eventsTable)
    val sourceWithTopic = if (includeTopic) {
      Builders.Source.events(Builders.Query(), table = eventsTable, topic = "my_user_events_topic")
    } else {
      source
    }

    Builders.GroupBy(
      sources = Seq(sourceWithTopic),
      keyColumns = Seq("user"),
      aggregations = aggregations,
      metaData = Builders.MetaData(namespace = "test_namespace", name = "user_charges"),
      accuracy = Accuracy.TEMPORAL
    )
  }
}
