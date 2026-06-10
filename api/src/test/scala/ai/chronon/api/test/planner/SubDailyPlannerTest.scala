package ai.chronon.api.test.planner

import ai.chronon.api.Extensions.{WindowOps, WindowUtils}
import ai.chronon.api.planner.{DependencyResolver, GroupByPlanner, MetaDataUtils, MonolithJoinPlanner}
import ai.chronon.api.{
  Accuracy,
  Builders,
  PartitionRange,
  PartitionSpec,
  TimeUnit,
  Window
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class SubDailyPlannerTest extends AnyFlatSpec with Matchers {

  private val threeHourlyAt1 = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000L, 60 * 60 * 1000L)

  // a sub-daily conf needs intraday-ready sources; timestamp-backed is the simplest declaration
  private def timePartitionedGroupBy() = {
    val groupBy = GroupByPlannerTest.buildGroupBy()
    groupBy.sources.asScala.foreach(_.getEvents.query.setTimePartitioned(true))
    groupBy
  }

  "GroupByPlanner" should "serialize sub-daily interval and offset onto node output table info" in {
    implicit val spec: PartitionSpec = threeHourlyAt1
    val plan = GroupByPlanner(timePartitionedGroupBy()).buildPlan

    val backfill = plan.nodes.asScala.find(_.content.isSetGroupByBackfill).get
    val outInfo = backfill.metaData.executionInfo.outputTableInfo
    outInfo.partitionInterval should equal(new Window(3, TimeUnit.HOURS))
    outInfo.partitionOffset should equal(new Window(1, TimeUnit.HOURS))
    outInfo.partitionFormat should equal("yyyy-MM-dd-HH-mm")
  }

  it should "not emit partitionOffset for daily specs (compiled-conf byte compatibility)" in {
    implicit val spec: PartitionSpec = PartitionSpec.daily
    val plan = GroupByPlanner(GroupByPlannerTest.buildGroupBy()).buildPlan

    plan.nodes.asScala.foreach { node =>
      val outInfo = node.metaData.executionInfo.outputTableInfo
      withClue(s"node ${node.metaData.name}") {
        outInfo.isSetPartitionOffset shouldBe false
        // daily serializes as 24 HOURS, matching the historical WindowUtils.hours output
        outInfo.partitionInterval should equal(new Window(24, TimeUnit.HOURS))
      }
    }
  }

  "MetaDataUtils.layer" should "respect author-declared output partition fields" in {
    implicit val spec: PartitionSpec = PartitionSpec.daily
    val authored = new ai.chronon.api.TableInfo()
      .setPartitionInterval(new Window(3, TimeUnit.HOURS))
      .setPartitionOffset(new Window(1, TimeUnit.HOURS))
    val base = Builders.MetaData(namespace = "test_namespace", name = "authored_gb")
    base.executionInfo = new ai.chronon.api.ExecutionInfo().setOutputTableInfo(authored)

    val layered = MetaDataUtils.layer(base, "group_by", "authored_gb__group_by", Seq.empty)
    val outInfo = layered.executionInfo.outputTableInfo
    outInfo.partitionInterval should equal(new Window(3, TimeUnit.HOURS))
    outInfo.partitionOffset should equal(new Window(1, TimeUnit.HOURS))
    // unset fields still fall back to the implicit spec
    outInfo.partitionColumn should equal("ds")
    outInfo.partitionFormat should equal("yyyy-MM-dd")
  }

  "MonolithJoinPlanner" should "validate sub-daily join support by left model and accuracy" in {
    implicit val spec: PartitionSpec = threeHourlyAt1

    def eventsJoin(name: String, accuracy: Accuracy, intradayReady: Boolean = true) = {
      def source(table: String) = {
        val src = Builders.Source.events(Builders.Query(), table = table)
        if (intradayReady) src.getEvents.query.setTimePartitioned(true)
        src
      }
      Builders.Join(
        metaData = Builders.MetaData(namespace = "test_namespace", name = name),
        left = source("test_namespace.left_events"),
        joinParts = Seq(
          Builders.JoinPart(groupBy =
            Builders.GroupBy(
              sources = Seq(source("test_namespace.right_events")),
              keyColumns = Seq("user"),
              metaData = Builders.MetaData(namespace = "test_namespace", name = s"${name}_gb"),
              accuracy = accuracy
            )))
      )
    }

    // events-left works for both accuracies: temporal computes at the output grain, snapshot
    // parts bind via the daily lookback
    noException should be thrownBy MonolithJoinPlanner(eventsJoin("temporal_join", Accuracy.TEMPORAL))
    noException should be thrownBy MonolithJoinPlanner(eventsJoin("snapshot_join", Accuracy.SNAPSHOT))

    // an ENTITIES left is itself a daily snapshot - rejected
    val entitiesJoin = Builders.Join(
      metaData = Builders.MetaData(namespace = "test_namespace", name = "entities_join"),
      left = Builders.Source.entities(Builders.Query(), snapshotTable = "test_namespace.left_entities"),
      joinParts = Seq.empty
    )
    val ex = the[IllegalArgumentException] thrownBy MonolithJoinPlanner(entitiesJoin)
    ex.getMessage should include("ENTITIES")
  }

  it should "require intraday-ready events sources for sub-daily outputs" in {
    implicit val spec: PartitionSpec = threeHourlyAt1

    // daily catalog-partitioned events source: cannot deliver sub-daily freshness
    val dailySourceJoin = Builders.Join(
      metaData = Builders.MetaData(namespace = "test_namespace", name = "daily_source_join"),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.daily_events"),
      joinParts = Seq.empty
    )
    val ex = the[IllegalArgumentException] thrownBy MonolithJoinPlanner(dailySourceJoin)
    ex.getMessage should include("cannot deliver sub-daily freshness")

    // declaring a grain at least as fine as the output makes it valid - offsets don't matter
    val hourlySourceJoin = Builders.Join(
      metaData = Builders.MetaData(namespace = "test_namespace", name = "hourly_source_join"),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.hourly_events"),
      joinParts = Seq.empty
    )
    hourlySourceJoin.left.getEvents.query.setPartitionInterval(new Window(1, TimeUnit.HOURS))
    noException should be thrownBy MonolithJoinPlanner(hourlySourceJoin)

    // daily outputs are unaffected by the rule
    noException should be thrownBy MonolithJoinPlanner(dailySourceJoin.deepCopy())(PartitionSpec.daily)
  }

  "DependencyResolver.getMissingSteps" should "step by a day's worth of partitions for sub-daily specs" in {
    val hourly = PartitionSpec.hourly()
    val required = PartitionRange("2024-01-01-00", "2024-01-02-23")(hourly)
    val steps = DependencyResolver.getMissingSteps(required, existingPartitions = Seq.empty, stepDays = 1)
    steps should have size 2
    steps.head.partitions should have size 24
  }
}
