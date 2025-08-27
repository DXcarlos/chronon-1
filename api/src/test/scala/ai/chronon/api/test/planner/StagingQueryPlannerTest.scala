package ai.chronon.api.test.planner

import ai.chronon.api.{EngineType, ExecutionInfo, PartitionSpec}
import ai.chronon.api.planner.{LocalRunner, StagingQueryPlanner}
import ai.chronon.api.Builders.{MetaData, StagingQuery}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ai.chronon.api.TableDependency
import ai.chronon.api.TableInfo

import java.nio.file.Paths
import scala.jdk.CollectionConverters._

class StagingQueryPlannerTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec = PartitionSpec.daily

  it should "staging query planner plans valid confs without exceptions" in {

    val stagingQueryRootDir = Paths.get(getClass.getClassLoader.getResource("canary/compiled/staging_queries").getPath)

    val stagingQueryConfs = LocalRunner.parseConfs[ai.chronon.api.StagingQuery](stagingQueryRootDir.toString)

    val stagingQueryPlanners = stagingQueryConfs.map(new StagingQueryPlanner(_))

    stagingQueryPlanners
      .foreach { planner =>
        noException should be thrownBy {
          val plan = planner.buildPlan
          plan.terminalNodeNames.asScala.size should be > 0

          // Validate that no node names contain forward slashes
          plan.nodes.asScala.foreach { node =>
            val nodeName = node.metaData.name
            withClue(s"Node name '$nodeName' contains forward slash") {
              nodeName should not contain "/"
            }
          }
        }
      }
  }

  it should "staging query planner should create valid plans without exceptions" in {
    val ti = new TableInfo().setTable("hello")
    val td = new TableDependency().setTableInfo(ti)
    val stagingQuery = StagingQuery(
      query = "SELECT * FROM test_table",
      metaData = MetaData(name = "testStagingQuery"),
      engineType = EngineType.SPARK,
      tableDependencies = Seq(td)
    )

    val planner = new StagingQueryPlanner(stagingQuery)

    noException should be thrownBy {
      val plan = planner.buildPlan
      plan.nodes.asScala should not be empty
      plan.terminalNodeNames.asScala should contain key ai.chronon.planner.Mode.BACKFILL

      // Validate that no node names contain forward slashes
      plan.nodes.asScala.foreach { node =>
        val nodeName = node.metaData.name
        withClue(s"Node name '$nodeName' contains forward slash") {
          nodeName should not contain "/"
        }
      }
    }
  }

  it should "staging query planner should avoid metadata when computing semantic hash" in {
    val firstStagingQuery = StagingQuery(
      query = "SELECT * FROM test_table",
      metaData = MetaData(name = "firstStagingQuery"),
      engineType = EngineType.SPARK
    )

    val secondStagingQuery = StagingQuery(
      query = "SELECT * FROM test_table",
      metaData = MetaData(name = "secondStagingQuery"),
      engineType = EngineType.SPARK
    )

    val firstPlanner = new StagingQueryPlanner(firstStagingQuery)
    val secondPlanner = new StagingQueryPlanner(secondStagingQuery)

    val firstPlan = firstPlanner.buildPlan
    val secondPlan = secondPlanner.buildPlan

    // Semantic hashes should be identical since metadata is excluded
    val firstSemanticHashes = firstPlan.nodes.asScala.map(_.semanticHash)
    val secondSemanticHashes = secondPlan.nodes.asScala.map(_.semanticHash)
    firstSemanticHashes should equal(secondSemanticHashes)
  }

  it should "staging query planner should create correct terminal node names" in {
    val stagingQuery = StagingQuery(
      query = "SELECT * FROM test_table",
      metaData = MetaData(name = "testStagingQuery"),
      engineType = EngineType.SPARK
    )

    val planner = new StagingQueryPlanner(stagingQuery)
    val plan = planner.buildPlan

    plan.terminalNodeNames.asScala should contain key ai.chronon.planner.Mode.BACKFILL
    plan.terminalNodeNames.asScala(ai.chronon.planner.Mode.BACKFILL) should equal("testStagingQuery__backfill")
  }

  it should "staging query planner should handle setups correctly" in {
    val stagingQueryWithSetups = StagingQuery(
      query = "SELECT * FROM test_table",
      metaData = MetaData(name = "testStagingQuery"),
      setups = Seq("CREATE TEMP VIEW temp_table AS SELECT 1 as id", "SET spark.sql.adaptive.enabled=true"),
      engineType = EngineType.SPARK
    )

    val planner = new StagingQueryPlanner(stagingQueryWithSetups)

    noException should be thrownBy {
      val plan = planner.buildPlan
      plan.nodes.asScala should have size 1
    }
  }

  it should "staging query planner should use default step days of 1" in {
    val stagingQuery = StagingQuery(
      query = "SELECT * FROM test_table",
      metaData = MetaData(name = "testStagingQuery"),
      engineType = EngineType.SPARK
    )

    val planner = new StagingQueryPlanner(stagingQuery)
    val plan = planner.buildPlan

    // Should create plan successfully with default step days
    plan.nodes.asScala should have size 1
    val node = plan.nodes.asScala.head
    node.metaData should not be null
    node.metaData.name should equal("testStagingQuery__backfill")
  }

  it should "staging query planner should produce same semantic hash with different executionInfo in metadata" in {
    val firstStagingQuery = StagingQuery(
      query = "SELECT * FROM test_table",
      metaData = MetaData(
        name = "testStagingQuery1",
        executionInfo = new ExecutionInfo().setStepDays(2)
      ),
      engineType = EngineType.SPARK
    )

    val secondStagingQuery = StagingQuery(
      query = "SELECT * FROM test_table",
      metaData = MetaData(
        name = "testStagingQuery2",
        executionInfo = new ExecutionInfo().setStepDays(5)
      ),
      engineType = EngineType.SPARK
    )

    val firstPlanner = new StagingQueryPlanner(firstStagingQuery)
    val secondPlanner = new StagingQueryPlanner(secondStagingQuery)

    val firstPlan = firstPlanner.buildPlan
    val secondPlan = secondPlanner.buildPlan

    // Semantic hashes should be identical since metadata (including executionInfo) is excluded
    val firstSemanticHashes = firstPlan.nodes.asScala.map(_.semanticHash)
    val secondSemanticHashes = secondPlan.nodes.asScala.map(_.semanticHash)
    firstSemanticHashes should equal(secondSemanticHashes)
  }

  it should "staging query planner should produce exactly one node wrapping a StagingQuery for canary confs" in {
    val stagingQueryRootDir = Paths.get(getClass.getClassLoader.getResource("canary/compiled/staging_queries").getPath)

    val stagingQueryConfs = LocalRunner.parseConfs[ai.chronon.api.StagingQuery](stagingQueryRootDir.toString)

    stagingQueryConfs.foreach { stagingQueryConf =>
      val planner = new StagingQueryPlanner(stagingQueryConf)
      val plan = planner.buildPlan

      val node = plan.nodes.asScala.head
      // Node should have content
      node.content should not be null
      // Content should have a stagingQuery set
      node.content.getStagingQuery should not be null
      // The wrapped staging query should not be null
      node.content.getStagingQuery.stagingQuery should not be null
    }
  }
}
