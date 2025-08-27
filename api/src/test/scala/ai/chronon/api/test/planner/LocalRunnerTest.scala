package ai.chronon.api.test.planner

import ai.chronon.api.{GroupBy, Join, PartitionSpec, StagingQuery}
import ai.chronon.api.planner.LocalRunner
import ai.chronon.planner.ConfPlan
import com.fasterxml.jackson.core.JsonParseException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class LocalRunnerTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec: PartitionSpec = PartitionSpec.daily

  // Use classpath resources instead of Bazel runfiles
  private val canaryResourcePath = getClass.getClassLoader.getResource("canary/compiled").getPath
  private val joinConfigPath = s"$canaryResourcePath/joins"
  private val groupByConfigPath = s"$canaryResourcePath/group_bys"
  private val stagingQueryConfigPath = s"$canaryResourcePath/staging_queries"

  it should "parse join configurations correctly" in {
    val joinConfs = LocalRunner.parseConfs[Join](joinConfigPath)

    joinConfs should not be empty
    joinConfs.foreach { joinConf =>
      joinConf.metaData should not be null
      joinConf.metaData.name should not be null
      joinConf.metaData.name should not be empty
      joinConf.left should not be null
    }

    // Verify specific test configuration exists
    val testJoinConf = joinConfs.find(_.metaData.name == "gcp.training_set.v1_test")
    testJoinConf should be(defined)
    testJoinConf.get.metaData.team shouldBe "gcp"
    testJoinConf.get.joinParts should not be empty
  }

  it should "parse groupby configurations correctly" in {
    val groupByConfs = LocalRunner.parseConfs[GroupBy](groupByConfigPath)

    groupByConfs should not be empty
    groupByConfs.foreach { groupByConf =>
      groupByConf.metaData should not be null
      groupByConf.metaData.name should not be null
      groupByConf.metaData.name should not be empty
      groupByConf.sources should not be empty
      groupByConf.keyColumns should not be empty
    }

    // Verify specific test configuration exists
    val testGroupByConf = groupByConfs.find(_.metaData.name == "gcp.purchases.v1_test")
    testGroupByConf should be(defined)
    testGroupByConf.get.metaData.team shouldBe "gcp"
    testGroupByConf.get.sources should have size 1
    testGroupByConf.get.keyColumns should contain("user_id")
    testGroupByConf.get.aggregations should not be empty
  }

  it should "parse staging query configurations correctly" in {
    val stagingQueryConfs = LocalRunner.parseConfs[StagingQuery](stagingQueryConfigPath)

    stagingQueryConfs should not be empty
    stagingQueryConfs.foreach { stagingQueryConf =>
      stagingQueryConf.metaData should not be null
      stagingQueryConf.metaData.name should not be null
      stagingQueryConf.metaData.name should not be empty
    }
  }

  it should "return ConfPlan objects for join configurations" in {
    val plans = LocalRunner.processConfigurations(joinConfigPath, "joins")

    plans should not be empty
    plans.foreach { plan =>
      plan shouldBe a[ConfPlan]
      plan.nodes should not be null
      plan.terminalNodeNames should not be null
    }
  }

  it should "return ConfPlan objects for groupby configurations" in {
    val plans = LocalRunner.processConfigurations(groupByConfigPath, "groupbys")

    plans should not be empty
    plans.foreach { plan =>
      plan shouldBe a[ConfPlan]
      plan.nodes should not be null
      plan.terminalNodeNames should not be null
    }
  }

  it should "return ConfPlan objects for staging query configurations" in {
    val plans = LocalRunner.processConfigurations(stagingQueryConfigPath, "staging_queries")

    plans should not be empty
    plans.foreach { plan =>
      plan shouldBe a[ConfPlan]
      plan.nodes should not be null
      plan.terminalNodeNames should not be null
    }
  }

  it should "throw UnsupportedOperationException for invalid conf type" in {
    an[UnsupportedOperationException] should be thrownBy {
      LocalRunner.processConfigurations(joinConfigPath, "invalid_type")
    }

    val exception = the[UnsupportedOperationException] thrownBy {
      LocalRunner.processConfigurations(joinConfigPath, "unsupported")
    }
    exception.getMessage should include("Unsupported conf type: unsupported")
    exception.getMessage should include("joins, staging_queries, groupbys")
  }

  it should "handle empty directories gracefully" in {
    val tempDir = Files.createTempDirectory("empty_test_dir")
    try {
      val result = LocalRunner.parseConfs[Join](tempDir.toString)
      result shouldBe empty

      val plans = LocalRunner.processConfigurations(tempDir.toString, "joins")
      plans shouldBe empty
    } finally {
      Files.deleteIfExists(tempDir)
    }
  }

  it should "throw exception for invalid configuration files" in {
    val tempDir = Files.createTempDirectory("invalid_conf_test")
    val invalidFile = tempDir.resolve("invalid.json")

    try {
      Files.write(invalidFile, "{ invalid json }".getBytes)

      an[JsonParseException] should be thrownBy {
        LocalRunner.parseConfs[Join](tempDir.toString)
      }

      an[JsonParseException] should be thrownBy {
        LocalRunner.processConfigurations(tempDir.toString, "joins")
      }
    } finally {
      Files.deleteIfExists(invalidFile)
      Files.deleteIfExists(tempDir)
    }
  }

  it should "filter out ignorable files correctly" in {
    val tempDir = Files.createTempDirectory("filter_test")
    val validFile = tempDir.resolve("valid.json")
    val dsStoreFile = tempDir.resolve(".DS_Store")
    val pycFile = tempDir.resolve("file.pyc")
    val gitDir = tempDir.resolve(".git")

    try {
      // Create valid GroupBy configuration
      val validJson = """{
        "metaData": {"name": "test", "team": "test"},
        "sources": [{"events": {"table": "test", "query": {"timeColumn": "ts"}}}],
        "keyColumns": ["key"],
        "aggregations": []
      }"""
      Files.write(validFile, validJson.getBytes)
      Files.write(dsStoreFile, "should be ignored".getBytes)
      Files.write(pycFile, "should be ignored".getBytes)
      Files.createDirectory(gitDir)

      val result = LocalRunner.parseConfs[GroupBy](tempDir.toString)
      result should have size 1
      result.head.metaData.name shouldBe "test"

      val plans = LocalRunner.processConfigurations(tempDir.toString, "groupbys")
      plans should have size 1
      plans.head shouldBe a[ConfPlan]

    } finally {
      Files.deleteIfExists(validFile)
      Files.deleteIfExists(dsStoreFile)
      Files.deleteIfExists(pycFile)
      Files.deleteIfExists(gitDir)
      Files.deleteIfExists(tempDir)
    }
  }

  it should "validate specific join plan structure" in {
    val plans = LocalRunner.processConfigurations(joinConfigPath, "joins")

    plans should not be empty

    // Find the test plan
    val testPlan = plans.find { plan =>
      import scala.jdk.CollectionConverters._
      plan.nodes.asScala.exists(_.metaData.name.contains("training_set.v1_test"))
    }

    testPlan should be(defined)
    val plan = testPlan.get

    import scala.jdk.CollectionConverters._
    plan.nodes.asScala should not be empty
    plan.terminalNodeNames.asScala should not be empty

    // Should have nodes for different modes
    plan.terminalNodeNames.asScala should contain key ai.chronon.planner.Mode.BACKFILL
  }

  it should "validate specific groupby plan structure" in {
    val plans = LocalRunner.processConfigurations(groupByConfigPath, "groupbys")

    plans should not be empty

    // Find the test plan
    val testPlan = plans.find { plan =>
      import scala.jdk.CollectionConverters._
      plan.nodes.asScala.exists(_.metaData.name.contains("purchases.v1_test"))
    }

    testPlan should be(defined)
    val plan = testPlan.get

    import scala.jdk.CollectionConverters._
    plan.nodes.asScala should not be empty
    plan.terminalNodeNames.asScala should not be empty

    // Should have nodes for both backfill and deploy modes
    plan.terminalNodeNames.asScala should contain key ai.chronon.planner.Mode.BACKFILL
    plan.terminalNodeNames.asScala should contain key ai.chronon.planner.Mode.DEPLOY
  }

  it should "handle mixed valid and invalid files" in {
    val tempDir = Files.createTempDirectory("mixed_test")
    val validFile = tempDir.resolve("valid.json")
    val invalidFile = tempDir.resolve("invalid.json")

    try {
      val validJson = """{
        "metaData": {"name": "valid_test", "team": "test"},
        "sources": [{"events": {"table": "test", "query": {"timeColumn": "ts"}}}],
        "keyColumns": ["key"],
        "aggregations": []
      }"""
      Files.write(validFile, validJson.getBytes)
      Files.write(invalidFile, "{ malformed json".getBytes)

      an[JsonParseException] should be thrownBy {
        LocalRunner.parseConfs[GroupBy](tempDir.toString)
      }

      an[JsonParseException] should be thrownBy {
        LocalRunner.processConfigurations(tempDir.toString, "groupbys")
      }

    } finally {
      Files.deleteIfExists(validFile)
      Files.deleteIfExists(invalidFile)
      Files.deleteIfExists(tempDir)
    }
  }

  it should "validate that processConfigurations returns plans that can be printed" in {
    val plans = LocalRunner.processConfigurations(joinConfigPath, "joins")

    plans should not be empty

    // Test that plans can be converted to string (for printing)
    plans.foreach { plan =>
      val planString = plan.toString
      planString should not be null
      planString should not be empty
    }
  }
}
