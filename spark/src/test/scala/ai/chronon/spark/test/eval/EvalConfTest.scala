package ai.chronon.spark.test.eval

import ai.chronon.api.ScalaJavaConversions.MapOps
import ai.chronon.api.{GroupBy, Join, StagingQuery, ThriftJsonCodec}
import ai.chronon.orchestration.CheckResult
import ai.chronon.spark.local.{EvalConf, SchemaUtils}
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.test.TableTestUtils
import ai.chronon.spark.submission.SparkSessionBuilder
import ai.chronon.spark.Extensions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.junit.Assert._

import java.io.File
import scala.util.{Failure, Success, Try}

class EvalConfTest extends AnyFlatSpec {

  val spark: SparkSession = SparkSessionBuilder.build("EvalConfTest", local = true)
  private implicit val tableUtils: TableTestUtils = TableTestUtils(spark)
  tableUtils.createDatabase("local_test")

  val testConfigRoot = "spark/src/test/scala/ai/chronon/spark/test/eval/resources/chronon/"

  // Test SchemaUtils implementation that uses Spark for both Spark and BigQuery
  private val testSchemaUtils = new SchemaUtils {
    def getBigQueryTableSchema(table: String): StructType = {
      // For testing, we'll delegate to Spark implementation
      getSparkTableSchema(table)(tableUtils)
    }

    def getBigQueryQuerySchema(query: String): StructType = {
      // For testing, we'll delegate to Spark implementation
      getSparkQuerySchema(query)(tableUtils)
    }
  }

  // Create mock table once for all tests
  createMockUserActivitiesTable()

  // Path to our test chronon configs (relative to this test file)

  it should "initialize EvalConf with compiled test configs" in {
    val evalConf = new EvalConf(testConfigRoot, testSchemaUtils)

    println(s"Found ${evalConf.tablesToStagingQuery.size} staging queries")
    println(s"Found ${evalConf.tablesToGroupBy.size} group bys")
    println(s"Found ${evalConf.tablesToJoin.size} joins")

    // Check that our compiled configs were loaded
    assertTrue("Should find staging queries", evalConf.tablesToStagingQuery.nonEmpty)
    assertTrue("Should find group bys", evalConf.tablesToGroupBy.nonEmpty)
    assertTrue("Should find joins", evalConf.tablesToJoin.nonEmpty)

    // Print out the actual table names we found
    println("Staging Query tables:")
    evalConf.tablesToStagingQuery.keys.foreach(println)

    println("GroupBy tables:")
    evalConf.tablesToGroupBy.keys.foreach(println)

    println("Join tables:")
    evalConf.tablesToJoin.keys.foreach(println)
  }

  it should "evaluate join from compiled config successfully" in {
    val evalConf = new EvalConf(testConfigRoot, testSchemaUtils)

    // Find the specific join config we want to test
    val joinConfPath = testConfigRoot + "compiled/joins/test/user_features.v1__0"

    // Get and evaluate the join
    val joinResult = evalConf.evalJoinConf(joinConfPath)

    println(s"Evaluated join result: $joinResult")

    // Check results
    // Should have left query schema (the event data)
    assertNotNull("Should have left query schema", joinResult.getLeftQuerySchema)
    assertTrue("Left schema should contain user_id", joinResult.getLeftQuerySchema.containsKey("user_id"))
    assertTrue("Left schema should contain row_id", joinResult.getLeftQuerySchema.containsKey("row_id"))

    // Should have right parts schema (the aggregated features)
    assertNotNull("Should have right parts schema", joinResult.getRightPartsSchema)
    val rightSchema = joinResult.getRightPartsSchema

    // Check for some expected aggregated feature columns
    println(s"Right parts schema keys: ${rightSchema.keySet()}")

    // The aggregations should include clicks and purchases with different windows
    assertTrue("Should have clicks sum aggregation", rightSchema.keySet().toString.contains("clicks_sum"))
    assertTrue("Should have purchases sum aggregation", rightSchema.keySet().toString.contains("purchases_sum"))
  }

  it should "evaluate staging query from compiled config successfully" in {
    val evalConf = new EvalConf(testConfigRoot, testSchemaUtils)

    // Find staging query config
    val stagingQueryPath = testConfigRoot + "compiled/staging_queries/test/user_activities.v0__0"

    // Evaluate the staging query
    val stagingQueryResult = evalConf.evalStagingQueryConf(stagingQueryPath)

    assertNotNull("Should have staging query result", stagingQueryResult)

    // Check that query evaluation was successful
    val queryCheck = stagingQueryResult.getQueryCheck
    assertNotNull("Should have query check", queryCheck)
    assertEquals("Query check should be successful", CheckResult.SUCCESS, queryCheck.getCheckResult)

    // Verify the output schema
    val outputSchema = stagingQueryResult.getOutputSchema
    assertNotNull("Should have output schema", outputSchema)
    assertTrue("Should have user_id in schema", outputSchema.containsKey("user_id"))
    assertTrue("Should have click_event in schema", outputSchema.containsKey("click_event"))
    assertTrue("Should have purchase_event in schema", outputSchema.containsKey("purchase_event"))
    assertTrue("Should have row_id in schema", outputSchema.containsKey("row_id"))
    assertTrue("Should have ts in schema", outputSchema.containsKey("ts"))
    assertTrue("Should have ds in schema", outputSchema.containsKey("ds"))

    println(s"Staging query output schema: ${outputSchema.toScala}")
  }

  it should "evaluate groupby from compiled config successfully" in {
    val evalConf = new EvalConf(testConfigRoot, testSchemaUtils)

    // Find groupby config
    val groupByPath = testConfigRoot + "compiled/group_bys/test/user_features.v0__0"

    // Evaluate the groupby
    val groupByResult = evalConf.evalGroupByConf(groupByPath)

    assertNotNull("Should have groupby result", groupByResult)

    // Should have successful source checks (we skip timestamp checks)
    val sourceExpressionCheck = groupByResult.getSourceExpressionCheck
    if (sourceExpressionCheck != null) {
      assertEquals("Source expression check should be successful",
                   CheckResult.SUCCESS,
                   sourceExpressionCheck.getCheckResult)
    }

    val aggExpressionCheck = groupByResult.getAggExpressionCheck
    if (aggExpressionCheck != null) {
      assertEquals("Aggregation expression check should be successful",
                   CheckResult.SUCCESS,
                   aggExpressionCheck.getCheckResult)
    }

    // Check schemas
    val keySchema = groupByResult.getKeySchema
    assertNotNull("Should have key schema", keySchema)
    assertTrue("Key schema should contain user_id", keySchema.containsKey("user_id"))

    val aggSchema = groupByResult.getAggSchema
    assertNotNull("Should have aggregation schema", aggSchema)
    // Should have click and purchase aggregations
    assertTrue("Should have clicks aggregations", aggSchema.keySet().toString.contains("clicks"))
    assertTrue("Should have purchases aggregations", aggSchema.keySet().toString.contains("purchases"))

    println(s"GroupBy key schema: ${keySchema.toScala}")
    println(s"GroupBy agg schema: ${aggSchema.toScala}")
  }

  it should "detect bad key mapping with type mismatch in join" in {
    val evalConf = new EvalConf(testConfigRoot, testSchemaUtils)

    // Find the bad key join config
    val badJoinPath = testConfigRoot + "compiled/joins/test/bad_key.v1__0"

    // Evaluate the join - this should detect the key schema mismatch
    val joinResult = evalConf.evalJoinConf(badJoinPath)
    println(s"Evaluated join result: $joinResult")

    assertNotNull("Should have join result", joinResult)

    // Check that the join part key schema check failed due to type mismatch
    val joinPartChecks = joinResult.getJoinPartChecks
    assertNotNull("Should have join part checks", joinPartChecks)
    assertTrue("Should have at least one join part", joinPartChecks.size() > 0)

    val firstJoinPartCheck = joinPartChecks.get(0)
    val keySchemaCheck = firstJoinPartCheck.getKeySchemaCheck
    assertNotNull("Should have key schema check", keySchemaCheck)
    assertEquals("Key schema check should fail due to type mismatch",
                 CheckResult.FAILURE,
                 keySchemaCheck.getCheckResult)

    // The error message should mention the type mismatch
    val errorMessage = keySchemaCheck.getMessage
    assertNotNull("Should have error message", errorMessage)
    assertTrue("Error message should mention type mismatch",
               errorMessage.toLowerCase.contains("type") || errorMessage.toLowerCase.contains("mismatch"))

    println(s"Key schema check failure message: $errorMessage")
  }

  it should "detect invalid aggregation in join" in {
    val evalConf = new EvalConf(testConfigRoot, testSchemaUtils)

    // Find the invalid_agg join config
    val invalidAggJoinPath = testConfigRoot + "compiled/joins/test/invalid_agg.v1__0"

    // Evaluate the join - this should detect the invalid aggregation
    val joinResult = evalConf.evalJoinConf(invalidAggJoinPath)
    println(s"Evaluated invalid_agg join result: $joinResult")

    assertNotNull("Should have join result", joinResult)

    // Check that the join part evaluation failed due to invalid aggregation
    val joinPartChecks = joinResult.getJoinPartChecks
    assertNotNull("Should have join part checks", joinPartChecks)
    assertTrue("Should have at least one join part", joinPartChecks.size() > 0)

    val firstJoinPart = joinPartChecks.get(0)
    val gbEvalResult = firstJoinPart.getGbEvalResult
    assertNotNull("Should have GroupBy eval result", gbEvalResult)

    // The aggregation expression check should fail
    val aggExpressionCheck = gbEvalResult.getAggExpressionCheck
    assertNotNull("Should have aggregation expression check", aggExpressionCheck)
    assertEquals("Aggregation expression check should fail due to invalid aggregation",
                 CheckResult.FAILURE,
                 aggExpressionCheck.getCheckResult)

    // The error message should mention the aggregation issue
    val errorMessage = aggExpressionCheck.getMessage
    assertNotNull("Should have error message", errorMessage)
    assertTrue("Error message should mention aggregation issue",
               errorMessage.toLowerCase.contains("aggregation") || errorMessage.toLowerCase.contains("invalid"))

    println(s"Invalid aggregation error message: $errorMessage")
  }

  it should "skip staging query evaluation when upstream join fails" in {
    val evalConf = new EvalConf(testConfigRoot, testSchemaUtils)

    // Find the downstream staging query that depends on the bad join
    val downstreamStagingQueryPath = testConfigRoot + "compiled/staging_queries/test/downstream_bad_join.v0__0"

    // Evaluate the staging query - it should be skipped because upstream join fails
    val stagingQueryResult = evalConf.evalStagingQueryConf(downstreamStagingQueryPath)
    println(s"Evaluated downstream staging query result: $stagingQueryResult")

    assertNotNull("Should have staging query result", stagingQueryResult)

    // Check that query evaluation was skipped due to upstream dependency failure
    val queryCheck = stagingQueryResult.getQueryCheck
    assertNotNull("Should have query check", queryCheck)
    assertEquals("Query check should be skipped due to upstream failure",
                 CheckResult.SKIPPED,
                 queryCheck.getCheckResult)

    // Verify that parent evaluations are set correctly
    val parentEvals = stagingQueryResult.getParentEvals
    assertNotNull("Should have parent evaluations", parentEvals)
    assertTrue("Should have at least one parent evaluation", parentEvals.size() > 0)

    // The parent should be the bad join evaluation result
    val joinParent = parentEvals.get(0)
    assertNotNull("Join parent should not be null", joinParent)
    assertNotNull("Parent should have join eval", joinParent.getJoinEval)

    // The join eval should have failed key schema checks
    val joinEval = joinParent.getJoinEval
    println("Upstream Join Eval: " + joinEval)
    val joinPartChecks = joinEval.getJoinPartChecks
    assertNotNull("Join should have part checks", joinPartChecks)
    assertTrue("Join should have at least one part", joinPartChecks.size() > 0)

    // Verify the key schema failure exists in the join part
    val firstJoinPart = joinPartChecks.get(0)
    val keySchemaCheck = firstJoinPart.getKeySchemaCheck
    assertNotNull("Should have key schema check", keySchemaCheck)
    assertEquals("Key schema should have failed", CheckResult.FAILURE, keySchemaCheck.getCheckResult)

    println(s"Downstream staging query has parent join evaluation with ${joinPartChecks.size()} parts")
    println(s"Join part key schema check result: ${keySchemaCheck.getCheckResult}")

    // Verify that no output schema is produced since it was skipped
    val outputSchema = stagingQueryResult.getOutputSchema
    assertTrue("Output schema should be null or empty when skipped", outputSchema == null || outputSchema.isEmpty)
  }

  private def createMockUserActivitiesTable(): Unit = {
    // Create the minimal table structure that the staging query expects
    val userActivitiesSchema = StructType(
      Array(
        StructField("user_id", StringType, nullable = false),
        StructField("click_event", IntegerType, nullable = true),
        StructField("purchase_event", IntegerType, nullable = true),
        StructField("row_id", StringType, nullable = false),
        StructField("ts", LongType, nullable = false),
        StructField("ds", StringType, nullable = false)
      ))

    // Create minimal data - we only need schema, but having a few rows helps with debugging
    val userActivitiesData = Seq(
      Row("user1", 1, 0, "row1", 1705318800000L, "2024-01-15"),
      Row("user2", 0, 1, "row2", 1705322400000L, "2024-01-15")
    )

    val userActivitiesDf = spark.createDataFrame(
      spark.sparkContext.parallelize(userActivitiesData),
      userActivitiesSchema
    )

    // Save as table like other unit tests do
    userActivitiesDf.save("local_test.user_activities")

    println(
      s"Created mock table local_test.user_activities with schema: ${userActivitiesSchema.fieldNames.mkString(", ")}")
  }
}
