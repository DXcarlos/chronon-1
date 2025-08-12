package ai.chronon.spark.local

import ai.chronon.api._
import ai.chronon.orchestration.{StagingQueryEvalResult, GroupByEvalResult, JoinEvalResult}
import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.types.{StructField, StructType => SparkStructType, StringType}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.parallel.ParMap

/** EvalConf orchestrates evaluation by delegating to specialized evaluator classes.
  * This provides better separation of concerns and makes the codebase more maintainable.
  */
class EvalConf(rootDir: String, schemaUtils: SchemaUtils)(implicit val tableUtils: TableUtils) {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  // Configuration index - shared across all evaluators
  var tablesToStagingQuery: ParMap[String, StagingQuery] = ParMap.empty
  var tablesToJoin: ParMap[String, Join] = ParMap.empty
  var tablesToGroupBy: ParMap[String, GroupBy] = ParMap.empty

  // Initialize specialized evaluators - these will be recreated when config changes
  private var tableDepEvaluator: EvalTableDependency = _
  private var stagingQueryEvaluator: EvalStagingQuery = _
  private var groupByEvaluator: EvalGroupBy = _
  private var joinEvaluator: EvalJoin = _

  buildConfIndex()

  def buildConfIndex(): Unit = {
    // Rebuild the configuration index to pick up any changes
    val confIndex = new ConfIndex(rootDir + "/compiled")
    this.tablesToStagingQuery = confIndex.tablesToStagingQuery
    this.tablesToJoin = confIndex.tablesToJoin
    this.tablesToGroupBy = confIndex.tablesToGroupBy

    // Recreate evaluators with the updated configuration
    buildEvaluators()
  }

  private def buildEvaluators(): Unit = {
    // Create evaluators with the current configuration - order matters due to dependencies
    tableDepEvaluator = new EvalTableDependency(
      tablesToStagingQuery,
      tablesToJoin,
      tablesToGroupBy,
      stagingQuery => if (stagingQueryEvaluator != null) stagingQueryEvaluator.evalStagingQuery(stagingQuery) else null,
      groupBy => if (groupByEvaluator != null) groupByEvaluator.evalGroupBy(groupBy) else null,
      join => if (joinEvaluator != null) joinEvaluator.evalJoin(join) else null,
      schemaUtils
    )

    stagingQueryEvaluator = new EvalStagingQuery(
      rootDir,
      schemaUtils,
      tableDepEvaluator.evalTableDependency,
      registerDummyTable
    )

    groupByEvaluator = new EvalGroupBy(
      tableDepEvaluator.evalTableDependency,
      registerDummyTable
    )

    joinEvaluator = new EvalJoin(
      groupByEvaluator.evalGroupBy,
      tableDepEvaluator.evalTableDependency,
      registerDummyTable
    )

    // Update the tableDepEvaluator with the correct function references now that all evaluators exist
    tableDepEvaluator = new EvalTableDependency(
      tablesToStagingQuery,
      tablesToJoin,
      tablesToGroupBy,
      stagingQueryEvaluator.evalStagingQuery,
      groupByEvaluator.evalGroupBy,
      joinEvaluator.evalJoin,
      schemaUtils
    )
  }

  def reset(): Unit = {
    // Rebuild configuration index and recreate all evaluators with fresh state
    buildConfIndex()
  }

  def registerDummyTable(tableName: String, schema: SparkStructType): Unit = {
    // Parse multi-part table name
    val parts = tableName.split("\\.")
    val database = parts.dropRight(1).mkString(".")
    val table = parts.last

    // Create database if it doesn't exist
    tableUtils.sparkSession.sql(s"CREATE DATABASE IF NOT EXISTS `$database`")

    // Generate CREATE TABLE SQL from schema
    val columnDefinitions = schema.fields
      .map { field =>
        val dataType = field.dataType.sql
        val nullable = if (field.nullable) "" else " NOT NULL"
        s"`${field.name}` $dataType$nullable"
      }
      .mkString(", ")

    val createTableSql = s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        $columnDefinitions
      ) USING PARQUET
    """.stripMargin

    // Execute the CREATE TABLE statement
    tableUtils.sparkSession.sql(createTableSql)
  }

  // Public API methods that delegate to the appropriate evaluators
  def evalStagingQueryConf(rootConfPath: String): StagingQueryEvalResult = {
    reset()
    stagingQueryEvaluator.evalStagingQueryConf(rootConfPath)
  }

  def evalGroupByConf(rootConfPath: String): GroupByEvalResult = {
    reset()
    groupByEvaluator.evalGroupByConf(rootConfPath)
  }

  def evalJoinConf(rootConfPath: String): JoinEvalResult = {
    reset()
    joinEvaluator.evalJoinConf(rootConfPath)
  }
}
