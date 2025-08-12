package ai.chronon.spark.local

import ai.chronon.api.ScalaJavaConversions.IterableOps
import ai.chronon.api._
import ai.chronon.online.Extensions.StructTypeOps
import ai.chronon.orchestration.{BaseEvalResult, CheckResult, EvalUnion, StagingQueryEvalResult}
import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.types.{StringType, StructField, StructType => SparkStructType}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class EvalStagingQuery(
    rootDir: String,
    schemaUtils: SchemaUtils,
    evalTableDependency: String => (EvalUnion, Option[SparkStructType]),
    registerDummyTable: (String, SparkStructType) => Unit
)(implicit val tableUtils: TableUtils) {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  var stagingQueryEvalResults: mutable.Map[String, (StagingQueryEvalResult, Option[SparkStructType])] =
    mutable.Map.empty

  private def ensurePartitionColumn(schema: SparkStructType): SparkStructType = {
    // Add ds partition column if it's not already there
    if (!schema.fieldNames.contains(tableUtils.partitionColumn)) {
      val partitionField = StructField(tableUtils.partitionColumn, StringType, nullable = true)
      SparkStructType(schema.fields :+ partitionField)
    } else {
      schema
    }
  }

  def reset(): Unit = {
    stagingQueryEvalResults.clear()
  }

  def evalBigQueryQuery(query: String, name: String): (StagingQueryEvalResult, Option[SparkStructType]) = {
    val result = new StagingQueryEvalResult()
    result.setName(name)

    val baseEvalResult = new BaseEvalResult()
    try {
      // Use the schemaUtils to get the schema from the BigQuery query
      val originalSchema = schemaUtils.getBigQueryQuerySchema(query)

      // Add ds partition column if it's not already there
      val schema = ensurePartitionColumn(originalSchema)

      println(s"POST QUERY - BigQuery schema with ds: ${schema.pretty}")
      baseEvalResult.setCheckResult(CheckResult.SUCCESS)
      result.setOverallCheck(baseEvalResult)
      (result, Some(schema))
    } catch {
      case e: Exception =>
        baseEvalResult.setCheckResult(CheckResult.FAILURE)
        baseEvalResult.setMessage(s"Failed to evaluate BigQuery query: ${e.getMessage}")
        result.setOverallCheck(baseEvalResult)
        (result, None)
    }
  }

  def evalStagingQuery(stagingQuery: StagingQuery): (StagingQueryEvalResult, Option[SparkStructType]) = {
    // First see if we've already evaluated this staging query (since we're recursively traversing)
    stagingQueryEvalResults.get(stagingQuery.metaData.getName) match {
      case Some(result) => return result
      case _            => logger.info(s"Evaluating StagingQuery: ${stagingQuery.metaData.getName}")
    }

    val dummyPartition = "2025-01-01"
    val rendered =
      ai.chronon.spark.batch.StagingQuery.substitute(tableUtils,
                                                     stagingQuery.query,
                                                     dummyPartition,
                                                     dummyPartition,
                                                     dummyPartition)

    if (stagingQuery.engineType == EngineType.BIGQUERY) {
      // If we're running BigQuery engine we assume that the source tables are already in BQ
      val (bigQueryResult, schemaOpt) = evalBigQueryQuery(rendered, stagingQuery.metaData.getName)
      // Cache BigQuery results just like Spark results for consistency
      stagingQueryEvalResults(stagingQuery.metaData.getName) = (bigQueryResult, schemaOpt)
      return (bigQueryResult, schemaOpt)
    }

    val tableDeps = stagingQuery.tableDependencies.toScala.map(_.tableInfo.table).toSet

    val inputTablesWithEvals = tableDeps.map { table =>
      table -> evalTableDependency(table)
    }

    val result = new StagingQueryEvalResult()
    result.setName(stagingQuery.metaData.getName)

    // Set parent evaluations
    val parentEvalsList = inputTablesWithEvals.map { case (_, (evalUnion, _)) => evalUnion }.toList.asJava
    result.setParentEvals(parentEvalsList)

    // If any of the input tables don't have schemas, return with skipped status
    val tablesWithoutSchemas = inputTablesWithEvals.filter { case (_, (_, schemaOpt)) => schemaOpt.isEmpty }
    if (tablesWithoutSchemas.nonEmpty) {
      val queryCheck = new BaseEvalResult()
      queryCheck.setCheckResult(CheckResult.SKIPPED)
      result.setOverallCheck(queryCheck)
      stagingQueryEvalResults(stagingQuery.metaData.getName) = (result, None)
      return (result, None)
    }

    // Else, registerDummyTable then attempt to run the `rendered` query to get the output schema or catch exception and set failure reason
    inputTablesWithEvals.foreach { case (table, (evalUnion, schemaOpt)) =>
      schemaOpt.foreach { sparkSchema =>
        registerDummyTable(table, sparkSchema)
      }
    }
    try {
      // Execute the rendered query to get the schema
      val queryDf = tableUtils.sql(rendered)
      val originalSchema = queryDf.schema

      // Add ds partition column if it's not already there (consistent with BigQuery logic)
      val outputSchema = ensurePartitionColumn(originalSchema)

      // Set success result
      val queryCheck = new BaseEvalResult()
      queryCheck.setCheckResult(CheckResult.SUCCESS)
      result.setOverallCheck(queryCheck)

      // Set the output schema on the result
      result.setOutputSchema(RenderUtils.structTypeToSchemaMap(outputSchema))

      // Cache and return the result
      stagingQueryEvalResults(stagingQuery.metaData.getName) = (result, Some(outputSchema))
      (result, Some(outputSchema))

    } catch {
      case e: Throwable =>
        val queryCheck = new BaseEvalResult()
        queryCheck.setCheckResult(CheckResult.FAILURE)
        queryCheck.setMessage(s"Failed to evaluate staging query: ${e.getMessage}")
        result.setOverallCheck(queryCheck)
        stagingQueryEvalResults(stagingQuery.metaData.getName) = (result, None)
        (result, None)
    }
  }

  def evalStagingQueryConf(rootConfPath: String): StagingQueryEvalResult = {
    assert(rootConfPath.contains("/staging_queries/"),
           s"Expected path to contain '/staging_queries/', but got: $rootConfPath")
    val stagingQuery = ThriftJsonCodec.fromJsonFile[StagingQuery](rootConfPath, check = false)
    evalStagingQuery(stagingQuery)._1
  }
}
