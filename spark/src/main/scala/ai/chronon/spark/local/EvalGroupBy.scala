package ai.chronon.spark.local

import ai.chronon.api.Extensions.{GroupByOps, SourceOps}
import ai.chronon.api._
import ai.chronon.orchestration.{BaseEvalResult, CheckResult, EvalUnion, GroupByEvalResult}
import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.types.{StructType => SparkStructType}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class EvalGroupBy(
    evalTableDependency: String => (EvalUnion, Option[SparkStructType]),
    registerDummyTable: (String, SparkStructType) => Unit
)(implicit val tableUtils: TableUtils) {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  var groupByEvalResults: mutable.Map[String, (GroupByEvalResult, Option[SparkStructType])] = mutable.Map.empty

  def reset(): Unit = {
    groupByEvalResults.clear()
  }

  def evalGroupBy(groupBy: GroupBy): (GroupByEvalResult, Option[SparkStructType]) = {
    // First see if we've already evaluated this group by (since we're recursively traversing)
    groupByEvalResults.get(groupBy.metaData.getName) match {
      case Some(result) => return result
      case _            => logger.info(s"Evaluating GroupBy: ${groupBy.metaData.getName}")
    }

    val result = new GroupByEvalResult()
    result.setName(groupBy.metaData.getName)

    // Step 1: Get source tables and evaluate their schemas
    val sourceTables = groupBy.sources.asScala.map(_.rootTable).toSet
    val inputTablesWithEvals = sourceTables.map { table =>
      table -> evalTableDependency(table)
    }

    // Set parent evaluations
    val parentEvalsList = inputTablesWithEvals.map { case (_, (evalUnion, _)) => evalUnion }.toList.asJava
    result.setParentEvals(parentEvalsList)

    // If any of the source tables don't have schemas, return with skipped status
    val tablesWithoutSchemas = inputTablesWithEvals.filter { case (_, (_, schemaOpt)) => schemaOpt.isEmpty }
    if (tablesWithoutSchemas.nonEmpty) {
      val sourceCheck = new BaseEvalResult()
      sourceCheck.setCheckResult(CheckResult.SKIPPED)
      sourceCheck.setMessage(s"Source tables are missing schemas: ${tablesWithoutSchemas.map(_._1).mkString(", ")}")
      result.setSourceExpressionCheck(sourceCheck)
      result.setOverallCheck(sourceCheck)
      groupByEvalResults(groupBy.metaData.getName) = (result, None)
      return (result, None)
    }

    // Step 2: Register dummy tables for all source dependencies
    inputTablesWithEvals.foreach { case (table, (evalUnion, schemaOpt)) =>
      schemaOpt.foreach { sparkSchema =>
        registerDummyTable(table, sparkSchema)
      }
    }

    // Step 3: Find the input schema
    val (inputSchema, keySchema) =
      try {
        val schemas = EvalHelpers.getGroupBySourceSchema(groupBy, tableUtils)

        // Set success for source expression check
        val sourceCheck = new BaseEvalResult()
        sourceCheck.setCheckResult(CheckResult.SUCCESS)
        result.setSourceExpressionCheck(sourceCheck)
        result.setKeySchema(RenderUtils.structTypeToSchemaMap(schemas.keySchema))
        (schemas.sourceSchema, schemas.keySchema)
      } catch {
        case e: Throwable =>
          val sourceCheck = new BaseEvalResult()
          sourceCheck.setCheckResult(CheckResult.FAILURE)
          sourceCheck.setMessage(s"Failed to evaluate source queries: ${e.getMessage}")
          result.setSourceExpressionCheck(sourceCheck)
          val skipCheck = new BaseEvalResult()
          skipCheck.setCheckResult(CheckResult.SKIPPED)
          skipCheck.setMessage("Skipping aggregation expression check due to source evaluation failure")
          result.setAggExpressionCheck(skipCheck)
          result.setOverallCheck(sourceCheck)
          groupByEvalResults(groupBy.metaData.getName) = (result, None)
          return (result, None)
      }

    // Step 4: Attempt to infer aggregation schema using RowAggregator
    val aggSchema =
      try {

        val outputSparkSchema = if (Option(groupBy.aggregations).isDefined) {
          EvalHelpers.getGroupByAggSchema(groupBy, inputSchema)
        } else {
          inputSchema
        }
        // Set success for aggregation expression check
        val aggCheck = new BaseEvalResult()
        aggCheck.setCheckResult(CheckResult.SUCCESS)
        result.setAggExpressionCheck(aggCheck)
        result.setAggSchema(RenderUtils.structTypeToSchemaMap(outputSparkSchema))
        outputSparkSchema
      } catch {
        case e: Throwable =>
          val aggCheck = new BaseEvalResult()
          aggCheck.setCheckResult(CheckResult.FAILURE)
          aggCheck.setMessage(s"Invalid aggregation definition: ${e.getMessage}")
          result.setAggExpressionCheck(aggCheck)
          result.setOverallCheck(aggCheck)
          groupByEvalResults(groupBy.metaData.getName) = (result, None)
          return (result, None)
      }

    // Step 5: Handle derivations if they exist
    val finalSchema = if (groupBy.hasDerivations) {
      try {
        val derivedSchema = EvalHelpers.getGroupByDerivationsSchema(groupBy, aggSchema, keySchema, tableUtils)

        // Set success for derivations check
        val derivationCheck = new BaseEvalResult()
        derivationCheck.setCheckResult(CheckResult.SUCCESS)
        result.setDerivationsExpressionCheck(derivationCheck)
        result.setDerivationsSchema(RenderUtils.structTypeToSchemaMap(derivedSchema))
        derivedSchema
      } catch {
        case e: Throwable =>
          val derivationCheck = new BaseEvalResult()
          derivationCheck.setCheckResult(CheckResult.FAILURE)
          derivationCheck.setMessage(s"Failed to evaluate derivations: ${e.getMessage}")
          result.setDerivationsExpressionCheck(derivationCheck)
          result.setOverallCheck(derivationCheck)
          groupByEvalResults(groupBy.metaData.getName) = (result, None)
          return (result, None)
      }
    } else {
      aggSchema
    }

    // Cache and return result
    val overallCheck = new BaseEvalResult()
    overallCheck.setCheckResult(CheckResult.SUCCESS)
    result.setOverallCheck(overallCheck)
    val finalEvalResult = (result, Option(finalSchema))
    groupByEvalResults(groupBy.metaData.getName) = finalEvalResult
    finalEvalResult
  }

  def evalGroupByConf(rootConfPath: String): GroupByEvalResult = {
    assert(rootConfPath.contains("/group_bys/"), s"Expected path to contain '/group_bys/', but got: $rootConfPath")
    val groupBy = ThriftJsonCodec.fromJsonFile[GroupBy](rootConfPath, check = false)
    evalGroupBy(groupBy)._1
  }
}
