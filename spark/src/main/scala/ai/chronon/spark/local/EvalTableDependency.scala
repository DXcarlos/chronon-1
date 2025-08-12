package ai.chronon.spark.local

import ai.chronon.api._
import ai.chronon.orchestration.{
  BaseEvalResult,
  CheckResult,
  TableDependencyEvalResult,
  EvalUnion,
  StagingQueryEvalResult,
  GroupByEvalResult,
  JoinEvalResult
}
import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.types.{StructType => SparkStructType}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.mutable
import scala.collection.parallel.ParMap

class EvalTableDependency(
    tablesToStagingQuery: ParMap[String, StagingQuery],
    tablesToJoin: ParMap[String, Join],
    tablesToGroupBy: ParMap[String, GroupBy],
    evalStagingQuery: StagingQuery => (StagingQueryEvalResult, Option[SparkStructType]),
    evalGroupBy: GroupBy => (GroupByEvalResult, Option[SparkStructType]),
    evalJoin: Join => (JoinEvalResult, Option[SparkStructType]),
    schemaUtils: SchemaUtils
)(implicit val tableUtils: TableUtils) {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  var sourceTableEvalResults: mutable.Map[String, (TableDependencyEvalResult, Option[SparkStructType])] =
    mutable.Map.empty

  def reset(): Unit = {
    sourceTableEvalResults.clear()
  }

  def evalTableDependency(table: String): (EvalUnion, Option[SparkStructType]) = {
    if (tablesToJoin.contains(table)) {
      val (joinEvalResult, schemaOpt) = evalJoin(tablesToJoin(table))
      (createEvalUnion(joinEvalResult), schemaOpt)
    } else if (tablesToGroupBy.contains(table)) {
      val (gbEvalResult, schemaOpt) = evalGroupBy(tablesToGroupBy(table))
      (createEvalUnion(gbEvalResult), schemaOpt)
    } else if (tablesToStagingQuery.contains(table)) {
      val (sqEvalResult, schemaOpt) = evalStagingQuery(tablesToStagingQuery(table))
      (createEvalUnion(sqEvalResult), schemaOpt)
    } else {
      // This is the case where the table is a source table
      if (sourceTableEvalResults.contains(table)) {
        val (tableResult, schemaOpt) = sourceTableEvalResults(table)
        return (createEvalUnion(tableResult), schemaOpt)
      }
      try {
        val schema = schemaUtils.getTableSchema(table)
        val result = new TableDependencyEvalResult()
        result.setTableName(table)
        result.setSchema(RenderUtils.structTypeToSchemaMap(schema))
        val check = new BaseEvalResult()
        check.setCheckResult(CheckResult.SUCCESS)
        result.setOverallCheck(check)
        val resultTuple = (result, Some(schema))
        sourceTableEvalResults.put(table, resultTuple)
        (createEvalUnion(result), Option(schema))
      } catch {
        case e: Exception =>
          val result = new TableDependencyEvalResult()
          result.setTableName(table)
          val check = new BaseEvalResult()
          check.setCheckResult(CheckResult.FAILURE)
          check.setMessage(s"Error getting schema for table $table: ${e.getMessage}")
          result.setOverallCheck(check)
          val resultTuple = (result, None)
          sourceTableEvalResults.put(table, resultTuple)
          (createEvalUnion(result), None)
      }
    }
  }

  private def createEvalUnion(tableResult: TableDependencyEvalResult): EvalUnion = {
    val evalUnion = new EvalUnion()
    evalUnion.setTableDepEval(tableResult)
    evalUnion
  }

  private def createEvalUnion(stagingQueryResult: StagingQueryEvalResult): EvalUnion = {
    val evalUnion = new EvalUnion()
    evalUnion.setStagingQueryEval(stagingQueryResult)
    evalUnion
  }

  private def createEvalUnion(groupByResult: GroupByEvalResult): EvalUnion = {
    val evalUnion = new EvalUnion()
    evalUnion.setGroupByEval(groupByResult)
    evalUnion
  }

  private def createEvalUnion(joinResult: JoinEvalResult): EvalUnion = {
    val evalUnion = new EvalUnion()
    evalUnion.setJoinEval(joinResult)
    evalUnion
  }

}
