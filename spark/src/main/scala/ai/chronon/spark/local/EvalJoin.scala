package ai.chronon.spark.local

import ai.chronon.api._
import ai.chronon.api.Extensions.{JoinOps, JoinPartOps, SourceOps}
import ai.chronon.api.ScalaJavaConversions.IterableOps
import ai.chronon.orchestration.{
  BaseEvalResult,
  CheckResult,
  EvalUnion,
  GroupByEvalResult,
  JoinEvalResult,
  JoinPartEvalResult
}
import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.types.{StructType => SparkStructType}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class EvalJoin(
    evalGroupBy: GroupBy => (GroupByEvalResult, Option[SparkStructType]),
    evalTableDependency: String => (EvalUnion, Option[SparkStructType]),
    registerDummyTable: (String, SparkStructType) => Unit
)(implicit val tableUtils: TableUtils) {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  var joinEvalResults: mutable.Map[String, (JoinEvalResult, Option[SparkStructType])] = mutable.Map.empty

  def reset(): Unit = {
    joinEvalResults.clear()
  }

  def evalJoin(join: Join): (JoinEvalResult, Option[SparkStructType]) = {
    // First see if we've already evaluated this join (since we're recursively traversing)
    joinEvalResults.get(join.metaData.getName) match {
      case Some(result) => return result
      case _            => logger.info(s"Evaluating Join: ${join.metaData.getName}")
    }

    val result = new JoinEvalResult()
    result.setName(join.metaData.getName)

    // Step 1: Evaluate left source tables
    val leftTable = join.left.table
    val (leftTableEval, leftSchemaOpt) = evalTableDependency(leftTable)

    // Set parent evaluations (we'll add more as we evaluate join parts)
    val leftParentEvals = List(leftTableEval).asJava
    result.setParentEvals(leftParentEvals)

    // Step 2: Register left source table if schema is available
    leftSchemaOpt.foreach { schema =>
      registerDummyTable(leftTable, schema)
    }

    // Step 3: Get left schema if available
    val leftQueryOutputSchemaOpt: Option[SparkStructType] = if (leftSchemaOpt.isDefined) {
      try {
        val schema = EvalHelpers.getJoinLeftSchema(join, tableUtils)

        // Set success for left expression check
        val leftCheck = new BaseEvalResult()
        leftCheck.setCheckResult(CheckResult.SUCCESS)
        result.setLeftExpressionCheck(leftCheck)
        result.setLeftQuerySchema(RenderUtils.structTypeToSchemaMap(schema))
        Some(schema)
      } catch {
        case e: Throwable =>
          val leftCheck = new BaseEvalResult()
          leftCheck.setCheckResult(CheckResult.FAILURE)
          leftCheck.setMessage(s"Failed to evaluate left query: ${e.getMessage}")
          result.setLeftExpressionCheck(leftCheck)
          None
      }
    } else {
      val leftCheck = new BaseEvalResult()
      leftCheck.setCheckResult(CheckResult.SKIPPED)
      leftCheck.setMessage("Left source table evaluation was skipped because of upstream failure")
      result.setLeftExpressionCheck(leftCheck)
      None
    }

    // Step 4: Evaluate join parts (always do this, even if left schema is not available)
    val (joinPartEvals, rightPartsSchemaOptions) = leftQueryOutputSchemaOpt match {
      case Some(leftSchema) => evalJoinParts(join, Some(leftSchema))
      case None => evalJoinParts(join, None)
    }

    result.setJoinPartChecks(joinPartEvals.asJava)

    // If we don't have a left query output schema, return early (join parts have been evaluated)
    if (leftQueryOutputSchemaOpt.isEmpty) {
      val overallCheck = new BaseEvalResult()
      overallCheck.setCheckResult(CheckResult.SKIPPED)
      overallCheck.setMessage("Join evaluation halted due to left schema failure")
      result.setOverallCheck(overallCheck)
      joinEvalResults(join.metaData.getName) = (result, None)
      return (result, None)
    }

    // If any join parts failed to produce schemas, return early
    if (rightPartsSchemaOptions.exists(_.isEmpty)) {
      joinEvalResults(join.metaData.getName) = (result, None)
      return (result, None)
    }

    val leftSchema = leftQueryOutputSchemaOpt.get // Safe to get since we checked above

    // Step 5: Combine right parts schemas
    val allRightPartFields = rightPartsSchemaOptions.flatMap(_.get.fields)
    val rightPartsSchema = SparkStructType(allRightPartFields.toArray)
    result.setRightPartsSchema(RenderUtils.structTypeToSchemaMap(rightPartsSchema))

    // Step 6: Handle external parts schema
    val externalPartsSchemaOpt = EvalHelpers.getJoinExternalPartsSchema(join)

    externalPartsSchemaOpt.foreach(schema => result.setExternalPartsSchema(RenderUtils.structTypeToSchemaMap(schema)))

    // Step 7: Handle derivations if they exist
    val finalSchema = if (join.hasDerivations) {
      try {
        val derivedSchema =
          EvalHelpers.getJoinDerivationsSchema(join, leftSchema, rightPartsSchema, externalPartsSchemaOpt, tableUtils)

        // Set success for derivations check
        val derivationCheck = new BaseEvalResult()
        derivationCheck.setCheckResult(CheckResult.SUCCESS)
        result.setDerivationValidityCheck(derivationCheck)
        result.setDerivationsSchema(RenderUtils.structTypeToSchemaMap(derivedSchema))
        derivedSchema
      } catch {
        case e: Throwable =>
          val derivationCheck = new BaseEvalResult()
          derivationCheck.setCheckResult(CheckResult.FAILURE)
          derivationCheck.setMessage(s"Failed to evaluate derivations: ${e.getMessage}")
          result.setDerivationValidityCheck(derivationCheck)
          joinEvalResults(join.metaData.getName) = (result, None)
          return (result, None)
      }
    } else {
      // Combine left and right schemas as the final output
      SparkStructType(
        leftSchema.fields ++ rightPartsSchema.fields ++ externalPartsSchemaOpt.map(_.fields).getOrElse(Array.empty))
    }

    // Cache and return result
    val overallCheck = new BaseEvalResult()
    overallCheck.setCheckResult(CheckResult.SUCCESS)
    result.setOverallCheck(overallCheck)
    val finalEvalResult = (result, Option(finalSchema))
    joinEvalResults(join.metaData.getName) = finalEvalResult
    finalEvalResult
  }

  private def evalJoinParts(
      join: Join,
      leftSchemaOpt: Option[SparkStructType]) = {

    val leftSchemaFormatted = leftSchemaOpt.map(RenderUtils.structTypeToSchemaMap(_).asScala.toMap)
    val gbToEvalResult = mutable.Map[String, (GroupByEvalResult, Option[SparkStructType])]()

    val results = join.joinParts.toScala.map { part =>
      val joinPartEval = new JoinPartEvalResult()
      val gbName = s"${part.groupBy.metaData.name}"
      val columnPrefix = part.columnPrefix
      val partName = s"$columnPrefix$gbName"
      joinPartEval.setPartName(partName)

      // Eval the GB if not seen before, else use existing eval
      val (gbEval, gbSchemaRawOpt) = gbToEvalResult.getOrElseUpdate(gbName, evalGroupBy(part.groupBy))

      // Also prefix the raw schema for derivation handling
      val jpSchemaRawOpt = gbSchemaRawOpt.map { schema =>
        SparkStructType(schema.fields.map { field =>
          field.copy(name = columnPrefix + field.name)
        })
      }

      joinPartEval.setGbEvalResult(gbEval)

      val gbKeySchema = Option(gbEval.getKeySchema)

      val keySchemaCheck = (leftSchemaFormatted, gbKeySchema) match {
        case (Some(leftSchema), Some(keySchema)) =>
          EvalHelpers.checkKeySchema(leftSchema, keySchema.asScala.toMap, part.rightToLeft)
        case (None, _) =>
          val skipCheck = new BaseEvalResult()
          skipCheck.setCheckResult(CheckResult.SKIPPED)
          skipCheck.setMessage("Left schema not available - skipping key schema check")
          skipCheck
        case (_, None) =>
          val skipCheck = new BaseEvalResult()
          skipCheck.setCheckResult(CheckResult.SKIPPED)
          skipCheck.setMessage("GroupBy evaluation failed to produce key schema")
          skipCheck
      }

      joinPartEval.setKeySchemaCheck(keySchemaCheck)

      if (keySchemaCheck.getCheckResult == CheckResult.SUCCESS) {
        (joinPartEval, jpSchemaRawOpt)
      } else {
        // If key schema check fails, don't return the output schema
        (joinPartEval, None)
      }
    }.toList

    results.unzip
  }

  def evalJoinConf(rootConfPath: String): JoinEvalResult = {
    assert(rootConfPath.contains("/joins/"), s"Expected path to contain '/joins/', but got: $rootConfPath")
    val join = ThriftJsonCodec.fromJsonFile[Join](rootConfPath, check = false)
    evalJoin(join)._1
  }
}
