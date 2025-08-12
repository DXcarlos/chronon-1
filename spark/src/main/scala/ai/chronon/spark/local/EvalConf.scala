package ai.chronon.spark.local

import ai.chronon.api.Extensions.{
  AggregationOps,
  DerivationOps,
  GroupByOps,
  JoinOps,
  JoinPartOps,
  MetadataOps,
  SourceOps
}
import ai.chronon.api.ScalaJavaConversions.{IterableOps, MapOps}
import ai.chronon.api._
import ai.chronon.orchestration.{
  BaseEvalResult,
  CheckResult,
  GroupByEvalResult,
  JoinEvalResult,
  JoinPartEvalResult,
  StagingQueryEvalResult,
  TableDependencyEvalResult,
  EvalUnion
}
import ai.chronon.spark.catalog.TableUtils
import py4j.GatewayServer
import ai.chronon.spark.Extensions._

import java.io.File
import java.util.{List => JavaList}
import scala.collection.immutable.{Map, Set, _}
import scala.collection.mutable
import java.nio.file.{Files, Path, Paths}
import scala.collection.parallel.{ParMap, ParSeq}
import scala.jdk.CollectionConverters._
import org.apache.spark.sql.types.{StructField, StructType => SparkStructType, StringType}
import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.convert.ImplicitConversions.`collection asJava`

class EvalConf(rootDir: String, schemaUtils: SchemaUtils)(implicit val tableUtils: TableUtils) {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  var tablesToStagingQuery: ParMap[String, StagingQuery] = ParMap.empty
  var tablesToJoin: ParMap[String, Join] = ParMap.empty
  var tablesToGroupBy: ParMap[String, GroupBy] = ParMap.empty
  buildConfIndex()

  var stagingQueryEvalResults: mutable.Map[String, (StagingQueryEvalResult, Option[SparkStructType])] =
    mutable.Map.empty
  var groupByEvalResults: mutable.Map[String, (GroupByEvalResult, Option[SparkStructType])] = mutable.Map.empty
  var joinEvalResults: mutable.Map[String, (JoinEvalResult, Option[SparkStructType])] = mutable.Map.empty
  var sourceTableEvalResults: mutable.Map[String, (TableDependencyEvalResult, Option[SparkStructType])] =
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

  def buildConfIndex(): Unit = {
    // Rebuild the configuration index to pick up any changes
    val confIndex = new ConfIndex(rootDir + "/compiled")
    this.tablesToStagingQuery = confIndex.tablesToStagingQuery
    this.tablesToJoin = confIndex.tablesToJoin
    this.tablesToGroupBy = confIndex.tablesToGroupBy
  }

  def reset(): Unit = {
    // Clear all cached evaluation results
    stagingQueryEvalResults.clear()
    groupByEvalResults.clear()
    joinEvalResults.clear()
    sourceTableEvalResults.clear()
    buildConfIndex()
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
      return evalBigQueryQuery(rendered, stagingQuery.metaData.getName)
    }

    val tableDeps = stagingQuery.tableDependencies.toScala.map(_.tableInfo.table).toSet

    val inputTablesWithEvals = tableDeps.map { table =>
      evalTableDependency(table)
    }

    val result = new StagingQueryEvalResult()
    result.setName(stagingQuery.metaData.getName)

    // Set parent evaluations
    val parentEvalsList = inputTablesWithEvals.map { case (evalUnion, _) => evalUnion }.toList.asJava
    result.setParentEvals(parentEvalsList)

    // If any of the input tables don't have schemas, return with skipped status
    val tablesWithoutSchemas = inputTablesWithEvals.filter { case (_, schemaOpt) => schemaOpt.isEmpty }
    if (tablesWithoutSchemas.nonEmpty) {
      val queryCheck = new BaseEvalResult()
      queryCheck.setCheckResult(CheckResult.SKIPPED)
      result.setOverallCheck(queryCheck)
      stagingQueryEvalResults(stagingQuery.metaData.getName) = (result, None)
      return (result, None)
    }

    // Else, registerDummyTable then attempt to run the `rendered` query to get the output schema or catch exception and set failure reason
    inputTablesWithEvals.foreach { case (evalUnion, schemaOpt) =>
      schemaOpt.foreach { sparkSchema =>
        val tableName = if (evalUnion.getTableDepEval != null) {
          evalUnion.getTableDepEval.getTableName
        } else {
          // For non-table dependencies, we need to get the table name from the original tableDeps
          tableDeps
            .find(table =>
              if (evalUnion.getJoinEval != null) tablesToJoin.get(table).isDefined
              else if (evalUnion.getGroupByEval != null) tablesToGroupBy.get(table).isDefined
              else if (evalUnion.getStagingQueryEval != null) tablesToStagingQuery.get(table).isDefined
              else false)
            .get
        }
        registerDummyTable(tableName, sparkSchema)
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

  def evalGroupBy(groupBy: GroupBy): (GroupByEvalResult, Option[SparkStructType]) = {
    // First see if we've already evaluated this group by (since we're recursively traversing)
    groupByEvalResults.get(groupBy.metaData.getName) match {
      case Some(result) => return result
      case _            => logger.info(s"Evaluating GroupBy: ${groupBy.metaData.getName}")
    }

    val result = new GroupByEvalResult()
    result.setName(groupBy.metaData.getName)

    // Step 1: Get source tables and evaluate their schemas
    val sourceTables = groupBy.sources.toScala.map(_.rootTable).toSet
    val inputTablesWithEvals = sourceTables.map { table =>
      evalTableDependency(table)
    }

    // Set parent evaluations
    val parentEvalsList = inputTablesWithEvals.map { case (evalUnion, _) => evalUnion }.toList.asJava
    result.setParentEvals(parentEvalsList)

    // If any of the source tables don't have schemas, return with skipped status
    val tablesWithoutSchemas = inputTablesWithEvals.filter { case (_, schemaOpt) => schemaOpt.isEmpty }
    if (tablesWithoutSchemas.nonEmpty) {
      val sourceCheck = new BaseEvalResult()
      sourceCheck.setCheckResult(CheckResult.SKIPPED)
      result.setSourceExpressionCheck(sourceCheck)
      result.setOverallCheck(sourceCheck)
      groupByEvalResults(groupBy.metaData.getName) = (result, None)
      return (result, None)
    }

    // Step 2: Register dummy tables for all source dependencies
    inputTablesWithEvals.foreach { case (evalUnion, schemaOpt) =>
      schemaOpt.foreach { sparkSchema =>
        val tableName = if (evalUnion.getTableDepEval != null) {
          evalUnion.getTableDepEval.getTableName
        } else {
          // For non-table dependencies, we need to get the table name from the original sourceTables
          sourceTables
            .find(table =>
              if (evalUnion.getJoinEval != null) tablesToJoin.get(table).isDefined
              else if (evalUnion.getGroupByEval != null) tablesToGroupBy.get(table).isDefined
              else if (evalUnion.getStagingQueryEval != null) tablesToStagingQuery.get(table).isDefined
              else false)
            .get
        }
        registerDummyTable(tableName, sparkSchema)
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

    if (leftSchemaOpt.isEmpty) {
      val leftCheck = new BaseEvalResult()
      leftCheck.setCheckResult(CheckResult.SKIPPED)
      result.setLeftExpressionCheck(leftCheck)
      result.setOverallCheck(leftCheck)
      joinEvalResults(join.metaData.getName) = (result, None)
      return (result, None)
    }

    // Step 2: Register left source table
    leftSchemaOpt.foreach { schema =>
      registerDummyTable(leftTable, schema)
    }

    // Step 3: Get left schema
    val leftSchema =
      try {
        val schema = EvalHelpers.getJoinLeftSchema(join, tableUtils)

        // Set success for left expression check
        val leftCheck = new BaseEvalResult()
        leftCheck.setCheckResult(CheckResult.SUCCESS)
        result.setLeftExpressionCheck(leftCheck)
        result.setLeftQuerySchema(RenderUtils.structTypeToSchemaMap(schema))
        schema
      } catch {
        case e: Throwable =>
          val leftCheck = new BaseEvalResult()
          leftCheck.setCheckResult(CheckResult.FAILURE)
          leftCheck.setMessage(s"Failed to evaluate left query: ${e.getMessage}")
          result.setLeftExpressionCheck(leftCheck)
          joinEvalResults(join.metaData.getName) = (result, None)
          return (result, None)
      }

    // Step 4: Evaluate join parts
    val (joinPartEvals, rightPartsSchemaOptions) =
      evalJoinParts(join, leftSchema, RenderUtils.structTypeToSchemaMap(leftSchema).asScala.toMap)

    result.setJoinPartChecks(joinPartEvals.asJava)

    // If any join parts failed to produce schemas, return early
    if (rightPartsSchemaOptions.exists(_.isEmpty)) {
      joinEvalResults(join.metaData.getName) = (result, None)
      return (result, None)
    }

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
      leftSchema: SparkStructType,
      leftSchemaFormatted: Map[String, String]): (List[JoinPartEvalResult], List[Option[SparkStructType]]) = {
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

      val keySchemaCheck = gbKeySchema match {
        case Some(keySchema) =>
          EvalHelpers.checkKeySchema(leftSchemaFormatted, keySchema.asScala.toMap, part.rightToLeft)
        case None =>
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

  private def evalTableDependency(table: String): (EvalUnion, Option[SparkStructType]) = {
    println(s"Evaluating table dependency for: $table")
    println(s"Tables to join: ${tablesToJoin.keys.mkString(", ")}")
    println("===")
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
        // TODO if we support BigQuery source (not only SQ) we'll need to handle that here
        val schema = tableUtils.getSchemaFromTable(table)
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

  def evalStagingQueryConf(rootConfPath: String): StagingQueryEvalResult = {
    reset()
    assert(rootConfPath.contains("/staging_queries/"),
           s"Expected path to contain '/staging_queries/', but got: $rootConfPath")
    val stagingQuery = ThriftJsonCodec.fromJsonFile[StagingQuery](rootConfPath, check = false)
    evalStagingQuery(stagingQuery)._1
  }

  def evalGroupByConf(rootConfPath: String): GroupByEvalResult = {
    reset()
    assert(rootConfPath.contains("/group_bys/"), s"Expected path to contain '/group_bys/', but got: $rootConfPath")
    val groupBy = ThriftJsonCodec.fromJsonFile[GroupBy](rootConfPath, check = false)
    evalGroupBy(groupBy)._1
  }

  def evalJoinConf(rootConfPath: String): JoinEvalResult = {
    reset()
    assert(rootConfPath.contains("/joins/"), s"Expected path to contain '/joins/', but got: $rootConfPath")
    val join = ThriftJsonCodec.fromJsonFile[Join](rootConfPath, check = false)
    val result = evalJoin(join)._1
    print("===================")
    print(result)
    result
  }

}
