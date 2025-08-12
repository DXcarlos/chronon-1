package ai.chronon.spark.local

import ai.chronon.api.Extensions.{
  AggregationOps,
  DerivationOps,
  ExternalPartOps,
  ExternalSourceOps,
  GroupByOps,
  JoinOps,
  SourceOps
}
import ai.chronon.api.{GroupBy, Join}
import ai.chronon.api.ScalaJavaConversions.{IterableOps, MapOps}
import ai.chronon.orchestration.{BaseEvalResult, CheckResult}
import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.types.{StringType, StructType}
import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.online.serde.SparkConversions
import org.apache.spark.sql.Row

import scala.collection.convert.ImplicitConversions.`collection asJava`
import scala.collection.immutable.Seq

case class GroupBySchemas(sourceSchema: StructType, keySchema: StructType)

object EvalHelpers {

  def getGroupBySourceSchema(groupBy: GroupBy, tableUtils: TableUtils): GroupBySchemas = {
    val sourceSchemas = groupBy.sources.toScala.map { source =>
      // Build query for this source
      val selects = Option(source.query.selects).map(_.toScala.toMap).getOrElse(Map.empty[String, String])
      val selectClause = if (selects.nonEmpty) {
        selects.map { case (alias, expr) => s"$expr AS $alias" }.mkString(", ")
      } else {
        "*"
      }

      val whereClause =
        Option(source.query.wheres).map(_.toScala.mkString(" AND ")).filter(_.nonEmpty).map("WHERE " + _).getOrElse("")
      val query = s"SELECT $selectClause FROM ${source.table} $whereClause LIMIT 0"

      val sourceDf = tableUtils.sql(query)
      sourceDf.schema
    }

    // Check that all source schemas are consistent
    val firstSchema = sourceSchemas.head
    val schemasConsistent = sourceSchemas.tail.forall { schema =>
      schema.fields.map(f => (f.name, f.dataType)) sameElements firstSchema.fields.map(f => (f.name, f.dataType))
    }

    if (!schemasConsistent) {
      throw new Exception(
        s"Inconsistent schemas across sources: ${sourceSchemas.map(_.fields.map(f => s"${f.name}:${f.dataType}").mkString(",")).mkString(" | ")}")
    }

    // Extract key schema from the source schema
    val keyColumns = groupBy.getKeyColumns.toScala
    val keyFields = firstSchema.fields.filter(f => keyColumns.contains(f.name))
    val keySchema = StructType(keyFields)

    GroupBySchemas(firstSchema, keySchema)
  }

  def getGroupByAggSchema(groupBy: GroupBy, inputSchema: StructType): StructType = {
    val aggregationParts = groupBy.aggregations.toScala.flatMap(_.unpack)
    val inputChrononSchema = SparkConversions.toChrononSchema(inputSchema)
    val rowAggregator = new RowAggregator(inputChrononSchema, aggregationParts.toSeq)

    // Get output schema from RowAggregator
    val outputChrononSchema = rowAggregator.outputSchema
    val outputSparkSchema = SparkConversions.fromChrononSchema(
      ai.chronon.api.StructType("", outputChrononSchema.map(tup => ai.chronon.api.StructField(tup._1, tup._2)))
    )

    outputSparkSchema
  }

  def getGroupByDerivationsSchema(groupBy: GroupBy,
                                  aggSchema: StructType,
                                  keySchema: StructType,
                                  tableUtils: TableUtils): StructType = {
    // Combine key, agg, and partition schemas
    val fullSchema = StructType(aggSchema.fields ++ keySchema.fields)
    val dummyDf = tableUtils.sparkSession.createDataFrame(
      tableUtils.sparkSession.sparkContext.parallelize(Seq.empty[Row]),
      fullSchema
    )

    // Apply derivations
    val finalOutputColumns = groupBy.derivationsScala.finalOutputColumn(dummyDf.columns).toSeq
    val derivedDf = dummyDf.select(finalOutputColumns: _*)

    // Extract just the derived columns (excluding keys and partition)
    val keyAndPartitionFields = keySchema.fields ++
      Seq(org.apache.spark.sql.types.StructField(tableUtils.partitionColumn, StringType))
    StructType(derivedDf.schema.fields.filterNot(keyAndPartitionFields.contains))
  }

  def getJoinLeftSchema(join: Join, tableUtils: TableUtils): StructType = {
    val leftSource = join.left
    // Build query for left source
    val selects = Option(leftSource.query.selects).map(_.toScala.toMap).getOrElse(Map.empty[String, String])
    val selectClause = if (selects.nonEmpty) {
      selects.map { case (alias, expr) => s"$expr AS $alias" }.mkString(", ")
    } else {
      "*"
    }

    val whereClause = Option(leftSource.query.wheres)
      .map(_.toScala.mkString(" AND "))
      .filter(_.nonEmpty)
      .map("WHERE " + _)
      .getOrElse("")
    val query = s"SELECT $selectClause FROM ${leftSource.table} $whereClause LIMIT 0"

    val sourceDf = tableUtils.sql(query)
    val schema = sourceDf.schema

    // Ensure the schema includes the partition column (added by spark engine)
    if (!schema.fieldNames.contains(tableUtils.partitionColumn)) {
      import org.apache.spark.sql.types._
      val partitionField = StructField(tableUtils.partitionColumn, StringType, nullable = true)
      StructType(schema.fields :+ partitionField)
    } else {
      schema
    }
  }

  def getJoinExternalPartsSchema(join: Join): Option[StructType] = {
    Option(join.onlineExternalParts).map { externalParts =>
      val fields = externalParts.toScala.flatMap { part =>
        // Only use value fields from the external part
        part.source.valueFields.map { field =>
          val fieldName = s"${part.fullName}_${field.name}"
          org.apache.spark.sql.types.StructField(
            fieldName,
            SparkConversions
              .fromChrononSchema(
                ai.chronon.api.StructType("", Array(ai.chronon.api.StructField(fieldName, field.fieldType)))
              )
              .fields
              .head
              .dataType
          )
        }
      }.toArray
      StructType(fields)
    }
  }

  def getJoinDerivationsSchema(join: Join,
                               leftSchema: StructType,
                               rightPartsSchema: StructType,
                               externalPartsSchemaOpt: Option[StructType],
                               tableUtils: TableUtils): StructType = {
    // Create the full input schema for derivations
    val leftSparkFields =
      leftSchema.fields ++ Seq(org.apache.spark.sql.types.StructField(tableUtils.partitionColumn, StringType))
    val rightSparkFields = rightPartsSchema.fields
    val externalPartsFields = externalPartsSchemaOpt.map(_.fields).getOrElse(Array.empty)

    val fullSparkSchema = StructType(leftSparkFields ++ rightSparkFields ++ externalPartsFields)

    // Create dummy DataFrame to compute derivation schema
    val dummyOutputDf = tableUtils.sparkSession.createDataFrame(
      tableUtils.sparkSession.sparkContext.parallelize(Seq.empty[Row]),
      fullSparkSchema
    )

    // Apply derivations to compute final output columns
    val finalOutputColumns = join.derivationsScala.finalOutputColumn(dummyOutputDf.columns).toSeq
    val derivedDummyOutputDf = dummyOutputDf.select(finalOutputColumns: _*)

    // Convert derived schema back to Spark format, excluding left and partition columns
    StructType(derivedDummyOutputDf.schema.filterNot(leftSparkFields.contains))
  }

  def checkKeySchema(left: Map[String, String],
                     right: Map[String, String],
                     keyMapping: Map[String, String]): BaseEvalResult = {
    val evalResult = new BaseEvalResult()

    // Key errors *might* be caught at compile time, however we could still miss issues
    // if there is "select *" behavior in the source config, or if there is a schema mismatch
    val keyErrors = keyMapping.flatMap {
      case (_, leftKey) if !left.contains(leftKey) =>
        Some(s"Left side of the join doesn't contain the key $leftKey. Available keys are [${left.keys.mkString(",")}]")
      case (rightKey, _) if !right.contains(rightKey) =>
        Some(
          s"Right side of the join doesn't contain the key $rightKey. Available keys are [${right.keys.mkString(",")}]")
      case (rightKey, leftKey) if left(leftKey) != right(rightKey) =>
        Some(
          s"Join key, '$leftKey', has mismatched data types - left type: ${left(leftKey)} vs. right type ${right(rightKey)}")
      case _ => None
    }

    if (keyErrors.nonEmpty) {
      evalResult.setCheckResult(CheckResult.FAILURE)
      evalResult.setMessage(keyErrors.mkString("\n"))
    } else {
      evalResult.setCheckResult(CheckResult.SUCCESS)
    }

    evalResult
  }

}
