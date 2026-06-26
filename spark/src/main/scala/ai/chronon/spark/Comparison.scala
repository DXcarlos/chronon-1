/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.spark

import ai.chronon.online.Extensions.StructTypeOps
import com.google.gson.GsonBuilder
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.MapType
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util

object Comparison {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  private val NumericTolerance = 0.00001

  // used for comparison
  def sortedJson(m: Map[String, Any]): String = {
    if (m == null) return null
    val tm = new util.TreeMap[String, Any]()
    m.iterator.foreach { case (key, value) => tm.put(key, value) }
    val gson = new GsonBuilder()
      .serializeSpecialFloatingPointValues()
      .create()
    gson.toJson(tm)
  }

  private def numericValuesAlmostEqual(left: Any, right: Any): Boolean = {
    if (left == null || right == null) return left == right

    val leftDouble = left.asInstanceOf[Number].doubleValue()
    val rightDouble = right.asInstanceOf[Number].doubleValue()
    if (leftDouble.isNaN || rightDouble.isNaN) {
      leftDouble.isNaN && rightDouble.isNaN
    } else if (leftDouble.isInfinity || rightDouble.isInfinity) {
      leftDouble == rightDouble
    } else {
      math.abs(leftDouble - rightDouble) <= NumericTolerance
    }
  }

  def numericMapsAlmostEqual(left: Map[String, Any], right: Map[String, Any]): Boolean = {
    if (left == null || right == null) return left == right
    if (left.keySet != right.keySet) return false

    left.forall { case (key, leftValue) =>
      numericValuesAlmostEqual(leftValue, right(key))
    }
  }

  def stringifyMaps(df: DataFrame, preserveColumns: Set[String] = Set.empty): DataFrame = {
    try {
      df.sparkSession.udf.register("sorted_json", (m: Map[String, Any]) => sortedJson(m))
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val selects = for (field <- df.schema.fields) yield {
      if (field.dataType.isInstanceOf[MapType] && !preserveColumns.contains(field.name)) {
        s"sorted_json(${field.name}) as `${field.name}`"
      } else {
        s"${field.name} as `${field.name}`"
      }
    }
    df.selectExpr(selects: _*)
  }

  // Produces a "comparison" dataframe - given two dataframes that are supposed to have same data
  // The result contains the differing rows of the same key
  def sideBySide(a: DataFrame,
                 b: DataFrame,
                 keys: List[String],
                 aName: String = "a",
                 bName: String = "b"): DataFrame = {

    logger.info(
      s"""
        |====== side-by-side comparison ======
        |keys: $keys\na_schema:\n${a.schema.pretty}\nb_schema:\n${b.schema.pretty}
        |""".stripMargin
    )

    val approximateMapCols = (a.schema.fields ++ b.schema.fields)
      .filter(field => isApproximateMapType(field.dataType))
      .map(_.name)
      .toSet

    if (approximateMapCols.nonEmpty) {
      try {
        a.sparkSession.udf.register(
          "numeric_maps_almost_equal",
          (left: Map[String, Any], right: Map[String, Any]) => numericMapsAlmostEqual(left, right))
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }

    val prefixedExpectedDf = prefixColumnName(stringifyMaps(a, approximateMapCols), s"${aName}_")
    val prefixedOutputDf = prefixColumnName(stringifyMaps(b, approximateMapCols), s"${bName}_")

    val joinExpr = keys
      .map(key => prefixedExpectedDf(s"${aName}_$key") <=> prefixedOutputDf(s"${bName}_$key"))
      .reduce((col1, col2) => col1.and(col2))

    val joined = prefixedExpectedDf.join(
      prefixedOutputDf,
      joinExpr,
      joinType = "full_outer"
    )

    var finalDf = joined
    val comparisonColumns =
      a.schema.fieldNames.toSet.diff(keys.toSet).toList.sorted
    val colOrder =
      keys.map(key => { finalDf(s"${aName}_$key").as(key) }) ++
        comparisonColumns.flatMap { col =>
          List(finalDf(s"${aName}_$col"), finalDf(s"${bName}_$col"))
        }
    // double columns need to be compared approximately
    val doubleCols = a.schema.fields
      .filter(field =>
        field.dataType == DoubleType || field.dataType == FloatType || field.dataType.isInstanceOf[DecimalType])
      .map(_.name)
      .toSet
    finalDf = finalDf.select(colOrder: _*)
    val comparisonFilters = comparisonColumns
      .flatMap { col =>
        val left = s"${aName}_$col"
        val right = s"${bName}_$col"
        val compareExpression =
          if (approximateMapCols.contains(col)) {
            s"($left is NOT NULL) AND ($right is NOT NULL) and (NOT numeric_maps_almost_equal($left, $right))"
          } else if (doubleCols.contains(col)) {
            s"($left is NOT NULL) AND ($right is NOT NULL) and (abs($left - $right) > $NumericTolerance)"
          } else { s"($left <> $right)" }
        Seq(s"(($left IS NULL AND $right IS NOT NULL) OR ($right IS NULL AND $left IS NOT NULL) OR $compareExpression)")
      }
    logger.info(s"Using comparison filter:\n  ${comparisonFilters.mkString("\n  ")}")
    if (comparisonFilters.nonEmpty) {
      finalDf.filter(comparisonFilters.mkString(" or "))
    } else {
      // all rows are good
      finalDf.filter("false")
    }
  }

  private def prefixColumnName(df: DataFrame, prefix: String): DataFrame = {
    val renamedColumns = df.columns.map(c => { df(c).as(s"$prefix$c") })
    df.select(renamedColumns: _*)
  }

  private def isApproximateType(dataType: DataType): Boolean =
    dataType == DoubleType || dataType == FloatType || dataType.isInstanceOf[DecimalType]

  private def isApproximateMapType(dataType: DataType): Boolean =
    dataType match {
      case MapType(_, valueType, _) => isApproximateType(valueType)
      case _                        => false
    }
}
