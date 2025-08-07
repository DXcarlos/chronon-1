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

package ai.chronon.spark.join

import ai.chronon.api
import ai.chronon.api.DataModel.EVENTS
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api._
import ai.chronon.api.planner.JoinPlanner
import ai.chronon.spark.Extensions._
import ai.chronon.spark.catalog.TableUtils
import com.google.gson.Gson
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{coalesce, col, udf}
import org.apache.spark.util.sketch.BloomFilter
import org.slf4j.{Logger, LoggerFactory}

import java.util
import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

object Utils {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    /** *
    * Util methods for join computation
    */

  def leftDf(joinConf: ai.chronon.api.Join,
             range: PartitionRange,
             tableUtils: TableUtils,
             allowEmpty: Boolean = false): Option[DataFrame] = {

    val timeProjection = if (joinConf.left.dataModel == EVENTS) {
      Seq(Constants.TimeColumn -> Option(joinConf.left.query).map(_.timeColumn).orNull)
    } else {
      Seq()
    }

    val effectiveLeftSpec = joinConf.left.query.partitionSpec(tableUtils.partitionSpec)
    val effectiveLeftRange = range.translate(effectiveLeftSpec)

    val partitionColumnOfLeft = effectiveLeftSpec.column

    val df = tableUtils.scanDf(joinConf.left.query,
                               joinConf.left.table,
                               Some((Map(partitionColumnOfLeft -> null) ++ timeProjection).toMap),
                               range = Some(effectiveLeftRange))

    val skewFilter = joinConf.skewFilter()
    val result = skewFilter
      .map(sf => {
        logger.info(s"left skew filter: $sf")
        df.filter(sf)
      })
      .getOrElse(df)

    if (!allowEmpty && result.isEmpty) {
      logger.info(s"Left side query produced 0 rows in range $effectiveLeftRange, and allowEmpty=false.")
      return None
    }

    Some(result.translatePartitionSpec(effectiveLeftSpec, tableUtils.partitionSpec))
  }


  /** *
    * join left and right dataframes, merging any shared columns if exists by the coalesce rule.
    * fails if there is any data type mismatch between shared columns.
    *
    * The order of output joined dataframe is:
    *   - all keys
    *   - all columns on left (incl. both shared and non-shared) in the original order of left
    *   - all columns on right that are NOT shared by left, in the original order of right
    */
  def coalescedJoin(leftDf: DataFrame, rightDf: DataFrame, keys: Seq[String], joinType: String = "left"): DataFrame = {
    leftDf.validateJoinKeys(rightDf, keys)
    val sharedColumns = rightDf.columns.intersect(leftDf.columns)
    sharedColumns.foreach { column =>
      val leftDataType = leftDf.schema(leftDf.schema.fieldIndex(column)).dataType
      val rightDataType = rightDf.schema(rightDf.schema.fieldIndex(column)).dataType
      assert(leftDataType == rightDataType,
             s"Column '$column' has mismatched data types - left type: $leftDataType vs. right type $rightDataType")
    }

    val joinedDf = leftDf.join(rightDf, keys.toSeq, joinType)

    // find columns that exist both on left and right that are not keys and coalesce them
    val selects = keys.map(col) ++
      leftDf.columns.flatMap { colName =>
        if (keys.contains(colName)) {
          None
        } else if (sharedColumns.contains(colName)) {
          Some(coalesce(leftDf(colName), rightDf(colName)).as(colName))
        } else {
          Some(leftDf(colName))
        }
      } ++
      rightDf.columns.flatMap { colName =>
        if (sharedColumns.contains(colName)) {
          None // already selected previously
        } else {
          Some(rightDf(colName))
        }
      }

    val finalDf = joinedDf.select(selects.toSeq: _*)
    finalDf
  }


  def skewFilter(keys: Option[Seq[String]] = None,
                 skewKeys: Option[Map[String, Seq[String]]],
                 leftKeyCols: Seq[String],
                 joiner: String = " OR "): Option[String] = {
    skewKeys.map { keysMap =>
      val result = keysMap
        .filterKeys(key =>
          keys.forall {
            _.contains(key)
          })
        .map { case (leftKey, values) =>
          assert(
            leftKeyCols.contains(leftKey),
            s"specified skew filter for $leftKey is not used as a key in any join part. " +
              s"Please specify key columns in skew filters: [${leftKeyCols.mkString(", ")}]"
          )
          generateSkewFilterSql(leftKey, values)
        }
        .filter(_.nonEmpty)
        .mkString(joiner)
      logger.info(s"Generated join left side skew filter:\n    $result")
      result
    }
  }

  private def generateSkewFilterSql(key: String, values: Seq[String]): String = {
    val nulls = Seq("null", "Null", "NULL")
    val nonNullFilters = Some(s"$key NOT IN (${values.filterNot(nulls.contains).mkString(", ")})")
    val nullFilters = if (values.exists(nulls.contains)) Some(s"$key IS NOT NULL") else None
    (nonNullFilters ++ nullFilters).mkString(" AND ")
  }

  def runSmallMode(tableUtils: TableUtils, leftDf: DataFrame): Boolean = {
    if (tableUtils.smallModelEnabled) {
      val thresholdCount = leftDf.limit(Some(tableUtils.smallModeNumRowsCutoff + 1).get).count()
      val result = thresholdCount <= tableUtils.smallModeNumRowsCutoff
      if (result) {
        logger.info(s"Counted $thresholdCount rows, running join in small mode.")
      } else {
        logger.info(
          s"Counted greater than ${tableUtils.smallModeNumRowsCutoff} rows, proceeding with normal computation.")
      }
      result
    } else {
      false
    }
  }

  def shiftDays(leftDataModel: DataModel, joinPart: JoinPart, leftRange: PartitionRange): PartitionRange = {

    //  left  | right  | acc
    // events | events | snapshot  => right part tables are not aligned - so scan by leftTimeRange
    // events | events | temporal  => already aligned - so scan by leftRange
    // events | entities | snapshot => right part tables are not aligned - so scan by leftTimeRange
    // events | entities | temporal => right part tables are aligned - so scan by leftRange
    // entities | entities | snapshot => right part tables are aligned - so scan by leftRange
    val rightRange = if (leftDataModel == EVENTS && joinPart.groupBy.inferredAccuracy == Accuracy.SNAPSHOT) {
      leftRange.shift(-1)
    } else {
      leftRange
    }
    rightRange
  }

  def computeLeftSourceTableName(join: api.Join)(implicit tableUtils: TableUtils): String = {
    new JoinPlanner(join)(tableUtils.partitionSpec).leftSourceNode.metaData.cleanName
  }

  def computeFullLeftSourceTableName(join: api.Join)(implicit tableUtils: TableUtils): String = {
    new JoinPlanner(join)(tableUtils.partitionSpec).leftSourceNode.metaData.outputTable
  }
}
