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

package ai.chronon.spark.stats

import ai.chronon
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api._
import ai.chronon.online.OnlineDerivationUtil.timeFields
import ai.chronon.online.metrics.Metrics
import ai.chronon.online.{fetcher, _}
import ai.chronon.spark.AvroKvEncoder
import ai.chronon.spark.Extensions._
import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util

class ConsistencyJob(session: SparkSession, joinConf: Join, endDate: String) extends Serializable {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private val tblProperties: Map[String, String] = Option(joinConf.metaData.tableProperties)
    .map(_.toScala)
    .getOrElse(Map.empty[String, String])
  implicit val tableUtils: TableUtils = TableUtils(session)
  implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec

  private def publishTelemetry(consistencyMetrics: fetcher.DataMetrics): Unit = {

    /** Publish to telemetry the latest result.
      * Telemetry cannot have time manipulation hence we aggregate a match % per hours
      * Grab the mismatch sum and total count for every feature defined.
      * Push a gauge of match % per feature.
      */
    val topSeries = consistencyMetrics.series.sortBy(-_._1).take(24)
    // Compute the % mismatch per map for each time bucket per feature
    val featureMatches = topSeries.flatMap { case (_, map) =>
      // Filter only keys that are *_mismatch_sum and *_total_count
      val features = map.keys.collect {
        case k if k.endsWith("_mismatch_sum") || k.endsWith("_total_count") => k
      }

      // Group by feature prefix: everything before the last two underscores
      val featurePrefixes = features.map { k =>
        val parts = k.split("_")
        // join all except the last two segments
        parts.dropRight(2).mkString("_")
      }.toSet
      logger.info(s"Found ${featurePrefixes.size} features, example: ${featurePrefixes.seq.head}")
      featurePrefixes.flatMap { prefix =>
        val mismatchOpt = map.get(s"${prefix}_mismatch_sum").collect { case n: Number => n.doubleValue() }
        val totalOpt = map.get(s"${prefix}_total_count").collect { case n: Number => n.doubleValue() }

        (mismatchOpt, totalOpt) match {
          case (Some(mismatch), Some(total)) if total > 0 =>
            Some(prefix -> (1.0 - (mismatch / total)))
          case _ => None // ignore if total missing or 0
        }
      }
    }

    val overallMatches: Map[String, Double] =
      featureMatches
        .groupBy(_._1) // group by feature name
        .map { case (feature, values) =>
          val product = values.map(_._2).product
          feature -> product * 100
        }
    logger.info(s"Publishing match data for ${overallMatches.size} features")
    val context = Metrics.Context(Metrics.Environment.JoinOOC, joinConf)
    overallMatches.foreach { case (name, match_pct) =>
      logger.info(s"Publishing feature_consistency[${Metrics.Tag.Feature}=$name]: ${match_pct} %")
      context.gauge("feature_consistency", match_pct, Map(Metrics.Tag.Feature -> name))
    }
    logger.info("Sleeping 30 seconds to allow metrics to be collected...")
    Thread.sleep(30000)
  }

  // Replace join's left side with the logged table events to determine offline values of the aggregations.
  private def buildComparisonJoin(): Join = {
    logger.info("Building Join With left as logged")
    val copiedJoin = joinConf.deepCopy()
    val loggedSource: Source = new Source()
    val loggedEvents: EventSource = new EventSource()
    val query = new Query()
    val mapping = joinConf.leftKeyCols.map(k => k -> k)
    val selects = new util.HashMap[String, String]()
    mapping.foreach { case (key, value) => selects.put(key, value) }
    query.setSelects(selects)
    query.setTimeColumn(Constants.TimeColumn)
    query.setStartPartition(joinConf.left.query.startPartition)
    // apply sampling logic to reduce OOC offline compute overhead
    val wheres = if (joinConf.metaData.consistencySamplePercent < 100) {
      Seq(s"RAND() <= ${joinConf.metaData.consistencySamplePercent / 100}")
    } else {
      Seq()
    }
    query.setWheres(wheres.toJava)
    loggedEvents.setQuery(query)
    loggedEvents.setTable(joinConf.metaData.loggedTable)
    loggedSource.setEvents(loggedEvents)
    copiedJoin.setLeft(loggedSource)
    val newName = joinConf.metaData.comparisonConfName
    copiedJoin.metaData.setName(newName)
    // mark OOC tables as chronon_ooc_table
    if (!copiedJoin.metaData.isSetTableProperties) {
      copiedJoin.metaData.setTableProperties(new util.HashMap[String, String]())
    }
    copiedJoin.metaData.tableProperties.put(Constants.ChrononOOCTable, true.toString)
    copiedJoin
  }

  def buildComparisonTable(): Unit = {
    // migrate legacy configs without consistencySamplePercent param
    if (!joinConf.metaData.isSetConsistencySamplePercent) {
      logger.info("consistencySamplePercent is unset and will default to 100")
      joinConf.metaData.consistencySamplePercent = 100
    }
    logger.info(s"consistencySamplePercent is ${joinConf.metaData.consistencySamplePercent}")

    if (joinConf.metaData.consistencySamplePercent == 0) {
      val errorMsg = s"Exit ConsistencyJob because consistencySamplePercent = 0 for join conf ${joinConf.metaData.name}"
      logger.error(errorMsg)
      throw new IllegalArgumentException(errorMsg)
    }

    val unfilledRanges = tableUtils
      .unfilledRanges(joinConf.metaData.comparisonTable,
                      PartitionRange(null, endDate),
                      Some(Seq(joinConf.metaData.loggedTable)))
      .getOrElse(Seq.empty)
    if (unfilledRanges.isEmpty) return
    val join = new chronon.spark.Join(buildComparisonJoin(), unfilledRanges.last.end, TableUtils(session))
    logger.info("Starting compute Join for comparison table")
    val compareDf = join.computeJoin(Some(30))
    logger.info("======= side-by-side comparison schema =======")
    logger.info(compareDf.schema.pretty)
  }

  def buildConsistencyMetrics(): fetcher.DataMetrics = {
    if (joinConf.metaData.consistencySamplePercent == 0) {
      logger.info(s"Exit ConsistencyJob because consistencySamplePercent = 0 for join conf ${joinConf.metaData.name}")
      return fetcher.DataMetrics(Seq())
    }

    buildComparisonTable()
    logger.info("Determining Range between consistency table and comparison table")
    val unfilledRanges = tableUtils
      .unfilledRanges(joinConf.metaData.consistencyTable,
                      PartitionRange(null, endDate),
                      Some(Seq(joinConf.metaData.comparisonTable)))
      .getOrElse(Seq.empty)
    if (unfilledRanges.isEmpty) return null
    val allMetrics = unfilledRanges.map { unfilled =>
      val comparisonDf = tableUtils.scanDf(null, joinConf.metaData.comparisonTable, range = Some(unfilled))
      val loggedDf =
        tableUtils.scanDf(null, joinConf.metaData.loggedTable, range = Some(unfilled)).drop(Constants.SchemaHash)
      // there could be external columns that are logged during online env, therefore they could not be used for computing OOC
      val loggedDfNoExternalCols = loggedDf.select(comparisonDf.columns.map(org.apache.spark.sql.functions.col): _*)
      logger.info("Starting compare job for stats")
      val joinKeys = if (joinConf.isSetRowIds) {
        joinConf.rowIds.toScala
      } else {
        timeFields.map(_.name).toList ++ joinConf.leftKeyCols
      }
      logger.info(s"Using ${joinKeys.mkString("[", ",", "]")} as join keys between log and backfill.")
      val (_, metricsFlatDf, metrics) =
        CompareBaseJob.compare(comparisonDf,
                               loggedDfNoExternalCols,
                               keys = joinKeys,
                               tableUtils,
                               name = joinConf.metaData.name)
      logger.info("Saving output.")
      val outputDf = metricsFlatDf.withTimeBasedColumn("ds")
      logger.info(s"output schema ${outputDf.schema.fields.map(sb => (sb.name, sb.dataType)).toMap.mkString("\n - ")}")
      outputDf.save(joinConf.metaData.consistencyTable, tableProperties = tblProperties, autoExpand = true)

      implicit val sparkSession: SparkSession = session
      val keyColumns = Seq("JoinPath")
      val valueColumns = metricsFlatDf.columns.filterNot(c => c == "JoinPath" || c == Constants.TimeColumn).toSeq
      AvroKvEncoder
        .encodeTimed(metricsFlatDf, keyColumns, valueColumns, storeSchemasPrefix = Some(joinConf.metaData.name))
        .withTimeBasedColumn(tableUtils.partitionColumn)
        .save(joinConf.metaData.consistencyUploadTable, tblProperties)
      metrics
    }
    val consistencyMetrics = fetcher.DataMetrics(allMetrics.flatMap(_.series))
    publishTelemetry(consistencyMetrics)
    consistencyMetrics
  }
}
