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

import ai.chronon.aggregator.windowing.{
  BatchIr,
  FinalBatchIr,
  FiveMinuteResolution,
  Resolution,
  SawtoothOnlineAggregator
}
import ai.chronon.aggregator.windowing.{FinalBatchIr, FiveMinuteResolution, Resolution, SawtoothOnlineAggregator}
import ai.chronon.api
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.api.Accuracy
import ai.chronon.api.Constants
import ai.chronon.api.DataModel
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.Extensions.SourceOps
import ai.chronon.api.GroupByServingInfo
import ai.chronon.api.PartitionSpec
import ai.chronon.api.QueryUtils
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.ThriftJsonCodec
import ai.chronon.online.Extensions.ChrononStructTypeOps
import ai.chronon.online.GroupByServingInfoParsed
import ai.chronon.api.PartitionRange
import ai.chronon.online.serde.SparkConversions
import ai.chronon.online.metrics.Metrics
import ai.chronon.spark.Extensions._
import ai.chronon.spark.submission.SparkSessionBuilder
import org.apache.spark.SparkEnv
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.not
import org.apache.spark.sql.types
import org.apache.spark.sql.types.{StructField, StructType}
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.util.Try

class GroupByUpload(endPartition: String, groupBy: GroupBy) extends Serializable {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  implicit val sparkSession: SparkSession = groupBy.sparkSession
  private val tableUtils: TableUtils = TableUtils(sparkSession)
  implicit private val partitionSpec: PartitionSpec = tableUtils.partitionSpec

  private def snapshotAccuracyNullCountCheck(rdd: RDD[(Array[Any], Array[Any])],
                                             fields: Array[StructField]): mutable.Map[String, Long] = {
    rdd.cache()

    val nullCounts = rdd
      .treeAggregate(mutable.HashMap.empty[String, Long])(
        seqOp = { case (counterMap, (_, values)) =>
          for (i <- values.indices) {
            if (values(i) == null) {
              val field = fields(i)
              val key = field.name
              counterMap.update(key, counterMap.getOrElse(key, 0L) + 1L)
            }
          }
          counterMap
        },
        combOp = { (map1, map2) =>
          map2.foreach { case (key, count) =>
            map1.update(key, map1.getOrElse(key, 0L) + count)
          }
          map1
        }
      )

    nullCounts
  }

  private def fromBase(rdd: RDD[(Array[Any], Array[Any])]): KvRdd = {

    val nullCounts = snapshotAccuracyNullCountCheck(rdd, groupBy.postAggSchema.fields)

    val pairRdd = rdd.map { case (keyAndDs, values) => keyAndDs.init -> values }

    KvRdd(pairRdd, groupBy.keySchema, groupBy.postAggSchema, nullCounts.toMap)
  }

  def snapshotEntities: KvRdd = {
    if (groupBy.aggregations == null || groupBy.aggregations.isEmpty) {

      val keySchema = groupBy.keySchema
      val keyBuilder = FastHashing.generateKeyBuilder(keySchema.fieldNames, groupBy.inputDf.schema)

      val valueSchema = groupBy.preAggSchema
      val valuesIndices = valueSchema.fieldNames.map(groupBy.inputDf.schema.fieldIndex)

      val rdd = groupBy.inputDf.rdd
        .map { row =>
          keyBuilder(row).data -> valuesIndices.map(row.get)
        }

      val nullCounts = snapshotAccuracyNullCountCheck(rdd, groupBy.preAggSchema.fields)

      logger.info(s"""
           |pre-agg upload:
           |  input schema: ${groupBy.inputDf.schema.catalogString}
           |    key schema: ${groupBy.keySchema.catalogString}
           |  value schema: ${groupBy.preAggSchema.catalogString}
           |""".stripMargin)

      KvRdd(rdd, groupBy.keySchema, groupBy.preAggSchema, nullCounts.toMap)

    } else {
      fromBase(groupBy.snapshotEntitiesBase)
    }
  }

  def snapshotEvents: KvRdd =
    fromBase(groupBy.snapshotEventsBase(PartitionRange(endPartition, endPartition)))

  // Shared between events and mutations (temporal entities).
  def temporalEvents(resolution: Resolution = FiveMinuteResolution): KvRdd = {
    val endTs = tableUtils.partitionSpec.epochMillis(endPartition)
    logger.info(s"TemporalEvents upload end ts: $endTs")

    val sawtoothOnlineAggregator = new SawtoothOnlineAggregator(
      endTs,
      groupBy.aggregations,
      SparkConversions.toChrononSchema(groupBy.inputDf.schema),
      resolution)

    val irSchema = SparkConversions.fromChrononSchema(sawtoothOnlineAggregator.batchIrSchema)
    val keyBuilder = FastHashing.generateKeyBuilder(groupBy.keyColumns.toArray, groupBy.inputDf.schema)

    val batchIrElementSize = SparkEnv.get.serializer
      .newInstance()
      .serialize(sawtoothOnlineAggregator.init)
      .capacity()

    logger.info(s"""
        |BatchIR Element Size: $batchIrElementSize
        |""".stripMargin)

    val shouldCombine = tableUtils.sparkSession.conf.get("spark.chronon.group_by.upload.combine", "true").toBoolean

    val outputRddKeyed = groupBy.inputDf.rdd
      .keyBy(keyBuilder)

    val outputRddAggregated = if (!shouldCombine) {
      outputRddKeyed
        .mapValues { row =>
          val result: Either[Row, BatchIr] = Left(row)
          result
        }
        .reduceByKey { (value1: Either[Row, BatchIr], value2: Either[Row, BatchIr]) =>
          val resultIr: BatchIr = (value1, value2) match {
            case (Left(row1), Left(row2)) =>
              val batchIr = sawtoothOnlineAggregator.init
              val batchIr1 =
                sawtoothOnlineAggregator.update(batchIr, SparkConversions.toChrononRow(row1, groupBy.tsIndex))
              sawtoothOnlineAggregator.update(batchIr1, SparkConversions.toChrononRow(row2, groupBy.tsIndex))
            case (Right(ir1), Left(row2)) =>
              sawtoothOnlineAggregator.update(ir1, SparkConversions.toChrononRow(row2, groupBy.tsIndex))
            case (Left(row1), Right(ir2)) =>
              sawtoothOnlineAggregator.update(ir2, SparkConversions.toChrononRow(row1, groupBy.tsIndex))
            case (Right(ir1), Right(ir2)) =>
              sawtoothOnlineAggregator.merge(ir1, ir2)
          }
          Right(resultIr)
        }
        .mapValues { resultIr: Either[Row, BatchIr] =>
          resultIr match {
            case Left(row1) =>
              val batchIr = sawtoothOnlineAggregator.init
              sawtoothOnlineAggregator.update(batchIr, SparkConversions.toChrononRow(row1, groupBy.tsIndex))
            case Right(batchIr) => batchIr
          }
        }
    } else {
      outputRddKeyed.aggregateByKey(sawtoothOnlineAggregator.init)( // shuffle point
        seqOp = { case (batchIr, row) =>
          sawtoothOnlineAggregator.update(batchIr, SparkConversions.toChrononRow(row, groupBy.tsIndex))
        },
        combOp = sawtoothOnlineAggregator.merge
      )
    }

    val outputAggregatedNormalized = outputRddAggregated
      .mapValues(sawtoothOnlineAggregator.normalizeBatchIr)

    // Going to produce nullCounts and also later .save
    outputAggregatedNormalized.cache()

    val nullCounts = outputAggregatedNormalized
      .treeAggregate(mutable.HashMap.empty[String, Long])(
        seqOp = { case (counterMap, (_, batchIr)) =>
          sawtoothOnlineAggregator.updateNullCounts(batchIr, counterMap)
          counterMap
        },
        combOp = { (map1, map2) =>
          map2.foreach { case (key, count) =>
            map1.update(key, map1.getOrElse(key, 0L) + count)
          }
          map1
        }
      )

    val outputRdd = outputAggregatedNormalized
      .map { case (keyWithHash: KeyWithHash, finalBatchIr: FinalBatchIr) =>
        val irArray = new Array[Any](2)
        irArray.update(0, finalBatchIr.collapsed)
        irArray.update(1, finalBatchIr.tailHops)
        keyWithHash.data -> irArray
      }

    KvRdd(outputRdd, groupBy.keySchema, irSchema, nullCounts.toMap)
  }

}

object GroupByUpload {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  // TODO - remove this if spark streaming can't reach hive tables
  private def buildServingInfo(groupByConf: api.GroupBy,
                               session: SparkSession,
                               endDs: String): GroupByServingInfoParsed = {
    val groupByServingInfo = new GroupByServingInfo()
    val tableUtils: TableUtils = TableUtils(session)
    implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec
    val nextDay = tableUtils.partitionSpec.after(endDs)

    val groupBy = ai.chronon.spark.GroupBy
      .from(groupByConf, PartitionRange(endDs, endDs), TableUtils(session), computeDependency = false)

    groupByServingInfo.setBatchEndDate(nextDay)
    groupByServingInfo.setGroupBy(groupByConf)
    groupByServingInfo.setKeyAvroSchema(groupBy.keySchema.toAvroSchema("Key").toString(true))
    groupByServingInfo.setSelectedAvroSchema(groupBy.preAggSchema.toAvroSchema("Value").toString(true))
    groupByServingInfo.setDateFormat(tableUtils.partitionFormat)

    if (groupByConf.streamingSource.isDefined) {
      val streamingSource = groupByConf.streamingSource.get

      // TODO: move this to SourceOps
      @tailrec
      def getInfo(source: api.Source): (String, api.Query, Boolean) = {
        if (source.isSetEvents) {
          (source.getEvents.getTable, source.getEvents.getQuery, false)
        } else if (source.isSetEntities) {
          (source.getEntities.getSnapshotTable, source.getEntities.getQuery, true)
        } else {
          val left = source.getJoinSource.getJoin.getLeft
          getInfo(left)
        }
      }

      val (rootTable, query, _) = getInfo(streamingSource)
      val fullInputSchema = tableUtils.getSchemaFromTable(rootTable)
      val inputSchema: types.StructType =
        if (Option(query.selects).isEmpty) fullInputSchema
        else {
          val selects = query.selects.toScala ++ Map(Constants.TimeColumn -> query.timeColumn)

          /** We don't need to actually use the real table here since we're just trying to extract columns
            * from a static query. We use a dummy table here since users with bigquery tables would have three part
            * names instead of a two part typical spark table name
            */
          val streamingQuery =
            QueryUtils.build(selects, "default.dummy_table", query.wheres.toScala)
          val reqColumns = tableUtils.getColumnsFromQuery(streamingQuery)
          types.StructType(fullInputSchema.filter(col => reqColumns.contains(col.name)))
        }
      groupByServingInfo.setInputAvroSchema(inputSchema.toAvroSchema(name = "Input").toString(true))
    } else {
      logger.info("Not setting InputAvroSchema to GroupByServingInfo as there is no streaming source defined.")
    }

    val result = new GroupByServingInfoParsed(groupByServingInfo)
    val firstSource = groupByConf.sources.get(0)
    logger.info(s"""
        |Built GroupByServingInfo for ${groupByConf.metaData.name}:
        |table: ${firstSource.table} / data-model: ${firstSource.dataModel}
        |     keySchema: ${Try(result.keyChrononSchema.catalogString)}
        |   valueSchema: ${Try(result.valueChrononSchema.catalogString)}
        |mutationSchema: ${Try(result.mutationChrononSchema.catalogString)}
        |   inputSchema: ${Try(result.inputChrononSchema.catalogString)}
        |selectedSchema: ${Try(result.selectedChrononSchema.catalogString)}
        |  streamSchema: ${Try(result.streamChrononSchema.catalogString)}
        |""".stripMargin)
    result
  }

  private[spark] def generateKvRdd(groupByConf: api.GroupBy,
                                   endDs: String,
                                   showDf: Boolean = false,
                                   tableUtils: TableUtils,
                                   maybeContext: Option[Metrics.Context] = None) = {
    implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec
    Option(groupByConf.setups).foreach(_.foreach(tableUtils.sql))
    // add 1 day to the batch end time to reflect data [ds 00:00:00.000, ds + 1 00:00:00.000)
    val batchEndDate = partitionSpec.after(endDs)
    // for snapshot accuracy - we don't need to scan mutations
    lazy val groupBy =
      GroupBy.from(groupByConf, PartitionRange(endDs, endDs), tableUtils, computeDependency = true, showDf = showDf)
    lazy val groupByUpload = new GroupByUpload(endDs, groupBy)
    // for temporal accuracy - we don't need to scan mutations for upload
    // when endDs = xxxx-01-02 the timestamp from airflow is more than (xxxx-01-03 00:00:00)
    // we wait for event partitions of (xxxx-01-02) which contain data until (xxxx-01-02 23:59:59.999)
    lazy val shiftedGroupBy =
      GroupBy.from(groupByConf,
                   PartitionRange(endDs, endDs).shift(1),
                   tableUtils,
                   computeDependency = true,
                   showDf = showDf)
    lazy val shiftedGroupByUpload = new GroupByUpload(batchEndDate, shiftedGroupBy)
    // for mutations I need the snapshot from the previous day, but a batch end date of ds +1
    lazy val otherGroupByUpload = new GroupByUpload(batchEndDate, groupBy)

    logger.info(s"""
                   |GroupBy upload for: ${groupByConf.metaData.team}.${groupByConf.metaData.name}
                   |Accuracy: ${groupByConf.inferredAccuracy}
                   |Data Model: ${groupByConf.dataModel}
                   |""".stripMargin)

    val result = (groupByConf.inferredAccuracy, groupByConf.dataModel) match {
      case (Accuracy.SNAPSHOT, DataModel.EVENTS)   => groupByUpload.snapshotEvents
      case (Accuracy.SNAPSHOT, DataModel.ENTITIES) => groupByUpload.snapshotEntities
      case (Accuracy.TEMPORAL, DataModel.EVENTS)   => shiftedGroupByUpload.temporalEvents()
      case (Accuracy.TEMPORAL, DataModel.ENTITIES) => otherGroupByUpload.temporalEvents()
    }

    // Emit null count map metrics
    if (maybeContext.isDefined) {
      logger.info(s"Emitting data quality metrics for ${result.nullCounts.keys.mkString("")} ")
      result.nullCounts.foreach({ case (field, count) =>
        maybeContext.get.gauge(s"NullCount.$field.$endDs", count)
      })
      Thread.sleep(Constants.ScrapeWaitSeconds.seconds.toMillis)
    }

    result
  }

  def run(groupByConf: api.GroupBy,
          endDs: String,
          tableUtilsOpt: Option[TableUtils] = None,
          showDf: Boolean = false,
          jsonPercent: Int = 1): Unit = {
    import ai.chronon.spark.submission.SparkSessionBuilder
    val tableUtils: TableUtils =
      tableUtilsOpt.getOrElse(
        TableUtils(
          SparkSessionBuilder
            .build(s"groupBy_${groupByConf.metaData.name}_upload")))
    val context = Metrics.Context(Metrics.Environment.GroupByUpload, groupByConf)
    val startTs = System.currentTimeMillis()
    val kvRdd = generateKvRdd(groupByConf = groupByConf,
                              endDs = endDs,
                              showDf = showDf,
                              tableUtils = tableUtils,
                              maybeContext = Option(context))

    val kvDf = kvRdd.toAvroDf(jsonPercent = jsonPercent)

    if (showDf) {
      kvRdd.toFlatDf.prettyPrint()
    }

    val groupByServingInfo = buildServingInfo(groupByConf, session = tableUtils.sparkSession, endDs).groupByServingInfo

    val metaRows = Seq(
      Row(
        Constants.GroupByServingInfoKey.getBytes(Constants.UTF8),
        ThriftJsonCodec.toJsonStr(groupByServingInfo).getBytes(Constants.UTF8),
        Constants.GroupByServingInfoKey,
        ThriftJsonCodec.toJsonStr(groupByServingInfo)
      ))
    val metaRdd = tableUtils.sparkSession.sparkContext.parallelize(metaRows.toSeq)
    val metaDf = tableUtils.sparkSession.createDataFrame(metaRdd, kvDf.schema)

    kvDf
      .union(metaDf)
      .withColumn("ds", lit(endDs))
      .save(groupByConf.metaData.uploadTable, groupByConf.metaData.tableProps, partitionColumns = List("ds"))

    // unpersist RDD
    kvRdd.data.unpersist()

    val kvDfReloaded = tableUtils
      .loadTable(groupByConf.metaData.uploadTable)
      .where(not(col("key_json").eqNullSafe(Constants.GroupByServingInfoKey)))

    val metricRow =
      kvDfReloaded.selectExpr("sum(bit_length(key_bytes))/8", "sum(bit_length(value_bytes))/8", "count(*)").collect()

    if (metricRow.length > 0 && metricRow(0).getLong(2) > 0) {
      context.gauge(Metrics.Name.KeyBytes, metricRow(0).getDouble(0).toLong)
      context.gauge(Metrics.Name.ValueBytes, metricRow(0).getDouble(1).toLong)
      context.gauge(Metrics.Name.RowCount, metricRow(0).getLong(2))
    } else {
      throw new RuntimeException("GroupBy upload resulted in zero rows.")
    }

    val jobDuration = (System.currentTimeMillis() - startTs) / 1000
    context.gauge(Metrics.Name.LatencyMinutes, (jobDuration / 60))
    logger.info(s"GroupBy upload completed in $jobDuration seconds")
  }
}
