package ai.chronon.spark.batch

import ai.chronon.api.DataModel.{ENTITIES, EVENTS}
import ai.chronon.api.Extensions.{DateRangeOps, DerivationOps, GroupByOps, JoinPartOps, MetadataOps}
import ai.chronon.api.PartitionRange.toTimeRange
import ai.chronon.api._
import ai.chronon.online.metrics.Metrics
import ai.chronon.planner.JoinPartNode
import ai.chronon.spark.Extensions._
import ai.chronon.spark.JoinUtils.coalescedJoin
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.join.UnionJoin
import ai.chronon.spark.{GroupBy, JoinUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.util.sketch.BloomFilter
import org.slf4j.{Logger, LoggerFactory}

import java.util
import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

case class JoinPartJobContext(leftDf: Option[DfWithStats],
                              joinLevelBloomMapOpt: Option[util.Map[String, BloomFilter]],
                              tableProps: Map[String, String],
                              runSmallMode: Boolean)

class JoinPartJob(node: JoinPartNode, metaData: MetaData, range: DateRange, showDf: Boolean = false)(implicit
    tableUtils: TableUtils) {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec

  private val leftTable = node.leftSourceTable
  private val joinPart = node.joinPart
  private val dateRange = range.toPartitionRange
  private val skewKeys: Option[Map[String, Seq[String]]] = Option(node.skewKeys).map { skewKeys =>
    skewKeys.asScala.map { case (k, v) => k -> v.asScala.toSeq }.toMap
  }

  def run(context: Option[JoinPartJobContext] = None): Option[DataFrame] = {

    logger.info(s"Running join part job for ${joinPart.groupBy.metaData.name} on range $dateRange")

    val jobContext = context.getOrElse {
      // LeftTable is already computed by SourceJob, no need to apply query/filters/etc
      val entityCols = joinPart.rightToLeft.keys.toArray
      val additionalCols = Seq(tableUtils.partitionColumn, Constants.RowIDColumn)
      val timeCol = node.leftDataModel match {
        case ENTITIES => None
        case EVENTS   => Some(Constants.TimeColumn)
      }

      val relevantLeftCols = entityCols ++ additionalCols ++ timeCol

      val query = Builders.Query(selects = relevantLeftCols.map(t => t -> t).toMap)
      val cachedLeftDf = tableUtils.scanDf(query = query, leftTable, range = Some(dateRange))

      val runSmallMode = JoinUtils.runSmallMode(tableUtils, cachedLeftDf)

      val leftWithStats = cachedLeftDf.withStats

      val joinLevelBloomMapOpt =
        JoinUtils.genBloomFilterIfNeeded(joinPart, node.leftDataModel, dateRange, None)

      JoinPartJobContext(Option(leftWithStats),
                         joinLevelBloomMapOpt,
                         Option(metaData.tableProps).getOrElse(Map.empty[String, String]),
                         runSmallMode)
    }

    // TODO: fix left df and left time range, bloom filter, small mode args
    computeRightTable(
      jobContext,
      joinPart,
      dateRange,
      metaData.outputTable
    )
  }

  private def computeRightTable(jobContext: JoinPartJobContext,
                                joinPart: JoinPart,
                                leftRange: PartitionRange, // missing left partitions
                                partTable: String): Option[DataFrame] = {

    // val partMetrics = Metrics.Context(metrics, joinPart) -- TODO is this metrics context sufficient, or should we pass thru for monolith join?
    val partMetrics = Metrics.Context(Metrics.Environment.JoinOffline, joinPart.groupBy)

    val rightRange = JoinUtils.shiftDays(node.leftDataModel, joinPart, leftRange)

    // Can kill the option after we deprecate monolith join job
    jobContext.leftDf.foreach { leftDf =>
      try {

        val start = System.currentTimeMillis()
        val prunedLeft = leftDf.prunePartitions(leftRange) // We can kill this after we deprecate monolith join job
        val filledDf =
          computeJoinPart(prunedLeft, joinPart, jobContext.joinLevelBloomMapOpt, skipBloom = jobContext.runSmallMode)

        // Cache join part data into intermediate table
        if (filledDf.isDefined) {
          logger.info(s"Writing to join part table: $partTable for partition range $rightRange")
          // Apply bucketing on row ID column if it exists in the DataFrame
          filledDf.get.save(partTable, tableProperties = jobContext.tableProps.toMap, bucketByRowId = true)
        } else {
          logger.info(s"Skipping $partTable because no data in computed joinPart.")
        }

        val elapsedMins = (System.currentTimeMillis() - start) / 60000
        partMetrics.gauge(Metrics.Name.LatencyMinutes, elapsedMins)
        logger.info(s"Wrote to join part table: $partTable in $elapsedMins minutes")
      } catch {
        case e: Exception =>
          logger.error(s"Error while processing groupBy: ${joinPart.groupBy.getMetaData.getName}")
          throw e
      }
    }

    if (tableUtils.tableReachable(partTable)) {
      Some(tableUtils.scanDf(query = null, partTable, range = Some(rightRange)))
    } else {
      // Happens when everything is handled by bootstrap
      None
    }
  }

  private def computeJoinPart(leftDfWithStats: Option[DfWithStats],
                              joinPart: JoinPart,
                              joinLevelBloomMapOpt: Option[util.Map[String, BloomFilter]],
                              skipBloom: Boolean): Option[DataFrame] = {

    if (leftDfWithStats.isEmpty) {
      // happens when all rows are already filled by bootstrap tables
      logger.info(s"\nBackfill is NOT required for ${joinPart.groupBy.metaData.name} since all rows are bootstrapped.")
      return None
    }

    val statsDf = leftDfWithStats.get

    logger.info(s"\nBackfill is required for ${joinPart.groupBy.metaData.name}")
    val rightBloomMap = if (skipBloom) {
      None
    } else {
      JoinUtils.genBloomFilterIfNeeded(joinPart, node.leftDataModel, dateRange, joinLevelBloomMapOpt)
    }

    val rightSkewFilter = JoinUtils.partSkewFilter(joinPart, skewKeys)

    def genGroupBy(partitionRange: PartitionRange) =
      GroupBy.from(joinPart.groupBy,
                   partitionRange,
                   tableUtils,
                   computeDependency = true,
                   rightBloomMap,
                   rightSkewFilter,
                   showDf = showDf)

    // all lazy vals - so evaluated only when needed by each case.
    lazy val partitionRangeGroupBy = genGroupBy(dateRange)

    lazy val unfilledPartitionRange = if (tableUtils.checkLeftTimeRange) {
      val timeRange = statsDf.timeRange
      logger.info(s"left unfilled time range checked to be: $timeRange")
      timeRange.toPartitionRange
    } else {
      logger.info(s"Not checking time range, but inferring it from partition range: $dateRange")
      dateRange
    }

    val leftSkewFilter =
      JoinUtils.skewFilter(Option(joinPart.rightToLeft.values.toSeq), skewKeys, joinPart.rightToLeft.values.toSeq)
    // this is the second time we apply skew filter - but this filters only on the keys
    // relevant for this join part.
    lazy val skewFilteredLeft = leftSkewFilter
      .map { sf =>
        val filtered = statsDf.df.filter(sf)
        logger.info(s"""Skew filtering left-df for
                       |GroupBy: ${joinPart.groupBy.metaData.name}
                       |filterClause: $sf
                       |""".stripMargin)
        filtered
      }
      .getOrElse(statsDf.df)

    /*
      For the corner case when the values of the key mapping also exist in the keys, for example:
      Map(user -> user_name, user_name -> user)
      the below logic will first rename the conflicted column with some random suffix and update the rename map
     */
    lazy val renamedLeftRawDf = {
      val columns = skewFilteredLeft.columns.flatMap { column =>
        if (joinPart.leftToRight.contains(column)) {
          Some(col(column).as(joinPart.leftToRight(column)))
        } else if (joinPart.rightToLeft.contains(column)) {
          None
        } else {
          Some(col(column))
        }
      }
      skewFilteredLeft.select(columns: _*)
    }

    lazy val shiftedPartitionRange = unfilledPartitionRange.shift(-1)

    val renamedLeftDf = renamedLeftRawDf.select(renamedLeftRawDf.columns.map {
      case c if c == tableUtils.partitionColumn =>
        date_format(renamedLeftRawDf.col(c), tableUtils.partitionFormat).as(c)
      case c => renamedLeftRawDf.col(c)
    }.toList: _*)

    // When we implement versioning on JoinPartJob, we can modify this to also include the reused columns
    val colsToJoinFromLeft = Seq(Constants.RowIDColumn)

    // RightDF is the joinPart data, shouldJoinToLeft indicates whether we need to join it back to the left to extract
    // additional `colsToJoinFromLeft` or not. For some compute modes, the relevant columns can be "passed through" computation
    // And we don't need to join them back to the leftDf.
    val (rightDf, shouldJoinToLeft) =
      (node.leftDataModel, joinPart.groupBy.dataModel, joinPart.groupBy.inferredAccuracy) match {
        case (ENTITIES, EVENTS, _)   => (partitionRangeGroupBy.snapshotEvents(dateRange), true)
        case (ENTITIES, ENTITIES, _) => (partitionRangeGroupBy.snapshotEntities, true)
        case (EVENTS, EVENTS, Accuracy.SNAPSHOT) =>
          (genGroupBy(shiftedPartitionRange).snapshotEvents(shiftedPartitionRange), true)
        case (EVENTS, EVENTS, Accuracy.TEMPORAL) =>
          val skewFreeMode = tableUtils.sparkSession.conf
            .get("spark.chronon.join.backfill.mode.skewFree", "false")
            .toBoolean

          if (skewFreeMode) {
            // Use UnionJoin for skewFree mode - it will handle column selection internally
            logger.info(s"Using UnionJoin for TEMPORAL events join part: ${joinPart.groupBy.metaData.name}")
            (UnionJoin.computeJoinPart(renamedLeftDf, joinPart, unfilledPartitionRange, produceFinalJoinOutput = false),
             false)
          } else {
            // Use traditional temporalEvents approach
            // TODO: Modify temporalEvents to include row ID column on output, then we can return false for shouldJoinToLeft
            (genGroupBy(unfilledPartitionRange).temporalEvents(renamedLeftDf,
                                                               Some(toTimeRange(unfilledPartitionRange))),
             true)
          }

        case (EVENTS, ENTITIES, Accuracy.SNAPSHOT) => (genGroupBy(shiftedPartitionRange).snapshotEntities, true)

        case (EVENTS, ENTITIES, Accuracy.TEMPORAL) =>
          // Snapshots and mutations are partitioned with ds holding data between <ds 00:00> and ds <23:59>.
          // TODO: Modify temporalEntities to include row ID column on output, then we can return false for shouldJoinToLeft
          (genGroupBy(shiftedPartitionRange).temporalEntities(renamedLeftDf), true)
      }

    val rightDfWithAllCols = if (shouldJoinToLeft) {
      joinWithLeft(renamedLeftDf, rightDf, colsToJoinFromLeft)
    } else {
      rightDf
    }

    val rightDfWithDerivations = if (joinPart.groupBy.hasDerivations) {

      val finalOutputColumns = joinPart.groupBy.derivationsScala.finalOutputColumn(
        rightDfWithAllCols.columns,
        ensureKeys = joinPart.groupBy.keys(tableUtils.partitionColumn) ++ Seq(Constants.RowIDColumn)
      )

      val result = rightDfWithAllCols.select(finalOutputColumns: _*)
      result

    } else {
      rightDfWithAllCols
    }

    if (showDf) {
      logger.info(s"printing results for joinPart: ${joinPart.groupBy.metaData.name}")
      rightDfWithDerivations.prettyPrint()
    }

    Some(rightDfWithDerivations)
  }

  def joinWithLeft(leftDf: DataFrame,
                   rightDf: DataFrame,
                   additionalLeftColumnsToInclude: Seq[String] = Seq.empty): DataFrame = {

    // This join logic does not do any bucket hinting because it is on pre-bucketed data.
    // The output of this will get bucketed and written, which MergeJob will benefit from.
    val partLeftKeys = joinPart.rightToLeft.keys.toArray

    // compute join keys, besides the groupBy keys -  like ds, ts etc.,
    val additionalKeys: Seq[String] = {
      if (node.leftDataModel == ENTITIES) {
        Seq(tableUtils.partitionColumn)
      } else if (joinPart.groupBy.inferredAccuracy == Accuracy.TEMPORAL) {
        Seq(Constants.TimeColumn, tableUtils.partitionColumn)
      } else { // left-events + snapshot => join-key = ds_of_left_ts
        Seq(Constants.TimePartitionColumn)
      }
    }

    val keys = partLeftKeys ++ additionalKeys

    val allLeftCols = keys ++ additionalLeftColumnsToInclude
    // Filter down left to only the columns that we want to keep on the joined output for the Joinpart
    val leftDfWithRelevantCols =
      if (node.leftDataModel == DataModel.EVENTS && !leftDf.columns.contains(Constants.TimePartitionColumn)) {
        leftDf.withTimeBasedColumn(Constants.TimePartitionColumn)
      } else {
        leftDf
      }.select(allLeftCols.map(column): _*)

    // adjust join keys
    val joinableRightDf = if (additionalKeys.contains(Constants.TimePartitionColumn)) {
      // increment one day to align with left side ts_ds
      // because one day was decremented from the partition range for snapshot accuracy
      rightDf
        .withColumn(
          Constants.TimePartitionColumn,
          date_format(date_add(to_date(col(tableUtils.partitionColumn), tableUtils.partitionSpec.format), 1),
                      tableUtils.partitionSpec.format)
        )
    } else {
      rightDf
    }

    logger.info(s"""
                   |Join keys for ${joinPart.groupBy.metaData.name}: ${keys.mkString(", ")}
                   |Left Schema:
                   |${leftDfWithRelevantCols.schema.pretty}
                   |Right Schema:
                   |${joinableRightDf.schema.pretty}""".stripMargin)

    val joinedDf = coalescedJoin(leftDfWithRelevantCols, joinableRightDf, keys)
    logger.info(s"""Final Schema:
                   |${joinedDf.schema.pretty}
                   |""".stripMargin)

    joinedDf
  }
}
