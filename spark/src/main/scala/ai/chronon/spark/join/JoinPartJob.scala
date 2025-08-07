package ai.chronon.spark.join

import ai.chronon.api.DataModel.{ENTITIES, EVENTS}
import ai.chronon.api.Extensions.{DateRangeOps, DerivationOps, GroupByOps, JoinPartOps, MetadataOps}
import ai.chronon.api.PartitionRange.toTimeRange
import ai.chronon.api.ScalaJavaConversions.ListOps
import ai.chronon.api._
import ai.chronon.online.metrics.Metrics
import ai.chronon.planner.JoinPartNode
import ai.chronon.spark.Extensions._
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.{GroupBy, JoinUtils}
import ai.chronon.spark.join.UnionJoin
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, column, date_format}
import org.apache.spark.util.sketch.BloomFilter
import org.slf4j.{Logger, LoggerFactory}

import java.util
import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._


class JoinPartJob(node: JoinPartNode, range: DateRange)(implicit tableUtils: TableUtils) {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec

  private val leftTable = node.leftSourceTable
  private val joinPart = node.joinPart
  private val dateRange = range.toPartitionRange

  private lazy val leftDf: DataFrame = {
    logger.info(s"Running join part job for ${joinPart.groupBy.metaData.name} on range $dateRange")

    val relevantLeftCols =
      joinPart.rightToLeft.keys.toArray ++ Seq(tableUtils.partitionColumn) ++ (node.leftDataModel match {
        case ENTITIES => None
        case EVENTS => Some(Constants.TimeColumn)
      })

    val query = Builders.Query(selects = relevantLeftCols.map(t => t -> t).toMap)
    tableUtils.scanDf(query = query, leftTable, range = Some(dateRange))
  }

  def run: DataFrame = {

    val rightRange = Utils.shiftDays(node.leftDataModel, joinPart, dateRange)


    def genGroupBy(partitionRange: PartitionRange) =
      GroupBy.from(joinPart.groupBy,
                   partitionRange,
                   tableUtils,
                   computeDependency = true)

    // all lazy vals - so evaluated only when needed by each case.
    lazy val partitionRangeGroupBy = genGroupBy(dateRange)

    /*
      For the corner case when the values of the key mapping also exist in the keys,
      for example:
           Map(user -> user_name, user_name -> user)
      the below logic will first rename the conflicted column with a random suffix
      and update the rename map
     */
    lazy val renamedLeftRawDf = {
      val columns = leftDf.columns.flatMap { column =>
        if (joinPart.leftToRight.contains(column)) {
          Some(col(column).as(joinPart.leftToRight(column)))
        } else if (joinPart.rightToLeft.contains(column)) {
          None
        } else {
          Some(col(column))
        }
      }
      leftDf.select(columns: _*)
    }

    lazy val shiftedPartitionRange = dateRange.shift(-1)

    val renamedLeftDf = renamedLeftRawDf.select(renamedLeftRawDf.columns.map {
      case c if c == tableUtils.partitionColumn =>
        date_format(renamedLeftRawDf.col(c), tableUtils.partitionFormat).as(c)
      case c => renamedLeftRawDf.col(c)
    }.toList: _*)

    val rightDf = (node.leftDataModel, joinPart.groupBy.dataModel, joinPart.groupBy.inferredAccuracy) match {
      case (ENTITIES, EVENTS, _)   => partitionRangeGroupBy.snapshotEvents(dateRange)
      case (ENTITIES, ENTITIES, _) => partitionRangeGroupBy.snapshotEntities
      case (EVENTS, EVENTS, Accuracy.SNAPSHOT) =>
        genGroupBy(shiftedPartitionRange).snapshotEvents(shiftedPartitionRange)
      case (EVENTS, EVENTS, Accuracy.TEMPORAL) =>
        val skewFreeMode = tableUtils.sparkSession.conf
          .get("spark.chronon.join.backfill.mode.skewFree", "false")
          .toBoolean

        if (skewFreeMode) {
          // Use UnionJoin for skewFree mode - it will handle column selection internally
          logger.info(s"Using UnionJoin for TEMPORAL events join part: ${joinPart.groupBy.metaData.name}")
          UnionJoin.computeJoinPart(renamedLeftDf, joinPart, dateRange, produceFinalJoinOutput = false)
        } else {
          // Use traditional temporalEvents approach
          genGroupBy(dateRange).temporalEvents(renamedLeftDf, Some(toTimeRange(dateRange)))
        }

      case (EVENTS, ENTITIES, Accuracy.SNAPSHOT) => genGroupBy(shiftedPartitionRange).snapshotEntities

      case (EVENTS, ENTITIES, Accuracy.TEMPORAL) =>
        // Snapshots and mutations are partitioned with ds holding data between <ds 00:00> and ds <23:59>.
        genGroupBy(shiftedPartitionRange).temporalEntities(renamedLeftDf)
    }

    if (joinPart.groupBy.hasDerivations) {

      val finalOutputColumns = joinPart.groupBy.derivationsScala.finalOutputColumn(
        rightDf.columns,
        ensureKeys = joinPart.groupBy.keys(tableUtils.partitionColumn)
      )

      val result = rightDf.select(finalOutputColumns: _*)
      result

    } else {

      rightDf
    }

  }
}
