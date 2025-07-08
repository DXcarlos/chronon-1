package ai.chronon.spark.batch

import ai.chronon.api.DataModel.ENTITIES
import ai.chronon.api.Extensions.{DateRangeOps, GroupByOps, JoinPartOps, MetadataOps, SourceOps}
import ai.chronon.api.planner.RelevantLeftForJoinPart
import ai.chronon.api.{
  Accuracy,
  Constants,
  DataModel,
  DateRange,
  JoinPart,
  MetaData,
  PartitionRange,
  PartitionSpec,
  QueryUtils
}
import ai.chronon.planner.JoinMergeNode
import ai.chronon.spark.Extensions._
import ai.chronon.spark.JoinUtils.coalescedJoin
import ai.chronon.spark.JoinUtils
import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_add, date_format, to_date}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.Seq
import scala.util.{Failure, Success}

/*
leftInputTable is either the output of the SourceJob or the output of the BootstrapJob depending on if there are bootstraps or external parts.

joinPartsToTables is a map of JoinPart to the table name of the output of that joinPart job. JoinParts that are being skipped for this range
due to bootstrap can be omitted from this map.
 */

class MergeJob(node: JoinMergeNode, metaData: MetaData, range: DateRange, joinParts: Seq[JoinPart])(implicit
    tableUtils: TableUtils) {

  implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private val join = node.join
  private val leftInputTable = if (join.bootstrapParts != null || join.onlineExternalParts != null) {
    join.metaData.bootstrapTable
  } else {
    JoinUtils.computeFullLeftSourceTableName(join)
  }
  // Use the node's Join's metadata for output table
  private val outputTable = metaData.outputTable
  private val dateRange = range.toPartitionRange

  def run(): Unit = {

    // This job benefits from a step day of 1 to avoid needing to shuffle on writing output (single partition)
    dateRange.steps(days = 1).foreach { dayStep =>
      val rightPartsData = getRightPartsData(dayStep)
      val leftDf = tableUtils.scanDf(query = null, table = leftInputTable, range = Some(dayStep))

      val joinedDfTry =
        try {
          Success(
            rightPartsData
              .foldLeft(leftDf) { case (partialDf, (rightPart, rightDf)) =>
                joinWithLeft(partialDf, rightDf, rightPart)
              }
              // drop all processing metadata columns
              .drop(Constants.MatchedHashes, Constants.TimePartitionColumn))
        } catch {
          case e: Exception =>
            e.printStackTrace()
            Failure(e)
        }

      joinedDfTry.get.save(outputTable, metaData.tableProps, autoExpand = true)
    }
  }

  private def getRightPartsData(dayStep: PartitionRange): Seq[(JoinPart, DataFrame)] = {
    joinParts.map { joinPart =>
      // Use the RelevantLeftForJoinPart utility to get the part table name
      val partTable = RelevantLeftForJoinPart.fullPartTableName(join, joinPart)
      val effectiveRange =
        if (join.left.dataModel == DataModel.EVENTS && joinPart.groupBy.inferredAccuracy == Accuracy.SNAPSHOT) {
          dayStep.shift(-1)
        } else {
          dayStep
        }
      val wheres = effectiveRange.whereClauses
      val sql = QueryUtils.build(null, partTable, wheres)
      logger.info(s"Pulling data from joinPart table with: $sql")
      (joinPart, tableUtils.scanDfBase(null, partTable, List.empty, wheres, None))
    }.toSeq
  }

  def joinWithLeft(leftDf: DataFrame, rightDf: DataFrame, joinPart: JoinPart): DataFrame = {
    val partLeftKeys = joinPart.rightToLeft.values.toArray

    // compute join keys, besides the groupBy keys -  like ds, ts etc.,
    val additionalKeys: Seq[String] = {
      if (join.left.dataModel == ENTITIES) {
        Seq(tableUtils.partitionColumn)
      } else if (joinPart.groupBy.inferredAccuracy == Accuracy.TEMPORAL) {
        Seq(Constants.TimeColumn, tableUtils.partitionColumn)
      } else { // left-events + snapshot => join-key = ds_of_left_ts
        Seq(Constants.TimePartitionColumn)
      }
    }
    val keys = partLeftKeys ++ additionalKeys

    // apply prefix to value columns
    val nonValueColumns = joinPart.rightToLeft.keys.toArray ++ Array(Constants.TimeColumn,
                                                                     tableUtils.partitionColumn,
                                                                     Constants.TimePartitionColumn)
    val valueColumns = rightDf.schema.names.filterNot(nonValueColumns.contains)
    val prefixedRightDf = rightDf.prefixColumnNames(joinPart.columnPrefix, valueColumns)

    // apply key-renaming to key columns
    val newColumns = prefixedRightDf.columns.map { column =>
      if (joinPart.rightToLeft.contains(column)) {
        col(column).as(joinPart.rightToLeft(column))
      } else {
        col(column)
      }
    }

    val keyRenamedRightDf = prefixedRightDf.select(newColumns: _*)

    // adjust join keys
    val joinableRightDf = if (additionalKeys.contains(Constants.TimePartitionColumn)) {
      // increment one day to align with left side ts_ds
      // because one day was decremented from the partition range for snapshot accuracy
      keyRenamedRightDf
        .withColumn(
          Constants.TimePartitionColumn,
          date_format(date_add(to_date(col(tableUtils.partitionColumn), tableUtils.partitionSpec.format), 1),
                      tableUtils.partitionSpec.format)
        )
        .drop(tableUtils.partitionColumn)
    } else {
      keyRenamedRightDf
    }

    logger.info(s"""
                   |Join keys for ${joinPart.groupBy.metaData.name}: ${keys.mkString(", ")}
                   |Left Schema:
                   |${leftDf.schema.pretty}
                   |Right Schema:
                   |${joinableRightDf.schema.pretty}""".stripMargin)
    // Use optimized join that can leverage bucketing when available
    val leftTableName = Some(leftInputTable)
    val rightTableName = Some(RelevantLeftForJoinPart.fullPartTableName(join, joinPart))
    
    val joinedDf = JoinUtils.coalescedJoinOptimized(
      leftDf, 
      joinableRightDf, 
      keys,
      leftTableName = leftTableName,
      rightTableName = rightTableName
    )
    logger.info(s"""Final Schema:
                   |${joinedDf.schema.pretty}
                   |""".stripMargin)

    joinedDf
  }
}
