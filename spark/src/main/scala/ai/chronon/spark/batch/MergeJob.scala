package ai.chronon.spark.batch

import ai.chronon.api.DataModel.ENTITIES
import ai.chronon.api.Extensions.{DateRangeOps, GroupByOps, JoinPartOps, MetadataOps, SourceOps}
import ai.chronon.api.ScalaJavaConversions.IterableOps
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
import org.apache.spark.sql.functions.{col, date_add, date_format, left, to_date}
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

  // Processing metadata columns that get dropped in final output
  private val processingColumns = Set(Constants.MatchedHashes, Constants.TimePartitionColumn)

  private val join = node.join
  private val leftInputTable = if (join.bootstrapParts != null || join.onlineExternalParts != null) {
    join.metaData.bootstrapTable
  } else {
    JoinUtils.computeFullLeftSourceTableName(join)
  }
  // Use the node's Join's metadata for output table
  private val outputTable = metaData.outputTable
  private val dateRange = range.toPartitionRange
  private val productionJoin = node.productionJoin

  def run(): Unit = {

    // This job benefits from a step day of 1 to avoid needing to shuffle on writing output (single partition)
    dateRange.steps(days = 1).foreach { dayStep =>
      // Scan left input table once to get schema and potentially reuse
      val leftInputDf = tableUtils.scanDf(query = null, table = leftInputTable, range = Some(dayStep))

      // Check if we can reuse columns from production table
      val (selectFromProdTableAsLeft, joinPartsToQueryAndJoin) = analyzeJoinPartsForReuse(dayStep, leftInputDf)

      // Get left DataFrame with potentially reused columns from production
      val leftDf = if (selectFromProdTableAsLeft.nonEmpty) {
        logger.info(
          s"Reusing ${selectFromProdTableAsLeft.length} columns from production table: ${selectFromProdTableAsLeft
            .mkString(", ")}")

        // Select left columns + reused columns from production table
        val productionTable = productionJoin.metaData.outputTable
        val leftColumns = leftInputDf.schema.fieldNames.filterNot(processingColumns.contains)
        val columnsToSelect = leftColumns ++ selectFromProdTableAsLeft
        val productionDf = tableUtils.scanDf(query = null, table = productionTable, range = Some(dayStep))

        val selectedDf = productionDf.select(columnsToSelect.map(col): _*)

        // Add back ts_ds column if this is an EVENTS source and the column is missing
        if (join.left.dataModel == DataModel.EVENTS && !selectedDf.columns.contains(Constants.TimePartitionColumn)) {
          selectedDf.withTimeBasedColumn(Constants.TimePartitionColumn)
        } else {
          selectedDf
        }
      } else {
        leftInputDf
      }

      // Get right parts data only for join parts that need to be computed
      val rightPartsData = getRightPartsData(dayStep, joinPartsToQueryAndJoin)

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

  private def getRightPartsData(dayStep: PartitionRange,
                                joinPartsToProcess: Seq[JoinPart] = joinParts): Seq[(JoinPart, DataFrame)] = {
    joinPartsToProcess.map { joinPart =>
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
    val joinedDf = coalescedJoin(leftDf, joinableRightDf, keys)
    logger.info(s"""Final Schema:
                   |${joinedDf.schema.pretty}
                   |""".stripMargin)

    joinedDf
  }

  /** Check if the left schemas are compatible for reuse purposes
    */
  private def leftSchemasAreCompatible(currentLeftDf: DataFrame, prodOutputColumns: Set[String]): Boolean = {
    // Exclude processing metadata columns that get dropped in final output
    val currentLeftColumns = currentLeftDf.schema.fieldNames.toSet -- processingColumns

    if (!currentLeftColumns.subsetOf(prodOutputColumns)) {
      logger.warn(
        s"Current left columns are not fully covered by production output table. Missing: ${currentLeftColumns -- prodOutputColumns}")
      false
    } else {
      true
    }
  }

  /** Check if production table exists and covers the given range
    */
  private def productionTableCoversRange(productionTable: String, range: PartitionRange): Boolean = {
    try {
      if (!tableUtils.tableReachable(productionTable)) {
        logger.info(s"Production table $productionTable does not exist")
        return false
      }

      val requiredPartitions = range.partitions.toSet
      val coveredPartitions = tableUtils.partitions(productionTable).toSet

      val isFullyCovered = requiredPartitions.subsetOf(coveredPartitions)
      if (!isFullyCovered) {
        logger.info(
          s"Production table $productionTable does not cover full range. Required: $requiredPartitions, Available: $coveredPartitions")
      }

      isFullyCovered
    } catch {
      case e: Exception =>
        logger.warn(s"Error checking production table coverage: ${e.getMessage}")
        false
    }
  }

  /** Analyze join parts to determine which can be reused from production table
    * Returns (selectFromProdTableAsLeft, joinPartsToQueryAndJoin)
    */
  def analyzeJoinPartsForReuse(dayStep: PartitionRange, currentLeftDf: DataFrame): (Seq[String], Seq[JoinPart]) = {
    Option(productionJoin) match {
      case Some(prodJoin) =>
        val productionTable = prodJoin.metaData.outputTable

        if (productionTableCoversRange(productionTable, dayStep)) {
          logger.info(s"Production table $productionTable covers range, analyzing join parts for reuse")

          // Get production table cols once and reuse
          val productionColumns =
            try {
              val schema = tableUtils.scanDf(query = null, table = productionTable, range = Some(dayStep)).schema
              schema.fieldNames.toSet
            } catch {
              case e: Exception =>
                logger.warn(s"Could not get production table schema: ${e.getMessage}")
                return (Seq.empty, joinParts)
            }

          // Check if left schemas are compatible using the production columns we just got
          if (!leftSchemasAreCompatible(currentLeftDf, productionColumns)) {
            return (Seq.empty, joinParts)
          }

          // Start with empty list - we'll only add base left columns and reusable join part columns
          val selectFromProdTableAsLeft = scala.collection.mutable.ListBuffer[String]()
          val joinPartsToQueryAndJoin = scala.collection.mutable.ListBuffer[JoinPart]()

          // Create a map of production join parts by groupBy name for safety checks
          // Use cleanNameWithoutVersion to exclude version suffix when comparing names
          val prodJoinPartsByName =
            prodJoin.joinParts.toScala.map(jp => jp.groupBy.metaData.cleanNameWithoutVersion -> jp).toMap

          joinParts.foreach { joinPart =>
            val joinPartGroupByName = joinPart.groupBy.metaData.cleanNameWithoutVersion

            // Safety check: Only reuse columns if there's a matching groupBy in production config
            // This prevents accidentally sharing columns when a user creates a new joinPart
            // that happens to have the same column names as a removed one
            prodJoinPartsByName.get(joinPartGroupByName) match {
              case Some(_) =>
                // Get expected output columns for this join part
                val partTable = RelevantLeftForJoinPart.fullPartTableName(join, joinPart)

                try {
                  // Get join part table schema to determine its value columns
                  val partSchema = tableUtils.scanDf(query = null, table = partTable, range = Some(dayStep)).schema
                  val partKeyColumns = joinPart.rightToLeft.keys.toSet ++ Set(Constants.TimeColumn,
                                                                              tableUtils.partitionColumn,
                                                                              Constants.TimePartitionColumn)
                  val partValueColumns =
                    partSchema.fieldNames.filterNot(partKeyColumns.contains).map(joinPart.columnPrefix + _)

                  // Check if all value columns from this join part are present in production table
                  if (partValueColumns.forall(productionColumns.contains)) {
                    logger.info(s"Join part ${joinPartGroupByName} can be reused from production table")
                    selectFromProdTableAsLeft ++= partValueColumns
                  } else {
                    logger.info(
                      s"Join part ${joinPartGroupByName} cannot be fully reused from production table. Missing columns: ${partValueColumns
                        .filterNot(productionColumns.contains)
                        .mkString(", ")}")
                    joinPartsToQueryAndJoin += joinPart
                  }
                } catch {
                  case e: Exception =>
                    logger.warn(s"Error analyzing join part ${joinPartGroupByName} for reuse: ${e.getMessage}")
                    joinPartsToQueryAndJoin += joinPart
                }
              case None =>
                logger.info(s"Join part ${joinPartGroupByName} not found in production config, cannot reuse")
                joinPartsToQueryAndJoin += joinPart
            }
          }

          (selectFromProdTableAsLeft.toSeq, joinPartsToQueryAndJoin.toSeq)
        } else {
          logger.info("Production table does not cover range, proceeding with normal join")
          (Seq.empty, joinParts)
        }
      case _ =>
        logger.info("No production join available or left schemas incompatible, proceeding with normal join")
        (Seq.empty, joinParts)
    }
  }
}
