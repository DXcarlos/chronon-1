package ai.chronon.spark.batch

import ai.chronon.api.Extensions._
import ai.chronon.api.planner.DependencyResolver
import ai.chronon.api.{PartitionRange, PartitionSpec, TableDependency, TableInfo}
import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.types.TimestampType
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

private[batch] class BatchNodeReadiness(tableUtils: TableUtils, scheduleOffsetMs: Option[Long]) {
  import BatchNodeReadiness._

  @transient private lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private def isTimestampColumn(tableName: String, column: String): Boolean =
    Try(tableUtils.getSchemaFromTable(tableName)(column).dataType == TimestampType).getOrElse(false)

  private def scheduledAtMillis(range: PartitionRange): Option[Long] =
    scheduleOffsetMs.map(offsetMs => range.partitionSpec.epochMillis(range.end) + offsetMs)

  private def requiredEndMillis(range: PartitionRange, tableDependency: TableDependency): Option[Long] =
    scheduledAtMillis(range).map { scheduledMs =>
      val dependencySpec = tableDependency.tableInfo.partitionSpec(range.partitionSpec)
      val offsetMs =
        if (tableDependency.isSetEndOffset) tableDependency.getEndOffset.millis else 0L
      val offsetRequiredMs = scheduledMs - offsetMs
      Option(tableDependency.getEndCutOff)
        .map(cutoff => dependencySpec.epochMillis(cutoff) + dependencySpec.spanMillis - 1)
        .map(cutoffMs => Math.min(offsetRequiredMs, cutoffMs))
        .getOrElse(offsetRequiredMs)
    }

  private def maxRequiredEndMillis(range: PartitionRange, deps: Iterable[TableDependency]): Option[Long] =
    deps.flatMap(td => requiredEndMillis(range, td)).reduceOption((left, right) => Math.max(left, right))

  def targetForScheduleOffset(tableName: String,
                              tableInfo: TableInfo,
                              range: PartitionRange,
                              deps: Iterable[TableDependency],
                              basePartitionSpec: PartitionSpec): Option[ReadinessTarget] = {
    val requiredEndMs = maxRequiredEndMillis(range, deps)
    val spec = tableInfo.partitionSpec(basePartitionSpec)
    val partitionColumn = Option(tableInfo.partitionColumn)

    for {
      endMs <- requiredEndMs
      column <- partitionColumn
    } yield {
      if (isTimestampColumn(tableName, column)) {
        TimestampStatsTarget(column, endMs)
      } else if (partitionSpecCoversRequiredEnd(spec, basePartitionSpec, endMs)) {
        PartitionTarget(spec, spec.at(endMs), normalizeToBasePartitionSpec = false)
      } else {
        UnalignedPartitionTarget(column, endMs, spec)
      }
    }
  }

  private def partitionSpecCoversRequiredEnd(spec: PartitionSpec,
                                             basePartitionSpec: PartitionSpec,
                                             requiredEndMs: Long): Boolean =
    spec.spanMillis >= basePartitionSpec.spanMillis || spec.epochMillis(spec.at(requiredEndMs)) == requiredEndMs

  def defaultPartitionTarget(tableInfo: TableInfo,
                             range: PartitionRange,
                             basePartitionSpec: PartitionSpec,
                             normalizeToBasePartitionSpec: Boolean): PartitionTarget = {
    val spec = tableInfo.partitionSpec(basePartitionSpec)
    PartitionTarget(spec, range.translate(spec).end, normalizeToBasePartitionSpec)
  }

  def checkTimestampStats(tableName: String, target: TimestampStatsTarget): Unit = {
    val columnType = tableUtils.getSchemaFromTable(tableName)(target.column).dataType
    if (columnType != TimestampType) {
      throw new RuntimeException(
        s"Subdaily sensor check requires timestamp partitionColumn for ${tableName}.${target.column}; found ${columnType}")
    }
    logger.info(
      s"Checking stats-backed max timestamp for ${tableName} column ${target.column}; required end ms: ${target.requiredEndMs}")
    val maxTimestampMs = tableUtils
      .maxTimestampMillisFromStats(tableName, target.column)
      .getOrElse(
        throw new RuntimeException(
          s"Subdaily sensor check is not ready: timestamp stats unavailable for ${tableName}.${target.column}"))

    logger.info(s"Max timestamp from stats: ${maxTimestampMs}, required end ms: ${target.requiredEndMs}")
    if (maxTimestampMs >= target.requiredEndMs) {
      logger.info(s"Sensor succeeded: ${maxTimestampMs} >= ${target.requiredEndMs}")
    } else {
      throw new RuntimeException(
        s"Sensor check failed: max timestamp from stats ${maxTimestampMs} < required end ms ${target.requiredEndMs}")
    }
  }

  def checkPartition(tableName: String, partitionColumn: String, target: PartitionTarget): Unit = {
    logger.info(s"Checking last available partition for ${tableName} column ${partitionColumn}")
    val lastPartition = tableUtils
      .lastAvailablePartition(tableName,
                              tablePartitionSpec = Some(target.spec),
                              normalizeToBasePartitionSpec = target.normalizeToBasePartitionSpec)
      .getOrElse(throw new RuntimeException(s"Could not determine last available partition for ${tableName}"))

    logger.info(s"Last available partition: ${lastPartition}, required end: ${target.requiredPartition}")

    if (lastPartition >= target.requiredPartition) {
      logger.info(s"Sensor succeeded: ${lastPartition} >= ${target.requiredPartition}")
    } else {
      throw new RuntimeException(
        s"Sensor check failed: last available partition ${lastPartition} < required end ${target.requiredPartition}")
    }
  }

  def checkSensorTarget(tableName: String,
                        partitionColumn: String,
                        target: ReadinessTarget,
                        retryCount: Long,
                        retryIntervalMin: Long): Try[Unit] =
    retrySensorCheck(retryCount, retryIntervalMin) {
      target match {
        case timestampTarget: TimestampStatsTarget => checkTimestampStats(tableName, timestampTarget)
        case target: UnalignedPartitionTarget =>
          throw new RuntimeException(
            s"Schedule offset ${target.requiredEndMs} does not align with partition spec " +
              s"${target.spec.format} for ${tableName}.${target.column}; timestamp stats are required")
        case partitionTarget: PartitionTarget => checkPartition(tableName, partitionColumn, partitionTarget)
      }
    }

  def inputTableStatus(tableName: String,
                       deps: Iterable[TableDependency],
                       range: PartitionRange): Option[PartitionReadinessStatus] = {
    targetForScheduleOffset(tableName,
                            deps.head.tableInfo,
                            range,
                            deps,
                            basePartitionSpec = tableUtils.partitionSpec) match {
      case Some(TimestampStatsTarget(timestampColumn, maxRequiredEndMs)) =>
        val maxTimestamp = tableUtils.maxTimestampMillisFromStats(tableName, timestampColumn)
        Some(
          PartitionReadinessStatus(None,
                                   maxTimestamp.map(_.toString),
                                   maxTimestamp.exists(_ >= maxRequiredEndMs),
                                   maxRequiredEndMs.toString))
      case Some(UnalignedPartitionTarget(_, requiredEndMs, _)) =>
        Some(PartitionReadinessStatus(None, None, ready = false, requiredEndMs.toString))
      case Some(target: PartitionTarget) =>
        Some(partitionStatus(tableName, target))
      case None =>
        val inputPartitionSpec = deps.head.tableInfo.partitionSpec(tableUtils.partitionSpec)
        dailyRequiredPartition(deps, range).map { requiredPartition =>
          val target =
            PartitionTarget(inputPartitionSpec, requiredPartition, normalizeToBasePartitionSpec = true)
          partitionStatus(tableName, target)
        }
    }
  }

  private def dailyRequiredPartition(deps: Iterable[TableDependency], range: PartitionRange): Option[String] =
    deps
      .flatMap { td =>
        DependencyResolver
          .computeInputRange(range, td)
          .map(_.translate(tableUtils.partitionSpec))
          .map(_.end)
      }
      .toSeq
      .sorted
      .lastOption

  private def partitionStatus(tableName: String, target: PartitionTarget): PartitionReadinessStatus = {
    val first =
      tableUtils.firstAvailablePartition(tableName,
                                         partitionSpec = target.spec,
                                         normalizeToBasePartitionSpec = target.normalizeToBasePartitionSpec)
    val last =
      tableUtils.lastAvailablePartition(tableName,
                                        tablePartitionSpec = Some(target.spec),
                                        normalizeToBasePartitionSpec = target.normalizeToBasePartitionSpec)
    PartitionReadinessStatus(first, last, last.exists(_ >= target.requiredPartition), target.requiredPartition)
  }

  private def retrySensorCheck(retryCount: Long, retryIntervalMin: Long)(check: => Unit): Try[Unit] = {
    @tailrec
    def retry(attempt: Long): Try[Unit] = {
      Try {
        check
      } match {
        case Success(_) => Success(())
        case Failure(e) if attempt < retryCount =>
          logger.warn(s"Attempt ${attempt + 1} failed: ${e.getMessage}. Retrying in ${retryIntervalMin} minutes")
          Thread.sleep(retryIntervalMin * 60 * 1000)
          retry(attempt + 1)
        case Failure(e) =>
          Failure(
            new RuntimeException(s"Sensor timed out after ${retryIntervalMin * attempt} minutes. ${e.getMessage}", e))
      }
    }
    retry(0)
  }
}

private[batch] object BatchNodeReadiness {
  sealed trait ReadinessTarget {
    def requiredEnd: String
  }
  case class TimestampStatsTarget(column: String, requiredEndMs: Long) extends ReadinessTarget {
    override def requiredEnd: String = requiredEndMs.toString
  }
  case class UnalignedPartitionTarget(column: String, requiredEndMs: Long, spec: PartitionSpec)
      extends ReadinessTarget {
    override def requiredEnd: String = requiredEndMs.toString
  }
  case class PartitionTarget(spec: PartitionSpec, requiredPartition: String, normalizeToBasePartitionSpec: Boolean)
      extends ReadinessTarget {
    override def requiredEnd: String = requiredPartition
  }
  case class PartitionReadinessStatus(firstAvailablePartition: Option[String],
                                      lastAvailablePartition: Option[String],
                                      ready: Boolean,
                                      requiredEnd: String)
}
