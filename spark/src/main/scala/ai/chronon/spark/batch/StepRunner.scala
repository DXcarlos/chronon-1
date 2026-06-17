package ai.chronon.spark.batch

import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.{DateRange, MetaData, PartitionRange, PartitionSpec}
import ai.chronon.spark.catalog.{Format, TableUtils}
import org.slf4j.{Logger, LoggerFactory}

case class StepRunner(
    tableName: String,
    producePartitionsFunc: DateRange => Unit,
    partitionFunc: String => Seq[String],
    stepSize: Option[Int] = None
)(implicit tableUtils: TableUtils, partitionSpec: PartitionSpec) {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def unfilledSteps(requestedRange: PartitionRange, skipFirstHole: Boolean = true): Seq[PartitionRange] = {
    val existingPartitions = partitionFunc(tableName).toSet
    val requestedPartitions = requestedRange.partitions.toSet

    val missingPartitions = requestedPartitions -- existingPartitions
    val sortedMissing = missingPartitions.toSeq.sorted

    if (sortedMissing.isEmpty) {
      logger.info(s"Table $tableName has all partitions in range $requestedRange")
      return Seq.empty
    }

    val chunked = sortedMissing.foldLeft(Seq[PartitionRange]()) { (ranges, nextDate) =>
      if (ranges.isEmpty || partitionSpec.after(ranges.last.end) != nextDate) {
        ranges :+ PartitionRange(nextDate, nextDate)(partitionSpec)
      } else {
        val newRange = PartitionRange(ranges.last.start, nextDate)(partitionSpec)
        ranges.dropRight(1) :+ newRange
      }
    }

    val unfilled = if (skipFirstHole && chunked.size > 1 && existingPartitions.nonEmpty) {
      logger.info(s"Skipping first hole in partitions for table $tableName")
      chunked.tail
    } else {
      chunked
    }

    logger.info(s"Table: $tableName, Requested range: $requestedRange, Unfilled ranges: $unfilled")

    val stepped = stepSize match {
      case Some(size) =>
        logger.info(s"Breaking ${unfilled.size} unfilled ranges into steps of size $size")
        unfilled.flatMap(_.steps(size))
      case None =>
        logger.info(s"Processing ${unfilled.size} unfilled ranges without stepping")
        unfilled
    }
    logger.info(s"Total steps to process: ${stepped.size}")

    stepped
  }

  def run(
      requestedRange: PartitionRange,
      skipFirstHole: Boolean = true
  ): Int = {
    val steps = unfilledSteps(requestedRange, skipFirstHole)

    if (steps.isEmpty) {
      logger.info(s"No unfilled ranges for table $tableName. Nothing to compute.")
      return 0
    }

    logger.info(s"Processing ${steps.size} steps for table $tableName")

    steps.zipWithIndex.foreach { case (stepRange, idx) =>
      logger.info(s"Processing step ${idx + 1}/${steps.size}: $stepRange")
      producePartitionsFunc(stepRange.toDateRange)
    }

    steps.size
  }
}

object StepRunner {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def apply(requestedDateRange: DateRange, metaData: MetaData)(body: DateRange => Unit)(implicit
      tableUtils: TableUtils): Unit =
    apply(requestedDateRange, metaData, deriveLogicalPartitions = false)(body)

  def apply(requestedDateRange: DateRange, metaData: MetaData, deriveLogicalPartitions: Boolean)(
      body: DateRange => Unit)(implicit tableUtils: TableUtils): Unit = {

    val requestedRange = PartitionRange(requestedDateRange.startDate, requestedDateRange.endDate)(PartitionSpec.daily)

    val tableName = metaData.outputTable
    val stepSize = metaData.stepSize
    val outputPartitions: String => Seq[String] = { t =>
      tableUtils.partitions(t, partitionRange = Some(requestedRange), deriveLogicalPartitions = deriveLogicalPartitions)
    }
    val stepRunner = StepRunner(
      tableName,
      body,
      outputPartitions,
      Some(stepSize)
    )(tableUtils, tableUtils.partitionSpec)

    stepRunner.run(requestedRange)

    val lastPartition =
      if (deriveLogicalPartitions) {
        Format.pickMaxPartition(outputPartitions(tableName))
      } else {
        tableUtils.lastAvailablePartition(tableName)
      }
    lastPartition match {
      case Some(lp) if lp >= requestedRange.end =>
        logger.info(s"Output table $tableName covers requested range (last: $lp >= end: ${requestedRange.end})")
      case Some(lp) =>
        logger.error(s"Output table $tableName last partition $lp < required end ${requestedRange.end}")
      case None =>
        logger.error(s"Output table $tableName has no partitions after run")
    }
  }
}
