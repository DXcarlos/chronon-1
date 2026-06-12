package ai.chronon.api.planner

import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.{PartitionRange, PartitionSpec, TableDependency, Window}

object DependencyResolver {

  private def minus(partition: String, offset: Window)(implicit partitionSpec: PartitionSpec): String = {
    if (partition == null) return null
    if (offset == null) return null
    partitionSpec.minus(partition, offset)
  }

  private def max(partition: String, cutOff: String): String = {
    if (partition == null) return cutOff
    if (cutOff == null) return partition
    Ordering[String].max(partition, cutOff)
  }

  private def min(partition: String, cutOff: String): String = {
    if (partition == null) return cutOff
    if (cutOff == null) return partition
    Ordering[String].min(partition, cutOff)
  }

  private def intervalEndMillis(range: PartitionRange): Long =
    range.partitionSpec.epochMillis(range.end) + range.partitionSpec.spanMillis

  private def inclusiveEndLabel(endExclusiveMillis: Long, targetSpec: PartitionSpec): String =
    targetSpec.at(endExclusiveMillis - 1)

  def computeOutputRange(parentRange: PartitionRange, tableDep: TableDependency): Option[PartitionRange] =
    computeOutputRange(parentRange, tableDep, parentRange.partitionSpec)

  def computeOutputRange(parentRange: PartitionRange,
                         tableDep: TableDependency,
                         outputPartitionSpec: PartitionSpec): Option[PartitionRange] = {
    require(parentRange != null, "Parent range cannot be null")
    require(parentRange.start != null, "Parent range start cannot be null")
    require(parentRange.end != null, "Parent range end cannot be null")
    require(parentRange.start <= parentRange.end, "Parent range start must be <= end")

    val parentStartMillis = parentRange.partitionSpec.epochMillis(parentRange.start)
    val parentEndExclusiveMillis = intervalEndMillis(parentRange)
    val childStartMillis = parentStartMillis + Option(tableDep.getEndOffset).map(_.millis).getOrElse(0L)
    val childEndExclusiveMillis = parentEndExclusiveMillis + Option(tableDep.getStartOffset).map(_.millis).getOrElse(0L)

    val start = outputPartitionSpec.at(childStartMillis)
    val end = inclusiveEndLabel(childEndExclusiveMillis, outputPartitionSpec)

    if (start != null && end != null && start > end) {
      return None
    }

    Some(PartitionRange(start, end)(outputPartitionSpec))
  }

  def computeInputRange(queryRange: PartitionRange, tableDep: TableDependency): Option[PartitionRange] = {

    require(queryRange != null, "Query range cannot be null")
    require(queryRange.start != null, "Query range start cannot be null")
    require(queryRange.end != null, "Query range end cannot be null")

    implicit val inputPartitionSpec: PartitionSpec =
      Option(tableDep.tableInfo).map(_.partitionSpec(queryRange.partitionSpec)).getOrElse(queryRange.partitionSpec)

    val queryStartMillis = queryRange.partitionSpec.epochMillis(queryRange.start)
    val queryEndExclusiveMillis = intervalEndMillis(queryRange)
    val inputStartMillis = queryStartMillis - Option(tableDep.getStartOffset).map(_.millis).getOrElse(0L)
    val inputEndExclusiveMillis = queryEndExclusiveMillis - Option(tableDep.getEndOffset).map(_.millis).getOrElse(0L)

    val offsetStart = inputPartitionSpec.at(inputStartMillis)
    val offsetEnd = inclusiveEndLabel(inputEndExclusiveMillis, inputPartitionSpec)
    val start = max(offsetStart, tableDep.getStartCutOff)
    val end = min(offsetEnd, tableDep.getEndCutOff)

    if (start != null && end != null && start > end) {
      return None
    }

    if (Option(tableDep.tableInfo).exists(_.isCumulative)) {

      // we should always compute the latest possible partition when end_cutoff is not set
      val latestValidInput = Option(tableDep.getEndCutOff).getOrElse(inputPartitionSpec.now)
      val latestValidInputWithOffset = minus(latestValidInput, tableDep.getEndOffset)

      return Some(PartitionRange(latestValidInputWithOffset, latestValidInputWithOffset)(inputPartitionSpec))

    }

    Some(PartitionRange(start, end)(inputPartitionSpec))
  }

  def getMissingSteps(requiredPartitionRange: PartitionRange,
                      existingPartitions: Seq[String],
                      stepDays: Int = 1): Seq[PartitionRange] = {
    val requiredPartitions = requiredPartitionRange.partitions

    val missingPartitions = requiredPartitions.filterNot(existingPartitions.contains)
    val missingPartitionRanges = PartitionRange.collapseToRange(missingPartitions)(requiredPartitionRange.partitionSpec)

    val missingSteps = missingPartitionRanges.flatMap(_.steps(stepDays))
    missingSteps
  }
}
