package ai.chronon.spark.catalog

import ai.chronon.api.PartitionSpec
import ai.chronon.spark.batch.iceberg.IcebergPartitionStatsExtractor
import org.apache.iceberg.{DataFile, DataOperations}
import org.apache.iceberg.spark.source.SparkTable
import org.apache.iceberg.types.Type
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructType, TimestampType}

import java.time.{LocalDate, ZoneOffset}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case object Iceberg extends Format {

  private case class FileBounds(lowerMillis: Long, upperMillis: Long, dataSequenceNumber: Option[Long])

  private case class StatsState(table: org.apache.iceberg.Table, bounds: Vector[FileBounds]) {
    def dateRange(partitionSpec: PartitionSpec): StatsDateRange =
      StatsDateRange(
        start = partitionSpec.at(bounds.map(_.lowerMillis).min),
        end = partitionSpec.at(bounds.map(_.upperMillis).max)
      )
  }

  override def tableTypeString: String = "iceberg"

  override def tableProperties: Map[String, String] = {
    Map(
      "commit.retry.num-retries" -> "20", // default = 4
      "commit.retry.min-wait-ms" -> (10 * 1000).toString,
      "commit.retry.max-wait-ms" -> (600 * 1000).toString,
      "commit.status-check.num-retries" -> "20", // default = 3
      "commit.status-check.min-wait-ms" -> (10 * 1000).toString, // default = 1000
      "commit.status-check.max-wait-ms" -> (600 * 1000).toString,
      "write.merge.isolation-level" -> "snapshot",
      "format-version" -> "2"
    )
  }

  def qualifyWithCatalog(tableName: String)(implicit sparkSession: SparkSession): String =
    Format.resolveTableName(tableName).quoted

  override def primaryPartitions(tableName: String,
                                 partitionColumn: String,
                                 partitionFilters: String,
                                 subPartitionsFilter: Map[String, String])(implicit
      sparkSession: SparkSession): List[String] = {

    if (!supportSubPartitionsFilter && subPartitionsFilter.nonEmpty) {
      throw new NotImplementedError("subPartitionsFilter is not supported on this format")
    }

    Try(getIcebergPartitions(tableName, partitionColumn)) match {
      case Success(p) => p
      case Failure(e) if Option(e.getMessage).exists(_.contains("TABLE_OR_VIEW_NOT_FOUND")) =>
        logger.warn(s"Failed to get partitions for $tableName: ${e.getMessage}")
        List.empty
      case Failure(e) =>
        logger.warn(
          s"Failed to get partitions for $tableName: ${e.getClass.getSimpleName}: ${Option(e.getMessage).getOrElse("(no message)")}")
        List.empty
    }
  }

  override def partitions(tableName: String, partitionFilters: String)(implicit
      sparkSession: SparkSession): List[Map[String, String]] = {
    val partitionsDf = sparkSession.table(s"${Format.resolveTableName(tableName).quoted}.partitions")

    val index = partitionsDf.schema.fieldIndex("partition")
    val partitionColumnNames = partitionsDf.schema(index).dataType.asInstanceOf[StructType].fieldNames

    partitionsDf
      .select(col("partition"))
      .collect()
      .map { row =>
        val partitionData = row.getStruct(0)
        partitionColumnNames.flatMap { colName =>
          Option(partitionData.getAs[Any](colName)).map(colName -> _.toString)
        }.toMap
      }
      .toList
      .distinct
  }

  /** Partition column names from the Iceberg partition spec (via the .partitions metadata
    * table, which the spark catalog's listColumns can't see). Empty if unpartitioned.
    */
  override def partitionColumnNames(tableName: String)(implicit sparkSession: SparkSession): Seq[String] =
    Try {
      val partitionsDf = sparkSession.table(s"${Format.resolveTableName(tableName).quoted}.partitions")
      val index = partitionsDf.schema.fieldIndex("partition")
      partitionsDf.schema(index).dataType.asInstanceOf[StructType].fieldNames.toSeq
    }.getOrElse(Seq.empty)

  override def virtualPartitions(tableName: String, timestampColumn: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): List[String] =
    metadataPartitions(tableName, timestampColumn)
      .filter(_.nonEmpty)
      .orElse(statsVirtualPartitions(tableName, timestampColumn, partitionSpec))
      .getOrElse(super.virtualPartitions(tableName, timestampColumn, partitionSpec))

  override def firstAvailablePartition(tableName: String, partitionColumn: String, partitionSpec: PartitionSpec)(
      implicit sparkSession: SparkSession): Option[String] =
    metadataFirstAvailablePartition(tableName, partitionColumn)
      .orElse(statsDateRange(tableName, partitionColumn, partitionSpec).map(_.firstAvailablePartition))
      .orElse(scanFirstAvailablePartition(tableName, partitionColumn, partitionSpec))

  override def lastAvailablePartition(tableName: String, partitionColumn: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[String] =
    metadataLastAvailablePartition(tableName, partitionColumn)
      .orElse(statsLastAvailablePartition(tableName, partitionColumn, partitionSpec))
      .orElse(scanLastAvailablePartition(tableName, partitionColumn, partitionSpec))

  private def statsLastAvailablePartition(tableName: String, columnName: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[String] =
    statsState(tableName, columnName, partitionSpec).map { state =>
      val range = state.dateRange(partitionSpec)
      sparkSession.read.table(tableName).schema(columnName).dataType match {
        case TimestampType =>
          Format.readinessPartition(range.lastAvailablePartition,
                                    partitionSpec,
                                    tailIntervalLandedComplete(tableName, columnName, state, partitionSpec, range))
        case _ => range.lastAvailablePartition
      }
    }

  private def statsVirtualPartitions(tableName: String, columnName: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[List[String]] =
    statsState(tableName, columnName, partitionSpec).map { state =>
      val range = state.dateRange(partitionSpec)
      sparkSession.read.table(tableName).schema(columnName).dataType match {
        case TimestampType =>
          val trimmedLast =
            if (tailIntervalLandedComplete(tableName, columnName, state, partitionSpec, range))
              range.lastAvailablePartition
            else partitionSpec.before(range.lastAvailablePartition)
          partitionSpec.expandRange(range.firstAvailablePartition, trimmedLast)
        case _ => range.virtualPartitions(partitionSpec)
      }
    }

  private[catalog] def statsDateRange(tableName: String, columnName: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[StatsDateRange] = {
    val result = statsState(tableName, columnName, partitionSpec).map(_.dateRange(partitionSpec))
    if (result.isDefined) {
      logger.info(s"Resolved Iceberg file stats boundaries for $tableName.$columnName: ${result.get}")
    } else {
      logger.info(s"Iceberg file stats were incomplete for $tableName.$columnName; falling back to table scan")
    }
    result
  }

  private def statsState(tableName: String, columnName: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[StatsState] =
    Try {
      val table = loadIcebergTable(tableName).getOrElse {
        throw new IllegalStateException(s"Could not load Iceberg table: $tableName")
      }
      val field = Option(table.schema().findField(columnName)).getOrElse {
        throw new IllegalArgumentException(s"Column $columnName not found in Iceberg schema for $tableName")
      }
      val fieldId = field.fieldId().asInstanceOf[java.lang.Integer]
      val fieldType = field.`type`()
      val extractor = new IcebergPartitionStatsExtractor(sparkSession)

      currentDataFilesBounds(table, fieldId, fieldType, partitionSpec, extractor)
    } match {
      case Success(result) => result
      case Failure(e) =>
        logger.warn(
          s"Failed to resolve Iceberg file stats boundaries for $tableName.$columnName: ${Option(e.getMessage).getOrElse("(no message)")}")
        None
    }

  /** Whether the sub-daily tail interval (the one holding the table's max timestamp) landed
    * complete: every live file carrying data for it was ingested by a snapshot committed at or
    * after the interval's end — the signature of a chunked batch writer, as opposed to
    * streaming ingest which commits during the interval and keeps the conservative one-behind
    * readiness of [[Format.readinessPartition]].
    *
    * Files are attributed to their ingest commit via dataSequenceNumber, which survives
    * compaction (rewritten files inherit the sequence number of the data they carry), so
    * compacted streaming data still maps back to its original in-interval commit. Anything
    * unmappable — v1 tables (all sequence numbers 0), expired snapshots, sequence numbers not
    * belonging to a data-writing snapshot — keeps conservative interval-end readiness.
    */
  private def tailIntervalLandedComplete(tableName: String,
                                         columnName: String,
                                         state: StatsState,
                                         partitionSpec: PartitionSpec,
                                         range: StatsDateRange): Boolean = {
    if (partitionSpec.spanMillis >= PartitionSpec.daily.spanMillis) return false

    Try {
      val tailPartition = range.lastAvailablePartition
      val intervalStart = partitionSpec.partitionStartMillis(tailPartition)
      val intervalEnd = partitionSpec.partitionEndMillis(tailPartition)

      val dataOps = Set(DataOperations.APPEND, DataOperations.OVERWRITE)
      val ingestMillisBySeq: Map[Long, Long] = state.table
        .snapshots()
        .asScala
        .filter(snapshot => dataOps.contains(snapshot.operation()))
        .map(snapshot => snapshot.sequenceNumber() -> snapshot.timestampMillis())
        .toMap

      val intersecting = state.bounds.filter(_.upperMillis >= intervalStart)
      intersecting.nonEmpty && intersecting.forall(
        _.dataSequenceNumber.filter(_ > 0).flatMap(ingestMillisBySeq.get).exists(_ >= intervalEnd))
    } match {
      case Success(complete) =>
        if (complete) {
          logger.info(
            s"Tail interval ${range.lastAvailablePartition} of $tableName.$columnName landed complete " +
              "(chunked write); reporting it as ready")
        }
        complete
      case Failure(e) =>
        logger.warn(
          s"Failed to check tail interval completeness for $tableName.$columnName: " +
            s"${Option(e.getMessage).getOrElse("(no message)")}")
        false
    }
  }

  private def fileDateRange(file: DataFile,
                            fieldId: java.lang.Integer,
                            fieldType: org.apache.iceberg.types.Type,
                            partitionSpec: PartitionSpec,
                            extractor: IcebergPartitionStatsExtractor): Option[(Long, Long)] = {
    val lower = Option(file.lowerBounds()).flatMap(bounds => Option(bounds.get(fieldId)))
    val upper = Option(file.upperBounds()).flatMap(bounds => Option(bounds.get(fieldId)))

    for {
      lowerBound <- lower
      upperBound <- upper
    } yield {
      val lowerMillis = boundMillis(extractor.convertBoundValue(lowerBound, fieldType), fieldType, partitionSpec)
      val upperMillis = boundMillis(extractor.convertBoundValue(upperBound, fieldType), fieldType, partitionSpec)
      lowerMillis -> upperMillis
    }
  }

  private def boundMillis(value: Any, fieldType: Type, partitionSpec: PartitionSpec): Long =
    fieldType.typeId() match {
      case Type.TypeID.TIMESTAMP =>
        Math.floorDiv(value.asInstanceOf[java.lang.Long].longValue(), 1000L)
      case Type.TypeID.DATE =>
        LocalDate
          .ofEpochDay(value.asInstanceOf[java.lang.Integer].longValue())
          .atStartOfDay()
          .toInstant(ZoneOffset.UTC)
          .toEpochMilli
      case Type.TypeID.STRING =>
        partitionSpec.epochMillis(value.toString)
      case other =>
        throw new IllegalArgumentException(s"Unsupported Iceberg bound type $other for value $value")
    }

  // Live data files' column bounds plus the dataSequenceNumber needed to attribute each file
  // to its ingest snapshot. Any file without usable bounds voids the whole result, matching
  // the all-or-nothing stats contract of statsDateRange.
  private def currentDataFilesBounds(table: org.apache.iceberg.Table,
                                     fieldId: java.lang.Integer,
                                     fieldType: org.apache.iceberg.types.Type,
                                     partitionSpec: PartitionSpec,
                                     extractor: IcebergPartitionStatsExtractor): Option[StatsState] =
    Option(table.currentSnapshot()).flatMap { _ =>
      val tasks = table.newScan().includeColumnStats().planFiles()
      try {
        val bounds = tasks
          .iterator()
          .asScala
          .map { task =>
            fileDateRange(task.file(), fieldId, fieldType, partitionSpec, extractor).map {
              case (lowerMillis, upperMillis) =>
                FileBounds(lowerMillis, upperMillis, Option(task.file().dataSequenceNumber()).map(_.longValue()))
            }
          }
          .toVector

        if (bounds.isEmpty || bounds.contains(None)) None
        else Some(StatsState(table, bounds.flatten))
      } finally {
        tasks.close()
      }
    }

  private def loadIcebergTable(tableName: String)(implicit
      sparkSession: SparkSession): Option[org.apache.iceberg.Table] =
    Try {
      val resolved = Format.resolveTableName(tableName)
      val catalog = sparkSession.sessionState.catalogManager
        .catalog(resolved.catalog)
        .asInstanceOf[TableCatalog]

      catalog.loadTable(resolved.toIdentifier) match {
        case sparkTable: SparkTable => sparkTable.table()
        case other => throw new IllegalStateException(s"Not an Iceberg SparkTable: ${other.getClass.getName}")
      }
    }.toOption

  private def getIcebergPartitions(tableName: String, partitionColumn: String)(implicit
      sparkSession: SparkSession): List[String] = {

    val partitionsDf = sparkSession.table(s"${Format.resolveTableName(tableName).quoted}.partitions")

    val index = partitionsDf.schema.fieldIndex("partition")
    if (partitionsDf.schema(index).dataType.asInstanceOf[StructType].fieldNames.contains("hr")) {
      // Hour filter is currently buggy in iceberg. https://github.com/apache/iceberg/issues/4718
      // so we collect and then filter.
      partitionsDf
        .select(col(s"partition.$partitionColumn").cast("string"), col("partition.hr"))
        .collect()
        .filter(_.get(1) == null)
        .flatMap(row => Option(row.getString(0)))
        .toList
    } else {
      partitionsDf
        .select(col(s"partition.$partitionColumn").cast("string"))
        .collect()
        .flatMap(row => Option(row.getString(0)))
        .toList
    }
  }

  override def supportSubPartitionsFilter: Boolean = false
}
