package ai.chronon.spark.batch.iceberg

import ai.chronon.api.PartitionSpec
import ai.chronon.observability.{TileKey, TileSummary}
import org.apache.iceberg.spark.source.SparkTable
import org.apache.iceberg.{DataFile, ManifestFiles}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}

import scala.collection.JavaConverters._
import scala.collection.mutable

object IcebergPartitionStatsExtractor {
  type PartitionKey = List[(String, String)]

  def extractPartitionMillisFromSlice(slice: String, partitionSpec: PartitionSpec): Long = {
    // Parse hive-style partition string (e.g., "day=2024-01-15/hour=00") to extract partition value
    val partitionPairs = slice
      .split("/")
      .map { pair =>
        val parts = pair.split("=", 2)
        if (parts.length == 2) {
          parts(0).trim -> parts(1).trim
        } else {
          throw new IllegalArgumentException(s"Invalid partition format in slice: $pair")
        }
      }
      .toMap

    // Extract the value for the partition column used by PartitionSpec
    val partitionValue = partitionPairs.getOrElse(
      partitionSpec.column,
      throw new IllegalArgumentException(s"Partition column '${partitionSpec.column}' not found in slice: $slice"))

    // Convert partition value to millis using PartitionSpec
    partitionSpec.epochMillis(partitionValue)
  }
}

case class ColumnStats(
    nullCount: Long,
    distinctCount: Option[Long] = None,
    minValue: Option[Any] = None,
    maxValue: Option[Any] = None
) {
  def aggregate(other: ColumnStats): ColumnStats = {
    ColumnStats(
      nullCount = this.nullCount + other.nullCount,
      distinctCount = (this.distinctCount, other.distinctCount) match {
        case (Some(a), Some(b)) => Some(math.max(a, b)) // Conservative estimate - take max
        case (Some(a), None)    => Some(a)
        case (None, Some(b))    => Some(b)
        case (None, None)       => None
      },
      minValue = (this.minValue, other.minValue) match {
        case (Some(a), Some(b)) => Some(compareValues(a, b, takeMin = true))
        case (Some(a), None)    => Some(a)
        case (None, Some(b))    => Some(b)
        case (None, None)       => None
      },
      maxValue = (this.maxValue, other.maxValue) match {
        case (Some(a), Some(b)) => Some(compareValues(a, b, takeMin = false))
        case (Some(a), None)    => Some(a)
        case (None, Some(b))    => Some(b)
        case (None, None)       => None
      }
    )
  }

  private def compareValues(a: Any, b: Any, takeMin: Boolean): Any = {
    try {
      (a, b) match {
        case (null, _) => if (takeMin) a else b
        case (_, null) => if (takeMin) a else b
        case (a1: java.lang.Comparable[_], b1) if a1.getClass == b1.getClass =>
          val comparison = a1.asInstanceOf[java.lang.Comparable[Any]].compareTo(b1)
          if ((comparison < 0) == takeMin) a1 else b1
        case (a1: Comparable[_], b1) if a1.getClass == b1.getClass =>
          val comparison = a1.asInstanceOf[Comparable[Any]].compareTo(b1)
          if ((comparison < 0) == takeMin) a1 else b1
        case _ => if (takeMin) a else b
      }
    } catch {
      case _: Exception => if (takeMin) a else b // Fallback to first value on comparison error
    }
  }
}

class PartitionAccumulator(
    val partitionKey: IcebergPartitionStatsExtractor.PartitionKey,
    val confName: String,
    val schema: org.apache.iceberg.Schema
)(implicit val partitionSpec: PartitionSpec) {
  var totalRowCount: Long = 0L
  val columnStats = mutable.Map[Int, ColumnStats]()

  def addFileStats(rowCount: Long, fileColumnStats: Map[Int, ColumnStats]): Unit = {
    totalRowCount += rowCount

    fileColumnStats.foreach { case (fieldId, stats) =>
      val currentStats = columnStats.getOrElse(fieldId, ColumnStats(0L))
      columnStats(fieldId) = currentStats.aggregate(stats)
    }
  }

  def toTileSummaries: Map[TileKey, TileSummary] = {
    columnStats.map { case (fieldId, stats) =>
      // Create partition key string from partition values
      val partitionKeyStr = partitionKey.map { case (col, value) => s"$col=$value" }.mkString("/")

      val tileKey = new TileKey()
        .setColumn(fieldId.toString)
        .setName(confName)
        .setSlice(partitionKeyStr) // Use slice to store partition key
        .setSizeMillis(partitionSpec.spanMillis)

      val tileSummary = new TileSummary()
        .setCount(totalRowCount)
        .setNullCount(stats.nullCount)

      tileKey -> tileSummary
    }.toMap
  }
}

class IcebergPartitionStatsExtractor(spark: SparkSession) {
  import IcebergPartitionStatsExtractor.PartitionKey

  def extractPartitionedStats(fullTableName: String, confName: String)(implicit
      partitionSpec: PartitionSpec): Map[TileKey, TileSummary] = {
    val parsed = spark.sessionState.sqlParser.parseMultipartIdentifier(fullTableName)
    val (catalogName, namespace, tableName) = parsed.toList match {
      case catalog :: namespace :: table :: Nil => (catalog, namespace, table)
      case namespace :: table :: Nil            => (spark.catalog.currentCatalog(), namespace, table)
      case table :: Nil                         => (spark.catalog.currentCatalog(), "default", table)
      case _ => throw new IllegalStateException(s"Invalid table naming convention specified: $fullTableName")
    }

    val catalog = spark.sessionState.catalogManager
      .catalog(catalogName)
      .asInstanceOf[TableCatalog]

    val tableId = Identifier.of(Array(namespace), tableName)
    val table = Option(catalog.loadTable(tableId)) match {
      case Some(sparkTable: SparkTable) => sparkTable.table()
      case _ => throw new IllegalStateException(s"Unsupported table type for Iceberg extraction: $fullTableName")
    }

    val tableSpec = Option(table.spec())
      .getOrElse(throw new IllegalStateException(s"Table spec is null for table: ${table.name()}"))

    require(
      tableSpec.isPartitioned,
      s"Illegal request to compute partition-stats of an un-partitioned table: ${table.name()}."
    )

    val currentSnapshot = Option(table.currentSnapshot())

    val partitionAccumulators = mutable.Map[PartitionKey, PartitionAccumulator]()

    currentSnapshot.foreach { snapshot =>
      val manifestFiles = snapshot.allManifests(table.io()).asScala
      manifestFiles.foreach { manifestFile =>
        val manifestReader = ManifestFiles.read(manifestFile, table.io())

        try {
          manifestReader.forEach((file: DataFile) => {

            val rowCount: Long = file.recordCount()
            val schema = Option(table.schema())
              .getOrElse(throw new IllegalStateException("Table schema is null"))
            val specs = Option(table.specs())
              .getOrElse(throw new IllegalStateException("Table specs is null"))
            val icebergPartitionSpec: org.apache.iceberg.PartitionSpec = Option(specs.get(file.specId()))
              .getOrElse(throw new IllegalStateException(s"Partition spec not found for specId: ${file.specId()}"))
            val partitionFieldIds = Option(icebergPartitionSpec.fields())
              .map(_.asScala.map(_.sourceId()).toSet)
              .getOrElse(Set.empty[Int])

            // Extract partition key
            val partitionColToValue: PartitionKey = icebergPartitionSpec
              .fields()
              .asScala
              .zipWithIndex
              .flatMap { case (field, index) =>
                Option(schema.findField(field.sourceId())).map { sourceField =>
                  val partition = Option(file.partition())
                    .getOrElse(throw new IllegalStateException("File partition data is null"))
                  val partitionValue = Option(partition.get(index, classOf[Object]))
                    .map(String.valueOf)
                    .getOrElse("null")
                  sourceField.name() -> partitionValue
                }
              }
              .toList

            // Extract column statistics for this file
            val columnStats = extractColumnStats(file, schema, partitionFieldIds)

            // Get or create partition accumulator and add file stats
            val accumulator = partitionAccumulators.getOrElseUpdate(
              partitionColToValue,
              new PartitionAccumulator(partitionColToValue, confName, schema)
            )
            accumulator.addFileStats(rowCount, columnStats)
          })
        } finally {
          manifestReader.close()
        }
      }
    }

    // Convert accumulators to TileKey -> TileSummary mapping
    val result = mutable.Map[TileKey, TileSummary]()

    partitionAccumulators.values.foreach { accumulator =>
      result ++= accumulator.toTileSummaries
    }

    result.toMap
  }

  private def extractColumnStats(
      file: DataFile,
      schema: org.apache.iceberg.Schema,
      partitionFieldIds: Set[Int]
  ): Map[Int, ColumnStats] = {

    val columnStatsMap = mutable.Map[Int, ColumnStats]()

    // Extract null counts
    val nullCounts = Option(file.nullValueCounts())
      .map(
        _.asScala
          .filterNot { case (fieldId, _) => partitionFieldIds.contains(fieldId) }
          .map { case (fieldId, nullCount) => fieldId.toInt -> nullCount.toLong }
          .toMap)
      .getOrElse(Map.empty[Int, Long])

    // Extract lower bounds (min values)
    val lowerBounds = Option(file.lowerBounds())
      .map(
        _.asScala
          .filterNot { case (fieldId, _) => partitionFieldIds.contains(fieldId) }
          .flatMap { case (fieldId, bound) =>
            Option(schema.findField(fieldId)).flatMap { field =>
              Option(bound).filter(_ != null).map { validBound =>
                fieldId.toInt -> convertBoundValue(validBound, field.`type`())
              }
            }
          }
          .toMap)
      .getOrElse(Map.empty[Int, Any])

    // Extract upper bounds (max values)
    val upperBounds = Option(file.upperBounds())
      .map(
        _.asScala
          .filterNot { case (fieldId, _) => partitionFieldIds.contains(fieldId) }
          .flatMap { case (fieldId, bound) =>
            Option(schema.findField(fieldId)).flatMap { field =>
              Option(bound).filter(_ != null).map { validBound =>
                fieldId.toInt -> convertBoundValue(validBound, field.`type`())
              }
            }
          }
          .toMap)
      .getOrElse(Map.empty[Int, Any])

    // Combine all statistics using fieldIds as keys
    // This ensures deterministic ordering since we use fieldId as the key
    val allFieldIds =
      (nullCounts.keySet ++ lowerBounds.keySet ++ upperBounds.keySet)

    allFieldIds.foreach { fieldId =>
      val nullCount = nullCounts.getOrElse(fieldId, 0L)
      val minValue = lowerBounds.get(fieldId)
      val maxValue = upperBounds.get(fieldId)

      val distinctCount: Option[Long] = None

      columnStatsMap(fieldId) = ColumnStats(
        nullCount = nullCount,
        distinctCount = distinctCount,
        minValue = minValue,
        maxValue = maxValue
      )
    }

    columnStatsMap.toMap
  }

  private[iceberg] def convertBoundValue(bound: java.nio.ByteBuffer, fieldType: org.apache.iceberg.types.Type): Any = {
    require(bound != null, "bound cannot be null")
    require(fieldType != null, "fieldType cannot be null")
    org.apache.iceberg.types.Conversions.fromByteBuffer(fieldType, bound)
  }
}
