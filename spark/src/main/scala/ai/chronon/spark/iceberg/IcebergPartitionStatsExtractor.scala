package ai.chronon.spark.iceberg

import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.spark.SparkSessionCatalog
import org.apache.iceberg.{DataFile, ManifestFiles, PartitionSpec, Table}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog

import scala.collection.JavaConverters._
import scala.collection.mutable

case class PartitionStats(partitionColToValue: Map[String, String], rowCount: Long, colToNullCount: Map[String, Long])

class IcebergPartitionStatsExtractor(spark: SparkSession) {

  def extractPartitionedStats(catalogName: String, namespace: String, tableName: String): Seq[PartitionStats] = {

    val catalog = spark.sessionState.catalogManager
      .catalog(catalogName)
      .asInstanceOf[SparkSessionCatalog[V2SessionCatalog]]
      .icebergCatalog()

    val tableId: TableIdentifier = TableIdentifier.of(namespace, tableName)
    val table: Table = catalog.loadTable(tableId)

    require(
      table.spec().isPartitioned,
      s"Illegal request to compute partition-stats of an un-partitioned table: ${table.name()}."
    )

    val currentSnapshot = Option(table.currentSnapshot())
    val partitionStatsBuffer = mutable.Buffer[PartitionStats]()

    currentSnapshot.foreach { snapshot =>
      val manifestFiles = snapshot.allManifests(table.io()).asScala
      manifestFiles.foreach { manifestFile =>
        val manifestReader = ManifestFiles.read(manifestFile, table.io())

        try {
          manifestReader.forEach((file: DataFile) => {

            val rowCount: Long = file.recordCount()

            val schema = table.schema()

            val partitionSpec: PartitionSpec = table.specs().get(file.specId())
            val partitionFieldIds = partitionSpec.fields().asScala.map(_.sourceId()).toSet

            val colToNullCount: Map[String, Long] = file
              .nullValueCounts()
              .asScala
              .filterNot { case (fieldId, _) => partitionFieldIds.contains(fieldId) }
              .map { case (fieldId, nullCount) =>
                schema.findField(fieldId).name() -> nullCount.toLong
              }
              .toMap

            val partitionColToValue = partitionSpec
              .fields()
              .asScala
              .zipWithIndex
              .map { case (field, index) =>
                val sourceField = schema.findField(field.sourceId())
                val partitionValue = file.partition().get(index, classOf[String])

                sourceField.name() -> String.valueOf(partitionValue)
              }
              .toMap

            val fileStats = PartitionStats(partitionColToValue, rowCount, colToNullCount)
            partitionStatsBuffer.append(fileStats)
          })
        } finally {
          manifestReader.close()
        }
      }
    }

    partitionStatsBuffer
  }
}
