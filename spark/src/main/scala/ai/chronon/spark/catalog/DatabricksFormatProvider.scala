package ai.chronon.spark.catalog

import org.apache.spark.sql.SparkSession

import scala.util.Try

/** Format provider for Databricks Runtime environments (AWS, Azure, or GCP workspaces).
  *
  * Extends [[DefaultFormatProvider]] with a single behavioral change: when a table is
  * detected as Delta format (via `DESCRIBE DETAIL`), returns [[DatabricksDeltaLake]] instead
  * of the OSS [[DeltaLake]] object. This avoids the `NoClassDefFoundError` on
  * `org.apache.spark.sql.delta.DeltaLog` that occurs because Databricks Runtime ships its
  * own internal Delta implementation rather than the OSS `delta-spark` package.
  *
  * To use this provider, set the Spark config:
  * {{{
  *   spark.chronon.table.format_provider.class=ai.chronon.spark.catalog.DatabricksFormatProvider
  * }}}
  *
  * This lives in the `spark` module (not in a cloud-specific module) because it has no
  * cloud-specific dependencies — it works on any Databricks workspace regardless of the
  * underlying cloud provider.
  */
class DatabricksFormatProvider(override val sparkSession: SparkSession) extends DefaultFormatProvider(sparkSession) {

  override def readFormat(tableName: String): Option[Format] = {
    Option(
      if (isIcebergTable(tableName)) {
        Iceberg
      } else if (isDeltaTable(tableName)) {
        DatabricksDeltaLake
      } else if (sparkSession.catalog.tableExists(tableName)) {
        Hive
      } else {
        null
      }
    )
  }

  override def writeFormat: Format = {
    val typeString = sparkSession.conf.get("spark.chronon.table_write.format", "").toLowerCase
    typeString match {
      case "delta" => DatabricksDeltaLake
      case other   => FormatProvider.formatFromTypeString(other)
    }
  }

  // Reuse parent's isDeltaTable (DESCRIBE DETAIL) — works on Databricks
  // Reuse parent's isIcebergTable — works on Databricks with Iceberg catalogs
}
