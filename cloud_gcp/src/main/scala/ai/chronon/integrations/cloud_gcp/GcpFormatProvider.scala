package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark.catalog.{DefaultFormatProvider, Format, Iceberg}
import org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog
import org.apache.iceberg.spark.SparkCatalog
import org.apache.spark.sql.SparkSession

class GcpFormatProvider(override val sparkSession: SparkSession) extends DefaultFormatProvider(sparkSession) {

  override def readFormat(tableName: String): scala.Option[Format] = {
    val resolved = Format.resolveTableName(tableName)(sparkSession)
    val cat = sparkSession.sessionState.catalogManager.catalog(resolved.catalog)
    cat match {
      case iceberg: SparkCatalog if (iceberg.icebergCatalog().isInstanceOf[BigQueryMetastoreCatalog]) =>
        scala.Option(Iceberg)
      case _ => super.readFormat(tableName).orElse(scala.Option(BigQueryNative))
    }
  }
}
