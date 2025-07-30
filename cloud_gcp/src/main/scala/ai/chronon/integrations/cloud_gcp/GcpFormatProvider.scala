package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark.catalog.{DefaultFormatProvider, Format, Iceberg}
import com.google.cloud.bigquery._
import com.google.cloud.spark.bigquery.v2.Spark31BigQueryTable
import org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog
import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetTable

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class GcpFormatProvider(override val sparkSession: SparkSession) extends DefaultFormatProvider(sparkSession) {

  /** Order of Precedence for Default Project:
    * - Explicitly configured project in code (e.g., setProjectId()).
    * - GOOGLE_CLOUD_PROJECT environment variable.
    * - project_id from the ADC service account JSON file.
    * - Active project in the gcloud CLI configuration.
    * - No default project: An error will occur if no project ID is available.
    */

  override def readFormat(tableName: String): scala.Option[Format] = {
    val parsedCatalog = Format.getCatalog(tableName)(sparkSession)
    val identifier = SparkBQUtils.toIdentifier(tableName)(sparkSession)
    val cat = sparkSession.sessionState.catalogManager.catalog(parsedCatalog)
    cat match {
      case delegating: DelegatingBigQueryMetastoreCatalog =>
        Try {
          val tbl = delegating.loadTable(identifier)
          tbl match {
            case iceberg: SparkTable            => Iceberg
            case bigquery: Spark31BigQueryTable => BigQueryNative
            case parquet: ParquetTable          => BigQueryExternal
            case unsupported => throw new IllegalStateException(s"Unsupported provider type: ${unsupported}")
          }
        } match {
          case s @ Success(_)     => s.toOption
          case Failure(exception) => throw exception
        }
      case iceberg: SparkCatalog if (iceberg.icebergCatalog().isInstanceOf[BigQueryMetastoreCatalog]) =>
        scala.Option(Iceberg)
      case _ => super.readFormat(tableName)
    }
  }
}
