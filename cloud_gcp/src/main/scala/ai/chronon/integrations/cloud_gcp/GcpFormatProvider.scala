package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark.catalog.{DefaultFormatProvider, Format, Iceberg}
import com.google.cloud.bigquery.TableId
import org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog
import org.apache.iceberg.spark.SparkCatalog
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

class GcpFormatProvider(override val sparkSession: SparkSession) extends DefaultFormatProvider(sparkSession) {

  override def readFormat(tableName: String): scala.Option[Format] = {
    val resolved = Format.resolveTableName(tableName)(sparkSession)
    val cat = sparkSession.sessionState.catalogManager.catalog(resolved.catalog)
    cat match {
      case iceberg: SparkCatalog if iceberg.icebergCatalog().isInstanceOf[BigQueryMetastoreCatalog] =>
        formatCache.getOrElseUpdate(tableName, {
          logger.info(s"$tableName: detected as Iceberg (BigQuery Metastore)")
          Some(Iceberg)
        })
      case _ =>
        formatCache.getOrElseUpdate(tableName, detectFormat(tableName))
    }
  }

  /** Extend the waterfall with BigQueryNative as the final catch-all. */
  override protected def formatChecks(tableName: String): Seq[(Format, Either[Throwable, Boolean])] = {
    super.formatChecks(tableName) :+ (BigQueryNative -> checkBigQuery(tableName))
  }

  private def checkBigQuery(tableName: String): Either[Throwable, Boolean] = {
    Try {
      implicit val spark: SparkSession = sparkSession
      val tableId = SparkBQUtils.toTableId(tableName)
      val project = SparkBQUtils.resolveProject(tableId)
      val qualifiedId = TableId.of(project, tableId.getDataset, tableId.getTable)
      val table = com.google.cloud.bigquery.BigQueryOptions.getDefaultInstance.getService.getTable(qualifiedId)
      table != null
    }.toEither
  }
}
