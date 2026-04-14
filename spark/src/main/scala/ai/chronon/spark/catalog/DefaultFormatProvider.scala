package ai.chronon.spark.catalog

import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

/** Default format provider implementation based on default Chronon supported open source library versions.
  */
class DefaultFormatProvider(val sparkSession: SparkSession) extends FormatProvider {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  // Checks the format of a given table if it exists.
  override def readFormat(tableName: String): Option[Format] = {
    Option(if (isIcebergTable(tableName)) {
      Iceberg
    } else if (isDeltaTable(tableName)) {
      DeltaLake
    } else if (sparkSession.catalog.tableExists(tableName)) {
      Hive
    } else { null })
  }

  // Format detection methods below are part of a waterfall: Iceberg → Delta → Hive.
  // Intermediate "not this format" results are expected and logged at debug level
  // without stack traces. Only the final caller should surface errors.

  protected def isIcebergTable(tableName: String): Boolean = {
    val resolved = Format.resolveTableName(tableName)(sparkSession)
    val catalog = sparkSession.sessionState.catalogManager.catalog(resolved.catalog)

    catalog match {
      case sparkCatalog: SparkCatalog =>
        Try(sparkCatalog.loadTable(resolved.toIdentifier)) match {
          case Success(_: SparkTable) =>
            logger.info(s"Detected iceberg table: $tableName")
            true
          case _ =>
            logger.debug(s"Table $tableName is not iceberg format")
            false
        }
      case tableCatalog: TableCatalog =>
        Try(tableCatalog.loadTable(resolved.toIdentifier)) match {
          case Success(_: SparkTable) =>
            logger.info(s"Detected iceberg table: $tableName")
            true
          case _ =>
            logger.debug(s"Table $tableName is not iceberg format")
            false
        }
      case _ =>
        logger.debug(s"Table $tableName is not iceberg format")
        false
    }
  }

  private def isDeltaTable(tableName: String): Boolean = {
    Try {
      val describeResult = sparkSession.sql(s"DESCRIBE DETAIL $tableName")
      describeResult.select("format").first().getString(0).toLowerCase
    } match {
      case Success(format) =>
        if (format == "delta") logger.info(s"Detected delta table: $tableName")
        format == "delta"
      case Failure(_) =>
        logger.debug(s"Table $tableName is not delta format")
        false
    }
  }
}
