package ai.chronon.spark.catalog

import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.functions.{col, lower}
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

  protected def isIcebergTable(tableName: String): Boolean = {
    if (isSparkConnectSession) {
      return isIcebergTableViaSql(tableName)
    }

    Try {
      val resolved = Format.resolveTableName(tableName)(sparkSession)
      val catalog = sparkSession.sessionState.catalogManager.catalog(resolved.catalog)

      catalog match {
        case sparkCatalog: SparkCatalog =>
          sparkCatalog.loadTable(resolved.toIdentifier).isInstanceOf[SparkTable]
        case tableCatalog: TableCatalog =>
          tableCatalog.loadTable(resolved.toIdentifier).isInstanceOf[SparkTable]
        case _ =>
          false
      }
    } match {
      case Success(true) =>
        logger.info(s"IcebergCheck: Detected iceberg formatted table $tableName.")
        true
      case Success(false) =>
        logger.info(s"IcebergCheck: Checked table $tableName is not iceberg format.")
        false
      case Failure(e) =>
        logger.info(s"IcebergCheck: Unable to inspect table $tableName via catalog API: ${e.getMessage}", e)
        false
    }
  }

  private def isSparkConnectSession: Boolean =
    sys.env.get("SPARK_CONNECT_URL").exists(_.nonEmpty) ||
      sys.env.get("SPARK_REMOTE").exists(_.nonEmpty) ||
      Try(sparkSession.conf.get("spark.remote")).toOption.exists(_.nonEmpty) ||
      sparkSession.getClass.getName.startsWith("org.apache.spark.sql.connect.")

  private def isIcebergTableViaSql(tableName: String): Boolean = {
    val resolvedTableName = Try {
      val resolved = Format.resolveTableName(tableName)(sparkSession)
      s"${QuotingUtils.quoteIdentifier(resolved.catalog)}.${QuotingUtils.quoteIdentifier(resolved.namespace)}.${QuotingUtils.quoteIdentifier(resolved.table)}"
    } match {
      case Success(resolved) => resolved
      case Failure(e) =>
        logger.info(s"IcebergCheck: Unable to resolve table $tableName for SQL metadata inspection: ${e.getMessage}", e)
        return false
    }

    Try {
      sparkSession
        .sql(s"DESCRIBE EXTENDED $resolvedTableName")
        .filter(lower(col("col_name")).contains("provider") &&
          lower(col("data_type")).contains("iceberg"))
        .take(1)
        .nonEmpty
    } match {
      case Success(true) =>
        logger.info(s"IcebergCheck: Detected iceberg formatted table $resolvedTableName via SQL metadata.")
        true
      case Success(false) =>
        logger.info(s"IcebergCheck: Checked table $resolvedTableName via SQL metadata is not iceberg format.")
        false
      case Failure(e) =>
        logger.info(s"IcebergCheck: Unable to inspect table $resolvedTableName via SQL metadata: ${e.getMessage}", e)
        false
    }
  }

  private def isDeltaTable(tableName: String): Boolean = {
    Try {
      val describeResult = sparkSession.sql(s"DESCRIBE DETAIL $tableName")
      describeResult.select("format").first().getString(0).toLowerCase
    } match {
      case Success(format) =>
        logger.info(s"Delta check: Successfully read the format of table: $tableName as $format")
        format == "delta"
      case Failure(e) =>
        logger.info(
          s"Delta check: Unable to read the format of the table $tableName using DESCRIBE DETAIL. Error: ${e.getMessage}",
          e)
        false
    }
  }
}
