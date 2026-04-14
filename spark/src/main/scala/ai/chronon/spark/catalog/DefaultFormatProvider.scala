package ai.chronon.spark.catalog

import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/** Default format provider implementation based on default Chronon supported open source library versions.
  *
  * Format detection is a waterfall: Iceberg → Delta → Hive. Each step returns either
  * a detected format or the exception that occurred. If the waterfall succeeds, we log
  * one info line. If all steps fail, we log all exceptions with full stack traces.
  */
class DefaultFormatProvider(val sparkSession: SparkSession) extends FormatProvider {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  // Per-table format cache — avoids repeated detection within the same driver process
  protected val formatCache = mutable.Map[String, Option[Format]]()

  override def readFormat(tableName: String): Option[Format] = {
    formatCache.getOrElseUpdate(tableName, detectFormat(tableName))
  }

  /** Run the format detection waterfall. Returns the detected format or None. */
  protected def detectFormat(tableName: String): Option[Format] = {
    val checks = formatChecks(tableName)
    for ((format, check) <- checks) {
      check match {
        case Right(true) =>
          logger.info(s"$tableName: detected as $format")
          return Some(format)
        case _ => // continue waterfall
      }
    }

    // All checks failed — log summary at warn, then full stack traces for each failure
    val summary = checks.map {
      case (format, Right(false))  => s"  - not $format"
      case (format, Left(e))       => s"  - not $format: ${e.getMessage.takeWhile(_ != '\n')}"
      case (format, Right(true))   => s"  - $format (matched)"
    }
    logger.warn(s"$tableName: no format detected\n${summary.mkString("\n")}")
    for ((format, Left(e)) <- checks) {
      logger.debug(s"$tableName: $format check failure", e)
    }
    None
  }

  /** The ordered list of format checks. Each returns Right(true) for match,
    * Right(false) for clean non-match, or Left(exception) for failure.
    * Subclasses can override to extend the waterfall.
    */
  protected def formatChecks(tableName: String): Seq[(Format, Either[Throwable, Boolean])] = {
    Seq(
      Iceberg -> checkIceberg(tableName),
      DeltaLake -> checkDelta(tableName),
      Hive -> checkHive(tableName)
    )
  }

  protected def checkIceberg(tableName: String): Either[Throwable, Boolean] = {
    Try {
      val resolved = Format.resolveTableName(tableName)(sparkSession)
      val catalog = sparkSession.sessionState.catalogManager.catalog(resolved.catalog)
      catalog match {
        case sparkCatalog: SparkCatalog =>
          sparkCatalog.loadTable(resolved.toIdentifier).isInstanceOf[SparkTable]
        case tableCatalog: TableCatalog =>
          tableCatalog.loadTable(resolved.toIdentifier).isInstanceOf[SparkTable]
        case _ => false
      }
    }.toEither
  }

  protected def checkDelta(tableName: String): Either[Throwable, Boolean] = {
    Try {
      val describeResult = sparkSession.sql(s"DESCRIBE DETAIL $tableName")
      describeResult.select("format").first().getString(0).toLowerCase == "delta"
    }.toEither
  }

  protected def checkHive(tableName: String): Either[Throwable, Boolean] = {
    Try(sparkSession.catalog.tableExists(tableName)).toEither
  }
}
