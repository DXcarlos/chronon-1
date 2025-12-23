package ai.chronon.integrations.aws

import ai.chronon.spark.catalog.{DefaultFormatProvider, Format, Iceberg}
import org.apache.iceberg.aws.glue.GlueCatalog
import org.apache.iceberg.spark.SparkCatalog
import org.apache.spark.sql.SparkSession

/** AWS-specific FormatProvider that handles Iceberg tables with AWS Glue Catalog.
  *
  * When using Iceberg with Glue, tables are managed through Iceberg's catalog
  * and should be treated as Iceberg format rather than Hive format.
  *
  * Configuration example:
  * {{{
  * spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog
  * spark.sql.catalog.spark_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
  * spark.sql.catalog.spark_catalog.warehouse=s3://bucket/path/
  * }}}
  */
class AwsFormatProvider(override val sparkSession: SparkSession) extends DefaultFormatProvider(sparkSession) {

  override def readFormat(tableName: String): Option[Format] = {
    // Check if Iceberg format is explicitly configured
    val configuredFormat = sparkSession.conf.getOption("spark.chronon.table_write.format")
    if (configuredFormat.exists(_.equalsIgnoreCase("iceberg"))) {
      logger.info(
        s"Iceberg format configured via spark.chronon.table_write.format, using Iceberg for table: $tableName")
      return Some(Iceberg)
    }

    try {
      val parsedCatalog = Format.getCatalog(tableName)(sparkSession)
      logger.info(s"Parsed catalog for table $tableName: $parsedCatalog")

      val cat = sparkSession.sessionState.catalogManager.catalog(parsedCatalog)
      logger.info(s"Catalog type for $tableName: ${cat.getClass.getName}")

      cat match {
        case iceberg: SparkCatalog =>
          logger.info(s"Found SparkCatalog for $tableName")
          val icebergCat = iceberg.icebergCatalog()
          logger.info(s"Iceberg catalog type: ${icebergCat.getClass.getName}")

          if (icebergCat.isInstanceOf[GlueCatalog]) {
            // This is an Iceberg table backed by AWS Glue
            logger.info(s"Detected Iceberg table with Glue catalog: $tableName")
            Some(Iceberg)
          } else {
            logger.info(s"SparkCatalog found but not using GlueCatalog, falling back to default")
            super.readFormat(tableName)
          }
        case _ =>
          logger.info(s"Not a SparkCatalog (${cat.getClass.getName}), falling back to default format detection")
          // Fall back to default format detection
          super.readFormat(tableName)
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error detecting format for table $tableName: ${e.getMessage}", e)
        throw e
    }
  }
}
