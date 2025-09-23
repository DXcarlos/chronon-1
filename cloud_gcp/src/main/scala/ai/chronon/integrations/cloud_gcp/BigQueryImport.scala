package ai.chronon.integrations.cloud_gcp

import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api.PartitionRange
import ai.chronon.spark.batch.StagingQuery
import ai.chronon.spark.catalog.{Format, TableUtils}
import scala.util.{Try, Failure, Success}
import com.google.cloud.bigquery.{
  BigQuery,
  BigQueryOptions,
  ExternalTableDefinition,
  FormatOptions,
  HivePartitioningOptions,
  JobInfo,
  QueryJobConfiguration,
  TableInfo,
  BigQueryException
}

class BigQueryImport(stagingQueryConf: api.StagingQuery, endPartition: String, tableUtils: TableUtils)
    extends StagingQuery(stagingQueryConf: api.StagingQuery, endPartition: String, tableUtils: TableUtils) {

  private lazy val bqOptions = BigQueryOptions.getDefaultInstance
  private[cloud_gcp] lazy val bigQueryClient: BigQuery = bqOptions.getService

  private val formatStr = "parquet"

  private[cloud_gcp] lazy val basePrefix = {
    val catalogName = Format.getCatalog(outputTable)(tableUtils.sparkSession)
    val warehouseLocation = tableUtils.sparkSession.sessionState.conf
      .getConfString(s"spark.sql.catalog.${catalogName}.warehouse")
      .stripSuffix("/")
    s"${warehouseLocation}/export/${outputTable.sanitize}"
  }

  private[cloud_gcp] lazy val sourcePrefix = s"${basePrefix}/*.${formatStr}"

  private[cloud_gcp] def destPrefix(datePartitionColumn: String, datePartitionValue: String) =
    s"${basePrefix}/${datePartitionColumn}=${datePartitionValue}/*.${formatStr}"

  private[cloud_gcp] def exportDataTemplate(uri: String, sql: String, setups: Seq[String]): String = {

    // Requirements for the sql string:
    // `ds` cannot be part of the projection, it is reserved for chronon.
    // It can be part of the WHERE clause.
    val setupStatements = setups.map(setup => s"${setup};").mkString("\n")
    val multiStatementQuery = if (setups.nonEmpty) {
      s"""BEGIN
         |${setupStatements}
         |
         |EXPORT DATA
         |  OPTIONS (
         |    uri = '${uri}',
         |    format = '${formatStr}',
         |    overwrite = true
         |    )
         |AS (
         |   ${sql}
         |);
         |END;""".stripMargin
    } else {
      f"""
       |EXPORT DATA
       |  OPTIONS (
       |    uri = '${uri}',
       |    format = '${formatStr}',
       |    overwrite = true
       |    )
       |AS (
       |   ${sql}
       |);
       |""".stripMargin
    }
    multiStatementQuery
  }

  override def compute(range: PartitionRange, setups: Seq[String], enableAutoExpand: Option[Boolean]): Unit = {
    // Export data for all partitions sequentially
    range.partitions.foreach { currPart =>
      val renderedQuery =
        StagingQuery.substitute(
          tableUtils,
          stagingQueryConf.query,
          currPart,
          currPart,
          endPartition
        )
      val destPath =
        destPrefix(range.partitionSpec.column, currPart)
      val exportTemplate =
        exportDataTemplate(destPath, renderedQuery, setups)
      logger.info(s"Rendered Staging Query to run is:\n$exportTemplate")
      val exportConf = QueryJobConfiguration.of(exportTemplate)
      val exportJobTry = Try {
        val job = bigQueryClient.create(JobInfo.of(exportConf))
        job.waitFor()
      }.flatMap { job =>
        scala.Option(job.getStatus.getError) match {
          case Some(err) => Failure(new RuntimeException(s"Error exporting data to BigQuery ${err.getMessage}"))
          case None      => Success(job)
        }
      }

      exportJobTry match {
        case Success(_) =>
          logger.info(s"Successfully exported partition: ${range.partitionSpec.column}=${currPart}")
        case Failure(exception) =>
          throw exception
      }
    }

    // Create the table in bigquery catalog once at the end after all partitions are exported
    val createTableTry = Try {
      val formatOptions = FormatOptions.of(formatStr.toUpperCase)
      val hivePartitioningOptions =
        HivePartitioningOptions
          .newBuilder()
          .setSourceUriPrefix(basePrefix)
          .setMode("AUTO")
          .setRequirePartitionFilter(true)
          .build()
      val tableDefinition = ExternalTableDefinition
        .of(sourcePrefix, formatOptions)
        .toBuilder
        .setAutodetect(true)
        .setHivePartitioningOptions(hivePartitioningOptions)
        .build
      val tableInfo = TableInfo.of(SparkBQUtils.toTableId(outputTable)(tableUtils.sparkSession), tableDefinition)
      bigQueryClient.create(tableInfo)
    }

    val existingTable = createTableTry match {
      case Success(table) =>
        logger.info(s"Created table ${table.getTableId} with all partitions: $range")
        table
      case Failure(exception) =>
        exception match {
          case b: BigQueryException if (b.getMessage.contains("Already Exists")) =>
            logger.info(s"Table already exists: $outputTable")
            bigQueryClient.getTable(SparkBQUtils.toTableId(outputTable)(tableUtils.sparkSession))
          case _ => throw exception
        }
    }

    logger.info(
      s"Wrote to table $outputTable as bigquery table ${existingTable.getTableId.toString}, into partitions: $range")
    logger.info(s"Finished writing Staging Query data to $outputTable")
  }

}
