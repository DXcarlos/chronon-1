package ai.chronon.integrations.cloud_gcp
import ai.chronon.spark.catalog.ChrononSparkConf
import com.google.cloud.bigquery.{BigQueryOptions, TableId}
import com.google.cloud.bigquery.connector.common.BigQueryUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Identifier

object SparkBQUtils {

  /** Resolve the BigQuery data project from spark catalog config, falling back to BQ client default.
    * On non-Dataproc environments the BQ client default is the host project, not the data project.
    */
  def resolveDefaultProject(implicit spark: SparkSession): String = {
    ChrononSparkConf.get(spark, "spark.sql.catalog.spark_catalog.gcp.bigquery.project-id",
      BigQueryOptions.getDefaultInstance.getProjectId)
  }

  /** Resolve the BigQuery project for a specific table.
    * Priority: explicit project in table name > spark catalog config > BQ client default.
    */
  def resolveProject(tableId: TableId)(implicit spark: SparkSession): String = {
    scala.Option(tableId.getProject).getOrElse(resolveDefaultProject)
  }

  def toTableId(tableName: String)(implicit spark: SparkSession): TableId = {
    val parseIdentifier = spark.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    val shadedTid = BigQueryUtil.parseTableId(parseIdentifier.mkString("."))
    scala
      .Option(shadedTid.getProject)
      .map(TableId.of(_, shadedTid.getDataset, shadedTid.getTable))
      .getOrElse(TableId.of(shadedTid.getDataset, shadedTid.getTable))
  }

  def toIdentifier(tableName: String)(implicit spark: SparkSession): Identifier = {
    val parseIdentifier = spark.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    Identifier.of(parseIdentifier.init.toArray, parseIdentifier.last)
  }

  def toIdentifierNoCatalog(tableName: String)(implicit spark: SparkSession): Identifier = {
    val identifier = toIdentifier(tableName)
    val namespace = identifier.namespace()
    if (namespace.isEmpty) {
      Identifier.of(Array.empty[String], identifier.name())
    } else {
      Identifier.of(Array(namespace.last), identifier.name())
    }
  }

}
