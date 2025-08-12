package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark.local.SchemaUtils
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, TableId}
import com.google.cloud.spark.bigquery.v2.Spark35BigQueryTableProvider
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession

class BigQuerySchemaUtils(implicit sparkSession: SparkSession) extends SchemaUtils {

  private val bqFormat = classOf[Spark35BigQueryTableProvider].getName
  private lazy val bqOptions = BigQueryOptions.getDefaultInstance
  private lazy val bigQueryClient: BigQuery = bqOptions.getService

  override def getTableSchema(table: String): StructType = {
    // Parse the table name to get BigQuery TableId
    val bqTableId = SparkBQUtils.toTableId(table)
    val providedProject = scala.Option(bqTableId.getProject).getOrElse(bqOptions.getProjectId)

    // Get the table from BigQuery to access its schema
    val bqTable = scala
      .Option(bigQueryClient.getTable(bqTableId))
      .getOrElse(throw new IllegalArgumentException(s"Table $table does not exist in BigQuery"))

    // Use Spark BigQuery connector to get schema by reading with LIMIT 0
    val df = sparkSession.read
      .format(bqFormat)
      .option("project", providedProject)
      .option("table", s"${bqTableId.getDataset}.${bqTableId.getTable}")
      .load()
      .limit(0)

    df.schema
  }

  override def getBigQueryQuerySchema(query: String): StructType = {
    // For BigQuery queries, we need to use the BigQuery connector to execute the query with LIMIT 0
    // to get the schema without actually running the full query

    // Create a temporary view or subquery with LIMIT 0 to get schema
    val schemaQuery = s"SELECT * FROM ($query) LIMIT 1"

    val df = sparkSession.read
      .format(bqFormat)
      .option("project", bqOptions.getProjectId) // .option("project", bqOptions.getProjectId)
      .option("query", schemaQuery)
      .option("viewsEnabled", true)
      .load()

    val schema = df.schema
    schema
  }
}

object BigQuerySchemaUtils {
  def apply()(implicit sparkSession: SparkSession): BigQuerySchemaUtils = {
    new BigQuerySchemaUtils()
  }
}
