package ai.chronon.integrations.cloud_gcp

import com.google.cloud.bigquery.{
  BigQuery,
  BigQueryOptions,
  ExternalTableDefinition,
  StandardTableDefinition,
  ViewDefinition,
  TableDefinition,
  TableId
}
import com.google.cloud.spark.bigquery.BigQueryCatalog
import org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog
import org.apache.iceberg.spark.SparkCatalog
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetTable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.jdk.CollectionConverters._
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import scala.util.{Failure, Success, Try}

/** Galactus catalog that allows us to interact with BigQuery metastore as a spark catalog. This allows for
  * querying of a variety of table types directly in spark sql or the dataframe api.
  * This is analogous to iceberg's [[org.apache.iceberg.spark.SparkSessionCatalog]] in that it will
  * apply a fallback when querying for tables. It will always attempt to load a table reference
  * as an iceberg table first and falling back to bigquery.
  *
  * To interact with iceberg, we use Google's https://cloud.google.com/blog/products/data-analytics/introducing-bigquery-metastore-fully-managed-metadata-service
  * metastore catalog library. By default, all catalog operations will delegate to this library, and this abstraction
  * is meant to remain incredibly thin. BE CAREFUL WHEN OVERRIDING THIS BEHAVIOR. You shouldn't be needing too much additional
  * functionality. Before you do this, consider upgrading the `iceberg_bigquery_catalog_lib` dependency and/or iceberg first.
  *
  * NOTE that this abstraction currently only supports querying tables that all belong to the same GCP project. Multi-project
  * support will depend on underlying libraries to support them.
  */
class DelegatingBigQueryMetastoreCatalog extends TableCatalog with SupportsNamespaces with FunctionCatalog {

  @transient private lazy val bqOptions = BigQueryOptions.getDefaultInstance
  @transient private lazy val bigQueryClient: BigQuery = bqOptions.getService

  @transient private lazy val icebergCatalog: SparkCatalog = new SparkCatalog()
  @transient private lazy val connectorCatalog: BigQueryCatalog = new BigQueryCatalog()

  private var catalogName: String =
    null // This corresponds to `spark_catalog in `spark.sql.catalog.spark_catalog`. This is necessary for spark to correctly choose which implementation to use.

  private var catalogProps: Map[String, String] = Map.empty[String, String]

  override def listNamespaces: Array[Array[String]] = icebergCatalog.listNamespaces()

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = icebergCatalog.listNamespaces(namespace)

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] =
    icebergCatalog.loadNamespaceMetadata(namespace)

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = {
    icebergCatalog.createNamespace(namespace, metadata)
  }

  override def purgeTable(ident: Identifier): Boolean = {
    icebergCatalog.purgeTable(ident)
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    icebergCatalog.alterNamespace(namespace, changes: _*)
  }

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean =
    icebergCatalog.dropNamespace(namespace, cascade)

  override def listTables(namespace: Array[String]): Array[Identifier] = icebergCatalog.listTables(namespace)

  override def loadTable(identNoCatalog: Identifier): Table = {
    Try {
      icebergCatalog.loadTable(identNoCatalog)
    }
      .recover {
        case noIcebergTableEx: NoSuchTableException => {
          val project =
            catalogProps.getOrElse(BigQueryMetastoreCatalog.PROJECT_ID, bqOptions.getProjectId)
          val tId = identNoCatalog.namespace().toList match {
            case database :: Nil            => TableId.of(project, database, identNoCatalog.name())
            case catalog :: database :: Nil => TableId.of(project, database, identNoCatalog.name())
            case Nil =>
              throw new IllegalArgumentException(
                s"Table identifier namespace ${identNoCatalog} must have at least one part.")
          }
          val table = scala
            .Option(bigQueryClient.getTable(tId))
            .getOrElse(throw new NoSuchTableException(s"BigQuery table $identNoCatalog not found."))
          table.getDefinition.asInstanceOf[TableDefinition] match {
            case view: ViewDefinition => {
              throw new UnsupportedOperationException(
                s"Cannot load view $identNoCatalog as a table. Views are not supported in this catalog. ")
            }
            case externalTable: ExternalTableDefinition => {
              val uris = externalTable.getSourceUris.asScala
              val uri = scala
                .Option(externalTable.getHivePartitioningOptions)
                .map(_.getSourceUriPrefix)
                .getOrElse {
                  require(uris.size == 1, s"External table ${table} can be backed by only one URI.")
                  uris.head.replaceAll("/\\*\\.parquet$", "")
                }

              val fileBasedTable = ParquetTable(
                tId.toString,
                SparkSession.active,
                new CaseInsensitiveStringMap(
                  Map(TableCatalog.PROP_EXTERNAL -> "true",
                      TableCatalog.PROP_LOCATION -> uri,
                      TableCatalog.PROP_PROVIDER -> "PARQUET").asJava),
                List(uri),
                None,
                classOf[ParquetFileFormat]
              )
              fileBasedTable
            }
            case _: StandardTableDefinition => {
              // todo(tchow): Support partitioning

              // Hack because there's a bug in the BigQueryCatalog where they ignore the projectId.
              // See: https://github.com/GoogleCloudDataproc/spark-bigquery-connector/pull/1340
              // ideally it should be the below:
              // val connectorTable = connectorCatalog.loadTable(ident)
              connectorCatalog.loadTable(Identifier.of(Array(tId.getDataset), tId.getTable))
            }
            case _ => throw new IllegalStateException(s"Cannot support table of type: ${table.getDefinition}")
          }
        }
        case other: Throwable => throw other
      } match {
      case Success(table)     => table
      case Failure(exception) => throw exception
    }
  }

  override def createTable(ident: Identifier,
                           schema: StructType,
                           partitions: Array[Transform],
                           properties: util.Map[String, String]): Table = {
    val provider = properties.get(TableCatalog.PROP_PROVIDER)
    if (provider.toUpperCase != "ICEBERG") {
      throw new UnsupportedOperationException("Only creating iceberg tables supported.")
    }
    icebergCatalog.createTable(ident, schema, partitions, properties)
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    icebergCatalog.alterTable(ident, changes: _*)
  }

  override def dropTable(ident: Identifier): Boolean = icebergCatalog.dropTable(ident)

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    icebergCatalog.renameTable(oldIdent, newIdent)
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    icebergCatalog.initialize(name, options)
    connectorCatalog.initialize(name, options)
    catalogName = name
    catalogProps = options.asCaseSensitiveMap.asScala.toMap
  }

  override def name(): String = catalogName

  override def listFunctions(namespace: Array[String]): Array[Identifier] = icebergCatalog.listFunctions(namespace)

  override def loadFunction(ident: Identifier): UnboundFunction = icebergCatalog.loadFunction(ident)
}
