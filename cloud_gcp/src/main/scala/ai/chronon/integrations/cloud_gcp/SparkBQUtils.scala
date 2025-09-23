package ai.chronon.integrations.cloud_gcp
import com.google.cloud.bigquery.connector.common.BigQueryUtil
import org.apache.spark.sql.SparkSession
import com.google.cloud.bigquery.TableId
import org.apache.spark.sql.connector.catalog.Identifier

object SparkBQUtils {

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
