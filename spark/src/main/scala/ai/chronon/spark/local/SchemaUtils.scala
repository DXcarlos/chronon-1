package ai.chronon.spark.local

import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.types.StructType

trait SchemaUtils {

  // Abstract methods for BigQuery implementations
  def getBigQueryTableSchema(table: String): StructType
  def getBigQueryQuerySchema(query: String): StructType

  // Concrete Spark implementations
  def getSparkTableSchema(table: String)(implicit tableUtils: TableUtils): StructType = {
    tableUtils.sql(s"SELECT * FROM $table LIMIT 0").schema
  }

  def getSparkQuerySchema(query: String)(implicit tableUtils: TableUtils): StructType = {
    tableUtils.sql(query).schema
  }

}
