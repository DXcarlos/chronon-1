package ai.chronon.spark.local

import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.types.StructType

trait SchemaUtils {
  // Abstract methods for BigQuery implementations
  def getBigQueryQuerySchema(query: String): StructType
  // Abstract methods for Spark implementations
  def getTableSchema(table: String): StructType
}
