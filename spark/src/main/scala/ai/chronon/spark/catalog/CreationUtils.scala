package ai.chronon.spark.catalog

import org.apache.spark.sql.types.StructType

object CreationUtils {

  private val ALLOWED_TABLE_TYPES = List("iceberg", "delta", "hive", "parquet", "hudi")

  /** Escapes a string value for use in SQL by doubling single quotes.
    * This follows the SQL standard for escaping string literals.
    *
    * @param value The string value to escape
    * @return The escaped string safe for SQL string literals
    */
  def escapeSqlStringValue(value: String): String = {
    value.replace("'", "''")
  }

  def createTableSql(tableName: String,
                     schema: StructType,
                     partitionColumns: List[String],
                     tableProperties: Map[String, String],
                     tableTypeString: String,
                     clusterByColumns: List[String] = List.empty): String = {

    require(
      tableTypeString.isEmpty || ALLOWED_TABLE_TYPES.contains(tableTypeString.toLowerCase),
      s"Invalid table type: ${tableTypeString}. Must be empty OR one of: ${ALLOWED_TABLE_TYPES}"
    )

    val useClusterBy = clusterByColumns != null && clusterByColumns.nonEmpty

    // Liquid clustering keeps the clustering columns (e.g. ds) as regular data columns rather
    // than extracting them into a Hive-style partition spec, since CLUSTER BY has no notion of
    // physically separate partition directories.
    val dataSchema =
      if (useClusterBy) schema
      else StructType(schema.filterNot(field => partitionColumns.contains(field.name)))

    val createFragment =
      s"""CREATE TABLE IF NOT EXISTS $tableName (
         |    ${dataSchema.toDDL}
         |)
         |${if (tableTypeString.isEmpty) "" else f"USING ${tableTypeString}"}
         |""".stripMargin

    val layoutFragment = if (useClusterBy) {
      s"""CLUSTER BY (
         |    ${clusterByColumns.mkString(",\n    ")}
         |)""".stripMargin
    } else if (partitionColumns != null && partitionColumns.nonEmpty) {

      val partitionDefinitions = schema
        .filter(field => partitionColumns.contains(field.name))
        .map(field => s"${field.name} ${field.dataType.catalogString}")

      s"""PARTITIONED BY (
         |    ${partitionDefinitions.mkString(",\n    ")}
         |)""".stripMargin

    } else {
      ""
    }

    val propertiesFragment = if (tableProperties != null && tableProperties.nonEmpty) {
      s"""TBLPROPERTIES (
         |    ${(tableProperties + ("file_format" -> "PARQUET") + ("table_type" -> tableTypeString))
          .transform((k, v) => s"'${escapeSqlStringValue(k)}'='${escapeSqlStringValue(v)}'")
          .values
          .mkString(",\n   ")}
         |)""".stripMargin
    } else {
      ""
    }

    Seq(createFragment, layoutFragment, propertiesFragment).mkString("\n")

  }

  // Needs provider
  def alterTablePropertiesSql(tableName: String, properties: Map[String, String]): String = {
    // Only SQL api exists for setting TBLPROPERTIES
    val propertiesString = properties
      .map { case (key, value) =>
        s"'${escapeSqlStringValue(key)}' = '${escapeSqlStringValue(value)}'"
      }
      .mkString(", ")
    s"ALTER TABLE $tableName SET TBLPROPERTIES ($propertiesString)"
  }

}
