package ai.chronon.spark.catalog

import org.apache.spark.sql.types.StructType

object CreationUtils {

  private val ALLOWED_TABLE_TYPES = List("iceberg", "delta", "hive", "parquet", "hudi")

  def createTableSql(tableName: String,
                     schema: StructType,
                     partitionColumns: List[String],
                     tableProperties: Map[String, String],
                     fileFormatString: String,
                     tableTypeString: String): String = {

    require(
      tableTypeString.isEmpty || ALLOWED_TABLE_TYPES.contains(tableTypeString.toLowerCase),
      s"Invalid table type: ${tableTypeString}. Must be empty OR one of: ${ALLOWED_TABLE_TYPES}"
    )

    val noPartitions = StructType(
      schema
        .filterNot(field => partitionColumns.contains(field.name)))

    val createFragment =
      s"""CREATE TABLE $tableName (
         |    ${noPartitions.toDDL}
         |)
         |${if (tableTypeString.isEmpty) "" else f"USING ${tableTypeString}"}
         |""".stripMargin

    val partitionFragment = if (partitionColumns != null && partitionColumns.nonEmpty) {

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
         |    ${(tableProperties + ("file_format" -> fileFormatString) + ("table_type" -> tableTypeString))
          .transform((k, v) => s"'$k'='$v'")
          .values
          .mkString(",\n   ")}
         |)""".stripMargin
    } else {
      ""
    }

    Seq(createFragment, partitionFragment, propertiesFragment).mkString("\n")

  }

  // Needs provider
  def alterTablePropertiesSql(tableName: String, properties: Map[String, String]): String = {
    // Only SQL api exists for setting TBLPROPERTIES
    val propertiesString = properties
      .map { case (key, value) =>
        s"'$key' = '$value'"
      }
      .mkString(", ")
    s"ALTER TABLE $tableName SET TBLPROPERTIES ($propertiesString)"
  }

}
