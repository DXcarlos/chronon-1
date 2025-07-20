package ai.chronon.spark.catalog

import org.apache.spark.sql.types.StructType

object CreationUtils {

  private val ALLOWED_TABLE_TYPES = List("iceberg", "delta", "hive", "parquet", "hudi")

  def createTableSql(tableName: String,
                     schema: StructType,
                     partitionColumns: List[String],
                     tableProperties: Map[String, String],
                     fileFormatString: String,
                     tableTypeString: String,
                     bucketColumnName: Option[String] = None,
                     bucketNumber: Option[Int] = None): String = {

    require(
      tableTypeString.isEmpty || ALLOWED_TABLE_TYPES.contains(tableTypeString.toLowerCase),
      s"Invalid table type: ${tableTypeString}. Must be empty OR one of: ${ALLOWED_TABLE_TYPES}"
    )

    val noPartitions = StructType(
      schema
        .filterNot(field => partitionColumns.contains(field.name)))

    val createFragment =
      s"""CREATE TABLE $tableName (
         |    ${schema.toDDL}
         |)
         |${if (tableTypeString.isEmpty) "" else f"USING ${tableTypeString}"}
         |""".stripMargin

    val partitionFragment = if (
      (partitionColumns != null && partitionColumns.nonEmpty) || (bucketColumnName.isDefined && bucketNumber.isDefined)
    ) {

      val partitionDefinitions = if (partitionColumns != null && partitionColumns.nonEmpty) {
        schema
          .filter(field => partitionColumns.contains(field.name))
          .map(field => s"${field.name}")
      } else {
        List.empty[String]
      }

      s"""PARTITIONED BY (
         |    ${partitionDefinitions.mkString(",\n    ")}${bucketColumnName
        .map((bucketCol) => s",\nbucket(${bucketNumber.get}, ${bucketCol})")
        .getOrElse("")}
         |
         |)""".stripMargin
    } else {
      ""
    }

//    val bucketFragment = if (bucketColumnName.isDefined && bucketNumber.isDefined) {
//      // Todo: add `SORTED BY (${bucketColumnName.get})` below?
//      s"CLUSTERED BY (${bucketColumnName.get}) INTO ${bucketNumber.get} BUCKETS"
//    } else {
//      ""
//    }

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
