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
      // Support Iceberg partition transforms if this is an Iceberg table
      if (tableTypeString.toLowerCase == "iceberg") {
        generateIcebergPartitionSpec(schema, partitionColumns, tableProperties)
      } else {
        // Standard Hive-style partitioning
        val partitionDefinitions = schema
          .filter(field => partitionColumns.contains(field.name))
          .map(field => s"${field.name} ${field.dataType.catalogString}")

        s"""PARTITIONED BY (
           |    ${partitionDefinitions.mkString(",\n    ")}
           |)""".stripMargin
      }
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

  /**
   * Generates Iceberg partition specification with support for bucketing transforms.
   */
  private def generateIcebergPartitionSpec(schema: StructType, 
                                         partitionColumns: List[String],
                                         tableProperties: Map[String, String]): String = {
    
    val partitionSpecs = scala.collection.mutable.ListBuffer[String]()
    
    // Add standard partition columns (typically time-based)
    val standardPartitions = schema
      .filter(field => partitionColumns.contains(field.name))
      .map(field => field.name)
    
    partitionSpecs ++= standardPartitions
    
    // Add bucketing transforms if configured
    if (tableProperties != null) {
      val hashColumns = tableProperties.get("write.hash-columns")
      val numBuckets = tableProperties.get("write.hash-buckets")
      
      (hashColumns, numBuckets) match {
        case (Some(columns), Some(buckets)) =>
          val bucketColumns = columns.split(",").map(_.trim)
          // Add bucket transform for each hash column
          bucketColumns.foreach { column =>
            partitionSpecs += s"bucket($buckets, $column)"
          }
        case _ => // No bucketing configuration
      }
    }
    
    if (partitionSpecs.nonEmpty) {
      s"""PARTITIONED BY (
         |    ${partitionSpecs.mkString(",\n    ")}
         |)""".stripMargin
    } else {
      ""
    }
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
