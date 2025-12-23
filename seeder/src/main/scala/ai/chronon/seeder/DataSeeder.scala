/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.seeder

import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.utils.DataFrameGen
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.rogach.scallop.ScallopConf

/** Main entry point for data seeding.
  * Generates test data based on schema definitions and writes to cloud storage.
  */
object DataSeeder {

  class Conf(args: Seq[String]) extends ScallopConf(args) {
    val seederClass = opt[String](
      required = true,
      descr = "Fully qualified class name of the seeder schema (e.g., com.example.MySeeder)"
    )

    val fileFormat = opt[String](
      default = Some("PARQUET"),
      descr = "File format for table storage (PARQUET, ORC, etc.)"
    )

    val dryRun = opt[Boolean](
      default = Some(false),
      descr = "If true, generate and show data without writing"
    )

    val startDate = opt[String](
      descr = "Start date for generated data (yyyy-MM-dd). Defaults to 30 days ago."
    )

    val endDate = opt[String](
      descr = "End date for generated data (yyyy-MM-dd). Defaults to today."
    )

    val saveMode = opt[String](
      default = Some("append"),
      descr = "Save mode: 'append' (default, safe for cloud) or 'overwrite'"
    )

    val batchSize = opt[Int](
      default = Some(30),
      descr = "Number of partitions to process in each batch (default: 30). Lower values reduce memory usage."
    )

    verify()
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)

    val spark = SparkSession
      .builder()
      .appName("Chronon Data Seeder")
      .enableHiveSupport()
      .getOrCreate()

    try {
      // Parse dates
      import java.time.LocalDate
      import java.time.format.DateTimeFormatter
      val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

      val endDate = conf.endDate.toOption match {
        case Some(date) => LocalDate.parse(date, dateFormatter)
        case None       => LocalDate.now()
      }

      val startDate = conf.startDate.toOption match {
        case Some(date) => LocalDate.parse(date, dateFormatter)
        case None       => endDate.minusDays(30)
      }

      val partitionCount = java.time.temporal.ChronoUnit.DAYS.between(startDate, endDate).toInt + 1

      println(s"Data generation range: $startDate to $endDate ($partitionCount days)")

      // Parse save mode
      val saveMode = conf.saveMode().toLowerCase match {
        case "overwrite" => SaveMode.Overwrite
        case "append"    => SaveMode.Append
        case other =>
          println(s"Invalid save mode: $other. Using 'append' (safe for cloud)")
          SaveMode.Append
      }

      println(s"Save mode: $saveMode")

      // Load the seeder config
      println(s"Loading seeder schema from: ${conf.seederClass()}")
      val config = SchemaLoader.load(conf.seederClass())

      // Validate the config
      SchemaLoader.validate(config)
      println(s"Loaded ${config.tables.size} table(s) to seed")

      // Create TableUtils instance
      val tableUtils = TableUtils(spark)

      // Process each table
      config.tables.foreach { tableSchema =>
        processTable(tableSchema,
                     conf.fileFormat(),
                     conf.dryRun(),
                     spark,
                     tableUtils,
                     saveMode,
                     partitionCount,
                     conf.batchSize())
      }

      println("Seeding completed successfully!")
    } catch {
      case e: Exception =>
        println(s"Error during seeding: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
    }
  }

  /** Process a single table schema: generate data and write to cloud
    *
    * For large date ranges, we process in batches to avoid OOM
    */
  private def processTable(
      schema: TableSchema,
      fileFormat: String,
      dryRun: Boolean,
      spark: SparkSession,
      tableUtils: TableUtils,
      saveMode: SaveMode,
      partitionCount: Int,
      batchSize: Int
  ): Unit = {
    println(s"\n=== Processing table: ${schema.tableName} ===")
    println(s"  Type: ${schema.tableType}")
    println(s"  Rows: ${schema.rowCount}")
    println(s"  Partitions: $partitionCount")
    println(s"  Batch size: $batchSize partitions")
    println(s"  Columns: ${schema.columns.map(_.name).mkString(", ")}")

    // Process in batches to avoid OOM for large date ranges
    val batches = (partitionCount + batchSize - 1) / batchSize

    if (partitionCount > batchSize) {
      println(s"  Processing in $batches batches of up to $batchSize partitions each")
    }

    for (batchIndex <- 0 until batches) {
      val batchStart = batchIndex * batchSize
      val batchEnd = Math.min(batchStart + batchSize, partitionCount)
      val batchPartitionCount = batchEnd - batchStart

      if (batches > 1) {
        println(
          s"\n  Batch ${batchIndex + 1}/$batches: Partitions $batchStart to ${batchEnd - 1} ($batchPartitionCount partitions)")
      }

      // Generate the data for this batch
      val df = generateData(schema, spark, batchPartitionCount)

      if (dryRun) {
        println("\n--- Dry run mode: showing sample data ---")
        df.show(20, truncate = false)
        df.printSchema()
        println(s"Total rows generated in this batch: ${df.count()}")
      } else {
        // For first batch, use the configured saveMode (overwrite or append)
        // For subsequent batches, always append
        val batchSaveMode = if (batchIndex == 0) saveMode else SaveMode.Append

        // Write to cloud storage using TableUtils
        writeToCloud(df, schema, fileFormat, tableUtils, batchSaveMode)
      }
    }
  }

  /** Generate test data for a table schema using DataFrameGen
    */
  private def generateData(schema: TableSchema, spark: SparkSession, partitionCount: Int): DataFrame = {
    val columns = schema.columns.map(_.toColumn)

    schema.tableType match {
      case Events =>
        DataFrameGen.events(
          spark,
          columns,
          schema.rowCount,
          partitionCount,
          schema.partitionColumn,
          schema.partitionFormat
        )

      case Entities =>
        DataFrameGen.entities(
          spark,
          columns,
          schema.rowCount,
          partitionCount,
          schema.partitionColumn,
          schema.partitionFormat
        )
    }
  }

  /** Write DataFrame to cloud storage using TableUtils.
    * TableUtils handles the format provider logic, supporting different table formats
    * (Iceberg, Hudi, Delta, Parquet, etc.) based on configuration.
    */
  private def writeToCloud(
      df: DataFrame,
      schema: TableSchema,
      fileFormat: String,
      tableUtils: TableUtils,
      saveMode: SaveMode
  ): Unit = {
    println(s"\nWriting to table: ${schema.tableName}")
    println(s"  File format: $fileFormat")
    println(s"  Save mode: $saveMode")

    // Use the partition column from the schema, or default to the tableUtils partition column
    val partitionCols = schema.partitionColumn match {
      case Some(col) => List(col)
      case None      => List(tableUtils.partitionColumn)
    }

    // Repartition by partition columns to ensure each partition is processed by a separate task
    // This reduces memory pressure per task by distributing the data
    val repartitionedDf = df.repartition(partitionCols.map(df(_)): _*)
    println(s"  Repartitioned by ${partitionCols.mkString(", ")} to distribute memory load across partitions")

    // Use TableUtils.insertPartitions which handles format provider logic
    tableUtils.insertPartitions(
      df = repartitionedDf,
      tableName = schema.tableName,
      tableProperties = null,
      partitionColumns = partitionCols,
      saveMode = saveMode,
      fileFormat = fileFormat,
      autoExpand = false
    )

    println(s"✓ Successfully wrote data to ${schema.tableName}")
  }
}
