package ai.chronon.spark.batch

import ai.chronon.aggregator.test.Column
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.utils.{DataFrameGen, SparkTestBase}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{
  IntegerType => SparkIntegerType,
  StringType => SparkStringType,
  StructField => SparkStructField,
  StructType => SparkStructType
}
import org.apache.spark.sql.{DataFrame, Row => SparkRow}

case class EntityJoinFixture(entityTable: String,
                             joinPart: JoinPart,
                             joinConf: Join,
                             range: DateRange,
                             partitionRange: PartitionRange)

case class GroupBySnapshotFixture(sourceTable: String, outputTable: String, range: PartitionRange)

trait StoragePartitionJoinTestBase extends SparkTestBase {

  protected implicit lazy val tableUtils: TableUtils = TableUtils(spark)
  protected def namespace: String

  protected lazy val today: String = tableUtils.partitionSpec.at(System.currentTimeMillis())
  protected lazy val threeDaysAgo: String = tableUtils.partitionSpec.minus(today, new Window(3, TimeUnit.DAYS))
  protected lazy val fiveDaysAgo: String = tableUtils.partitionSpec.minus(today, new Window(5, TimeUnit.DAYS))
  protected lazy val sevenDaysAgo: String = tableUtils.partitionSpec.minus(today, new Window(7, TimeUnit.DAYS))
  protected lazy val yearAgo: String = tableUtils.partitionSpec.minus(today, new Window(365, TimeUnit.DAYS))

  // Both positive and negative tests disable AQE and broadcast for predictable sort-merge join plans
  protected def baseSparkConfs: Map[String, String] = Map(
    "spark.sql.adaptive.enabled" -> "false",
    "spark.sql.autoBroadcastJoinThreshold" -> "-1",
    "spark.chronon.table_write.format" -> "iceberg"
  )

  protected def countExchanges(plan: SparkPlan): Int = {
    plan.toString.split("\n").count(line =>
      line.contains("Exchange") || line.contains("ShuffleExchange"))
  }

  protected def verifyNoShuffle(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan
    val exchangeCount = countExchanges(plan)
    assert(exchangeCount == 0,
      s"Expected no Exchange operators but found $exchangeCount.\nPlan:\n${plan.toString}")
  }

  protected def countExchangesInString(planStr: String): Int = {
    planStr.split("\n").count(line =>
      line.contains("Exchange") || line.contains("ShuffleExchange"))
  }

  protected def getWritePlan(tableName: String, viewName: String): String = {
    val explainResult = spark.sql(s"EXPLAIN INSERT INTO $tableName SELECT * FROM $viewName").collect()
    explainResult.map(_.getString(0)).mkString("\n")
  }

  protected def createToyTables(tableA: String, tableB: String): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $tableA")
    spark.sql(s"DROP TABLE IF EXISTS $tableB")

    spark.sql(
      s"""CREATE TABLE $tableA (
         |  id STRING,
         |  value_a INT,
         |  ds STRING
         |) USING iceberg
         |PARTITIONED BY (ds)""".stripMargin)

    spark.sql(
      s"""CREATE TABLE $tableB (
         |  id STRING,
         |  value_b INT,
         |  ds STRING
         |) USING iceberg
         |PARTITIONED BY (ds)""".stripMargin)

    val days = Seq("2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05")

    val schemaA = SparkStructType(Array(
      SparkStructField("id", SparkStringType),
      SparkStructField("value_a", SparkIntegerType),
      SparkStructField("ds", SparkStringType)))

    val schemaB = SparkStructType(Array(
      SparkStructField("id", SparkStringType),
      SparkStructField("value_b", SparkIntegerType),
      SparkStructField("ds", SparkStringType)))

    val rowsA = days.flatMap { day =>
      (1 to 100).map(i => SparkRow(s"user_$i", i * 10, day))
    }
    val rowsB = days.flatMap { day =>
      (1 to 100).map(i => SparkRow(s"user_$i", i * 20, day))
    }

    spark.createDataFrame(java.util.Arrays.asList(rowsA: _*), schemaA)
      .write.mode("append").insertInto(tableA)
    spark.createDataFrame(java.util.Arrays.asList(rowsB: _*), schemaB)
      .write.mode("append").insertInto(tableB)
  }

  protected def buildEntityJoinFixture(testName: String): EntityJoinFixture = {
    val entitySchema = List(
      Column("user", StringType, 10),
      Column("amount", LongType, 1000)
    )

    val entityTable = s"$namespace.${testName}_entities"
    spark.sql(s"DROP TABLE IF EXISTS $entityTable")
    DataFrameGen.entities(spark, entitySchema, 800, partitions = 8).save(entityTable)

    val groupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.entities(
          query = Builders.Query(
            selects = Builders.Selects("user", "amount"),
            startPartition = yearAgo
          ),
          snapshotTable = entityTable
        )
      ),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM, inputColumn = "amount")
      ),
      metaData = Builders.MetaData(name = s"$testName.user_amounts", namespace = namespace, team = "test_team")
    )

    val joinPart = Builders.JoinPart(groupBy = groupBy)

    val joinConf = Builders.Join(
      left = Builders.Source.entities(
        query = Builders.Query(startPartition = sevenDaysAgo, endPartition = threeDaysAgo),
        snapshotTable = entityTable
      ),
      joinParts = Seq(joinPart),
      metaData = Builders.MetaData(name = s"$testName.user_features", namespace = namespace, team = "test_team")
    )

    val range = new DateRange().setStartDate(sevenDaysAgo).setEndDate(threeDaysAgo)
    val partitionRange = range.toPartitionRange(tableUtils.partitionSpec)

    EntityJoinFixture(entityTable, joinPart, joinConf, range, partitionRange)
  }

  protected def buildGroupByEntitySnapshotFixture(testName: String): GroupBySnapshotFixture = {
    val entitySchema = List(
      Column("user", StringType, 10),
      Column("amount", LongType, 1000)
    )

    val entityTable = s"$namespace.${testName}_entities"
    spark.sql(s"DROP TABLE IF EXISTS $entityTable")
    DataFrameGen.entities(spark, entitySchema, 800, partitions = 8).save(entityTable)

    val range = PartitionRange(sevenDaysAgo, threeDaysAgo)(tableUtils.partitionSpec)

    val groupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.entities(
          query = Builders.Query(
            selects = Builders.Selects("user", "amount"),
            startPartition = yearAgo
          ),
          snapshotTable = entityTable
        )
      ),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM, inputColumn = "amount")
      ),
      metaData = Builders.MetaData(name = s"$testName.user_amounts", namespace = namespace, team = "test_team")
    )

    val gb = ai.chronon.spark.GroupBy.from(groupBy, range, tableUtils, computeDependency = true)
    val snapshotDf = gb.snapshotEntities

    val outputTable = s"$namespace.${testName}_output"
    spark.sql(s"DROP TABLE IF EXISTS $outputTable")
    snapshotDf.save(outputTable)

    GroupBySnapshotFixture(entityTable, outputTable, range)
  }

  protected def buildGroupByEventSnapshotFixture(testName: String): GroupBySnapshotFixture = {
    val eventSchema = List(
      Column("user", StringType, 10),
      Column("amount", LongType, 1000)
    )

    val eventTable = s"$namespace.${testName}_events"
    spark.sql(s"DROP TABLE IF EXISTS $eventTable")
    DataFrameGen.events(spark, eventSchema, 800, partitions = 8).save(eventTable)

    val range = PartitionRange(sevenDaysAgo, threeDaysAgo)(tableUtils.partitionSpec)

    val groupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(
            selects = Builders.Selects("user", "amount"),
            startPartition = yearAgo
          ),
          table = eventTable
        )
      ),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "amount",
          windows = Seq(new Window(7, TimeUnit.DAYS))
        )
      ),
      metaData = Builders.MetaData(name = s"$testName.user_events", namespace = namespace, team = "test_team")
    )

    val gb = ai.chronon.spark.GroupBy.from(groupBy, range, tableUtils, computeDependency = true)
    val snapshotDf = gb.snapshotEvents(range)

    val outputTable = s"$namespace.${testName}_output"
    spark.sql(s"DROP TABLE IF EXISTS $outputTable")
    snapshotDf.save(outputTable)

    GroupBySnapshotFixture(eventTable, outputTable, range)
  }
}
