package ai.chronon.spark.join

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.utils.DataFrameGen
import org.scalatest.matchers.should.Matchers._

class UnionJoinTest extends BaseJoinTest {

  it should "test events events temporal case for UnionJoin.computeJoinAndSave" in {

    val viewsSchema = List(
      Column("user", api.StringType, 1),
      Column("item", api.StringType, 1),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_union_temporal"
    DataFrameGen
      .events(spark, viewsSchema, count = 10000, partitions = 20)
      .save(viewsTable, Map("tblProp1" -> "1"))

    val viewsDf = tableUtils.loadTable(viewsTable)

    val viewsSource = Builders.Source.events(
      table = viewsTable,
      topic = "",
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"),
                             startPartition = tableUtils.partitionSpec.minus(today, new Window(20, TimeUnit.DAYS)))
    )

    val viewsGroupBy = Builders
      .GroupBy(
        sources = Seq(viewsSource),
        keyColumns = Seq("item"),
        aggregations = Seq(
          Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms"),
          Builders.Aggregation(
            operation = Operation.LAST_K,
            argMap = Map("k" -> "50"),
            inputColumn = "time_spent_ms",
            windows = Seq(new Window(2, TimeUnit.DAYS))
          )
        ),
        metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace)
      )
      .setAccuracy(Accuracy.TEMPORAL)

    // left side
    val itemQueries = List(Column("item", api.StringType, 1))
    val itemQueriesTable = s"$namespace.item_queries_union_temporal"
    val itemQueriesDf = DataFrameGen
      .events(spark, itemQueries, 10000, partitions = 10)

    // duplicate the events
    itemQueriesDf.union(itemQueriesDf).save(itemQueriesTable)

    val queriesDf = tableUtils.loadTable(itemQueriesTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(20, TimeUnit.DAYS))
    val dateRange = PartitionRange(start, today)(tableUtils.partitionSpec)

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user")),
      metaData =
        Builders.MetaData(name = s"test.item_temporal_features.union_join", namespace = namespace, team = "item_team")
    )

    // Test UnionJoin.computeJoinAndSave method

    UnionJoin.computeJoinAndSave(joinConf, dateRange)

    val outputDf = tableUtils.loadTable(joinConf.metaData.outputTable)

    val outputData = outputDf.where("item IS NOT NULL and ts IS NOT NULL").collect()
    val queriesData = queriesDf.where("item IS NOT NULL and ts IS NOT NULL").collect()
    outputData.length shouldBe queriesData.length
  }

  it should "test UnionJoin with GroupBy and Join derivations" in {

    val eventsSchema = List(
      Column("user_id", api.StringType, 1),
      Column("item_id", api.StringType, 1),
      Column("amount", api.LongType, 100),
      Column("category", api.StringType, 3)
    )

    val eventsTable = s"$namespace.events_with_derivations"
    DataFrameGen
      .events(spark, eventsSchema, count = 50000, partitions = 40) // Increased count and partitions
      .save(eventsTable)

    val eventsSource = Builders.Source.events(
      table = eventsTable,
      query = Builders.Query(
        selects = Builders.Selects("amount", "category"),
        startPartition = tableUtils.partitionSpec.minus(today, new Window(40, TimeUnit.DAYS)) // Increased window
      )
    )

    // GroupBy with derivations
    val groupByWithDerivations = Builders
      .GroupBy(
        sources = Seq(eventsSource),
        keyColumns = Seq("user_id"),
        aggregations = Seq(
          Builders.Aggregation(operation = Operation.SUM,
                               inputColumn = "amount",
                               windows = Seq(new Window(7, TimeUnit.DAYS))),
          Builders.Aggregation(operation = Operation.COUNT,
                               inputColumn = "amount",
                               windows = Seq(new Window(7, TimeUnit.DAYS))),
          Builders.Aggregation(operation = Operation.SUM,
                               inputColumn = "amount",
                               windows = Seq(new Window(30, TimeUnit.DAYS)))
        ),
        derivations = Seq(
          Builders.Derivation(name = "*", expression = "*"), // Include all base columns
          Builders.Derivation(name = "avg_amount_7d", expression = "amount_sum_7d / amount_count_7d"),
          Builders.Derivation(name = "amount_ratio", expression = "amount_sum_7d / amount_sum_30d")
        ),
        metaData = Builders.MetaData(name = "unit_test.user_features_with_derivations", namespace = namespace)
      )
      .setAccuracy(Accuracy.TEMPORAL)

    // Left side events
    val leftSchema = List(Column("user_id", api.StringType, 1))
    val leftTable = s"$namespace.user_queries_with_derivations"
    DataFrameGen
      .events(spark, leftSchema, count = 5000, partitions = 20) // Increased count and partitions
      .save(leftTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(40, TimeUnit.DAYS))
    val dateRange = PartitionRange(start, today)(tableUtils.partitionSpec)

    // Join with derivations
    val joinWithDerivations = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = leftTable),
      joinParts = Seq(Builders.JoinPart(groupBy = groupByWithDerivations).setUseLongNames(false)),
      derivations = Seq(
        Builders.Derivation(name = "*", expression = "*"), // Include all columns
        Builders.Derivation(name = "spend_efficiency", expression = "user_id_avg_amount_7d * user_id_amount_ratio"),
        Builders.Derivation(
          name = "user_tier",
          expression =
            "CASE WHEN user_id_amount_sum_30d > 1000 THEN 'high' WHEN user_id_amount_sum_30d > 500 THEN 'medium' ELSE 'low' END")
      ),
      metaData =
        Builders.MetaData(name = "test.user_features_derived.union_join", namespace = namespace, team = "user_team")
    )

    // Execute UnionJoin with derivations
    UnionJoin.computeJoinAndSave(joinWithDerivations, dateRange)

    val outputDf = tableUtils.loadTable(joinWithDerivations.metaData.outputTable)

    // Verify that derived columns exist
    val schema = outputDf.schema
    schema.fieldNames should contain("user_id_avg_amount_7d") // GroupBy derivation
    schema.fieldNames should contain("user_id_amount_ratio") // GroupBy derivation
    schema.fieldNames should contain("spend_efficiency") // Join derivation
    schema.fieldNames should contain("user_tier") // Join derivation

    // Verify that base aggregation columns still exist due to wildcard
    schema.fieldNames should contain("user_id_amount_sum_7d")
    schema.fieldNames should contain("user_id_amount_count_7d")
    schema.fieldNames should contain("user_id_amount_sum_30d")

    val outputData = outputDf.where("user_id IS NOT NULL and ts IS NOT NULL").collect()
    outputData.length should be > 0

    // Verify derivation logic works - ensure we actually test the calculation
    val validRows = outputData.filter { row =>
      val avgAmount7d = Option(row.getAs[Double]("user_id_avg_amount_7d"))
      val amountSum7d = Option(row.getAs[Long]("user_id_amount_sum_7d"))
      val amountCount7d = Option(row.getAs[Long]("user_id_amount_count_7d"))
      avgAmount7d.isDefined && amountSum7d.isDefined && amountCount7d.isDefined &&
      amountCount7d.get > 0 && amountSum7d.get > 0
    }

    validRows.length should be > 0 // Ensure we have at least one row to test

    // Verify GroupBy derivation calculation on a valid row
    val sampleRow = validRows.head
    val avgAmount7d = sampleRow.getAs[Double]("user_id_avg_amount_7d")
    val amountSum7d = sampleRow.getAs[Long]("user_id_amount_sum_7d")
    val amountCount7d = sampleRow.getAs[Long]("user_id_amount_count_7d")

    Math.abs(avgAmount7d - (amountSum7d.toDouble / amountCount7d)) should be < 0.001
  }

  it should "test UnionJoin schema with GB derivations" in {

    val namespace = "union_join_gb_derivation"
    createDatabase(namespace)

    val eventsSchema = List(
      Column("user_id", api.StringType, 1),
      Column("item_id", api.StringType, 1),
      Column("amount", api.LongType, 100),
      Column("category", api.StringType, 3)
    )

    val eventsTable = s"$namespace.events_with_derivations"
    DataFrameGen
      .events(spark, eventsSchema, count = 50000, partitions = 40) // Increased count and partitions
      .save(eventsTable)

    val eventsSource = Builders.Source.events(
      table = eventsTable,
      query = Builders.Query(
        selects = Builders.Selects("amount", "category"),
        startPartition = tableUtils.partitionSpec.minus(today, new Window(40, TimeUnit.DAYS)) // Increased window
      )
    )

    // GroupBy with derivations
    val groupByWithDerivations = Builders
      .GroupBy(
        sources = Seq(eventsSource),
        keyColumns = Seq("user_id"),
        aggregations = Seq(
          Builders.Aggregation(operation = Operation.SUM,
                               inputColumn = "amount",
                               windows = Seq(new Window(7, TimeUnit.DAYS))),
          Builders.Aggregation(operation = Operation.COUNT,
                               inputColumn = "amount",
                               windows = Seq(new Window(7, TimeUnit.DAYS))),
          Builders.Aggregation(operation = Operation.SUM,
                               inputColumn = "amount",
                               windows = Seq(new Window(30, TimeUnit.DAYS)))
        ),
        derivations = Seq(
          Builders.Derivation(name = "avg_amount_7d", expression = "amount_sum_7d / amount_count_7d"),
          Builders.Derivation(name = "amount_ratio", expression = "amount_sum_7d / amount_sum_30d")
        ),
        metaData = Builders.MetaData(name = "unit_test.user_features_with_derivations", namespace = namespace)
      )
      .setAccuracy(Accuracy.TEMPORAL)

    val start = tableUtils.partitionSpec.minus(today, new Window(40, TimeUnit.DAYS))
    val dateRange = PartitionRange(start, today)(tableUtils.partitionSpec)

    // Join with derivations
    val joinWithSingleJP = Builders.Join(
      left = Builders.Source.events(
        Builders.Query(selects =
                         Builders.Selects("user_id", "item_id", "amount", "category"), // Select all left cols here
                       startPartition = start),
        table = eventsTable),
      joinParts = Seq(Builders.JoinPart(groupBy = groupByWithDerivations)),
      metaData =
        Builders.MetaData(name = "test.user_features_derived.union_join", namespace = namespace, team = "user_team")
    )

    // Execute UnionJoin with derivations
    UnionJoin.computeJoinAndSave(joinWithSingleJP, dateRange)

    val outputDf = tableUtils.loadTable(joinWithSingleJP.metaData.outputTable)

    // Verify that derived columns exist
    val schema = outputDf.schema
    println(schema.pretty)
    schema.fieldNames should contain("user_id")
    schema.fieldNames should contain("unit_test_user_features_with_derivations_item_id")
    schema.fieldNames should contain("unit_test_user_features_with_derivations_amount")
    schema.fieldNames should contain("unit_test_user_features_with_derivations_category")

    // Derivation cols
    schema.fieldNames should contain("unit_test_user_features_with_derivations_avg_amount_7d")
    schema.fieldNames should contain("unit_test_user_features_with_derivations_amount_ratio")
  }
}
