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

package ai.chronon.spark.test.groupby

import ai.chronon.aggregator.test.Column
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api._
import ai.chronon.online.fetcher.Fetcher
import ai.chronon.spark.Extensions.DataframeOps
import ai.chronon.spark.submission.SparkSessionBuilder
import ai.chronon.spark.GroupByUpload
import ai.chronon.spark.test.{DataFrameGen, OnlineUtils}
import ai.chronon.spark.utils.MockApi
import com.google.gson.Gson
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import ai.chronon.spark.catalog.TableUtils

import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class GroupByUploadTest extends AnyFlatSpec {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  lazy val spark: SparkSession = SparkSessionBuilder.build("GroupByUploadTest", local = true)
  private val namespace = "group_by_upload_test"
  private val tableUtils = TableUtils(spark)

  it should "temporal events last k" in {
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val yesterday = tableUtils.partitionSpec.before(today)
    tableUtils.createDatabase(namespace)
    tableUtils.sql(s"USE $namespace")
    val eventsTable = "events_last_k_dup" // occurs in groupByTest
    val eventSchema = List(
      Column("user", StringType, 10),
      Column("list_event", StringType, 100)
    )
    val eventDf = DataFrameGen.events(spark, eventSchema, count = 1000, partitions = 18)
    eventDf.save(s"$namespace.$eventsTable")

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.LAST_K, "list_event", Seq(WindowUtils.Unbounded), argMap = Map("k" -> "30"))
    )
    val keys = Seq("user").toArray
    val groupByConf =
      Builders.GroupBy(
        sources = Seq(Builders.Source.events(Builders.Query(), table = eventsTable)),
        keyColumns = keys,
        aggregations = aggregations,
        metaData = Builders.MetaData(namespace = namespace, name = "test_last_k_upload"),
        accuracy = Accuracy.TEMPORAL
      )
    GroupByUpload.run(groupByConf, endDs = yesterday)
  }

  it should "struct support" in {
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val yesterday = tableUtils.partitionSpec.before(today)
    tableUtils.createDatabase(namespace)
    tableUtils.sql(s"USE $namespace")
    val eventsBase = "events_source"
    val eventsTable = "events_table_struct"
    val eventSchema = List(
      Column("user", StringType, 10),
      Column("event1", IntType, 100),
      Column("event2", DoubleType, 100),
      Column("event3", LongType, 100),
      Column("event4", StringType, 100)
    )
    val eventBaseDf = DataFrameGen.events(spark, eventSchema, count = 1000, partitions = 18)
    eventBaseDf.save(s"$namespace.$eventsBase")

    val eventDf = spark.sql(s"""
      SELECT
        user
        , ts
        , ds
        , NAMED_STRUCT('event1', event1, 'event2', event2, 'nested', NAMED_STRUCT('event3', event3, 'event4', event4)) as event_struct
      FROM $namespace.$eventsBase
      """)
    eventDf.save(s"$namespace.$eventsTable")
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.LAST_K, "event_struct", Seq(WindowUtils.Unbounded), argMap = Map("k" -> "30"))
    )
    val keys = Seq("user").toArray
    val groupByConf =
      Builders.GroupBy(
        sources = Seq(Builders.Source.events(Builders.Query(), table = eventsTable)),
        keyColumns = keys,
        aggregations = aggregations,
        metaData = Builders.MetaData(namespace = namespace, name = "test_last_k_upload"),
        accuracy = Accuracy.TEMPORAL
      )
    GroupByUpload.run(groupByConf, endDs = yesterday)
  }

  it should "multiple avg counters" in {
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val yesterday = tableUtils.partitionSpec.before(today)
    tableUtils.createDatabase(namespace)
    tableUtils.sql(s"USE $namespace")
    val eventsTable = "my_events"
    val eventSchema = List(
      Column("user", StringType, 10),
      Column("list_event", StringType, 100),
      Column("views", IntType, 10),
      Column("rating", IntType, 10)
    )
    val eventDf = DataFrameGen.events(spark, eventSchema, count = 1000, partitions = 18)
    eventDf.save(s"$namespace.$eventsTable")

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.LAST_K, "list_event", Seq(WindowUtils.Unbounded), argMap = Map("k" -> "30")),
      Builders.Aggregation(Operation.AVERAGE, "views", Seq(WindowUtils.Unbounded, new Window(1, TimeUnit.DAYS)))
    )
    val keys = Seq("user").toArray
    val groupByConf =
      Builders.GroupBy(
        sources = Seq(Builders.Source.events(Builders.Query(), table = eventsTable)),
        keyColumns = keys,
        aggregations = aggregations,
        metaData = Builders.MetaData(namespace = namespace, name = "test_multiple_avg_upload"),
        accuracy = Accuracy.TEMPORAL
      )
    GroupByUpload.run(groupByConf, endDs = yesterday)
  }

  //  joinLeft = (review, category, rating)  [ratings]
  //  joinPart = (review, user, listing)     [reviews]
  // groupBy = keys:[listing, category], aggs:[avg(rating)]
  it should "listing rating category join source" in {
    tableUtils.createDatabase(namespace)
    tableUtils.sql(s"USE $namespace")

    val ratingsTable = s"${namespace}.ratings"
    def ts(arg: String) = TsUtils.datetimeToTs(s"2023-$arg:00")

    val ratingsColumns = Seq("review", "rating", "category_ratings", "ts", "ds")
    val ratingsData = Seq(
      ("review1", 4, Map("location" -> 4, "cleanliness" -> 4), ts("07-13 11:00"), "2023-08-14"),
      ("review2", 5, Map("location" -> 5, "cleanliness" -> 4), ts("07-13 12:00"), "2023-08-14"), // to delete
      ("review3", 3, Map("location" -> 4, "cleanliness" -> 2), ts("08-15 09:00"), "2023-08-15"), // insert
      ("review1", 2, Map("location" -> 1, "cleanliness" -> 3), ts("08-15 10:00"), "2023-08-15") // update
    )
    val ratingsRdd = spark.sparkContext.parallelize(ratingsData)
    val ratingsDf = spark.createDataFrame(ratingsRdd).toDF(ratingsColumns: _*)
    ratingsDf.save(ratingsTable)
    ratingsDf.show()

    val ratingsMutationsColumns = Seq("is_before", "mutation_ts", "review", "rating", "category_ratings", "ts", "ds")

    val ds = "2023-08-15"
    val ratingsMutations = Seq(
      // delete
      (true, ts("08-15 06:00"), "review2", 5, Map("location" -> 5, "cleanliness" -> 4), ts("07-13 12:00"), ds),
      // insert
      (false, ts("08-15 09:00"), "review3", 3, Map("location" -> 4, "cleanliness" -> 2), ts("08-15 09:00"), ds),
      // update - before
      (true, ts("08-15 10:00"), "review1", 4, Map("location" -> 4, "cleanliness" -> 4), ts("07-13 11:00"), ds),
      // update - after
      (false, ts("08-15 10:00"), "review1", 2, Map("location" -> 1, "cleanliness" -> 3), ts("08-15 10:00"), ds)
    )
    val ratingsMutationsRdd = spark.sparkContext.parallelize(ratingsMutations)
    val ratingsMutationsDf = spark.createDataFrame(ratingsMutationsRdd).toDF(ratingsMutationsColumns: _*)
    ratingsMutationsDf.save(s"${ratingsTable}_mutations")
    ratingsMutationsDf.show()

    val reviewsTable = s"${namespace}.reviews"
    val reviewsColumns = Seq("review", "listing", "ts", "ds")
    val reviewsData = Seq(
      ("review1", "listing1", ts("07-13 10:00"), "2023-08-14"),
      ("review2", "listing1", ts("07-13 11:00"), "2023-08-14"), // delete (next day)
      ("review3", "listing2", ts("08-15 08:00"), "2023-08-15") // insert
    )
    val reviewsRdd = spark.sparkContext.parallelize(reviewsData)
    val reviewsDf = spark.createDataFrame(reviewsRdd).toDF(reviewsColumns: _*)
    reviewsDf.save(reviewsTable)
    reviewsDf.show()

    val reviewsMutationsColumns = Seq("is_before", "mutation_ts", "review", "listing", "ts", "ds")
    val reviewsMutations = Seq(
      (true, ts("08-15 06:00"), "review2", "listing1", ts("07-13 11:00"), "2023-08-15"), // delete
      (false, ts("08-15 08:00"), "review3", "listing2", ts("08-15 08:00"), "2023-08-15") // insert
    )
    val reviewsMutationsRdd = spark.sparkContext.parallelize(reviewsMutations)
    val reviewsMutationsDf = spark.createDataFrame(reviewsMutationsRdd).toDF(reviewsMutationsColumns: _*)
    reviewsMutationsDf.save(s"${reviewsTable}_mutations")
    reviewsMutationsDf.show()

    val leftRatings =
      Builders.Source.entities(
        Builders.Query(selects = Builders.Selects("review", "rating", "category_ratings", "ts")),
        snapshotTable = ratingsTable,
        mutationTopic = s"${ratingsTable}_mutations",
        mutationTable = s"${ratingsTable}_mutations"
      )

    val reviewGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace, name = "review_attrs"),
      sources = Seq(
        Builders.Source.entities(
          Builders.Query(selects = Builders.Selects("review", "listing", "ts")),
          snapshotTable = reviewsTable,
          mutationTopic = s"${reviewsTable}_mutations",
          mutationTable = s"${reviewsTable}_mutations"
        )),
      keyColumns = collection.Seq("review"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.LAST,
          inputColumn = "listing"
        ))
    )

    val joinConf = Builders.Join(
      metaData = Builders.MetaData(namespace = namespace, name = "review_enrichment"),
      left = leftRatings,
      joinParts = Seq(Builders.JoinPart(groupBy = reviewGroupBy))
    )

    val listingRatingGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace, name = "listing_ratings"),
      sources = Seq(
        Builders.Source.joinSource(
          join = joinConf,
          query = Builders.Query(selects =
            Builders.Selects("review", "review_attrs_listing_last", "rating", "category_ratings", "ts"))
        )),
      keyColumns = collection.Seq("review_attrs_listing_last"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.AVERAGE,
          inputColumn = "rating"
        ),
        Builders.Aggregation(
          operation = Operation.AVERAGE,
          inputColumn = "category_ratings"
        )
      )
    )

    val kvStore = OnlineUtils.buildInMemoryKVStore("chaining_test")
    val endDs = "2023-08-15"
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("chaining_test")

    // DO-NOT-SET debug=true here since the streaming job won't put data into kv store
    joinConf.joinParts.toScala.foreach(jp =>
      OnlineUtils.serve(tableUtils, kvStore, kvStoreFunc, "chaining_test", endDs, jp.groupBy, dropDsOnWrite = true))

    OnlineUtils.serve(tableUtils, kvStore, kvStoreFunc, "chaining_test", endDs, listingRatingGroupBy, debug = false)

    kvStoreFunc().show()

    // visualizing values by time to help reason about the tests
    //
    // listing1    08-15    hr = 00      hr = 06       hr = 10
    //   review 1           4, (4, 4)                  2, (1, 3)
    //   review 2           5, (5, 4)     absent       absent
    //                      4.5 (4.5, 4)  4, (4, 4)    2, (1, 3)
    //
    //                      location
    //
    // listing2    08-15    hr = 00    hr = 09
    //   review 3            absent    3, (4, 2)
    //                   null, null     3, (4, 2)
    //
    val api = new MockApi(kvStoreFunc, "chaining_test")
    val fetcher = api.buildFetcher(debug = true)
    val requestResponse = Seq(
      Fetcher.Request("listing_ratings",
                      Map("review_attrs_listing_last" -> "listing1"),
                      Some(ts("08-15 05:00"))) -> 4.5,
      Fetcher.Request("listing_ratings", Map("review_attrs_listing_last" -> "listing1"), Some(ts("08-15 08:00"))) -> 4,
      Fetcher.Request("listing_ratings", Map("review_attrs_listing_last" -> "listing1"), Some(ts("08-15 11:00"))) -> 2,
      Fetcher.Request("listing_ratings",
                      Map("review_attrs_listing_last" -> "listing2"),
                      Some(ts("08-15 07:00"))) -> null,
      Fetcher.Request("listing_ratings", Map("review_attrs_listing_last" -> "listing2"), Some(ts("08-15 10:00"))) -> 3
    )
    val responseF = fetcher.fetchGroupBys(requestResponse.map(_._1))

    val responses = Await.result(responseF, 10.seconds)
    val results = responses.map(r => r.values.get("rating_average"))
    val categoryRatingResults = responses.map(r => r.values.get("category_ratings_average")).toArray
    def cRating(location: Double, cleanliness: Double): java.util.Map[String, Double] =
      Map("location" -> location, "cleanliness" -> cleanliness).toJava
    val gson = new Gson()
    assertEquals(results, requestResponse.map(_._2))

    val expectedCategoryRatings = Array(
      cRating(4.5, 4.0),
      cRating(4.0, 4.0),
      cRating(1.0, 3.0),
      null,
      cRating(4.0, 2.0)
    )
    logger.info(gson.toJson(categoryRatingResults))
    logger.info(gson.toJson(expectedCategoryRatings))
    categoryRatingResults.zip(expectedCategoryRatings).foreach { case (actual, expected) =>
      assertEquals(actual, expected)
    }
  }

  // This test is to ensure that the GroupByUpload can handle a GroupBy with lastK struct and derivations
  it should "upload groupBy with lastK struct + derivations" in {
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val yesterday = tableUtils.partitionSpec.before(today)
    tableUtils.createDatabase(namespace)
    tableUtils.sql(s"USE $namespace")

    val eventsTable = "test_gb_with_derivations"

    // Create test data with the columns needed for the derivations GroupBy
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val testData = Seq(
      ("test_user_123", 100, 42.5, System.currentTimeMillis() - 86400000L), // 1 day ago
      ("test_user_123", 200, 33.3, System.currentTimeMillis() - 172800000L), // 2 days ago
      ("test_user_456", 150, 25.0, System.currentTimeMillis() - 86400000L)
    ).toDF("id", "int_val", "double_val", "ts")
      .withColumn(tableUtils.partitionColumn, from_unixtime(col("ts") / 1000, tableUtils.partitionFormat))

    testData.save(s"$namespace.$eventsTable")

    val groupByConf = makeDerivationsGroupBy(namespace, eventsTable)
    GroupByUpload.run(groupByConf, endDs = yesterday)
  }

  private def makeDerivationsGroupBy(namespace: String, eventsTable: String): GroupBy =
    Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = s"$namespace.$eventsTable",
          topic = "events.my_stream",
          query = Builders.Query(
            selects = Map(
              "id" -> "id",
              "int_val" -> "int_val",
              "double_val" -> "double_val",
              "named_struct" -> "IF(id IS NOT NULL, NAMED_STRUCT('id', id, 'int_val', int_val), NULL)",
              "another_named_struct" -> "IF(id IS NOT NULL, NAMED_STRUCT('id', id, 'double_val', double_val), NULL)"
            ),
            wheres = Seq.empty,
            timeColumn = "ts",
            startPartition = "20231106"
          )
        )
      ),
      keyColumns = Seq("id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "double_val",
          windows = Seq(
            new Window(1, TimeUnit.DAYS)
          )
        ),
        Builders.Aggregation(
          operation = Operation.LAST_K,
          inputColumn = "named_struct",
          windows = Seq(
            new Window(1, TimeUnit.DAYS),
            new Window(2, TimeUnit.DAYS)
          ),
          argMap = Map("k" -> "2")
        ),
        Builders.Aggregation(
          operation = Operation.LAST_K,
          inputColumn = "another_named_struct",
          windows = Seq(
            new Window(1, TimeUnit.DAYS),
            new Window(2, TimeUnit.DAYS)
          ),
          argMap = Map("k" -> "2")
        ),
        Builders.Aggregation(
          operation = Operation.LAST,
          inputColumn = "int_val",
          windows = Seq(
            new Window(1, TimeUnit.DAYS)
          )
        )
      ),
      metaData = Builders.MetaData(
        namespace = namespace,
        name = "derivations_test_group_by"
      ),
      accuracy = Accuracy.TEMPORAL,
      derivations = Seq(
        Builders.Derivation(
          name = "int_val",
          expression = "int_val_last_1d"
        ),
        Builders.Derivation(
          name = "id_last2_1d",
          expression = "transform(named_struct_last2_1d, x -> x.id)"
        ),
        Builders.Derivation(
          name = "id2_last2_1d",
          expression = "transform(another_named_struct_last2_1d, x -> x.id)"
        )
      )
    )
}
