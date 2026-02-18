package ai.chronon.spark.fetcher

import ai.chronon.api._
import ai.chronon.api.Constants.MetadataDataset
import ai.chronon.api.Extensions._
import ai.chronon.online.fetcher.{FetchContext, Fetcher, MetadataStore}
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.utils.{MockApi, OnlineUtils, SparkTestBase}
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.util.TimeZone
import scala.concurrent.Await
import scala.concurrent.duration._

class FetcherDecimalTest extends SparkTestBase with Matchers {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  private val tableUtils = TableUtils(spark)
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  it should "test decimal types with aggregations" in {
    val namespace = "decimal_fetcher_test"
    val joinConf = generateDecimalData(namespace, tableUtils, spark)
    FetcherTestUtil.compareTemporalFetch(joinConf,
                                         "2023-08-14",
                                         namespace,
                                         consistencyCheck = false,
                                         dropDsOnWrite = true)(spark)
  }

  def generateDecimalData(namespace: String, tableUtils: TableUtils, spark: SparkSession): Join = {
    SparkTestBase.createDatabase(spark, namespace)

    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types.{DecimalType => SparkDecimalType}

    def ts(arg: String) = TsUtils.datetimeToTs(s"2023-$arg:00")

    val transactionsTable = s"${namespace}.transactions"
    val transactionsColumns = Seq("user_id", "product_id", "price", "discount", "quantity", "ts", "ds")
    val transactionsData = Seq(
      ("user1", "prod_a", new java.math.BigDecimal("100.50"), new java.math.BigDecimal("10.05"), new java.math.BigDecimal("2.5"), ts("08-13 10:00"), "2023-08-13"),
      ("user1", "prod_b", new java.math.BigDecimal("50.25"), new java.math.BigDecimal("5.00"), new java.math.BigDecimal("1.0"), ts("08-13 11:00"), "2023-08-13"),
      ("user1", "prod_c", new java.math.BigDecimal("75.00"), new java.math.BigDecimal("7.50"), new java.math.BigDecimal("3.0"), ts("08-13 12:00"), "2023-08-13"),
      ("user2", "prod_c", new java.math.BigDecimal("200.75"), new java.math.BigDecimal("20.00"), new java.math.BigDecimal("3.0"), ts("08-13 09:00"), "2023-08-13"),
      ("user2", "prod_d", new java.math.BigDecimal("150.25"), new java.math.BigDecimal("15.00"), new java.math.BigDecimal("2.0"), ts("08-13 10:00"), "2023-08-13")
    )
    val transactionsRdd = spark.sparkContext.parallelize(transactionsData)
    val transactionsDf = spark.createDataFrame(transactionsRdd).toDF(transactionsColumns: _*)
      .withColumn("price", col("price").cast(SparkDecimalType(10, 2)))
      .withColumn("discount", col("discount").cast(SparkDecimalType(8, 2)))
      .withColumn("quantity", col("quantity").cast(SparkDecimalType(8, 2)))

    import ai.chronon.spark.Extensions.DataframeOps
    transactionsDf.save(transactionsTable)
    transactionsDf.show()

    val userEventsTable = s"${namespace}.user_events"
    val userEventsColumns = Seq("user_id", "ts", "ds")
    val userEventsData = Seq(
      ("user1", ts("08-14 12:00"), "2023-08-14"),
      ("user2", ts("08-14 12:00"), "2023-08-14")
    )
    val userEventsDf = spark.createDataFrame(userEventsData).toDF(userEventsColumns: _*)
    userEventsDf.save(userEventsTable)
    userEventsDf.show()

    val userTransactionsGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace, name = "user_transactions_decimal"),
      sources = Seq(
        Builders.Source.events(
          Builders.Query(selects = Builders.Selects("user_id", "price", "discount", "quantity", "product_id", "ts")),
          table = transactionsTable
        )
      ),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "price",
          windows = Seq(WindowUtils.Unbounded)
        ),
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "discount",
          windows = Seq(WindowUtils.Unbounded)
        ),
        Builders.Aggregation(
          operation = Operation.AVERAGE,
          inputColumn = "price",
          windows = Seq(WindowUtils.Unbounded)
        ),
        Builders.Aggregation(
          operation = Operation.AVERAGE,
          inputColumn = "quantity",
          windows = Seq(WindowUtils.Unbounded)
        ),
        Builders.Aggregation(
          operation = Operation.TOP_K,
          inputColumn = "price",
          argMap = Map("k" -> "2"),
          windows = Seq(WindowUtils.Unbounded)
        )
      ),
      derivations = Seq(
        Builders.Derivation(
          name = "net_price",
          expression = "price_sum - discount_sum"
        ),
        Builders.Derivation(
          name = "discount_rate",
          expression = "discount_sum / price_sum"
        )
      ),
      accuracy = Accuracy.TEMPORAL
    )

    val joinConf = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(
          selects = Builders.Selects("user_id", "ts"),
          startPartition = "2023-08-14"
        ),
        table = userEventsTable
      ),
      joinParts = Seq(Builders.JoinPart(groupBy = userTransactionsGroupBy)),
      metaData = Builders.MetaData(name = "user_transactions_decimal_join", namespace = namespace, team = "chronon")
    )
    joinConf
  }
}
