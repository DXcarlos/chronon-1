package ai.chronon.spark.batch.iceberg

import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.nio.file.Files

/** Comprehensive test suite for Apache Iceberg Storage Partitioned Join (SPJ) optimization.
  * Tests verify that SPJ correctly eliminates exchange stages when joining partitioned tables.
  */
class IcebergSparkSPJTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private var spark: SparkSession = _
  private val regions = Seq("North", "South", "East", "West")

  private var warehouseDir: java.nio.file.Path = _

  override def beforeAll(): Unit = {
    warehouseDir = Files.createTempDirectory("storage-partition-join-test")
    spark = SparkSession
      .builder()
      .appName("IcebergSPJTest")
      .master("local[*]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.warehouse.dir", warehouseDir.toString)
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.sql.catalog.spark_catalog.type", "hadoop")
      .config("spark.sql.catalog.spark_catalog.warehouse", warehouseDir.toString)
      .enableHiveSupport()
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (warehouseDir != null) {
      org.apache.commons.io.FileUtils.deleteDirectory(warehouseDir.toFile)
    }
  }

  override def beforeEach(): Unit = {
    // Clean up any existing test tables
    Seq("customers", "orders", "shipping").foreach { table =>
      spark.sql(s"DROP TABLE IF EXISTS $table")
    }
  }

  /** Configure Spark session for SPJ optimization
    */
  private def enableSPJ(): Unit = {
    spark.conf.set("spark.sql.sources.v2.bucketing.enabled", "true")
    spark.conf.set("spark.sql.iceberg.planning.preserve-data-grouping", "true")
    spark.conf.set("spark.sql.sources.v2.bucketing.pushPartValues.enabled", "true")
    spark.conf.set("spark.sql.requireAllClusterKeysForCoPartition", "false")
    spark.conf.set("spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled", "true")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1") // Disable broadcast joins
    spark.conf.set("spark.sql.adaptive.enabled", "false") // Disable AQE for predictable plans
  }

  /** Disable SPJ optimization
    */
  private def disableSPJ(): Unit = {
    spark.conf.set("spark.sql.sources.v2.bucketing.enabled", "false")
    spark.conf.set("spark.sql.iceberg.planning.preserve-data-grouping", "false")
  }

  /** Create test tables with matching partition schemes
    */
  private def createTestTables(): Unit = {
    // Create customers table with bucketed partitioning
    spark.sql("""
      CREATE TABLE customers (
        customer_id BIGINT,
        customer_name STRING,
        region STRING,
        email STRING,
        created_date STRING
      ) USING iceberg
      PARTITIONED BY (region, bucket(4, customer_id))
    """)

    // Create orders table with identical partitioning
    spark.sql("""
      CREATE TABLE orders (
        order_id BIGINT,
        customer_id BIGINT,
        region STRING,
        amount DECIMAL(10,2),
        order_date STRING
      ) USING iceberg
      PARTITIONED BY (region, bucket(4, customer_id))
    """)

    // Insert test data for customers
    val customerData = (1 to 1000).map { i =>
      (i.toLong, s"Customer_$i", regions(i % 4), s"customer_$i@test.com", "2024-01-01")
    }

    spark.createDataFrame(customerData).write.mode("append").insertInto("customers")

    // Insert test data for orders
    val orderData = (1 to 5000).flatMap { orderId =>
      val customerId = (orderId % 1000) + 1
      val region = regions(customerId.toInt % 4)
      Seq((orderId.toLong, customerId.toLong, region, BigDecimal(orderId * 10.5), "2024-01-15"))
    }

    spark.createDataFrame(orderData).write.mode("append").insertInto("orders")
  }

  /** Check if a Spark plan contains Exchange operators
    */
  private def hasExchange(plan: SparkPlan): Boolean = {
    val planString = plan.toString
    planString.contains("Exchange") || planString.contains("ShuffleExchange")
  }

  /** Count Exchange operators in a plan
    */
  private def countExchanges(plan: SparkPlan): Int = {
    val planString = plan.toString
    val exchangeLines =
      planString.split("\n").filter(line => line.contains("Exchange") || line.contains("ShuffleExchange"))
    exchangeLines.length
  }

  /** Assert that a DataFrame's execution plan contains no Exchange operators
    */
  private def assertNoExchange(df: DataFrame, message: String = ""): Unit = {
    val plan = df.queryExecution.executedPlan
    val exchangeCount = countExchanges(plan)
    assert(exchangeCount == 0,
           s"Expected no Exchange operators but found $exchangeCount. $message\nPlan:\n${plan.toString}")
  }

  /** Assert that a DataFrame's execution plan contains Exchange operators
    */
  private def assertHasExchange(df: DataFrame, message: String = ""): Unit = {
    val plan = df.queryExecution.executedPlan
    assert(hasExchange(plan), s"Expected Exchange operators in plan but found none. $message\nPlan:\n${plan.toString}")
  }

  /** Analyze execution plan details
    */
  private case class PlanAnalysis(
      hasExchange: Boolean,
      exchangeCount: Int,
      hasShuffle: Boolean,
      hasBatchScan: Boolean,
      hasSort: Boolean,
      planString: String
  )

  private def analyzePlan(df: DataFrame): PlanAnalysis = {
    val plan = df.queryExecution.executedPlan
    val planString = plan.toString

    df.explain()
    PlanAnalysis(
      hasExchange = hasExchange(plan),
      exchangeCount = countExchanges(plan),
      hasShuffle = planString.contains("Exchange hashpartitioning"),
      hasBatchScan = planString.contains("BatchScan"),
      hasSort = plan.exists(_.isInstanceOf[SortExec]),
      planString = planString
    )
  }

  it should "eliminate exchange stages when SPJ is enabled and conditions are met" in {
    createTestTables()
    enableSPJ()

    val customers = spark.table("customers")
    val orders = spark.table("orders")

    val joinDf = customers.join(orders,
                                customers("region") === orders("region") &&
                                  customers("customer_id") === orders("customer_id"))

    assertNoExchange(joinDf, "SPJ should eliminate exchange for co-partitioned join")

    // Verify the join still produces correct results
    val resultCount = joinDf.count()
    resultCount should be > 0L
  }

  it should "contain exchange stages when SPJ is disabled" in {
    createTestTables()
    disableSPJ()

    val customers = spark.table("customers")
    val orders = spark.table("orders")

    val joinDf = customers.join(orders,
                                customers("region") === orders("region") &&
                                  customers("customer_id") === orders("customer_id"))

    assertHasExchange(joinDf, "Join without SPJ should contain exchange")
  }

  it should "produce identical results with and without SPJ" in {
    createTestTables()

    val customers = spark.table("customers")
    val orders = spark.table("orders")

    // Get results without SPJ
    disableSPJ()
    val joinWithoutSPJ = customers.join(orders,
                                        customers("region") === orders("region") &&
                                          customers("customer_id") === orders("customer_id"))
    val countWithoutSPJ = joinWithoutSPJ.count()
    val samplesWithoutSPJ = joinWithoutSPJ.orderBy("order_id").limit(10).collect()

    // Get results with SPJ
    enableSPJ()
    val joinWithSPJ = customers.join(orders,
                                     customers("region") === orders("region") &&
                                       customers("customer_id") === orders("customer_id"))
    val countWithSPJ = joinWithSPJ.count()
    val samplesWithSPJ = joinWithSPJ.orderBy("order_id").limit(10).collect()

    // Compare results
    countWithSPJ shouldEqual countWithoutSPJ
    samplesWithSPJ should contain theSameElementsInOrderAs samplesWithoutSPJ
  }

  it should "require all partition columns in join condition" in {
    createTestTables()
    enableSPJ()

    val customers = spark.table("customers")
    val orders = spark.table("orders")

    // Join only on region (missing customer_id from partition columns)
    val partialJoin = customers.join(orders, Seq("region"))

    // This should still have exchange because not all partition columns are used
    assertHasExchange(partialJoin, "SPJ requires all partition columns in join condition")
  }

  it should "handle missing partition values with pushPartValues enabled" in {
    createTestTables()

    // Delete orders for West region to create missing partition scenario
    spark.sql("DELETE FROM orders WHERE region = 'West'")

    enableSPJ()

    val customers = spark.table("customers")
    val orders = spark.table("orders")

    val leftJoin = customers.join(orders,
                                  customers("region") === orders("region") &&
                                    customers("customer_id") === orders("customer_id"),
                                  "left")

    // SPJ should still work with missing partitions when pushPartValues is enabled
    assertNoExchange(leftJoin, "SPJ should handle missing partitions")

    // Verify West customers appear with null order data
    val westResults = leftJoin
      .filter(col("customers.region") === "West")
      .select("customers.customer_id", "orders.order_id")
      .collect()

    westResults.length should be > 0
    westResults.foreach { row =>
      row.isNullAt(1) shouldBe true // order_id should be null
    }
  }

  it should "work with different join types" in {
    createTestTables()
    enableSPJ()

    val customers = spark.table("customers")
    val orders = spark.table("orders")
    val joinCondition = customers("region") === orders("region") &&
      customers("customer_id") === orders("customer_id")

    // Test different join types
    val joinTypes = Seq("inner", "left", "right", "left_semi", "left_anti")

    joinTypes.foreach { joinType =>
      val joinDf = customers.join(orders, joinCondition, joinType)
      assertNoExchange(joinDf, s"SPJ should work for $joinType join")
      joinDf.count() // Force execution
    }
  }

  it should "verify detailed plan characteristics with SPJ" in {
    createTestTables()
    enableSPJ()

    val customers = spark.table("customers")
    val orders = spark.table("orders")

    val joinDf = customers.join(orders,
                                customers("region") === orders("region") &&
                                  customers("customer_id") === orders("customer_id"))

    val analysis = analyzePlan(joinDf)

    // Verify plan characteristics
    analysis.hasExchange shouldBe false
    analysis.exchangeCount shouldBe 0
    analysis.hasShuffle shouldBe false
    analysis.hasBatchScan shouldBe true

    // Print plan for debugging
    info(s"Execution plan with SPJ:\n${analysis.planString}")
  }

  it should "compare execution plans with and without SPJ" in {
    createTestTables()

    val customers = spark.table("customers")
    val orders = spark.table("orders")
    val joinExpr = customers("region") === orders("region") &&
      customers("customer_id") === orders("customer_id")

    // Analyze without SPJ
    disableSPJ()
    val withoutSPJ = analyzePlan(customers.join(orders, joinExpr))

    // Analyze with SPJ
    enableSPJ()
    val withSPJ = analyzePlan(customers.join(orders, joinExpr))

    // Compare analyses
    withoutSPJ.hasExchange shouldBe true
    withSPJ.hasExchange shouldBe false

    withoutSPJ.exchangeCount should be > 0
    withSPJ.exchangeCount shouldBe 0

    info(s"Plan without SPJ (${withoutSPJ.exchangeCount} exchanges):\n${withoutSPJ.planString}")
    info(s"Plan with SPJ (${withSPJ.exchangeCount} exchanges):\n${withSPJ.planString}")
  }
}
