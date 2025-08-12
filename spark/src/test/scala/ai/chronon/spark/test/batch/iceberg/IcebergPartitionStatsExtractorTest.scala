package ai.chronon.spark.test.batch.iceberg

import ai.chronon.spark.iceberg.IcebergPartitionStatsExtractor
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.nio.file.Files

class IcebergPartitionStatsExtractorTest
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {

    spark = SparkSession
      .builder()
      .appName("IcebergPartitionStatsExtractorTest")
      .master("local[*]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.warehouse.dir", s"${System.getProperty("java.io.tmpdir")}/warehouse")
      .config("spark.sql.catalog.spark_catalog.type", "hadoop")
      .config("spark.sql.catalog.spark_catalog.warehouse", Files.createTempDirectory("partition-stats-test").toString)
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.ui.enabled", "false")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  override def beforeEach(): Unit = {
    spark.sql("DROP TABLE IF EXISTS test_partitioned_table")
  }

  "IcebergPartitionStatsExtractor" should "throw exception for unpartitioned table" in {
    spark.sql("""
      CREATE TABLE test_partitioned_table (
        id BIGINT,
        name STRING,
        value DOUBLE
      ) USING iceberg
      """)

    val extractor = new IcebergPartitionStatsExtractor(spark)

    val exception = intercept[IllegalArgumentException] {
      extractor.extractPartitionedStats("spark_catalog", "default", "test_partitioned_table")
    }

    exception.getMessage should include(
      "Illegal request to compute partition-stats of an un-partitioned table: spark_catalog.default.test_partitioned_table")
  }

  it should "return empty sequence for empty partitioned table" in {
    spark.sql("""
      CREATE TABLE test_partitioned_table (
        id BIGINT,
        name STRING,
        region STRING,
        value DOUBLE
      ) USING iceberg
      PARTITIONED BY (region)
      """)

    val extractor = new IcebergPartitionStatsExtractor(spark)
    val stats = extractor.extractPartitionedStats("spark_catalog", "default", "test_partitioned_table")

    stats should be(empty)
  }

  it should "extract partition stats from table with data" in {
    spark.sql("""
      CREATE TABLE test_partitioned_table (
        id BIGINT,
        name STRING,
        region STRING,
        value DOUBLE
      ) USING iceberg
      PARTITIONED BY (region)
      """)

    spark.sql("""
      INSERT INTO test_partitioned_table VALUES
      (1, 'Alice', 'North', 100.0),
      (2, 'Bob', 'North', 200.0),
      (3, 'Charlie', 'South', 150.0),
      (4, NULL, 'South', 300.0),
      (5, 'Eve', 'East', NULL)
      """)

    // Force table refresh to ensure snapshot is available
    spark.sql("REFRESH TABLE test_partitioned_table")

    // Verify data was inserted
    val rowCount = spark.sql("SELECT COUNT(*) FROM test_partitioned_table").collect()(0).getLong(0)
    rowCount should be(5)

    val extractor = new IcebergPartitionStatsExtractor(spark)
    val stats = extractor.extractPartitionedStats("spark_catalog", "default", "test_partitioned_table")

    stats should not be empty
    stats.length should be(3)

    val totalRows = stats.map(_.rowCount).sum
    totalRows should be(5)

    // Verify specific partition stats
    val southStats = stats.find(_.partitionColToValue("region") == "South").get
    southStats.rowCount should be(2)
    southStats.colToNullCount("value") should be(0) // No null values in South partition
    southStats.colToNullCount("name") should be(1) // One null name (Charlie, NULL)
    southStats.colToNullCount("id") should be(0) // No null ids

    val northStats = stats.find(_.partitionColToValue("region") == "North").get
    northStats.rowCount should be(2)
    northStats.colToNullCount("value") should be(0) // No null values in North partition
    northStats.colToNullCount("name") should be(0) // No null names (Alice, Bob)
    northStats.colToNullCount("id") should be(0) // No null ids

    val eastStats = stats.find(_.partitionColToValue("region") == "East").get
    eastStats.rowCount should be(1)
    eastStats.colToNullCount("value") should be(1) // One null value (Eve has NULL value)
    eastStats.colToNullCount("name") should be(0) // No null names (Eve)
    eastStats.colToNullCount("id") should be(0) // No null ids
  }
}
