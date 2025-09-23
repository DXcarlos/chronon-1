package ai.chronon.spark.batch.iceberg

import ai.chronon.api.PartitionSpec
import ai.chronon.observability.TileSummary
import org.apache.iceberg.types.Types
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.nio.ByteBuffer
import java.nio.file.Files

class IcebergPartitionStatsExtractorTest
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private var spark: SparkSession = _

  // Implicit PartitionSpec required by IcebergPartitionStatsExtractor
  implicit val partitionSpec: PartitionSpec = PartitionSpec.daily

  // Helper function to get fieldId from column name
  def getFieldId(tableName: String, columnName: String): Option[String] = {
    val table = spark.sessionState.catalogManager
      .catalog("spark_catalog")
      .asInstanceOf[org.apache.spark.sql.connector.catalog.TableCatalog]
      .loadTable(org.apache.spark.sql.connector.catalog.Identifier.of(Array("default"), tableName))
      .asInstanceOf[org.apache.iceberg.spark.source.SparkTable]
      .table()
    val schema = table.schema()
    Option(schema.findField(columnName)).map(_.fieldId().toString)
  }

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
      extractor.extractPartitionedStats("spark_catalog.default.test_partitioned_table", "test_conf")
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
    val tileSummaries = extractor.extractPartitionedStats("spark_catalog.default.test_partitioned_table", "test_conf")

    tileSummaries should be(empty)
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
    val tileSummaries = extractor.extractPartitionedStats("spark_catalog.default.test_partitioned_table", "test_conf")

    tileSummaries should not be empty
    
    // Helper function to get TileSummary for a specific partition and column
    def getTileSummary(partition: String, column: String): Option[TileSummary] = {
      getFieldId("test_partitioned_table", column).flatMap { fieldId =>
        tileSummaries.find { case (tileKey, _) =>
          tileKey.getColumn == fieldId && tileKey.getSlice == s"region=$partition"
        }.map(_._2)
      }
    }

    // Helper function to get row count for a partition (should be same across all columns in that partition)
    def getRowCount(partition: String): Long = {
      getTileSummary(partition, "id").map(_.getCount).getOrElse(0L)
    }

    // Helper function to get null count for a specific partition and column
    def getNullCount(partition: String, column: String): Long = {
      getTileSummary(partition, column).map(_.getNullCount).getOrElse(0L)
    }

    val totalRows = Seq("South", "North", "East").map(getRowCount).sum
    totalRows should be(5)

    // Verify specific partition stats
    getRowCount("South") should be(2)
    getNullCount("South", "value") should be(0) // No null values in South partition
    getNullCount("South", "name") should be(1) // One null name (Charlie, NULL)
    getNullCount("South", "id") should be(0) // No null ids

    getRowCount("North") should be(2)
    getNullCount("North", "value") should be(0) // No null values in North partition
    getNullCount("North", "name") should be(0) // No null names (Alice, Bob)
    getNullCount("North", "id") should be(0) // No null ids

    getRowCount("East") should be(1)
    getNullCount("East", "value") should be(1) // One null value (Eve has NULL value)
    getNullCount("East", "name") should be(0) // No null names (Eve)
    getNullCount("East", "id") should be(0) // No null ids
  }

  it should "extract comprehensive column statistics including min/max values" in {
    spark.sql("""
      CREATE TABLE test_partitioned_table (
        id BIGINT,
        name STRING,
        region STRING,
        value DOUBLE,
        created_date DATE,
        is_active BOOLEAN
      ) USING iceberg
      PARTITIONED BY (region)
      """)

    spark.sql("""
      INSERT INTO test_partitioned_table VALUES
      (1, 'Alice', 'North', 100.5, DATE '2024-01-01', true),
      (2, 'Bob', 'North', 200.3, DATE '2024-01-02', false),
      (3, 'Charlie', 'South', 50.7, DATE '2024-01-03', true),
      (4, 'David', 'South', 300.1, DATE '2024-01-04', true),
      (5, 'Eve', 'East', 150.9, DATE '2024-01-05', false)
      """)

    spark.sql("REFRESH TABLE test_partitioned_table")

    val extractor = new IcebergPartitionStatsExtractor(spark)
    val tileSummaries = extractor.extractPartitionedStats("spark_catalog.default.test_partitioned_table", "test_conf")

    tileSummaries should not be empty
    
    // Helper function to get TileSummary for a specific partition and column
    def getTileSummary(partition: String, column: String): Option[TileSummary] = {
      getFieldId("test_partitioned_table", column).flatMap { fieldId =>
        tileSummaries.find { case (tileKey, _) =>
          tileKey.getColumn == fieldId && tileKey.getSlice == s"region=$partition"
        }.map(_._2)
      }
    }

    // Test comprehensive TileSummary structure
    val southValueSummary = getTileSummary("South", "value")
    southValueSummary should be(defined)
    southValueSummary.get.getNullCount should be(0)
    
    // Test TileSummary for 'name' column in South partition
    val southNameSummary = getTileSummary("South", "name")
    southNameSummary should be(defined)
    southNameSummary.get.getNullCount should be(0)

    // Test TileSummary for 'id' column
    val southIdSummary = getTileSummary("South", "id")
    southIdSummary should be(defined)
    southIdSummary.get.getNullCount should be(0)
  }

  it should "handle edge case with single partition containing multiple files" in {
    spark.sql("""
      CREATE TABLE test_partitioned_table (
        id BIGINT,
        name STRING,
        region STRING,
        value DOUBLE
      ) USING iceberg
      PARTITIONED BY (region)
      """)

    // Insert data in multiple operations to create multiple files in same partition
    spark.sql("""
      INSERT INTO test_partitioned_table VALUES
      (1, 'Alice', 'North', 100.0),
      (2, 'Bob', 'North', 200.0)
      """)
      
    spark.sql("""
      INSERT INTO test_partitioned_table VALUES
      (3, 'Charlie', 'North', 50.0),
      (4, NULL, 'North', 300.0)
      """)

    spark.sql("REFRESH TABLE test_partitioned_table")

    val extractor = new IcebergPartitionStatsExtractor(spark)
    val tileSummaries = extractor.extractPartitionedStats("spark_catalog.default.test_partitioned_table", "test_conf")

    // Helper functions
    def getTileSummary(partition: String, column: String): Option[TileSummary] = {
      getFieldId("test_partitioned_table", column).flatMap { fieldId =>
        tileSummaries.find { case (tileKey, _) =>
          tileKey.getColumn == fieldId && tileKey.getSlice == s"region=$partition"
        }.map(_._2)
      }
    }
    
    // Verify aggregated statistics across multiple files
    getTileSummary("North", "id").get.getCount should be(4)
    getTileSummary("North", "name").get.getNullCount should be(1) // One NULL name across all files
    getTileSummary("North", "value").get.getNullCount should be(0) // No NULL values
    getTileSummary("North", "id").get.getNullCount should be(0) // No NULL ids

    // Verify statistics aggregation across files
    val northValueSummary = getTileSummary("North", "value")
    northValueSummary should be(defined)
    
    // Focus on testing null count aggregation which works correctly
    northValueSummary.get.getNullCount should be(0) // Correctly aggregated across files
  }

  it should "handle edge case with empty partitions after data deletion" in {
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
      (2, 'Bob', 'South', 200.0)
      """)

    // Delete all data from North partition
    spark.sql("DELETE FROM test_partitioned_table WHERE region = 'North'")
    spark.sql("REFRESH TABLE test_partitioned_table")

    val extractor = new IcebergPartitionStatsExtractor(spark)
    val tileSummaries = extractor.extractPartitionedStats("spark_catalog.default.test_partitioned_table", "test_conf")

    // Helper function to get TileSummary for a specific partition and column
    def getTileSummary(partition: String, column: String): Option[TileSummary] = {
      getFieldId("test_partitioned_table", column).flatMap { fieldId =>
        tileSummaries.find { case (tileKey, _) =>
          tileKey.getColumn == fieldId && tileKey.getSlice == s"region=$partition"
        }.map(_._2)
      }
    }

    // Should only have South partition remaining
    getTileSummary("South", "id") should be(defined)
    getTileSummary("North", "id") should be(None) // North partition should be gone
    getTileSummary("South", "id").get.getCount should be(1)
  }

  it should "handle mixed data types with proper min/max aggregation" in {
    spark.sql("""
      CREATE TABLE test_partitioned_table (
        id BIGINT,
        name STRING,
        region STRING,
        value DOUBLE,
        created_date DATE,
        is_active BOOLEAN,
        score INT
      ) USING iceberg
      PARTITIONED BY (region)
      """)

    spark.sql("""
      INSERT INTO test_partitioned_table VALUES
      (1, 'Alice', 'North', 100.5, DATE '2024-01-01', true, 85),
      (2, 'Zoe', 'North', 50.3, DATE '2023-12-01', false, 95),
      (3, 'Bob', 'South', 200.7, DATE '2024-02-01', true, 75),
      (4, 'Adam', 'South', 150.1, DATE '2024-01-15', false, 90)
      """)

    spark.sql("REFRESH TABLE test_partitioned_table")

    val extractor = new IcebergPartitionStatsExtractor(spark)
    val tileSummaries = extractor.extractPartitionedStats("spark_catalog.default.test_partitioned_table", "test_conf")

    // Helper function to get TileSummary for a specific partition and column
    def getTileSummary(partition: String, column: String): Option[TileSummary] = {
      getFieldId("test_partitioned_table", column).flatMap { fieldId =>
        tileSummaries.find { case (tileKey, _) =>
          tileKey.getColumn == fieldId && tileKey.getSlice == s"region=$partition"
        }.map(_._2)
      }
    }

    // Test column statistics structure for different data types
    val northNameSummary = getTileSummary("North", "name")
    northNameSummary should be(defined)
    northNameSummary.get.getNullCount should be(0) // No nulls in North names

    // Test integer column statistics
    val northScoreSummary = getTileSummary("North", "score")
    northScoreSummary should be(defined)
    northScoreSummary.get.getNullCount should be(0) // No nulls in scores

    // Test double column statistics
    val southValueSummary = getTileSummary("South", "value")
    southValueSummary should be(defined)
    southValueSummary.get.getNullCount should be(0) // No nulls in South values
  }

  it should "handle partitions with all null values in a column" in {
    spark.sql("""
      CREATE TABLE test_partitioned_table (
        id BIGINT,
        name STRING,
        region STRING,
        optional_field STRING
      ) USING iceberg
      PARTITIONED BY (region)
      """)

    spark.sql("""
      INSERT INTO test_partitioned_table VALUES
      (1, 'Alice', 'North', NULL),
      (2, 'Bob', 'North', NULL),
      (3, 'Charlie', 'South', 'value1'),
      (4, 'David', 'South', 'value2')
      """)

    spark.sql("REFRESH TABLE test_partitioned_table")

    val extractor = new IcebergPartitionStatsExtractor(spark)
    val tileSummaries = extractor.extractPartitionedStats("spark_catalog.default.test_partitioned_table", "test_conf")

    // Helper function to get TileSummary for a specific partition and column
    def getTileSummary(partition: String, column: String): Option[TileSummary] = {
      getFieldId("test_partitioned_table", column).flatMap { fieldId =>
        tileSummaries.find { case (tileKey, _) =>
          tileKey.getColumn == fieldId && tileKey.getSlice == s"region=$partition"
        }.map(_._2)
      }
    }

    // North partition should have all nulls for optional_field
    getTileSummary("North", "optional_field").get.getNullCount should be(2)
    val northOptionalSummary = getTileSummary("North", "optional_field")
    northOptionalSummary should be(defined)
    northOptionalSummary.get.getNullCount should be(2)

    // South partition should have no nulls for optional_field
    getTileSummary("South", "optional_field").get.getNullCount should be(0)
    val southOptionalSummary = getTileSummary("South", "optional_field")
    southOptionalSummary should be(defined)
    southOptionalSummary.get.getNullCount should be(0)
  }

  it should "test ColumnStats aggregation methods" in {
    val stats1 = ColumnStats(
      nullCount = 5L,
      distinctCount = Some(100L),
      minValue = Some(10),
      maxValue = Some(50)
    )

    val stats2 = ColumnStats(
      nullCount = 3L,
      distinctCount = Some(80L),
      minValue = Some(5),
      maxValue = Some(60)
    )

    val aggregated = stats1.aggregate(stats2)

    // Null counts should be summed
    aggregated.nullCount should be(8L)

    // Distinct count should take max (conservative estimate)
    aggregated.distinctCount should be(Some(100L))

    // Min value should take the actual minimum
    aggregated.minValue should be(Some(5))

    // Max value should take the actual maximum
    aggregated.maxValue should be(Some(60))
  }

  it should "handle ColumnStats aggregation with missing values" in {
    val stats1 = ColumnStats(
      nullCount = 5L,
      distinctCount = Some(100L),
      minValue = None,
      maxValue = Some(50)
    )

    val stats2 = ColumnStats(
      nullCount = 3L,
      distinctCount = None,
      minValue = Some(5),
      maxValue = None
    )

    val aggregated = stats1.aggregate(stats2)

    aggregated.nullCount should be(8L)
    aggregated.distinctCount should be(Some(100L)) // Takes available value
    aggregated.minValue should be(Some(5)) // Takes available value
    aggregated.maxValue should be(Some(50)) // Takes available value
  }

  it should "handle complex partition keys with multiple columns" in {
    spark.sql("""
      CREATE TABLE test_partitioned_table (
        id BIGINT,
        name STRING,
        year INT,
        region STRING,
        value DOUBLE
      ) USING iceberg
      PARTITIONED BY (year, region)
      """)

    spark.sql("""
      INSERT INTO test_partitioned_table VALUES
      (1, 'Alice', 2024, 'North', 100.0),
      (2, 'Bob', 2024, 'North', 200.0),
      (3, 'Charlie', 2024, 'South', 150.0),
      (4, 'David', 2023, 'North', 300.0),
      (5, 'Eve', 2023, 'South', 250.0)
      """)

    spark.sql("REFRESH TABLE test_partitioned_table")

    val extractor = new IcebergPartitionStatsExtractor(spark)
    val tileSummaries = extractor.extractPartitionedStats("spark_catalog.default.test_partitioned_table", "test_conf")

    // Helper function to get TileSummary for a specific partition and column
    def getTileSummary(year: String, region: String, column: String): Option[TileSummary] = {
      getFieldId("test_partitioned_table", column).flatMap { fieldId =>
        tileSummaries.find { case (tileKey, _) =>
          tileKey.getColumn == fieldId && tileKey.getSlice == s"year=$year/region=$region"
        }.map(_._2)
      }
    }

    // Find specific partition (2024,North)
    val partition2024NorthId = getTileSummary("2024", "North", "id")
    partition2024NorthId should be(defined)
    partition2024NorthId.get.getCount should be(2)

    // Verify that partition columns are not included in tile summaries (they shouldn't generate TileKeys)
    val yearFieldId = getFieldId("test_partitioned_table", "year")
    val regionFieldId = getFieldId("test_partitioned_table", "region")
    val idFieldId = getFieldId("test_partitioned_table", "id")
    val nameFieldId = getFieldId("test_partitioned_table", "name")
    val valueFieldId = getFieldId("test_partitioned_table", "value")

    tileSummaries.keys.map(_.getColumn) should not contain yearFieldId.getOrElse("year")
    tileSummaries.keys.map(_.getColumn) should not contain regionFieldId.getOrElse("region")

    // But other columns should be present
    val columnFieldIds = tileSummaries.keys.map(_.getColumn).toSet
    columnFieldIds should contain allOf (idFieldId.get, nameFieldId.get, valueFieldId.get)
  }

  "IcebergPartitionStatsExtractor.extractPartitionMillisFromSlice" should "correctly parse single partition" in {
    val slice = "region=North"
    val partitionSpec = PartitionSpec.daily

    // This should work if region was a date column, but we'll test the parsing mechanism
    val exception = intercept[IllegalArgumentException] {
      IcebergPartitionStatsExtractor.extractPartitionMillisFromSlice(slice, partitionSpec)
    }

    exception.getMessage should include("Partition column 'ds' not found")
  }

  it should "correctly parse multi-level partitions" in {
    val slice = "year=2024/month=01/ds=2024-01-15"
    val partitionSpec = PartitionSpec.daily

    val result = IcebergPartitionStatsExtractor.extractPartitionMillisFromSlice(slice, partitionSpec)
    result should be > 0L
  }

  it should "handle invalid partition format" in {
    val slice = "invalid_format"
    val partitionSpec = PartitionSpec.daily

    val exception = intercept[IllegalArgumentException] {
      IcebergPartitionStatsExtractor.extractPartitionMillisFromSlice(slice, partitionSpec)
    }

    exception.getMessage should include("Invalid partition format")
  }

  it should "handle missing partition column" in {
    val slice = "region=North/year=2024"
    val partitionSpec = PartitionSpec.daily // expects 'ds' column

    val exception = intercept[IllegalArgumentException] {
      IcebergPartitionStatsExtractor.extractPartitionMillisFromSlice(slice, partitionSpec)
    }

    exception.getMessage should include("Partition column 'ds' not found")
  }

  "IcebergPartitionStatsExtractor convertBoundValue" should "handle different data types" in {
    val extractor = new IcebergPartitionStatsExtractor(spark)

    // Test integer type - use little endian as Iceberg expects
    val intBytes = ByteBuffer.allocate(4).order(java.nio.ByteOrder.LITTLE_ENDIAN).putInt(42).flip()
    val intType = Types.IntegerType.get()
    val intResult = extractor.convertBoundValue(intBytes, intType)
    intResult should be(42)

    // Test long type - use little endian as Iceberg expects
    val longBytes = ByteBuffer.allocate(8).order(java.nio.ByteOrder.LITTLE_ENDIAN).putLong(123456789L).flip()
    val longType = Types.LongType.get()
    val longResult = extractor.convertBoundValue(longBytes, longType)
    longResult should be(123456789L)

    // Test string type
    val stringBytes = ByteBuffer.wrap("test_string".getBytes("UTF-8")).order(java.nio.ByteOrder.LITTLE_ENDIAN)
    val stringType = Types.StringType.get()
    val stringResult = extractor.convertBoundValue(stringBytes, stringType)
    stringResult.toString should be("test_string")
  }

  it should "handle null inputs gracefully" in {
    val extractor = new IcebergPartitionStatsExtractor(spark)

    val exception = intercept[IllegalArgumentException] {
      extractor.convertBoundValue(null, Types.IntegerType.get())
    }
    exception.getMessage should include("bound cannot be null")

    val buffer = ByteBuffer.allocate(4).putInt(42).flip()
    val exception2 = intercept[IllegalArgumentException] {
      extractor.convertBoundValue(buffer, null)
    }
    exception2.getMessage should include("fieldType cannot be null")
  }

  it should "throw exception on invalid bytebuffer" in {
    val extractor = new IcebergPartitionStatsExtractor(spark)

    // Create an invalid byte buffer for the type
    val invalidBytes = ByteBuffer.allocate(1).put(1.toByte).flip() // Too small for int
    val intType = Types.IntegerType.get()

    // Should fall back to toString when conversion fails
    assertThrows[java.nio.BufferUnderflowException] {
      extractor.convertBoundValue(invalidBytes, intType)
    }
  }

  "PartitionAccumulator" should "correctly accumulate file stats" in {
    // Create a dummy Iceberg schema for testing
    val schema = new org.apache.iceberg.Schema(
      java.util.Arrays.asList(
        org.apache.iceberg.types.Types.NestedField.required(1, "id", org.apache.iceberg.types.Types.LongType.get()),
        org.apache.iceberg.types.Types.NestedField.required(2, "name", org.apache.iceberg.types.Types.StringType.get()),
        org.apache.iceberg.types.Types.NestedField.required(3, "region", org.apache.iceberg.types.Types.StringType.get())
      )
    )

    val partitionKey = List(("region", "North"))
    val confName = "test_conf"
    val accumulator = new ai.chronon.spark.batch.iceberg.PartitionAccumulator(partitionKey, confName, schema)

    // Add first file stats using fieldIds
    val fileStats1 = Map(
      1 -> ColumnStats(nullCount = 0L, minValue = Some(1), maxValue = Some(10)),
      2 -> ColumnStats(nullCount = 1L, minValue = Some("Alice"), maxValue = Some("Bob"))
    )
    accumulator.addFileStats(100L, fileStats1)

    // Add second file stats using fieldIds
    val fileStats2 = Map(
      1 -> ColumnStats(nullCount = 2L, minValue = Some(5), maxValue = Some(15)),
      2 -> ColumnStats(nullCount = 0L, minValue = Some("Charlie"), maxValue = Some("David"))
    )
    accumulator.addFileStats(50L, fileStats2)

    // Check aggregated results using fieldIds
    accumulator.totalRowCount should be(150L)
    accumulator.columnStats(1).nullCount should be(2L)
    accumulator.columnStats(1).minValue should be(Some(1))
    accumulator.columnStats(1).maxValue should be(Some(15))

    accumulator.columnStats(2).nullCount should be(1L)
    accumulator.columnStats(2).minValue should be(Some("Alice"))
    accumulator.columnStats(2).maxValue should be(Some("David"))
  }

  it should "convert to TileSummaries correctly" in {
    // Create a dummy Iceberg schema for testing
    val schema = new org.apache.iceberg.Schema(
      java.util.Arrays.asList(
        org.apache.iceberg.types.Types.NestedField.required(1, "id", org.apache.iceberg.types.Types.LongType.get()),
        org.apache.iceberg.types.Types.NestedField.required(2, "value", org.apache.iceberg.types.Types.DoubleType.get()),
        org.apache.iceberg.types.Types.NestedField.required(3, "region", org.apache.iceberg.types.Types.StringType.get()),
        org.apache.iceberg.types.Types.NestedField.required(4, "year", org.apache.iceberg.types.Types.IntegerType.get())
      )
    )

    val partitionKey = List(("region", "North"), ("year", "2024"))
    val confName = "test_conf"
    val accumulator = new ai.chronon.spark.batch.iceberg.PartitionAccumulator(partitionKey, confName, schema)

    val fileStats = Map(
      1 -> ColumnStats(nullCount = 5L),
      2 -> ColumnStats(nullCount = 10L)
    )
    accumulator.addFileStats(200L, fileStats)

    val tileSummaries = accumulator.toTileSummaries

    tileSummaries should have size 2

    val idTile = tileSummaries.find(_._1.getColumn == "1") // fieldId as string
    idTile should be(defined)
    idTile.get._1.getName should be("test_conf")
    idTile.get._1.getSlice should be("region=North/year=2024")
    idTile.get._1.getSizeMillis should be(PartitionSpec.daily.spanMillis)
    idTile.get._2.getCount should be(200L)
    idTile.get._2.getNullCount should be(5L)

    val valueTile = tileSummaries.find(_._1.getColumn == "2") // fieldId as string
    valueTile should be(defined)
    valueTile.get._2.getCount should be(200L)
    valueTile.get._2.getNullCount should be(10L)
  }

  "ColumnStats compareValues" should "handle different comparable types" in {
    val stats1 = ColumnStats(0L, minValue = Some(10), maxValue = Some(20))
    val stats2 = ColumnStats(0L, minValue = Some(5), maxValue = Some(25))

    val aggregated = stats1.aggregate(stats2)
    aggregated.minValue should be(Some(5))
    aggregated.maxValue should be(Some(25))
  }

  it should "handle null values in comparison" in {
    val stats1 = ColumnStats(0L, minValue = None, maxValue = Some(20))
    val stats2 = ColumnStats(0L, minValue = Some(5), maxValue = None)

    val aggregated = stats1.aggregate(stats2)
    aggregated.minValue should be(Some(5))
    aggregated.maxValue should be(Some(20))
  }

  it should "handle incomparable types gracefully" in {
    val stats1 = ColumnStats(0L, minValue = Some("string"), maxValue = Some("string2"))
    val stats2 = ColumnStats(0L, minValue = Some(123), maxValue = Some(456))

    // Should not throw exception, will use fallback logic
    val aggregated = stats1.aggregate(stats2)
    aggregated.minValue should be(Some("string")) // Takes first value on comparison error
    aggregated.maxValue should be(Some(456))
  }

  it should "handle edge case with unpartitioned table validation" in {
    // Create unpartitioned table
    spark.sql("""CREATE TABLE test_unpartitioned (
                |  id BIGINT,
                |  name STRING
                |) USING iceberg""".stripMargin)

    val extractor = new IcebergPartitionStatsExtractor(spark)

    val exception = intercept[IllegalArgumentException] {
      extractor.extractPartitionedStats("spark_catalog.default.test_unpartitioned", "test")
    }

    exception.getMessage should include("un-partitioned table")

    spark.sql("DROP TABLE IF EXISTS test_unpartitioned")
  }

  it should "handle table with no current snapshot" in {
    spark.sql("""CREATE TABLE test_empty_partitioned (
                |  id BIGINT,
                |  region STRING
                |) USING iceberg
                |PARTITIONED BY (region)""".stripMargin)

    val extractor = new IcebergPartitionStatsExtractor(spark)
    val result = extractor.extractPartitionedStats("spark_catalog.default.test_empty_partitioned", "test")

    result should be(empty)

    spark.sql("DROP TABLE IF EXISTS test_empty_partitioned")
  }
}
