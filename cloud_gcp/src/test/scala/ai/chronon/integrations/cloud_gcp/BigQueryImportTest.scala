package ai.chronon.integrations.cloud_gcp

import ai.chronon.api
import ai.chronon.api.planner.MetaDataUtils
import ai.chronon.api.{MetaData, PartitionRange, PartitionSpec, TableDependency}
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.submission.SparkSessionBuilder
import com.google.cloud.bigquery._
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertTrue
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

class BigQueryImportTest extends AnyFlatSpec with MockitoSugar {

  private val partitionSpec = PartitionSpec.daily

  lazy val spark: SparkSession = SparkSessionBuilder.build(
    "BigQueryStagingQueryTest",
    local = true,
    additionalConfig = Some(
      Map(
        "spark.sql.catalog.test_catalog.warehouse" -> "gs://test-warehouse/data"
      )
    )
  )

  lazy val tableUtils: TableUtils = TableUtils(spark)

  private def createTestStagingQuery(outputTable: String = "test_catalog.test_dataset.test_table"): api.StagingQuery = {
    // Create base metadata
    val baseMetaData = new MetaData()
    baseMetaData.setName("base_staging_query")
    baseMetaData.setOutputNamespace("test_catalog.test_namespace")

    // Use MetaDataUtils to layer the metadata with proper configuration
    implicit val implicitPartitionSpec: PartitionSpec = partitionSpec
    val layeredMetaData = MetaDataUtils.layer(
      baseMetadata = baseMetaData,
      modeName = "test",
      nodeName = "test_staging_query",
      tableDependencies = Seq.empty[TableDependency],
      outputTableOverride = Some(outputTable)
    )

    val stagingQuery = new api.StagingQuery()
    stagingQuery.setMetaData(layeredMetaData)
    stagingQuery.setQuery("SELECT * FROM source_table WHERE ds = '{{ ds }}'")
    stagingQuery.setStartPartition("2024-01-01")
    stagingQuery
  }

  private def mockBigQueryClient(): BigQuery = {
    val mockClient = mock[BigQuery]
    val mockJob = mock[Job]
    val mockJobStatus = mock[JobStatus]
    val mockTable = mock[Table]
    val mockTableId = mock[TableId]

    // Mock successful job execution
    when(mockJob.waitFor()).thenReturn(mockJob)
    when(mockJob.getStatus).thenReturn(mockJobStatus)
    when(mockJobStatus.getError).thenReturn(null)
    when(mockClient.create(any[JobInfo])).thenReturn(mockJob)
    when(mockClient.create(any[TableInfo])).thenReturn(mockTable)
    when(mockTable.getTableId).thenReturn(mockTableId)

    mockClient
  }

  private def mockBigQueryClientWithError(errorMessage: String): BigQuery = {
    val mockClient = mock[BigQuery]
    val mockJob = mock[Job]
    val mockJobStatus = mock[JobStatus]
    val mockError = mock[BigQueryError]

    when(mockJob.waitFor()).thenReturn(mockJob)
    when(mockJob.getStatus).thenReturn(mockJobStatus)
    when(mockJobStatus.getError).thenReturn(mockError)
    when(mockError.getMessage).thenReturn(errorMessage)
    when(mockClient.create(any[JobInfo])).thenReturn(mockJob)

    mockClient
  }

  "BigQueryStagingQuery" should "initialize with correct configuration" in {
    val stagingQueryConf = createTestStagingQuery()
    val endPartition = "2024-01-31"

    val bigQueryStagingQuery = new BigQueryImport(stagingQueryConf, endPartition, tableUtils)

    // Verify the class is initialized correctly
    assertTrue("BigQueryStagingQuery should be initialized", bigQueryStagingQuery != null)
  }

  it should "generate correct export data template without setups" in {
    val stagingQueryConf = createTestStagingQuery()
    val endPartition = "2024-01-31"

    val bigQueryStagingQuery = new BigQueryImport(stagingQueryConf, endPartition, tableUtils)

    val uri = "gs://test-bucket/test-path/*.parquet"
    val sql = "SELECT * FROM test_table WHERE ds = '2024-01-01'"
    val setups = Seq.empty[String]
    val result = bigQueryStagingQuery.exportDataTemplate(uri, sql, setups)

    assertTrue("Export template should contain URI", result.contains(uri))
    assertTrue("Export template should contain SQL", result.contains(sql))
    assertTrue("Export template should use parquet format", result.contains("format = 'parquet'"))
    assertTrue("Export template should have overwrite = true", result.contains("overwrite = true"))
    assertTrue("Export template should not contain BEGIN/END when no setups", !result.contains("BEGIN"))
  }

  it should "generate correct multi-statement query with setups" in {
    val stagingQueryConf = createTestStagingQuery()
    val endPartition = "2024-01-31"

    val bigQueryStagingQuery = new BigQueryImport(stagingQueryConf, endPartition, tableUtils)

    val uri = "gs://test-bucket/test-path/*.parquet"
    val sql = "SELECT * FROM test_table WHERE ds = '2024-01-01'"
    val setups = Seq(
      "CREATE TEMP TABLE temp_data AS SELECT * FROM source_data",
      "CREATE TEMP FUNCTION process_data(input STRING) AS (UPPER(input))"
    )
    val result = bigQueryStagingQuery.exportDataTemplate(uri, sql, setups)

    assertTrue("Multi-statement query should contain BEGIN", result.contains("BEGIN"))
    assertTrue("Multi-statement query should contain END;", result.contains("END;"))
    assertTrue("Multi-statement query should contain first setup", result.contains(setups(0)))
    assertTrue("Multi-statement query should contain second setup", result.contains(setups(1)))
    assertTrue("Multi-statement query should contain URI", result.contains(uri))
    assertTrue("Multi-statement query should contain SQL", result.contains(sql))
    assertTrue("Multi-statement query should use parquet format", result.contains("format = 'parquet'"))
    assertTrue("Multi-statement query should have overwrite = true", result.contains("overwrite = true"))

    // Verify that setup statements are properly terminated with semicolons
    assertTrue("Setup statements should end with semicolon", result.contains(setups(0) + ";"))
    assertTrue("Setup statements should end with semicolon", result.contains(setups(1) + ";"))
  }

  it should "generate correct destination prefix" in {
    val stagingQueryConf = createTestStagingQuery()
    val endPartition = "2024-01-31"

    val bigQueryStagingQuery = new BigQueryImport(stagingQueryConf, endPartition, tableUtils)

    val datePartitionColumn = "ds"
    val datePartitionValue = "2024-01-01"
    val result = bigQueryStagingQuery.destPrefix(datePartitionColumn, datePartitionValue)

    assertTrue("Destination prefix should contain partition column",
               result.contains(s"${datePartitionColumn}=${datePartitionValue}"))
    assertTrue("Destination prefix should end with parquet extension", result.endsWith("*.parquet"))
  }

  it should "successfully compute with single partition" in {
    val stagingQueryConf = createTestStagingQuery()
    val endPartition = "2024-01-31"
    val mockClient = mockBigQueryClient()

    // Create a testable version of BigQueryStagingQuery
    val bigQueryStagingQuery = new BigQueryImport(stagingQueryConf, endPartition, tableUtils) {
      override private[cloud_gcp] lazy val bigQueryClient: BigQuery = mockClient
    }

    val range = PartitionRange("2024-01-01", "2024-01-01")(partitionSpec)
    val setups = Seq.empty[String]

    // This should not throw an exception
    bigQueryStagingQuery.compute(range, setups, Some(true))

    // Verify that BigQuery client methods were called
    verify(mockClient).create(any[JobInfo])
    verify(mockClient).create(any[TableInfo])
  }

  it should "successfully compute with multiple partitions" in {
    val stagingQueryConf = createTestStagingQuery()
    val endPartition = "2024-01-31"
    val mockClient = mockBigQueryClient()

    val bigQueryStagingQuery = new BigQueryImport(stagingQueryConf, endPartition, tableUtils) {
      override private[cloud_gcp] lazy val bigQueryClient: BigQuery = mockClient
    }

    val range = PartitionRange("2024-01-01", "2024-01-03")(partitionSpec)
    val setups = Seq.empty[String]

    bigQueryStagingQuery.compute(range, setups, Some(true))

    // Verify BigQuery client was called for each partition (3 times for export, 1 times for table creation)
    verify(mockClient, org.mockito.Mockito.times(3)).create(any[JobInfo])
    verify(mockClient, org.mockito.Mockito.times(1)).create(any[TableInfo])
  }

  it should "handle BigQuery job errors gracefully" in {
    val stagingQueryConf = createTestStagingQuery()
    val endPartition = "2024-01-31"
    val errorMessage = "BigQuery job failed"
    val mockClient = mockBigQueryClientWithError(errorMessage)

    val bigQueryStagingQuery = new BigQueryImport(stagingQueryConf, endPartition, tableUtils) {
      override private[cloud_gcp] lazy val bigQueryClient: BigQuery = mockClient
    }

    val range = PartitionRange("2024-01-01", "2024-01-01")(partitionSpec)
    val setups = Seq.empty[String]

    // This should throw a RuntimeException with the BigQuery error message
    val exception = intercept[RuntimeException] {
      bigQueryStagingQuery.compute(range, setups, Some(true))
    }

    assertTrue("Exception should contain BigQuery error message", exception.getMessage.contains(errorMessage))
  }

  it should "create external table with correct configuration" in {
    val stagingQueryConf = createTestStagingQuery()
    val endPartition = "2024-01-31"
    val mockClient = mock[BigQuery]
    val mockJob = mock[Job]
    val mockJobStatus = mock[JobStatus]
    val mockTable = mock[Table]
    val mockTableId = mock[TableId]

    // Capture the TableInfo that gets passed to create()
    var capturedTableInfo: TableInfo = null
    when(mockJob.waitFor()).thenReturn(mockJob)
    when(mockJob.getStatus).thenReturn(mockJobStatus)
    when(mockJobStatus.getError).thenReturn(null)
    when(mockClient.create(any[JobInfo])).thenReturn(mockJob)
    when(mockClient.create(any[TableInfo])).thenAnswer { invocation =>
      capturedTableInfo = invocation.getArgument(0).asInstanceOf[TableInfo]
      mockTable
    }
    when(mockTable.getTableId).thenReturn(mockTableId)

    val bigQueryStagingQuery = new BigQueryImport(stagingQueryConf, endPartition, tableUtils) {
      override private[cloud_gcp] lazy val bigQueryClient: BigQuery = mockClient
    }

    val range = PartitionRange("2024-01-01", "2024-01-01")(partitionSpec)
    val setups = Seq.empty[String]

    bigQueryStagingQuery.compute(range, setups, Some(true))

    // Verify table was created with external table definition
    assertTrue("TableInfo should be captured", capturedTableInfo != null)
    val definition = capturedTableInfo.getDefinition.asInstanceOf[ExternalTableDefinition]
    assertTrue("Should use parquet format",
               definition.getFormatOptions.asInstanceOf[FormatOptions].getType == "PARQUET")
    assertTrue("Should have autodetect enabled", definition.getAutodetect)
    assertTrue("Should have hive partitioning options", definition.getHivePartitioningOptions != null)
  }

  it should "use correct warehouse location for base prefix" in {
    val customWarehouse = "gs://custom-warehouse/data"

    // Update the global spark configuration
    spark.conf.set("spark.sql.catalog.custom_catalog.warehouse", customWarehouse)

    val stagingQueryConf = createTestStagingQuery("custom_catalog.test_dataset.test_table")
    val endPartition = "2024-01-31"

    val bigQueryStagingQuery = new BigQueryImport(stagingQueryConf, endPartition, tableUtils)

    val basePrefix = bigQueryStagingQuery.basePrefix

    assertTrue("Base prefix should contain custom warehouse location",
               basePrefix.contains(customWarehouse.stripSuffix("/")))
    assertTrue("Base prefix should contain sanitized table name",
               basePrefix.contains("export/custom_catalog_test_dataset_test_table"))
  }

  it should "handle null enableAutoExpand parameter" in {
    val stagingQueryConf = createTestStagingQuery()
    val endPartition = "2024-01-31"
    val mockClient = mockBigQueryClient()

    val bigQueryStagingQuery = new BigQueryImport(stagingQueryConf, endPartition, tableUtils) {
      override private[cloud_gcp] lazy val bigQueryClient: BigQuery = mockClient
    }

    val range = PartitionRange("2024-01-01", "2024-01-01")(partitionSpec)
    val setups = Seq.empty[String]

    // This should not throw an exception even with null enableAutoExpand
    bigQueryStagingQuery.compute(range, setups, None)

    verify(mockClient).create(any[JobInfo])
    verify(mockClient).create(any[TableInfo])
  }

  it should "properly sanitize table names in paths" in {
    val stagingQueryConf = createTestStagingQuery("test_catalog.`test-dataset`.test_table")
    val endPartition = "2024-01-31"

    val bigQueryStagingQuery = new BigQueryImport(stagingQueryConf, endPartition, tableUtils)

    val basePrefix = bigQueryStagingQuery.basePrefix

    // The sanitize method should replace dots and hyphens with underscores
    assertTrue("Base prefix should contain sanitized table name",
               basePrefix.contains("test_catalog__test_dataset__test_table"))
  }

  it should "successfully compute with setups and multi-statement query" in {
    val stagingQueryConf = createTestStagingQuery()
    val endPartition = "2024-01-31"
    val mockClient = mockBigQueryClient()

    val bigQueryStagingQuery = new BigQueryImport(stagingQueryConf, endPartition, tableUtils) {
      override private[cloud_gcp] lazy val bigQueryClient: BigQuery = mockClient
    }

    val range = PartitionRange("2024-01-01", "2024-01-01")(partitionSpec)
    val setups = Seq(
      "CREATE TEMP TABLE temp_data AS SELECT * FROM source_data",
      "CREATE TEMP FUNCTION process_data(input STRING) AS (UPPER(input))"
    )

    // This should not throw an exception and should execute the multi-statement query
    bigQueryStagingQuery.compute(range, setups, Some(true))

    // Verify that BigQuery client methods were called
    verify(mockClient).create(any[JobInfo])
    verify(mockClient).create(any[TableInfo])
  }
}
