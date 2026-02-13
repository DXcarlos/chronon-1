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

package ai.chronon.spark.batch

import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.api.planner.{MetaDataUtils, NodeRunner, TableDependencies}
import ai.chronon.observability.{TileSummaryKey, TileSummary}
import ai.chronon.online.KVStore.PutRequest
import ai.chronon.planner.{ExternalSourceSensorNode, MonolithJoinNode, Node, NodeContent}
import ai.chronon.spark.other.MockKVStore
import ai.chronon.spark.utils.{MockApi, SparkTestBase}
import ai.chronon.spark.catalog.TableUtils
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.junit.Assert._
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}


import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

class MockKVStoreWithTracking extends MockKVStore {
  var putRequests: Seq[PutRequest] = Seq.empty

  override def put(putRequest: PutRequest): Future[Boolean] = {
    putRequests = putRequests :+ putRequest
    Future.successful(true)
  }

  override def multiPut(putRequests: Seq[PutRequest]): Future[Seq[Boolean]] = {
    this.putRequests = this.putRequests ++ putRequests
    Future.successful(putRequests.map(_ => true))
  }

  def reset(): Unit = {
    putRequests = Seq.empty
    num_puts.clear()
  }

  def getTileSummaryRequests: Seq[PutRequest] = {
    putRequests.filter { req =>
      try {
        // Check if the key can be deserialized as a TileKey (indicates it's a tile summary request)
        val keyStr = new String(req.keyBytes)
        ThriftJsonCodec.fromJsonStr[TileSummaryKey](keyStr, check = false, classOf[TileSummaryKey])
        true
      } catch {
        case _: Exception => false
      }
    }
  }

  def getTileKeysAndSummaries: Seq[(TileSummaryKey, TileSummary)] = {
    getTileSummaryRequests.map { req =>
      val tileKey = ThriftJsonCodec.fromJsonStr[TileSummaryKey](new String(req.keyBytes), check = false, classOf[TileSummaryKey])
      val tileSummary = ThriftJsonCodec.fromJsonStr[TileSummary](new String(req.valueBytes), check = false, classOf[TileSummary])
      (tileKey, tileSummary)
    }
  }

  def getDataQualityMetricsRequests: Seq[PutRequest] = {
    putRequests.filter { req =>
      val keyStr = new String(req.keyBytes)
      keyStr.contains("#null_count")
    }
  }

  def getDataQualityMetricsJson: Seq[(String, Map[String, Any])] = {
    getDataQualityMetricsRequests.map { req =>
      val keyStr = new String(req.keyBytes)
      val valueStr = new String(req.valueBytes)
      val objectMapper = new ObjectMapper()
      objectMapper.registerModule(DefaultScalaModule)
      val valueMap = objectMapper.readValue(valueStr, classOf[Map[String, Any]])
      keyStr -> valueMap
    }
  }
}

class BatchNodeRunnerTest extends SparkTestBase with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private val tableUtils = new TableUtils(spark)
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val yesterday = tableUtils.partitionSpec.before(today)
  private val twoDaysAgo = tableUtils.partitionSpec.before(yesterday)

  private val mockKVStore = new MockKVStoreWithTracking()
  private val mockApi = new MockApi(() => mockKVStore, "test_namespace")

  def createTestMetadata(inputTable: String,
                         outputTable: String,
                         partitionColumn: String = "ds",
                         partitionFormat: String = "yyyy-MM-dd",
                         semanticHash: Option[String] = None,
                         isSoftDependency: Boolean = false): MetaData = {
    implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec

    val query = new Query().setPartitionColumn(partitionColumn).setPartitionFormat(partitionFormat)
    val tableDependency = TableDependencies.fromTable(inputTable, query)

    semanticHash.foreach(tableDependency.setSemanticHash)
    if (isSoftDependency) {
      tableDependency.setIsSoftNodeDependency(true)
    }

    val baseMetadata = new MetaData()
      .setOutputNamespace("test_db")
      .setTeam("test_team")

    MetaDataUtils.layer(
      baseMetadata = baseMetadata,
      modeName = "test_mode",
      nodeName = "test_batch_node",
      tableDependencies = Seq(tableDependency),
      stepDays = Some(1),
      outputTableOverride = Some(outputTable)
    )
  }

  def createTestMetadataWithMultipleDeps(inputTable: String,
                                         outputTable: String,
                                         semanticHashes: Seq[Option[String]],
                                         partitionColumn: String = "ds",
                                         partitionFormat: String = "yyyy-MM-dd"): MetaData = {
    implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec

    val query = new Query().setPartitionColumn(partitionColumn).setPartitionFormat(partitionFormat)
    val tableDependencies = semanticHashes.map { hashOpt =>
      val dep = TableDependencies.fromTable(inputTable, query)
      hashOpt.foreach(dep.setSemanticHash)
      dep
    }

    val baseMetadata = new MetaData()
      .setOutputNamespace("test_db")
      .setTeam("test_team")

    MetaDataUtils.layer(
      baseMetadata = baseMetadata,
      modeName = "test_mode",
      nodeName = "test_batch_node",
      tableDependencies = tableDependencies,
      stepDays = Some(1),
      outputTableOverride = Some(outputTable)
    )(partitionSpec)
  }

  def createTestNodeContent(inputTable: String = "test_db.input_table",
                            outputTable: String = "test_db.output_table",
                            leftTable: String = "test_db.left_table",
                            partitionColumn: String = "ds",
                            partitionFormat: String = "yyyy-MM-dd"): NodeContent = {
    val join = new Join(
    ).setMetaData(createTestMetadata(inputTable, outputTable, partitionColumn, partitionFormat))
      .setLeft({
        val src = new Source()
        src.setEvents(
          new EventSource()
            .setTable(leftTable)
            .setQuery(new Query().setPartitionColumn(partitionColumn).setPartitionFormat(partitionFormat)))
        src
      })
      .setJoinParts(Seq.empty.asJava)
    val monolithJoin = new MonolithJoinNode().setJoin(join)
    val nodeContent = new NodeContent()
    nodeContent.setMonolithJoin(monolithJoin)
    nodeContent
  }

  def setupTestTables(): Unit = {
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db")

    // Create input table with some partitions
    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS test_db.input_table (
         |  id INT,
         |  value STRING,
         |  ds STRING
         |)
         |PARTITIONED BY (ds)
         |""".stripMargin
    )

    // Create left table
    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS test_db.left_table (
         |  id INT,
         |  ds STRING
         |)
         |PARTITIONED BY (ds)
         |""".stripMargin
    )

    // Create output table
    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS test_db.output_table (
         |  id INT,
         |  value STRING,
         |  ds STRING
         |)
         |PARTITIONED BY (ds)
         |""".stripMargin
    )

    // Insert test data for available partitions
    spark.sql(
      s"""
         |INSERT INTO test_db.input_table VALUES
         |(1, 'value1', '$yesterday'),
         |(2, 'value2', '$twoDaysAgo')
         |""".stripMargin
    )

    spark.sql(
      s"""
         |INSERT INTO test_db.left_table VALUES
         |(1, '$yesterday'),
         |(2, '$twoDaysAgo')
         |""".stripMargin
    )
  }

  def createTestConfigFile(startDs: String,
                           endDs: String,
                           inputTable: String = "test_db.input_table",
                           outputTable: String = "test_db.output_table",
                           leftTable: String = "test_db.left_table",
                           partitionColumn: String = "ds",
                           partitionFormat: String = "yyyy-MM-dd"): String = {
    val tempFile = java.io.File.createTempFile("test_node_config", ".json")
    tempFile.deleteOnExit()

    val nodeContent = createTestNodeContent(inputTable, outputTable, leftTable, partitionColumn, partitionFormat)
    val metadata = createTestMetadata(inputTable, outputTable, partitionColumn, partitionFormat)
    val node = new Node()
      .setMetaData(metadata)
      .setContent(nodeContent)
    val json = ThriftJsonCodec.toJsonStr(node)

    val writer = new java.io.FileWriter(tempFile)
    writer.write(json)
    writer.close()

    tempFile.getAbsolutePath
  }

  override def beforeEach(): Unit = {
    // Drop all test tables to ensure fresh start
    spark.sql("DROP TABLE IF EXISTS test_db.input_table")
    spark.sql("DROP TABLE IF EXISTS test_db.left_table")
    spark.sql("DROP TABLE IF EXISTS test_db.output_table")
    spark.sql("DROP TABLE IF EXISTS test_db.input_table_alt")
    spark.sql("DROP TABLE IF EXISTS test_db.output_table_alt")
    spark.sql("DROP TABLE IF EXISTS test_db.left_table_alt")

    setupTestTables()
    mockKVStore.reset()
  }

  "BatchNodeRunner.runFromArgs" should "calculate input table partitions and write them to kvStore" ignore {

    val configPath = createTestConfigFile(twoDaysAgo, yesterday)
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    val exitCode = runner.runFromArgs(twoDaysAgo, yesterday, NodeRunner.DefaultTablePartitionsDataset, None)

    assertEquals("runFromArgs should return 0 on success", 0, exitCode)

    // Verify that partitions were written to kvStore
    assertTrue("KVStore should have received put requests", mockKVStore.putRequests.nonEmpty)

    // Should have requests for input table partitions and output table partitions
    val inputTableRequests = mockKVStore.putRequests.filter(req => new String(req.keyBytes).contains("input_table"))
    val outputTableRequests =
      mockKVStore.putRequests.filter(req => new String(req.keyBytes).contains("output_table"))

    assertTrue("Should have input table partition requests", inputTableRequests.nonEmpty)
    assertTrue("Should have output table partition requests", outputTableRequests.nonEmpty)

    // Verify dataset name
    assertTrue("Should use TABLE_PARTITIONS dataset",
                mockKVStore.putRequests.forall(_.dataset == NodeRunner.DefaultTablePartitionsDataset))
  }

  it should "short circuit and throw exception when missing partitions are present" in {

    val configPath = createTestConfigFile(twoDaysAgo, today) // today's partition doesn't exist
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    val exitCode = runner.runFromArgs(twoDaysAgo, today, NodeRunner.DefaultTablePartitionsDataset, None)

    assertEquals("runFromArgs should return 1 on failure", 1, exitCode)

    // Should still have written input table partitions to kvStore before failing
    val inputTableRequests = mockKVStore.putRequests.filter(req => new String(req.keyBytes).contains("input_table"))
    assertTrue("Should have written input table partitions before failing", inputTableRequests.nonEmpty)

    // Should NOT have written output table partitions since execution was short-circuited
    val outputTableRequests =
      mockKVStore.putRequests.filter(req => new String(req.keyBytes).contains("output_table"))
    assertTrue("Should NOT have written output table partitions after short circuit", outputTableRequests.isEmpty)
  }

  it should "calculate and write output table partitions after successful execution" ignore {

    // Insert some data into output table to simulate successful execution
    spark.sql(
      s"""
         |INSERT INTO test_db.output_table VALUES
         |(1, 'output1', '$yesterday'),
         |(2, 'output2', '$twoDaysAgo')
         |""".stripMargin
    )

    val configPath = createTestConfigFile(twoDaysAgo, yesterday)
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    val exitCode = runner.runFromArgs(twoDaysAgo, yesterday, NodeRunner.DefaultTablePartitionsDataset, None)

    assertEquals("runFromArgs should return 0 on success", 0, exitCode)

    // Verify both input and output table partitions were written
    val inputTableRequests = mockKVStore.putRequests.filter(req => new String(req.keyBytes).contains("input_table"))
    val outputTableRequests =
      mockKVStore.putRequests.filter(req => new String(req.keyBytes).contains("output_table"))

    assertTrue("Should have input table partition requests", inputTableRequests.nonEmpty)
    assertTrue("Should have output table partition requests", outputTableRequests.nonEmpty)

    // Verify the sequence: input partitions written first (in multiPut), then output partitions (in put)
    assertTrue("Should have made multiple put requests", mockKVStore.putRequests.size >= 2)
  }

  it should "handle empty partition ranges correctly" in {

    // Use a date range where no partitions exist
    val futureDate1 = tableUtils.partitionSpec.after(today)
    val futureDate2 = tableUtils.partitionSpec.after(futureDate1)

    val configPath = createTestConfigFile(futureDate1, futureDate2)
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    val exitCode = runner.runFromArgs(futureDate1, futureDate2, NodeRunner.DefaultTablePartitionsDataset, None)

    assertEquals("runFromArgs should return 1 on failure due to missing all partitions", 1, exitCode)
  }

  it should "correctly identify missing vs available partitions" in {

    // Add one more partition to test partial availability
    spark.sql(
      s"""
         |INSERT INTO test_db.input_table VALUES
         |(3, 'value3', '$today')
         |""".stripMargin
    )

    spark.sql(
      s"""
         |INSERT INTO test_db.left_table VALUES
         |(3, '$today')
         |""".stripMargin
    )

    mockKVStore.reset()

    // Request a range that includes both available and missing partitions
    val threeDaysAgo = tableUtils.partitionSpec.before(twoDaysAgo)
    val configPath = createTestConfigFile(threeDaysAgo, today)
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    val exitCode = runner.runFromArgs(threeDaysAgo, today, NodeRunner.DefaultTablePartitionsDataset, None)

    assertEquals("runFromArgs should return 1 on failure due to missing partitions", 1, exitCode)
  }

  it should "correctly translate partition ranges before diffing against existing partitions" ignore {
    import spark.implicits._

    // Convert dates to different format for the alternative partition format
    val yesterdayAlt = yesterday.replace("-", "") // yyyyMMdd format
    val twoDaysAgoAlt = twoDaysAgo.replace("-", "") // yyyyMMdd format

    // Insert data with different partition format using TableUtils.insertPartitions
    val inputData = Seq(
      (1, "value1", yesterdayAlt),
      (2, "value2", twoDaysAgoAlt)
    ).toDF("id", "value", "partition_date")

    val leftData = Seq(
      (1, yesterdayAlt),
      (2, twoDaysAgoAlt)
    ).toDF("id", "partition_date")

    tableUtils.insertPartitions(inputData, "test_db.input_table_alt", partitionColumns = List("partition_date"))
    tableUtils.insertPartitions(leftData, "test_db.left_table_alt", partitionColumns = List("partition_date"))

    val configPath = createTestConfigFile(
      twoDaysAgoAlt,
      yesterdayAlt,
      inputTable = "test_db.input_table_alt",
      outputTable = "test_db.output_table_alt",
      leftTable = "test_db.left_table_alt",
      partitionColumn = "partition_date",
      partitionFormat = "yyyyMMdd"
    )
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    val exitCode = runner.runFromArgs(twoDaysAgo, yesterday, NodeRunner.DefaultTablePartitionsDataset, None)

    assertEquals("runFromArgs should return 0 on success", 0, exitCode)

    // Verify that partitions were written to kvStore - this validates the translation worked
    assertTrue("KVStore should have received put requests", mockKVStore.putRequests.nonEmpty)

    // Check that the partition data is properly formatted (indicates successful translation)
    val inputTableRequests =
      mockKVStore.putRequests.filter(req => new String(req.keyBytes).contains("input_table_alt"))
    assertTrue("Should have input table partition requests", inputTableRequests.nonEmpty)

    // Verify the partition values are correctly translated to the default partition spec format (yyyy-MM-dd)
    inputTableRequests.foreach { req =>
      val partitionData = new String(req.valueBytes)
      // The key assertion: partitions should be translated from yyyyMMdd to yyyy-MM-dd format
      assertTrue("Partition data should contain yesterday in translated format", partitionData.contains(yesterday))
      assertTrue("Partition data should contain twoDaysAgo in translated format",
                  partitionData.contains(twoDaysAgo))
      // Verify it does NOT contain the original format
      assertFalse("Partition data should NOT contain original date format (yyyyMMdd)",
                  partitionData.contains(yesterdayAlt) || partitionData.contains(twoDaysAgoAlt))
    }
  }

  "BatchNodeRunner.checkPartitions" should "succeed when all partitions are available" in {
    val tableDependency =
      TableDependencies.fromTable("test_db.input_table",
                                  new Query().setPartitionColumn("ds").setPartitionFormat("yyyy-MM-dd"))
    val sensorNode = new ExternalSourceSensorNode()
      .setSourceTableDependency(tableDependency)
      .setRetryCount(0L)
      .setRetryIntervalMin(1L)

    val metadata = createTestMetadata("test_db.input_table", "test_db.output_table")
    val range = PartitionRange(twoDaysAgo, yesterday)(tableUtils.partitionSpec)
    val configPath = createTestConfigFile(twoDaysAgo, yesterday)
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    val result = runner.checkPartitions(sensorNode, range)

    result match {
      case Success(_) =>
      // Test passed
      case Failure(exception) =>
        fail(s"checkPartitions should have succeeded but failed with: ${exception.getMessage}")
    }
  }

  it should "fail when partitions are missing and no retries configured" in {
    val tableDependency =
      TableDependencies.fromTable("test_db.external_table",
                                  new Query().setPartitionColumn("ds").setPartitionFormat("yyyy-MM-dd"))
    val sensorNode = new ExternalSourceSensorNode()
      .setSourceTableDependency(tableDependency)
      .setRetryCount(0L)
      .setRetryIntervalMin(1L)

    val metadata = createTestMetadata("test_db.external_table", "test_db.output_table")
    val range = PartitionRange(today, today)(tableUtils.partitionSpec) // today's partition doesn't exist
    val configPath = createTestConfigFile(today, today, "test_db.external_table")
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    val result = runner.checkPartitions(sensorNode, range)

    result match {
      case Success(_) =>
        fail("checkPartitions should have failed due to missing partitions")
      case Failure(exception) =>
        assertTrue("Exception should mention missing partitions", exception.getMessage.contains("missing partitions"))
        assertTrue("Exception should mention table name", exception.getMessage.contains("test_db.external_table"))
        assertTrue("Exception should mention specific partition", exception.getMessage.contains(today))
    }
  }

  it should "use default retry values when not set" in {
    val tableDependency =
      TableDependencies.fromTable("test_db.external_table",
                                  new Query().setPartitionColumn("ds").setPartitionFormat("yyyy-MM-dd"))
    val sensorNode = new ExternalSourceSensorNode()
      .setSourceTableDependency(tableDependency)
      .setRetryCount(0)
    // Not setting retryCount and retryIntervalMin to test defaults

    val metadata = createTestMetadata("test_db.external_table", "test_db.output_table")
    val range = PartitionRange(today, today)(tableUtils.partitionSpec) // today's partition doesn't exist
    val configPath = createTestConfigFile(today, today, "test_db.external_table")
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    val result = runner.checkPartitions(sensorNode, range)

    result match {
      case Success(_) =>
        fail("checkPartitions should have failed due to missing partitions")
      case Failure(exception) =>
        // Should fail with default retry count (3) and retry interval (3 minutes)
        assertTrue("Exception should mention missing partitions", exception.getMessage.contains("missing partitions"))
    }
  }

  it should "retry when configured but eventually fail if partitions never appear" in {
    val tableDependency =
      TableDependencies.fromTable("test_db.external_table",
                                  new Query().setPartitionColumn("ds").setPartitionFormat("yyyy-MM-dd"))
    val sensorNode = new ExternalSourceSensorNode()
      .setSourceTableDependency(tableDependency)
      .setRetryCount(2L) // Will try 3 times total (initial + 2 retries)
      .setRetryIntervalMin(0L) // Set to 0 to avoid actual delays in test

    val metadata = createTestMetadata("test_db.external_table", "test_db.output_table")
    val range = PartitionRange(today, today)(tableUtils.partitionSpec) // today's partition doesn't exist
    val configPath = createTestConfigFile(today, today, "test_db.external_table")
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    val startTime = System.currentTimeMillis()
    val result = runner.checkPartitions(sensorNode, range)
    val endTime = System.currentTimeMillis()

    result match {
      case Success(_) =>
        fail("checkPartitions should have failed due to missing partitions")
      case Failure(exception) =>
        assertTrue("Exception should mention missing partitions", exception.getMessage.contains("missing partitions"))
        // Since we set retry interval to 0, the test should complete quickly
        assertTrue("Test should complete within reasonable time", (endTime - startTime) < 5000)
    }
  }

  it should "handle non-existent table gracefully" in {
    val tableDependency =
      TableDependencies.fromTable("test_db.nonexistent_table",
                                  new Query().setPartitionColumn("ds").setPartitionFormat("yyyy-MM-dd"))
    val sensorNode = new ExternalSourceSensorNode()
      .setSourceTableDependency(tableDependency)
      .setRetryCount(0L)
      .setRetryIntervalMin(1L)

    val metadata = createTestMetadata("test_db.nonexistent_table", "test_db.output_table")
    val range = PartitionRange(yesterday, yesterday)(tableUtils.partitionSpec)
    val configPath = createTestConfigFile(yesterday, yesterday, "test_db.nonexistent_table")
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    val result = runner.checkPartitions(sensorNode, range)

    result match {
      case Success(_) =>
        fail("checkPartitions should have failed for nonexistent table")
      case Failure(exception) =>
        // Should fail with some kind of table not found or similar error
        assertTrue(
          "Exception should indicate table issue",
          exception.getMessage.contains("nonexistent_table") ||
            exception.getMessage.toLowerCase.contains("not found") ||
            exception.getMessage.toLowerCase.contains("table")
        )
    }
  }

  "BatchNodeRunner.computeInputTablePartitionStatuses" should "correctly compute partitions for a single table" in {
    val configPath = createTestConfigFile(twoDaysAgo, yesterday)
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)
    val metadata = node.metaData
    val range = PartitionRange(twoDaysAgo, yesterday)(tableUtils.partitionSpec)

    val statuses = runner.computeInputTablePartitionStatuses(metadata, range, tableUtils).toSeq

    assertTrue("Should have statuses for input tables", statuses.nonEmpty)

    val inputTableStatus = statuses.find(_.name == "test_db.input_table")
    assertTrue("Should have status for input_table", inputTableStatus.isDefined)

    inputTableStatus.foreach { status =>
      assertEquals("Should have existing partitions", 2, status.existingPartitions.size)
      assertTrue("Should contain yesterday partition", status.existingPartitions.contains(yesterday))
      assertTrue("Should contain twoDaysAgo partition", status.existingPartitions.contains(twoDaysAgo))
      assertEquals("Should have no missing partitions", 0, status.missingPartitions.size)
    }
  }

  it should "identify missing partitions correctly" in {
    val configPath = createTestConfigFile(twoDaysAgo, today)
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)
    val metadata = node.metaData
    val range = PartitionRange(twoDaysAgo, today)(tableUtils.partitionSpec)

    val statuses = runner.computeInputTablePartitionStatuses(metadata, range, tableUtils).toSeq

    val inputTableStatus = statuses.find(_.name == "test_db.input_table")
    assertTrue("Should have status for input_table", inputTableStatus.isDefined)

    inputTableStatus.foreach { status =>
      assertTrue("Should have missing partitions", status.missingPartitions.nonEmpty)
      assertTrue("Should identify today as missing", status.missingPartitions.contains(today))
    }
  }

  it should "handle same table used for multiple dependencies (labels and groupBy)" in {
    // Create a join that uses the same table for both labels and features
    val groupBySource = new Source()
    groupBySource.setEvents(
      new EventSource()
        .setTable("test_db.input_table")
        .setQuery(new Query().setPartitionColumn("ds").setPartitionFormat("yyyy-MM-dd")))

    val join = new Join()
      .setMetaData(createTestMetadata("test_db.input_table", "test_db.output_table"))
      .setLeft({
        val src = new Source()
        src.setEvents(
          new EventSource()
            .setTable("test_db.input_table")
            .setQuery(new Query().setPartitionColumn("ds").setPartitionFormat("yyyy-MM-dd")))
        src
      })
      .setJoinParts(Seq(
        new JoinPart()
          .setGroupBy(
            new GroupBy()
              .setSources(Seq(groupBySource).asJava)
          )
      ).asJava)

    val monolithJoin = new MonolithJoinNode()
    monolithJoin.setJoin(join)

    val nodeContent = new NodeContent()
    nodeContent.setMonolithJoin(monolithJoin)

    val metadata = MetaDataUtils.layer(
      baseMetadata = new MetaData().setOutputNamespace("test_db").setTeam("test_team"),
      modeName = "test_mode",
      nodeName = "test_batch_node",
      tableDependencies = Seq(
        TableDependencies.fromTable("test_db.input_table", new Query().setPartitionColumn("ds").setPartitionFormat("yyyy-MM-dd")),
        TableDependencies.fromTable("test_db.input_table", new Query().setPartitionColumn("ds").setPartitionFormat("yyyy-MM-dd"))
      ),
      stepDays = Some(1),
      outputTableOverride = Some("test_db.output_table")
    )(tableUtils.partitionSpec)

    val node = new Node()
    node.setMetaData(metadata)
    node.setContent(nodeContent)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)
    val range = PartitionRange(twoDaysAgo, yesterday)(tableUtils.partitionSpec)

    val statuses = runner.computeInputTablePartitionStatuses(metadata, range, tableUtils).toSeq

    // Should have only one status for the table despite it being used twice
    val inputTableStatuses = statuses.filter(_.name == "test_db.input_table")
    assertEquals("Should have exactly one status for input_table", 1, inputTableStatuses.size)

    inputTableStatuses.head match {
      case status =>
        assertTrue("Should have existing partitions", status.existingPartitions.nonEmpty)
        assertEquals("Should have no missing partitions", 0, status.missingPartitions.size)
    }
  }

  it should "filter out soft dependencies" in {
    val metadata = createTestMetadata("test_db.input_table", "test_db.output_table", isSoftDependency = true)
    val nodeContent = createTestNodeContent()
    val node = new Node()
    node.setMetaData(metadata)
    node.setContent(nodeContent)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)
    val range = PartitionRange(twoDaysAgo, yesterday)(tableUtils.partitionSpec)

    val statuses = runner.computeInputTablePartitionStatuses(metadata, range, tableUtils).toSeq

    val inputTableStatus = statuses.find(_.name == "test_db.input_table")
    assertTrue("Should not have status for soft dependency", inputTableStatus.isEmpty)
  }

  it should "capture semantic hash when set on table dependency" in {
    val metadata = createTestMetadata("test_db.input_table", "test_db.output_table", semanticHash = Some("test_hash_123"))
    val nodeContent = createTestNodeContent()
    val node = new Node()
    node.setMetaData(metadata)
    node.setContent(nodeContent)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)
    val range = PartitionRange(twoDaysAgo, yesterday)(tableUtils.partitionSpec)

    val statuses = runner.computeInputTablePartitionStatuses(metadata, range, tableUtils).toSeq

    val inputTableStatus = statuses.find(_.name == "test_db.input_table")
    assertTrue("Should have status for input_table", inputTableStatus.isDefined)

    inputTableStatus.foreach { status =>
      assertTrue("Should have semantic hash", status.semanticHash.isDefined)
      assertEquals("Semantic hash should match", "test_hash_123", status.semanticHash.get)
    }
  }

  it should "return None for semantic hash when not set" in {
    val metadata = createTestMetadata("test_db.input_table", "test_db.output_table")
    val nodeContent = createTestNodeContent()
    val node = new Node()
    node.setMetaData(metadata)
    node.setContent(nodeContent)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)
    val range = PartitionRange(twoDaysAgo, yesterday)(tableUtils.partitionSpec)

    val statuses = runner.computeInputTablePartitionStatuses(metadata, range, tableUtils).toSeq

    val inputTableStatus = statuses.find(_.name == "test_db.input_table")
    assertTrue("Should have status for input_table", inputTableStatus.isDefined)

    inputTableStatus.foreach { status =>
      assertTrue("Semantic hash should be None", status.semanticHash.isEmpty)
    }
  }

  it should "use consistent semantic hash when same table used multiple times with same hash" in {
    val metadata = createTestMetadataWithMultipleDeps(
      "test_db.input_table",
      "test_db.output_table",
      Seq(Some("consistent_hash"), Some("consistent_hash"))
    )
    val nodeContent = createTestNodeContent()
    val node = new Node()
    node.setMetaData(metadata)
    node.setContent(nodeContent)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)
    val range = PartitionRange(twoDaysAgo, yesterday)(tableUtils.partitionSpec)

    val statuses = runner.computeInputTablePartitionStatuses(metadata, range, tableUtils).toSeq

    val inputTableStatus = statuses.find(_.name == "test_db.input_table")
    assertTrue("Should have status for input_table", inputTableStatus.isDefined)

    inputTableStatus.foreach { status =>
      assertTrue("Should have semantic hash", status.semanticHash.isDefined)
      assertEquals("Semantic hash should match", "consistent_hash", status.semanticHash.get)
    }
  }

  it should "return None when same table has inconsistent semantic hashes across dependencies" in {
    val metadata = createTestMetadataWithMultipleDeps(
      "test_db.input_table",
      "test_db.output_table",
      Seq(Some("hash_1"), Some("hash_2"))
    )
    val nodeContent = createTestNodeContent()
    val node = new Node()
    node.setMetaData(metadata)
    node.setContent(nodeContent)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)
    val range = PartitionRange(twoDaysAgo, yesterday)(tableUtils.partitionSpec)

    val statuses = runner.computeInputTablePartitionStatuses(metadata, range, tableUtils).toSeq

    val inputTableStatus = statuses.find(_.name == "test_db.input_table")
    assertTrue("Should have status for input_table", inputTableStatus.isDefined)

    inputTableStatus.foreach { status =>
      assertTrue("Semantic hash should be None due to inconsistency", status.semanticHash.isEmpty)
    }
  }

  "BatchNodeRunner.isSensorNode" should "identify nodes with 'sensor' in name (case-insensitive)" in {
    // Test with lowercase "sensor"
    val metadata1 = createTestMetadata("test_db.input_table", "test_db.output_table")
    val baseMetadata1 = new MetaData().setOutputNamespace("test_db").setTeam("test_team")
    val sensorMetadata1 = MetaDataUtils.layer(
      baseMetadata = baseMetadata1,
      modeName = "test_mode",
      nodeName = "external_source_sensor_test",
      tableDependencies = Seq.empty,
      stepDays = Some(1),
      outputTableOverride = Some("test_db.output_table")
    )(tableUtils.partitionSpec)

    val node1 = new Node()
    node1.setMetaData(sensorMetadata1)
    node1.setContent(createTestNodeContent())
    val runner1 = new BatchNodeRunner(node1, tableUtils, mockApi)

    assertTrue("Should identify node with 'sensor' in name", runner1.isSensorNode)

    // Test with uppercase "SENSOR"
    val sensorMetadata2 = MetaDataUtils.layer(
      baseMetadata = new MetaData().setOutputNamespace("test_db").setTeam("test_team"),
      modeName = "test_mode",
      nodeName = "EXTERNAL_SOURCE_SENSOR_TEST",
      tableDependencies = Seq.empty,
      stepDays = Some(1),
      outputTableOverride = Some("test_db.output_table")
    )(tableUtils.partitionSpec)

    val node2 = new Node()
    node2.setMetaData(sensorMetadata2)
    node2.setContent(createTestNodeContent())
    val runner2 = new BatchNodeRunner(node2, tableUtils, mockApi)

    assertTrue("Should identify node with 'SENSOR' (uppercase) in name", runner2.isSensorNode)

    // Test with mixed case "SenSor"
    val sensorMetadata3 = MetaDataUtils.layer(
      baseMetadata = new MetaData().setOutputNamespace("test_db").setTeam("test_team"),
      modeName = "test_mode",
      nodeName = "external_SenSor_node",
      tableDependencies = Seq.empty,
      stepDays = Some(1),
      outputTableOverride = Some("test_db.output_table")
    )(tableUtils.partitionSpec)

    val node3 = new Node()
    node3.setMetaData(sensorMetadata3)
    node3.setContent(createTestNodeContent())
    val runner3 = new BatchNodeRunner(node3, tableUtils, mockApi)

    assertTrue("Should identify node with 'SenSor' (mixed case) in name", runner3.isSensorNode)
  }

  it should "not identify regular nodes as sensor nodes" in {
    val metadata = createTestMetadata("test_db.input_table", "test_db.output_table")
    val regularMetadata = MetaDataUtils.layer(
      baseMetadata = new MetaData().setOutputNamespace("test_db").setTeam("test_team"),
      modeName = "test_mode",
      nodeName = "regular_batch_node",
      tableDependencies = Seq.empty,
      stepDays = Some(1),
      outputTableOverride = Some("test_db.output_table")
    )(tableUtils.partitionSpec)

    val node = new Node()
    node.setMetaData(regularMetadata)
    node.setContent(createTestNodeContent())
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    assertFalse("Should NOT identify regular node as sensor node", runner.isSensorNode)
  }

  override def afterAll(): Unit = {
    spark.sql("DROP DATABASE IF EXISTS test_db CASCADE")
    spark.stop()
  }
}
