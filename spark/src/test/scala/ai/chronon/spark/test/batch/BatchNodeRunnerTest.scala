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

package ai.chronon.spark.test.batch

import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.api.planner.TableDependencies
import ai.chronon.online.KVStore.PutRequest
import ai.chronon.planner.{MonolithJoinNode, Node, NodeContent}
import ai.chronon.spark.batch.BatchNodeRunner
import ai.chronon.spark.submission.SparkSessionBuilder
import ai.chronon.spark.test.{MockKVStore, TableTestUtils}
import ai.chronon.spark.utils.MockApi
import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.slf4j.LoggerFactory

import scala.collection.Seq
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
}

class BatchNodeRunnerTest extends AnyFlatSpec with BeforeAndAfterAll with BeforeAndAfterEach {

  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  private val spark: SparkSession = SparkSessionBuilder.build("BatchNodeRunnerTest", local = true)
  private val tableUtils = TableTestUtils(spark)
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val yesterday = tableUtils.partitionSpec.before(today)
  private val twoDaysAgo = tableUtils.partitionSpec.before(yesterday)

  private val mockKVStore = new MockKVStoreWithTracking()
  private val mockApi = new MockApi(() => mockKVStore, "test_namespace")

  def createTestMetadata(inputTable: String,
                         outputTable: String,
                         partitionColumn: String = "ds",
                         partitionFormat: String = "yyyy-MM-dd"): MetaData = {
    val outputTableInfo = new TableInfo(
    ).setTable(outputTable).setPartitionColumn(partitionColumn).setPartitionFormat(partitionFormat)
    val query = new Query().setPartitionColumn(partitionColumn).setPartitionFormat(partitionFormat)
    val tableDependency = TableDependencies.fromTable(inputTable, query)
    val executionInfo = new ExecutionInfo(
    ).setStepDays(1).setTableDependencies(Seq(tableDependency).asJava).setOutputTableInfo(outputTableInfo)
    new MetaData()
      .setName("test_batch_node")
      .setExecutionInfo(executionInfo)
      .setOutputNamespace("test_db")
      .setTeam("test_team")
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
    setupTestTables()
    mockKVStore.reset()
  }

  "BatchNodeRunner.runFromArgs" should "calculate input table partitions and write them to kvStore" in {

    val configPath = createTestConfigFile(twoDaysAgo, yesterday)

    val result = BatchNodeRunner.runFromArgs(mockApi, configPath, twoDaysAgo, yesterday)

    result match {
      case Success(_) =>
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
                   mockKVStore.putRequests.forall(_.dataset == "TABLE_PARTITIONS"))

      case Failure(exception) =>
        fail(s"runFromArgs should have succeeded but failed with: ${exception.getMessage}")
    }
  }

  it should "short circuit and throw exception when missing partitions are present" in {

    val configPath = createTestConfigFile(twoDaysAgo, today) // today's partition doesn't exist

    val result = BatchNodeRunner.runFromArgs(mockApi, configPath, twoDaysAgo, today)

    result match {
      case Success(_) =>
        fail("runFromArgs should have failed due to missing partitions")

      case Failure(exception) =>
        assertTrue("Exception should mention missing partitions", exception.getMessage.contains("missing partitions"))

        // Should still have written input table partitions to kvStore before failing
        val inputTableRequests = mockKVStore.putRequests.filter(req => new String(req.keyBytes).contains("input_table"))
        assertTrue("Should have written input table partitions before failing", inputTableRequests.nonEmpty)

        // Should NOT have written output table partitions since execution was short-circuited
        val outputTableRequests =
          mockKVStore.putRequests.filter(req => new String(req.keyBytes).contains("output_table"))
        assertTrue("Should NOT have written output table partitions after short circuit", outputTableRequests.isEmpty)
    }
  }

  it should "calculate and write output table partitions after successful execution" in {

    // Insert some data into output table to simulate successful execution
    spark.sql(
      s"""
         |INSERT INTO test_db.output_table VALUES
         |(1, 'output1', '$yesterday'),
         |(2, 'output2', '$twoDaysAgo')
         |""".stripMargin
    )

    val configPath = createTestConfigFile(twoDaysAgo, yesterday)

    val result = BatchNodeRunner.runFromArgs(mockApi, configPath, twoDaysAgo, yesterday)

    result match {
      case Success(_) =>
        // Verify both input and output table partitions were written
        val inputTableRequests = mockKVStore.putRequests.filter(req => new String(req.keyBytes).contains("input_table"))
        val outputTableRequests =
          mockKVStore.putRequests.filter(req => new String(req.keyBytes).contains("output_table"))

        assertTrue("Should have input table partition requests", inputTableRequests.nonEmpty)
        assertTrue("Should have output table partition requests", outputTableRequests.nonEmpty)

        // Verify the sequence: input partitions written first (in multiPut), then output partitions (in put)
        assertTrue("Should have made multiple put requests", mockKVStore.putRequests.size >= 2)

      case Failure(exception) =>
        fail(s"runFromArgs should have succeeded but failed with: ${exception.getMessage}")
    }
  }

  it should "handle empty partition ranges correctly" in {

    // Use a date range where no partitions exist
    val futureDate1 = tableUtils.partitionSpec.after(today)
    val futureDate2 = tableUtils.partitionSpec.after(futureDate1)

    val configPath = createTestConfigFile(futureDate1, futureDate2)

    val result = BatchNodeRunner.runFromArgs(mockApi, configPath, futureDate1, futureDate2)

    result match {
      case Success(_) =>
        fail("runFromArgs should have failed due to missing all partitions")

      case Failure(exception) =>
        assertTrue("Exception should mention missing partitions", exception.getMessage.contains("missing partitions"))
    }
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

    val result = BatchNodeRunner.runFromArgs(mockApi, configPath, threeDaysAgo, today)

    result match {
      case Success(_) =>
        fail("runFromArgs should have failed due to some missing partitions")

      case Failure(exception) =>
        assertTrue("Exception should mention missing partitions", exception.getMessage.contains("missing partitions"))

        // The exception message should specifically mention which partitions are missing
        assertTrue("Exception should mention the specific missing partition",
                   exception.getMessage.contains(threeDaysAgo))
    }
  }

  it should "correctly translate partition ranges before diffing against existing partitions" in {

    // Create tables with different partition column and format to test translation
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS test_db.input_table_alt (
         |  id INT,
         |  value STRING,
         |  partition_date STRING
         |)
         |PARTITIONED BY (partition_date)
         |""".stripMargin
    )

    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS test_db.output_table_alt (
         |  id INT,
         |  value STRING,
         |  partition_date STRING
         |)
         |PARTITIONED BY (partition_date)
         |""".stripMargin
    )

    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS test_db.left_table_alt (
         |  id INT,
         |  partition_date STRING
         |)
         |PARTITIONED BY (partition_date)
         |""".stripMargin
    )

    // Convert dates to different format for the alternative partition format
    val yesterdayAlt = yesterday.replace("-", "") // yyyyMMdd format
    val twoDaysAgoAlt = twoDaysAgo.replace("-", "") // yyyyMMdd format

    // Insert data with different partition format
    spark.sql(
      s"""
         |INSERT INTO test_db.input_table_alt VALUES
         |(1, 'value1', '$yesterdayAlt'),
         |(2, 'value2', '$twoDaysAgoAlt')
         |""".stripMargin
    )

    spark.sql(
      s"""
         |INSERT INTO test_db.left_table_alt VALUES
         |(1, '$yesterdayAlt'),
         |(2, '$twoDaysAgoAlt')
         |""".stripMargin
    )

    val configPath = createTestConfigFile(
      twoDaysAgoAlt,
      yesterdayAlt,
      inputTable = "test_db.input_table_alt",
      outputTable = "test_db.output_table_alt",
      leftTable = "test_db.left_table_alt",
      partitionColumn = "partition_date",
      partitionFormat = "yyyyMMdd"
    )

    val result = BatchNodeRunner.runFromArgs(mockApi, configPath, twoDaysAgo, yesterday)

    result match {
      case Success(_) =>
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

      case Failure(exception) =>
        fail(s"runFromArgs should have succeeded but failed with: ${exception.getMessage}")
    }
  }

  override def afterAll(): Unit = {
    spark.sql("DROP DATABASE IF EXISTS test_db CASCADE")
    spark.stop()
  }
}
