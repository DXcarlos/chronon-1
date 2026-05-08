package ai.chronon.spark

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.{Builders => B, _}
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.online.{DeployModelRequest, ModelPlatform, ModelPlatformProvider, InferRequest, InferResponse, TrainingRequest}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.utils.{DataFrameGen, SparkTestBase}
import org.apache.spark.sql.DataFrame
import org.scalatest.matchers.should.Matchers._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Success

class InferenceJobTest extends SparkTestBase {

  implicit val tableUtils: TableUtils = TableUtils(spark)

  val namespace = "test_namespace_inference"
  val today: String = tableUtils.partitionSpec.at(System.currentTimeMillis())
  val monthAgo: String = tableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))

  createDatabase(namespace)

  def createTestModels(
      includeInputMapping: Boolean = false,
      includeOutputMapping: Boolean = false
  ): (api.Model, api.Model) = {
    val model1ValueSchema = B.structSchema("model1_output",
      "score" -> DoubleType,
      "category" -> StringType
    )

    val model2ValueSchema = B.structSchema("model2_output",
      "prediction" -> StringType,
      "confidence" -> DoubleType
    )

    val model1InputMapping = if (includeInputMapping) {
      Map("normalized_feature1" -> "feature1 / 100.0")
    } else {
      null
    }

    val model1OutputMapping = if (includeOutputMapping) {
      Map(
        "final_score" -> "model1__score * 2.0",
        "category" -> "model1__category"  // Pass through category
      )
    } else {
      null
    }

    val model1 = B.Model(
      metaData = B.MetaData(name = "model1"),
      runtime = B.ModelRuntime(
        backend = ModelBackend.VertexAI,
        params = Map("project" -> "test")
      ),
      outputSchema = model1ValueSchema,
      inputs = model1InputMapping,
      outputs = model1OutputMapping
    )

    val model2 = B.Model(
      metaData = B.MetaData(name = "model2"),
      runtime = B.ModelRuntime(
        backend = ModelBackend.SageMaker,
        params = Map("endpoint" -> "test")
      ),
      outputSchema = model2ValueSchema
    )

    (model1, model2)
  }

  def createMockPlatformProvider(): TestModelPlatformProvider = {
    val testPlatformProvider = new TestModelPlatformProvider()

    // Mock platform for model1 - returns score and category
    val model1Platform = new TestModelPlatform(Map.empty, (_: Map[String, AnyRef]) => {
      Map(
        "score" -> 0.85.asInstanceOf[AnyRef],
        "category" -> "premium".asInstanceOf[AnyRef]
      )
    })
    testPlatformProvider.addPlatform(ModelBackend.VertexAI, model1Platform)

    // Mock platform for model2 - returns prediction and confidence
    val model2Platform = new TestModelPlatform(Map.empty, (_: Map[String, AnyRef]) => {
      Map(
        "prediction" -> "positive".asInstanceOf[AnyRef],
        "confidence" -> 0.92.asInstanceOf[AnyRef]
      )
    })
    testPlatformProvider.addPlatform(ModelBackend.SageMaker, model2Platform)

    testPlatformProvider
  }

  def createJoinSourceTable(tableName: String): String = {
    val schema = List(
      Column("user_id", api.StringType, 5),
      Column("item_id", api.StringType, 5),
      Column("feature1", api.DoubleType, 100),
      Column("feature2", api.LongType, 1000)
    )

    val fullTableName = s"$namespace.$tableName"
    DataFrameGen
      .events(spark, schema, count = 100, partitions = 5)
      .save(fullTableName)

    fullTableName
  }

  def createEventsSourceTable(tableName: String): String = {
    val schema = List(
      Column("user_id", api.StringType, 5),
      Column("event_type", api.StringType, 3),
      Column("feature1", api.DoubleType, 100),
      Column("feature2", api.LongType, 1000)
    )

    val fullTableName = s"$namespace.$tableName"
    DataFrameGen
      .events(spark, schema, count = 100, partitions = 5)
      .save(fullTableName)

    fullTableName
  }

  def verifyOutputSchema(
      outputDf: DataFrame,
      expectedColumns: Set[String],
      excludedColumns: Set[String] = Set.empty
  ): Unit = {
    val outputColumns = outputDf.columns.toSet
    expectedColumns.foreach { col =>
      outputColumns should contain(col)
    }
    excludedColumns.foreach { col =>
      outputColumns should not contain col
    }
  }

  it should "process join source with 2 models" in {
    val joinSourceTable = createJoinSourceTable("join_source_basic")
    val sourceDf = tableUtils.loadTable(joinSourceTable)
    val sourceCount = sourceDf.count()
    sourceCount should be > 0L

    // Create a JoinSource referencing the join table
    val joinMetadata = B.MetaData(name = "test_join")
      .setOutputNamespace(namespace)

    val outputTableInfo = new TableInfo().setTable(joinSourceTable)
    val executionInfo = new ExecutionInfo().setOutputTableInfo(outputTableInfo)
    joinMetadata.setExecutionInfo(executionInfo)

    val joinConf = B.Join(
      left = B.Source.events(
        query = B.Query(startPartition = monthAgo),
        table = joinSourceTable
      ),
      joinParts = Seq.empty,
      metaData = joinMetadata
    )

    val joinSource = new JoinSource()
      .setJoin(joinConf)
      .setQuery(null)

    val source = new Source()
    source.setJoinSource(joinSource)

    val (model1, model2) = createTestModels()

    val inference = new Inference()
      .setMetaData(B.MetaData(name = "test_inference_basic").setOutputNamespace(namespace))
      .setModels(Seq(model1, model2).asJava)
      .setPassthrough(Seq("user_id", "item_id").asJava)
      .setFeatures(Seq(source).asJava)

    val testPlatformProvider = createMockPlatformProvider()

    val dateRange = PartitionRange(monthAgo, today)(tableUtils.partitionSpec)
    InferenceJob.computeBackfill(
      inference,
      dateRange,
      tableUtils,
      testPlatformProvider
    )

    val outputDf = tableUtils.loadTable(inference.metaData.outputTable)
    outputDf.count() shouldBe sourceCount

    verifyOutputSchema(
      outputDf,
      expectedColumns = Set("user_id", "item_id", "model1__score", "model1__category",
        "model2__prediction", "model2__confidence"),
      excludedColumns = Set("feature1", "feature2")
    )

    val firstRow = outputDf.take(1).head
    firstRow.getAs[Double]("model1__score") shouldBe 0.85
    firstRow.getAs[String]("model1__category") shouldBe "premium"
    firstRow.getAs[String]("model2__prediction") shouldBe "positive"
    firstRow.getAs[Double]("model2__confidence") shouldBe 0.92
  }

  it should "process events source with 2 models" in {
    val eventsSourceTable = createEventsSourceTable("events_source_basic")
    val sourceDf = tableUtils.loadTable(eventsSourceTable)
    val sourceCount = sourceDf.count()
    sourceCount should be > 0L

    // Create an events Source
    val source = B.Source.events(
      query = B.Query(startPartition = monthAgo),
      table = eventsSourceTable
    )

    val (model1, model2) = createTestModels()

    val inference = new Inference()
      .setMetaData(B.MetaData(name = "test_inference_events").setOutputNamespace(namespace))
      .setModels(Seq(model1, model2).asJava)
      .setPassthrough(Seq("user_id", "event_type").asJava)
      .setFeatures(Seq(source).asJava)

    val testPlatformProvider = createMockPlatformProvider()

    val dateRange = PartitionRange(monthAgo, today)(tableUtils.partitionSpec)
    InferenceJob.computeBackfill(
      inference,
      dateRange,
      tableUtils,
      testPlatformProvider
    )

    val outputDf = tableUtils.loadTable(inference.metaData.outputTable)
    outputDf.count() shouldBe sourceCount

    verifyOutputSchema(
      outputDf,
      expectedColumns = Set("user_id", "event_type", "model1__score", "model1__category",
        "model2__prediction", "model2__confidence"),
      excludedColumns = Set("feature1", "feature2")
    )

    val firstRow = outputDf.take(1).head
    firstRow.getAs[Double]("model1__score") shouldBe 0.85
    firstRow.getAs[String]("model1__category") shouldBe "premium"
  }

  it should "process source with query that renames columns" in {
    val joinSourceTable = createJoinSourceTable("join_source_with_query")
    val sourceDf = tableUtils.loadTable(joinSourceTable)
    val sourceCount = sourceDf.count()

    val joinMetadata = B.MetaData(name = "test_join_with_query")
      .setOutputNamespace(namespace)

    val outputTableInfo = new TableInfo().setTable(joinSourceTable)
    val executionInfo = new ExecutionInfo().setOutputTableInfo(outputTableInfo)
    joinMetadata.setExecutionInfo(executionInfo)

    val joinConf = B.Join(
      left = B.Source.events(
        query = B.Query(startPartition = monthAgo),
        table = joinSourceTable
      ),
      joinParts = Seq.empty,
      metaData = joinMetadata
    )

    // Add a query that renames columns
    val joinSourceQuery = B.Query(
      selects = Map(
        "user_id" -> "user_id",
        "product_id" -> "item_id",
        "feature1" -> "feature1"
      )
    )

    val joinSource = new JoinSource()
      .setJoin(joinConf)
      .setQuery(joinSourceQuery)

    val source = new Source()
    source.setJoinSource(joinSource)

    val (model1, model2) = createTestModels()

    val inference = new Inference()
      .setMetaData(B.MetaData(name = "test_inference_query").setOutputNamespace(namespace))
      .setModels(Seq(model1, model2).asJava)
      .setPassthrough(Seq("user_id", "product_id").asJava)
      .setFeatures(Seq(source).asJava)

    val testPlatformProvider = createMockPlatformProvider()

    val dateRange = PartitionRange(monthAgo, today)(tableUtils.partitionSpec)
    InferenceJob.computeBackfill(
      inference,
      dateRange,
      tableUtils,
      testPlatformProvider
    )

    val outputDf = tableUtils.loadTable(inference.metaData.outputTable)
    outputDf.count() shouldBe sourceCount

    verifyOutputSchema(
      outputDf,
      expectedColumns = Set("user_id", "product_id", "model1__score", "model1__category",
        "model2__prediction", "model2__confidence"),
      excludedColumns = Set("item_id", "feature2")  // item_id renamed to product_id, feature2 not selected
    )
  }

  it should "process models with input and output mappings" in {
    val joinSourceTable = createJoinSourceTable("join_source_mappings")
    val sourceDf = tableUtils.loadTable(joinSourceTable)
    val sourceCount = sourceDf.count()

    val joinMetadata = B.MetaData(name = "test_join_mappings")
      .setOutputNamespace(namespace)

    val outputTableInfo = new TableInfo().setTable(joinSourceTable)
    val executionInfo = new ExecutionInfo().setOutputTableInfo(outputTableInfo)
    joinMetadata.setExecutionInfo(executionInfo)

    val joinConf = B.Join(
      left = B.Source.events(
        query = B.Query(startPartition = monthAgo),
        table = joinSourceTable
      ),
      joinParts = Seq.empty,
      metaData = joinMetadata
    )

    val joinSource = new JoinSource()
      .setJoin(joinConf)
      .setQuery(null)

    val source = new Source()
    source.setJoinSource(joinSource)

    // Create models with input and output mappings
    val (model1, model2) = createTestModels(
      includeInputMapping = true,
      includeOutputMapping = true
    )

    val inference = new Inference()
      .setMetaData(B.MetaData(name = "test_inference_mappings").setOutputNamespace(namespace))
      .setModels(Seq(model1, model2).asJava)
      .setPassthrough(Seq("user_id", "item_id").asJava)
      .setFeatures(Seq(source).asJava)

    val testPlatformProvider = createMockPlatformProvider()

    val dateRange = PartitionRange(monthAgo, today)(tableUtils.partitionSpec)
    InferenceJob.computeBackfill(
      inference,
      dateRange,
      tableUtils,
      testPlatformProvider
    )

    val outputDf = tableUtils.loadTable(inference.metaData.outputTable)
    outputDf.count() shouldBe sourceCount

    // With output mapping, model1 should have final_score instead of score (raw output is dropped)
    verifyOutputSchema(
      outputDf,
      expectedColumns = Set("user_id", "item_id", "model1__final_score", "model1__category",
        "model2__prediction", "model2__confidence"),
      excludedColumns = Set("feature1", "feature2", "model1__score")  // score is dropped when output mapping exists
    )

    // Verify output mapping applied correctly (final_score = score * 2.0)
    val firstRow = outputDf.take(1).head
    firstRow.getAs[Double]("model1__final_score") shouldBe 1.7  // 0.85 * 2.0
  }

  it should "fail with clear error when model metadata name is null" in {
    val joinSourceTable = createJoinSourceTable("join_source_null_name")
    val sourceDf = tableUtils.loadTable(joinSourceTable)
    val sourceCount = sourceDf.count()
    sourceCount should be > 0L

    val joinMetadata = B.MetaData(name = "test_join")
      .setOutputNamespace(namespace)

    val outputTableInfo = new TableInfo().setTable(joinSourceTable)
    val executionInfo = new ExecutionInfo().setOutputTableInfo(outputTableInfo)
    joinMetadata.setExecutionInfo(executionInfo)

    val joinConf = B.Join(
      left = B.Source.events(
        query = B.Query(startPartition = monthAgo),
        table = joinSourceTable
      ),
      joinParts = Seq.empty,
      metaData = joinMetadata
    )

    val joinSource = new JoinSource()
      .setJoin(joinConf)
      .setQuery(null)

    val source = new Source()
    source.setJoinSource(joinSource)

    val modelWithoutName = new Model()
    modelWithoutName.setMetaData(new MetaData()) // metaData exists but name is null
    modelWithoutName.setRuntime(
      new ModelRuntime()
        .setBackend(ModelBackend.VertexAI)
        .setParams(Map("project" -> "test").asJava)
    )
    val outputSchema = B.structSchema("model_output", "score" -> DoubleType)
    modelWithoutName.setOutputSchema(outputSchema)

    val inference = new Inference()
      .setMetaData(B.MetaData(name = "test_inference_null_name").setOutputNamespace(namespace))
      .setModels(Seq(modelWithoutName).asJava)
      .setPassthrough(Seq("user_id", "item_id").asJava)
      .setFeatures(Seq(source).asJava)

    val testPlatformProvider = createMockPlatformProvider()

    val dateRange = PartitionRange(monthAgo, today)(tableUtils.partitionSpec)

    val exception = intercept[IllegalArgumentException] {
      InferenceJob.computeBackfill(
        inference,
        dateRange,
        tableUtils,
        testPlatformProvider
      )
    }

    exception.getMessage should include("must have a non-null, non-empty name in metaData")
    exception.getMessage should include("model JSON has the 'metaData.name' field set")
  }

  it should "handle model inference failures gracefully" in {
    val joinSourceTable = createJoinSourceTable("join_source_failure")
    val sourceDf = tableUtils.loadTable(joinSourceTable)
    val sourceCount = sourceDf.count()
    sourceCount should be > 0L

    val joinMetadata = B.MetaData(name = "test_join_failure")
      .setOutputNamespace(namespace)

    val outputTableInfo = new TableInfo().setTable(joinSourceTable)
    val executionInfo = new ExecutionInfo().setOutputTableInfo(outputTableInfo)
    joinMetadata.setExecutionInfo(executionInfo)

    val joinConf = B.Join(
      left = B.Source.events(
        query = B.Query(startPartition = monthAgo),
        table = joinSourceTable
      ),
      joinParts = Seq.empty,
      metaData = joinMetadata
    )

    val joinSource = new JoinSource()
      .setJoin(joinConf)
      .setQuery(null)

    val source = new Source()
    source.setJoinSource(joinSource)

    val model = B.Model(
      metaData = B.MetaData(name = "failing_model"),
      runtime = B.ModelRuntime(
        backend = ModelBackend.VertexAI,
        params = Map("project" -> "test")
      ),
      outputSchema = B.structSchema("model_output",
        "score" -> DoubleType
      )
    )

    val inference = new Inference()
      .setMetaData(B.MetaData(name = "test_inference_failure").setOutputNamespace(namespace))
      .setModels(Seq(model).asJava)
      .setPassthrough(Seq("user_id", "item_id").asJava)
      .setFeatures(Seq(source).asJava)

    val testPlatformProvider = new TestModelPlatformProvider()

    // Mock platform that returns a successful Future but with a Failure in the InferResponse
    val mockPlatform = new FailingResponseModelPlatform()
    testPlatformProvider.addPlatform(ModelBackend.VertexAI, mockPlatform)

    val dateRange = PartitionRange(monthAgo, today)(tableUtils.partitionSpec)

    // Run the backfill - it should complete but with empty model outputs
    InferenceJob.computeBackfill(
      inference,
      dateRange,
      tableUtils,
      testPlatformProvider
    )

    val outputTable = inference.metaData.outputTable
    val outputDf = tableUtils.loadTable(outputTable)

    // Should have passthrough fields and null model outputs
    val fieldNames = outputDf.schema.fieldNames.toSet
    fieldNames should contain("user_id")
    fieldNames should contain("item_id")
    fieldNames should contain("failing_model__score")

    // Verify we got some data (with null scores due to failure)
    outputDf.count() should be > 0L

    // All scores should be null due to inference failure
    outputDf.select("failing_model__score").collect().foreach { row =>
      row.isNullAt(0) shouldBe true
    }
  }

  it should "handle empty models sequence" in {
    val joinSourceTable = createJoinSourceTable("join_source_empty_models")
    val sourceDf = tableUtils.loadTable(joinSourceTable)
    val sourceCount = sourceDf.count()
    sourceCount should be > 0L

    val joinMetadata = B.MetaData(name = "test_join_empty_models")
      .setOutputNamespace(namespace)

    val outputTableInfo = new TableInfo().setTable(joinSourceTable)
    val executionInfo = new ExecutionInfo().setOutputTableInfo(outputTableInfo)
    joinMetadata.setExecutionInfo(executionInfo)

    val joinConf = B.Join(
      left = B.Source.events(
        query = B.Query(startPartition = monthAgo),
        table = joinSourceTable
      ),
      joinParts = Seq.empty,
      metaData = joinMetadata
    )

    val joinSource = new JoinSource()
      .setJoin(joinConf)
      .setQuery(null)

    val source = new Source()
    source.setJoinSource(joinSource)

    val inference = new Inference()
      .setMetaData(B.MetaData(name = "test_inference_empty_models").setOutputNamespace(namespace))
      .setModels(Seq.empty.asJava)  // Empty models sequence
      .setPassthrough(Seq("user_id", "item_id").asJava)
      .setFeatures(Seq(source).asJava)

    val testPlatformProvider = new TestModelPlatformProvider()

    val dateRange = PartitionRange(monthAgo, today)(tableUtils.partitionSpec)

    // Run the backfill - should just pass through the specified fields
    InferenceJob.computeBackfill(
      inference,
      dateRange,
      tableUtils,
      testPlatformProvider
    )

    val outputTable = inference.metaData.outputTable
    val outputDf = tableUtils.loadTable(outputTable)

    // Should only have passthrough fields
    val fieldNames = outputDf.schema.fieldNames.toSet
    val expectedFieldNames = Set("user_id", "item_id", tableUtils.partitionColumn, "ts")
    fieldNames shouldEqual expectedFieldNames

    // Verify we got the same number of rows
    outputDf.count() shouldBe sourceCount
  }

  it should "handle complex struct types in model output" in {
    val joinSourceTable = createJoinSourceTable("join_source_struct_output")
    val sourceDf = tableUtils.loadTable(joinSourceTable)
    val sourceCount = sourceDf.count()
    sourceCount should be > 0L

    val joinMetadata = B.MetaData(name = "test_join_struct_output")
      .setOutputNamespace(namespace)

    val outputTableInfo = new TableInfo().setTable(joinSourceTable)
    val executionInfo = new ExecutionInfo().setOutputTableInfo(outputTableInfo)
    joinMetadata.setExecutionInfo(executionInfo)

    val joinConf = B.Join(
      left = B.Source.events(
        query = B.Query(startPartition = monthAgo),
        table = joinSourceTable
      ),
      joinParts = Seq.empty,
      metaData = joinMetadata
    )

    val joinSource = new JoinSource()
      .setJoin(joinConf)
      .setQuery(null)

    val source = new Source()
    source.setJoinSource(joinSource)

    // Create a model with complex struct output
    val model = B.Model(
      metaData = B.MetaData(name = "struct_model"),
      runtime = B.ModelRuntime(
        backend = ModelBackend.VertexAI,
        params = Map("project" -> "test")
      ),
      outputSchema = B.structSchema("model_output",
        "result" -> api.StructType.from("result_struct", Array(
          "score" -> DoubleType,
          "metadata" -> api.StructType.from("metadata_struct", Array(
            "confidence" -> DoubleType,
            "category" -> StringType
          ))
        ))
      )
    )

    val inference = new Inference()
      .setMetaData(B.MetaData(name = "test_inference_struct").setOutputNamespace(namespace))
      .setModels(Seq(model).asJava)
      .setPassthrough(Seq("user_id", "item_id").asJava)
      .setFeatures(Seq(source).asJava)

    val testPlatformProvider = new TestModelPlatformProvider()

    // Mock platform that returns nested struct using java.util.Map
    val mockPlatform = new TestModelPlatform(Map.empty, (_: Map[String, AnyRef]) => {
      val metadataMap = new java.util.HashMap[String, AnyRef]()
      metadataMap.put("confidence", 0.95.asInstanceOf[AnyRef])
      metadataMap.put("category", "premium")

      val resultMap = new java.util.HashMap[String, AnyRef]()
      resultMap.put("score", 0.87.asInstanceOf[AnyRef])
      resultMap.put("metadata", metadataMap)

      Map("result" -> resultMap.asInstanceOf[AnyRef])
    })
    testPlatformProvider.addPlatform(ModelBackend.VertexAI, mockPlatform)

    val dateRange = PartitionRange(monthAgo, today)(tableUtils.partitionSpec)

    // Run the backfill
    InferenceJob.computeBackfill(
      inference,
      dateRange,
      tableUtils,
      testPlatformProvider
    )

    val outputTable = inference.metaData.outputTable
    val outputDf = tableUtils.loadTable(outputTable)

    // Verify the output schema contains the struct field
    val fieldNames = outputDf.schema.fieldNames.toSet
    fieldNames should contain("struct_model__result")

    // Verify we got some data
    outputDf.count() shouldBe sourceCount

    // Verify the nested struct values are correctly converted
    val firstRow = outputDf.select("struct_model__result").take(1).head
    val resultStruct = firstRow.getStruct(0)
    resultStruct.getDouble(resultStruct.fieldIndex("score")) shouldBe 0.87

    val metadataStruct = resultStruct.getStruct(resultStruct.fieldIndex("metadata"))
    metadataStruct.getDouble(metadataStruct.fieldIndex("confidence")) shouldBe 0.95
    metadataStruct.getString(metadataStruct.fieldIndex("category")) shouldBe "premium"
  }

  it should "compute output schema with input and output mappings" in {
    val joinSourceTable = createJoinSourceTable("join_source_schema_test")
    val sourceDf = tableUtils.loadTable(joinSourceTable)

    val joinMetadata = B.MetaData(name = "test_join_schema")
      .setOutputNamespace(namespace)

    val outputTableInfo = new TableInfo().setTable(joinSourceTable)
    val executionInfo = new ExecutionInfo().setOutputTableInfo(outputTableInfo)
    joinMetadata.setExecutionInfo(executionInfo)

    val joinConf = B.Join(
      left = B.Source.events(
        query = B.Query(startPartition = monthAgo),
        table = joinSourceTable
      ),
      joinParts = Seq.empty,
      metaData = joinMetadata
    )

    val joinSource = new JoinSource()
      .setJoin(joinConf)
      .setQuery(null)

    val source = new Source()
    source.setJoinSource(joinSource)

    // Create models with input and output mappings
    val (model1, model2) = createTestModels(
      includeInputMapping = true,
      includeOutputMapping = true
    )

    val inference = new Inference()
      .setMetaData(B.MetaData(name = "test_inference_schema").setOutputNamespace(namespace))
      .setModels(Seq(model1, model2).asJava)
      .setPassthrough(Seq("user_id", "item_id").asJava)
      .setFeatures(Seq(source).asJava)

    // Compute the output schema
    val outputSchema = InferenceJob.computeOutputSchema(sourceDf, inference, tableUtils)

    // Verify the schema contains exactly the expected fields
    val actualFields = outputSchema.fieldNames.toSet
    val expectedFields = Set(
      "user_id", "item_id", "ts", tableUtils.partitionColumn,
      "model1__final_score", "model1__category",  // model1 with output mapping
      "model2__prediction", "model2__confidence"   // model2 without output mapping
    )

    actualFields shouldEqual expectedFields

    // Verify model1__score is NOT present (since output mapping exists for model1)
    actualFields should not contain "model1__score"

    // Verify source fields not in passthrough are NOT present
    actualFields should not contain "feature1"
    actualFields should not contain "feature2"
  }
}

// Test helper classes
class TestModelPlatformProvider extends ModelPlatformProvider {
  private val platforms = mutable.Map[ModelBackend, ModelPlatform]()

  def addPlatform(backend: ModelBackend, platform: ModelPlatform): Unit = {
    platforms(backend) = platform
  }

  override def getPlatform(modelBackend: ModelBackend, backendParams: Map[String, String]): ModelPlatform = {
    platforms.getOrElse(modelBackend, new TestModelPlatform())
  }
}

class TestModelPlatform(
    responseMap: Map[Map[String, AnyRef], Map[String, AnyRef]] = Map.empty,
    responseFn: Map[String, AnyRef] => Map[String, AnyRef] = _ => Map("default_output" -> "test_result".asInstanceOf[AnyRef])
) extends ModelPlatform {

  override def infer(inferRequest: InferRequest): Future[InferResponse] = {
    val outputs = inferRequest.inputRequests.map { inputRequest =>
      responseMap.get(inputRequest) match {
        case Some(outputs) => outputs
        case None => responseFn(inputRequest)
      }
    }
    Future.successful(InferResponse(inferRequest, Success(outputs)))
  }

  override def submitTrainingJob(trainingRequest: TrainingRequest): Future[String] = ???
  override def createEndpoint(endpointConfig: EndpointConfig): Future[String] = ???
  override def deployModel(deployModelRequest: DeployModelRequest): Future[String] = ???
  override def getJobStatus(operation: ai.chronon.online.ModelOperation, id: String): Future[ai.chronon.online.ModelJobStatus] = ???
}

// Test platform that returns a successful Future with a Failure in InferResponse.outputs
class FailingResponseModelPlatform extends ModelPlatform {
  override def infer(inferRequest: InferRequest): Future[InferResponse] = {
    // Future succeeds, but the response contains a Failure
    Future.successful(
      InferResponse(inferRequest, scala.util.Failure(new RuntimeException("Model inference failed")))
    )
  }

  override def submitTrainingJob(trainingRequest: TrainingRequest): Future[String] = ???
  override def createEndpoint(endpointConfig: EndpointConfig): Future[String] = ???
  override def deployModel(deployModelRequest: DeployModelRequest): Future[String] = ???
  override def getJobStatus(operation: ai.chronon.online.ModelOperation, id: String): Future[ai.chronon.online.ModelJobStatus] = ???
}
