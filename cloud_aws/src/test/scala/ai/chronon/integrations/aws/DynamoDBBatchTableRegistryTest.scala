package ai.chronon.integrations.aws

import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.online.KVStore.{GetRequest, PutRequest}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeDefinition,
  AttributeValue,
  CreateTableRequest,
  DescribeTableRequest,
  KeySchemaElement,
  KeyType,
  ProvisionedThroughput,
  PutItemRequest,
  ResourceInUseException,
  ScalarAttributeType
}

import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.concurrent.CompletionException
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class DynamoDBBatchTableRegistryTest extends AnyFlatSpec with BeforeAndAfterAll {

  import DynamoDBKVStoreConstants._

  var dynamoContainer: GenericContainer[_] = _
  var client: DynamoDbAsyncClient = _

  override def beforeAll(): Unit = {
    dynamoContainer = new GenericContainer(DockerImageName.parse("amazon/dynamodb-local:latest"))
    dynamoContainer.withExposedPorts(8000: Integer)
    dynamoContainer.withCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb")
    dynamoContainer.start()

    val dynamoEndpoint = s"http://${dynamoContainer.getHost}:${dynamoContainer.getMappedPort(8000)}"
    client = DynamoDbAsyncClient
      .builder()
      .endpointOverride(URI.create(dynamoEndpoint))
      .region(Region.US_WEST_2)
      .credentialsProvider(
        StaticCredentialsProvider.create(
          AwsBasicCredentials.create("dummy", "dummy")
        ))
      .build()
  }

  override def afterAll(): Unit = {
    if (client != null) client.close()
    if (dynamoContainer != null) dynamoContainer.stop()
  }

  it should "resolve batch dataset to physical table via registry" in {
    val kvStore = new DynamoDBKVStoreImpl(client)

    createStringKeyTable(batchTableRegistry)

    val logicalName = "MY_GROUPBY_BATCH"
    val physicalName = "MY_GROUPBY_BATCH_2026_02_17"

    // Write registry entry
    val item = Map(
      registryKeyColumn -> AttributeValue.builder.s(logicalName).build,
      registryValueColumn -> AttributeValue.builder.s(physicalName).build,
      registryTimestampColumn -> AttributeValue.builder.n(System.currentTimeMillis().toString).build
    )
    client.putItem(PutItemRequest.builder.tableName(batchTableRegistry).item(item.toJava).build()).join()

    val resolved = kvStore.resolveTableName(logicalName)
    resolved shouldBe physicalName
  }

  it should "use dataset name directly for non-batch datasets" in {
    val kvStore = new DynamoDBKVStoreImpl(client)

    val dataset = "MY_GROUPBY_STREAMING"
    val resolved = kvStore.resolveTableName(dataset)
    resolved shouldBe dataset
  }

  it should "return cached value on subsequent lookups" in {
    val kvStore = new DynamoDBKVStoreImpl(client)

    createStringKeyTable(batchTableRegistry)

    val logicalName = "CACHED_GROUPBY_BATCH"
    val physicalName = "CACHED_GROUPBY_BATCH_2026_02_17"

    val item = Map(
      registryKeyColumn -> AttributeValue.builder.s(logicalName).build,
      registryValueColumn -> AttributeValue.builder.s(physicalName).build,
      registryTimestampColumn -> AttributeValue.builder.n(System.currentTimeMillis().toString).build
    )
    client.putItem(PutItemRequest.builder.tableName(batchTableRegistry).item(item.toJava).build()).join()

    // First lookup populates the cache
    val resolved1 = kvStore.resolveTableName(logicalName)
    resolved1 shouldBe physicalName

    // Update the registry to a different value
    val updatedItem = Map(
      registryKeyColumn -> AttributeValue.builder.s(logicalName).build,
      registryValueColumn -> AttributeValue.builder.s("CACHED_GROUPBY_BATCH_2026_02_18").build,
      registryTimestampColumn -> AttributeValue.builder.n(System.currentTimeMillis().toString).build
    )
    client.putItem(PutItemRequest.builder.tableName(batchTableRegistry).item(updatedItem.toJava).build()).join()

    // Second lookup should still return cached value (TTL hasn't expired)
    val resolved2 = kvStore.resolveTableName(logicalName)
    resolved2 shouldBe physicalName
  }

  it should "resolve batch table names in multiGet for get lookups" in {
    val kvStore = new DynamoDBKVStoreImpl(client)

    createStringKeyTable(batchTableRegistry)

    val logicalName = "MULTIGET_GROUPBY_BATCH"
    val physicalName = "MULTIGET_GROUPBY_BATCH_2026_02_17"

    // Write registry entry
    val registryItem = Map(
      registryKeyColumn -> AttributeValue.builder.s(logicalName).build,
      registryValueColumn -> AttributeValue.builder.s(physicalName).build,
      registryTimestampColumn -> AttributeValue.builder.n(System.currentTimeMillis().toString).build
    )
    client.putItem(PutItemRequest.builder.tableName(batchTableRegistry).item(registryItem.toJava).build()).join()

    // Create the physical table and insert data
    kvStore.create(physicalName, Map.empty)
    val keyBytes = "test-key".getBytes(StandardCharsets.UTF_8)
    val valueBytes = "test-value".getBytes(StandardCharsets.UTF_8)
    val putResult = Await.result(kvStore.multiPut(Seq(PutRequest(keyBytes, valueBytes, physicalName, None))), 1.minute)
    putResult shouldBe Seq(true)

    // multiGet using the logical name should resolve and read from the physical table
    val getRequest = GetRequest(keyBytes, logicalName, None, None)
    val getResult = Await.result(kvStore.multiGet(Seq(getRequest)), 1.minute)
    getResult.length shouldBe 1
    getResult.head.values.isSuccess shouldBe true
    val returnedValue = new String(getResult.head.values.get.head.bytes, StandardCharsets.UTF_8)
    returnedValue shouldBe "test-value"
  }

  it should "use dataset name directly in multiGet for non-batch datasets" in {
    val kvStore = new DynamoDBKVStoreImpl(client)

    val dataset = "DIRECT_TABLE"
    kvStore.create(dataset, Map.empty)

    val keyBytes = "direct-key".getBytes(StandardCharsets.UTF_8)
    val valueBytes = "direct-value".getBytes(StandardCharsets.UTF_8)
    val putResult = Await.result(kvStore.multiPut(Seq(PutRequest(keyBytes, valueBytes, dataset, None))), 1.minute)
    putResult shouldBe Seq(true)

    val getRequest = GetRequest(keyBytes, dataset, None, None)
    val getResult = Await.result(kvStore.multiGet(Seq(getRequest)), 1.minute)
    getResult.length shouldBe 1
    getResult.head.values.isSuccess shouldBe true
    val returnedValue = new String(getResult.head.values.get.head.bytes, StandardCharsets.UTF_8)
    returnedValue shouldBe "direct-value"
  }

  /** Helper to create the registry table with a String partition key (idempotent). */
  private def createStringKeyTable(tableName: String): Unit = {
    try {
      client.createTable(
        CreateTableRequest.builder
          .tableName(tableName)
          .attributeDefinitions(
            AttributeDefinition.builder.attributeName(registryKeyColumn).attributeType(ScalarAttributeType.S).build
          )
          .keySchema(
            KeySchemaElement.builder.attributeName(registryKeyColumn).keyType(KeyType.HASH).build
          )
          .provisionedThroughput(
            ProvisionedThroughput.builder.readCapacityUnits(5L).writeCapacityUnits(5L).build
          )
          .build
      ).join()
      val waiterResponse = client.waiter.waitUntilTableExists(
        DescribeTableRequest.builder.tableName(tableName).build
      ).join()
      if (waiterResponse.matched.exception().isPresent)
        throw waiterResponse.matched.exception().get()
    } catch {
      case _: ResourceInUseException => // table already exists
      case e: CompletionException if e.getCause.isInstanceOf[ResourceInUseException] => // table already exists
    }
  }
}
