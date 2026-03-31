package ai.chronon.integrations.aws

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbAsyncWaiter

import java.util.concurrent.CompletableFuture

class PrefixedDynamoDbAsyncClientTest extends AnyFlatSpec with Matchers with MockitoSugar with BeforeAndAfterEach {

  private var mockDelegate: DynamoDbAsyncClient = _
  private var mockWaiter: DynamoDbAsyncWaiter = _
  private val testPrefix = "test_prefix_"

  override def beforeEach(): Unit = {
    mockDelegate = mock[DynamoDbAsyncClient]
    mockWaiter = mock[DynamoDbAsyncWaiter]
    when(mockDelegate.waiter()).thenReturn(mockWaiter)
  }

  "PrefixedDynamoDbAsyncClient" should "prefix table name in getItem request" in {
    val client = new PrefixedDynamoDbAsyncClient(mockDelegate, testPrefix)
    val originalTableName = "my_table"
    val request = GetItemRequest.builder().tableName(originalTableName).build()

    when(mockDelegate.getItem(any[GetItemRequest]())).thenReturn(
      CompletableFuture.completedFuture(GetItemResponse.builder().build())
    )

    client.getItem(request)

    val captor = ArgumentCaptor.forClass(classOf[GetItemRequest])
    verify(mockDelegate).getItem(captor.capture())
    captor.getValue.tableName() shouldBe s"$testPrefix$originalTableName"
  }

  it should "prefix table name in putItem request" in {
    val client = new PrefixedDynamoDbAsyncClient(mockDelegate, testPrefix)
    val originalTableName = "my_table"
    val request = PutItemRequest.builder().tableName(originalTableName).build()

    when(mockDelegate.putItem(any[PutItemRequest]())).thenReturn(
      CompletableFuture.completedFuture(PutItemResponse.builder().build())
    )

    client.putItem(request)

    val captor = ArgumentCaptor.forClass(classOf[PutItemRequest])
    verify(mockDelegate).putItem(captor.capture())
    captor.getValue.tableName() shouldBe s"$testPrefix$originalTableName"
  }

  it should "prefix table name in query request" in {
    val client = new PrefixedDynamoDbAsyncClient(mockDelegate, testPrefix)
    val originalTableName = "my_table"
    val request = QueryRequest.builder().tableName(originalTableName).build()

    when(mockDelegate.query(any[QueryRequest]())).thenReturn(
      CompletableFuture.completedFuture(QueryResponse.builder().build())
    )

    client.query(request)

    val captor = ArgumentCaptor.forClass(classOf[QueryRequest])
    verify(mockDelegate).query(captor.capture())
    captor.getValue.tableName() shouldBe s"$testPrefix$originalTableName"
  }

  it should "prefix table name in scan request" in {
    val client = new PrefixedDynamoDbAsyncClient(mockDelegate, testPrefix)
    val originalTableName = "my_table"
    val request = ScanRequest.builder().tableName(originalTableName).build()

    when(mockDelegate.scan(any[ScanRequest]())).thenReturn(
      CompletableFuture.completedFuture(ScanResponse.builder().build())
    )

    client.scan(request)

    val captor = ArgumentCaptor.forClass(classOf[ScanRequest])
    verify(mockDelegate).scan(captor.capture())
    captor.getValue.tableName() shouldBe s"$testPrefix$originalTableName"
  }

  it should "prefix table name in createTable request" in {
    val client = new PrefixedDynamoDbAsyncClient(mockDelegate, testPrefix)
    val originalTableName = "my_table"
    val request = CreateTableRequest.builder().tableName(originalTableName).build()

    when(mockDelegate.createTable(any[CreateTableRequest]())).thenReturn(
      CompletableFuture.completedFuture(CreateTableResponse.builder().build())
    )

    client.createTable(request)

    val captor = ArgumentCaptor.forClass(classOf[CreateTableRequest])
    verify(mockDelegate).createTable(captor.capture())
    captor.getValue.tableName() shouldBe s"$testPrefix$originalTableName"
  }

  it should "prefix table name in deleteTable request" in {
    val client = new PrefixedDynamoDbAsyncClient(mockDelegate, testPrefix)
    val originalTableName = "my_table"
    val request = DeleteTableRequest.builder().tableName(originalTableName).build()

    when(mockDelegate.deleteTable(any[DeleteTableRequest]())).thenReturn(
      CompletableFuture.completedFuture(DeleteTableResponse.builder().build())
    )

    client.deleteTable(request)

    val captor = ArgumentCaptor.forClass(classOf[DeleteTableRequest])
    verify(mockDelegate).deleteTable(captor.capture())
    captor.getValue.tableName() shouldBe s"$testPrefix$originalTableName"
  }

  it should "prefix table name in describeTable request" in {
    val client = new PrefixedDynamoDbAsyncClient(mockDelegate, testPrefix)
    val originalTableName = "my_table"
    val request = DescribeTableRequest.builder().tableName(originalTableName).build()

    when(mockDelegate.describeTable(any[DescribeTableRequest]())).thenReturn(
      CompletableFuture.completedFuture(DescribeTableResponse.builder().build())
    )

    client.describeTable(request)

    val captor = ArgumentCaptor.forClass(classOf[DescribeTableRequest])
    verify(mockDelegate).describeTable(captor.capture())
    captor.getValue.tableName() shouldBe s"$testPrefix$originalTableName"
  }

  it should "prefix table name in updateTimeToLive request" in {
    val client = new PrefixedDynamoDbAsyncClient(mockDelegate, testPrefix)
    val originalTableName = "my_table"
    val request = UpdateTimeToLiveRequest.builder().tableName(originalTableName).build()

    when(mockDelegate.updateTimeToLive(any[UpdateTimeToLiveRequest]())).thenReturn(
      CompletableFuture.completedFuture(UpdateTimeToLiveResponse.builder().build())
    )

    client.updateTimeToLive(request)

    val captor = ArgumentCaptor.forClass(classOf[UpdateTimeToLiveRequest])
    verify(mockDelegate).updateTimeToLive(captor.capture())
    captor.getValue.tableName() shouldBe s"$testPrefix$originalTableName"
  }

  it should "prefix table name in importTable request" in {
    val client = new PrefixedDynamoDbAsyncClient(mockDelegate, testPrefix)
    val originalTableName = "my_table"
    val tableParams = TableCreationParameters.builder()
      .tableName(originalTableName)
      .build()
    val request = ImportTableRequest.builder()
      .tableCreationParameters(tableParams)
      .build()

    when(mockDelegate.importTable(any[ImportTableRequest]())).thenReturn(
      CompletableFuture.completedFuture(ImportTableResponse.builder().build())
    )

    client.importTable(request)

    val captor = ArgumentCaptor.forClass(classOf[ImportTableRequest])
    verify(mockDelegate).importTable(captor.capture())
    captor.getValue.tableCreationParameters().tableName() shouldBe s"$testPrefix$originalTableName"
  }

  it should "not modify describeImport request (uses ARN, not table name)" in {
    val client = new PrefixedDynamoDbAsyncClient(mockDelegate, testPrefix)
    val importArn = "arn:aws:dynamodb:us-west-2:123456789012:table/my_table/import/01234567890123-12345678"
    val request = DescribeImportRequest.builder().importArn(importArn).build()

    when(mockDelegate.describeImport(any[DescribeImportRequest]())).thenReturn(
      CompletableFuture.completedFuture(DescribeImportResponse.builder().build())
    )

    client.describeImport(request)

    val captor = ArgumentCaptor.forClass(classOf[DescribeImportRequest])
    verify(mockDelegate).describeImport(captor.capture())
    captor.getValue.importArn() shouldBe importArn
  }

  it should "prefix table name in waitUntilTableExists" in {
    val client = new PrefixedDynamoDbAsyncClient(mockDelegate, testPrefix)
    val originalTableName = "my_table"

    import software.amazon.awssdk.core.waiters.WaiterResponse
    val mockWaiterResponse = mock[WaiterResponse[DescribeTableResponse]]
    when(mockWaiter.waitUntilTableExists(any[DescribeTableRequest]())).thenReturn(
      CompletableFuture.completedFuture(mockWaiterResponse)
    )

    client.waitUntilTableExists(originalTableName)

    val captor = ArgumentCaptor.forClass(classOf[DescribeTableRequest])
    verify(mockWaiter).waitUntilTableExists(captor.capture())
    captor.getValue.tableName() shouldBe s"$testPrefix$originalTableName"
  }

  it should "prefix table name in waitUntilTableNotExists" in {
    val client = new PrefixedDynamoDbAsyncClient(mockDelegate, testPrefix)
    val originalTableName = "my_table"

    import software.amazon.awssdk.core.waiters.WaiterResponse
    val mockWaiterResponse = mock[WaiterResponse[DescribeTableResponse]]
    when(mockWaiter.waitUntilTableNotExists(any[DescribeTableRequest]())).thenReturn(
      CompletableFuture.completedFuture(mockWaiterResponse)
    )

    client.waitUntilTableNotExists(originalTableName)

    val captor = ArgumentCaptor.forClass(classOf[DescribeTableRequest])
    verify(mockWaiter).waitUntilTableNotExists(captor.capture())
    captor.getValue.tableName() shouldBe s"$testPrefix$originalTableName"
  }

  it should "handle null table name without throwing" in {
    val client = new PrefixedDynamoDbAsyncClient(mockDelegate, testPrefix)
    val request = GetItemRequest.builder().tableName(null).build()

    when(mockDelegate.getItem(any[GetItemRequest]())).thenReturn(
      CompletableFuture.completedFuture(GetItemResponse.builder().build())
    )

    client.getItem(request)

    val captor = ArgumentCaptor.forClass(classOf[GetItemRequest])
    verify(mockDelegate).getItem(captor.capture())
    captor.getValue.tableName() shouldBe null
  }

  it should "handle empty table name without adding prefix" in {
    val client = new PrefixedDynamoDbAsyncClient(mockDelegate, testPrefix)
    val request = GetItemRequest.builder().tableName("").build()

    when(mockDelegate.getItem(any[GetItemRequest]())).thenReturn(
      CompletableFuture.completedFuture(GetItemResponse.builder().build())
    )

    client.getItem(request)

    val captor = ArgumentCaptor.forClass(classOf[GetItemRequest])
    verify(mockDelegate).getItem(captor.capture())
    captor.getValue.tableName() shouldBe ""
  }

  it should "handle empty prefix (no prefix applied)" in {
    val client = new PrefixedDynamoDbAsyncClient(mockDelegate, "")
    val originalTableName = "my_table"
    val request = GetItemRequest.builder().tableName(originalTableName).build()

    when(mockDelegate.getItem(any[GetItemRequest]())).thenReturn(
      CompletableFuture.completedFuture(GetItemResponse.builder().build())
    )

    client.getItem(request)

    val captor = ArgumentCaptor.forClass(classOf[GetItemRequest])
    verify(mockDelegate).getItem(captor.capture())
    captor.getValue.tableName() shouldBe originalTableName
  }

  it should "preserve other request parameters when prefixing table name" in {
    val client = new PrefixedDynamoDbAsyncClient(mockDelegate, testPrefix)
    val originalTableName = "my_table"
    val keyCondition = "id = :val"
    val request = QueryRequest.builder()
      .tableName(originalTableName)
      .keyConditionExpression(keyCondition)
      .build()

    when(mockDelegate.query(any[QueryRequest]())).thenReturn(
      CompletableFuture.completedFuture(QueryResponse.builder().build())
    )

    client.query(request)

    val captor = ArgumentCaptor.forClass(classOf[QueryRequest])
    verify(mockDelegate).query(captor.capture())
    val capturedRequest = captor.getValue
    capturedRequest.tableName() shouldBe s"$testPrefix$originalTableName"
    capturedRequest.keyConditionExpression() shouldBe keyCondition
  }
}