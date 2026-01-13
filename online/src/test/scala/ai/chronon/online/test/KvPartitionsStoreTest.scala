package ai.chronon.online.test

import ai.chronon.api.{PartitionSpec, TimeUnit, Window}
import ai.chronon.api.Extensions.WindowOps
import ai.chronon.online.{KvPartitions, KvPartitionsStore}
import ai.chronon.online.InMemoryKvStore
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

class KvPartitionsStoreTest extends AnyFlatSpec with Matchers {

  implicit val partitionSpec: PartitionSpec =
    new PartitionSpec("ds", "yyyy-MM-dd", new Window(1, TimeUnit.DAYS).millis)
  implicit val ec: ExecutionContext = ExecutionContext.global

  val dataset = "test_partitions"

  "KvPartitionsStore" should "put and get partitions" in {
    val kvStore = InMemoryKvStore.build(s"KvPartitionsStoreTest_put_get_${System.nanoTime()}")
    kvStore.create(dataset)
    val store = new KvPartitionsStore(kvStore, dataset)

    val kvPartitions = KvPartitions(
      partitions = Seq("2020-01-01", "2020-01-02", "2020-01-03"),
      semanticHash = Some("hash123")
    )

    Await.result(store.put("my_table", kvPartitions), 5.seconds)
    val result = Await.result(store.get("my_table"), 5.seconds)

    result shouldBe defined
    result.get.partitions should contain theSameElementsInOrderAs kvPartitions.partitions
    result.get.semanticHash shouldBe Some("hash123")
  }

  it should "return None for non-existent table" in {
    val kvStore = InMemoryKvStore.build(s"KvPartitionsStoreTest_nonexistent_${System.nanoTime()}")
    kvStore.create(dataset)
    val store = new KvPartitionsStore(kvStore, dataset)

    val result = Await.result(store.get("non_existent_table"), 5.seconds)

    result shouldBe None
  }

  it should "multiGet partitions for multiple tables" in {
    val kvStore = InMemoryKvStore.build(s"KvPartitionsStoreTest_multiget_${System.nanoTime()}")
    kvStore.create(dataset)
    val store = new KvPartitionsStore(kvStore, dataset)

    val partitions1 = KvPartitions(Seq("2020-01-01"), semanticHash = Some("h1"))
    val partitions2 = KvPartitions(Seq("2020-02-01", "2020-02-02"), semanticHash = Some("h2"))

    Await.result(store.put("table1", partitions1), 5.seconds)
    Await.result(store.put("table2", partitions2), 5.seconds)

    val results = Await.result(store.multiGet(Seq("table1", "table2", "table3")), 5.seconds)

    results("table1") shouldBe defined
    results("table1").get.partitions shouldBe Seq("2020-01-01")
    results("table1").get.semanticHash shouldBe Some("h1")

    results("table2") shouldBe defined
    results("table2").get.partitions shouldBe Seq("2020-02-01", "2020-02-02")

    results("table3") shouldBe None
  }

  it should "return empty map for empty multiGet" in {
    val kvStore = InMemoryKvStore.build(s"KvPartitionsStoreTest_empty_${System.nanoTime()}")
    kvStore.create(dataset)
    val store = new KvPartitionsStore(kvStore, dataset)

    val results = Await.result(store.multiGet(Seq.empty), 5.seconds)

    results shouldBe empty
  }
}
