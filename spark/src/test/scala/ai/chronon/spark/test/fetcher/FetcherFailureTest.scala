package ai.chronon.spark.test.fetcher

import ai.chronon.api.Constants.MetadataDataset
import ai.chronon.api.Extensions.JoinOps
import ai.chronon.online.fetcher
import ai.chronon.online.fetcher.{FetchContext, MetadataStore}
import ai.chronon.online.fetcher.Fetcher.Request
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.submission
import ai.chronon.spark.test.utils.OnlineUtils
import ai.chronon.spark.utils.MockApi
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.junit.Assert.{assertEquals, assertTrue}
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.{Logger, LoggerFactory}

import java.util.TimeZone
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

class FetcherFailureTest extends AnyFlatSpec {

  val sessionName = "FetcherFailureTest"
  val spark: SparkSession = submission.SparkSessionBuilder.build(sessionName, local = true)
  private val tableUtils = TableUtils(spark)

  private val topic = "test_topic"
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val yesterday = tableUtils.partitionSpec.before(today)

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  // test soft-fail on missing keys
  it should "test empty request" in {
    val namespace = "empty_request"
    val joinConf = FetcherTestUtil.generateRandomData(namespace, tableUtils, spark, topic, today, yesterday, 5, 5)
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("FetcherFailureTest#empty_request")
    val inMemoryKvStore = kvStoreFunc()
    val mockApi = new MockApi(kvStoreFunc, namespace)

    val metadataStore = new fetcher.MetadataStore(FetchContext(inMemoryKvStore))
    inMemoryKvStore.create(MetadataDataset)
    metadataStore.putJoinConf(joinConf)

    val request = Request(joinConf.metaData.name, Map.empty)
    val (responses, _) = FetcherTestUtil.joinResponses(spark, Array(request), mockApi)
    val responseMap = responses.head.values.get

    logger.info("====== Empty request response map ======")
    logger.info(responseMap.toString)
    // In this case because of empty keys, both attempts to compute derivation will fail
    val derivationExceptionTypes = Seq("derivation_fetch_exception", "derivation_rename_exception")
    assertEquals(joinConf.joinParts.size() + derivationExceptionTypes.size, responseMap.size)
    assertTrue(responseMap.keys.forall(_.endsWith("_exception")))
  }

  it should "test KVStore partial failure" in {
    val namespace = "test_kv_store_partial_failure"
    val joinConf = FetcherTestUtil.generateRandomData(namespace, tableUtils, spark, topic, today, yesterday, 5, 5)
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    val kvStoreFunc = () =>
      OnlineUtils.buildInMemoryKVStore("FetcherFailureTest#test_kv_store_partial_failure",
                                       hardFailureOnInvalidDataset = true)
    val inMemoryKvStore = kvStoreFunc()
    val mockApi = new MockApi(kvStoreFunc, namespace)

    val metadataStore = new MetadataStore(FetchContext(inMemoryKvStore))
    inMemoryKvStore.create(MetadataDataset)
    metadataStore.putJoinConf(joinConf)

    val keys = joinConf.leftKeyCols
    val keyData = spark.table(s"$namespace.queries_table").select(keys.map(col): _*).head
    val keyMap = keys.indices.map { idx =>
      keys(idx) -> keyData.get(idx).asInstanceOf[AnyRef]
    }.toMap

    val request = Request(joinConf.metaData.name, keyMap)
    val (responses, _) = FetcherTestUtil.joinResponses(spark, Array(request), mockApi)
    val responseMap = responses.head.values.get
    val exceptionKeys = joinConf.joinPartOps.map(jp => jp.columnPrefix + "exception")

    println(responseMap)
    exceptionKeys.foreach(k => assertTrue(responseMap.contains(k)))
  }

}
