package ai.chronon.spark.fetcher

import ai.chronon.api.Constants.MetadataDataset
import ai.chronon.api.Extensions.{JoinOps, MetadataOps}
import ai.chronon.api.ScalaJavaConversions.IterableOps
import ai.chronon.online.KVStore.GetRequest
import ai.chronon.online.fetcher.FetchContext
import ai.chronon.online.{MetadataDirWalker, MetadataEndPoint, fetcher}
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.submission
import ai.chronon.spark.utils.OnlineUtils
// Removed Bazel runfiles import - using standard resource loading instead
import org.apache.spark.sql.SparkSession
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.scalatest.flatspec.AnyFlatSpec
import ai.chronon.spark.Extensions._
import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source

class FetcherMetadataTest extends AnyFlatSpec {

  val sessionName = "FetcherMetadataTest"
  val spark: SparkSession = submission.SparkSessionBuilder.build(sessionName, local = true)

  it should "test metadata store" in {
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    implicit val tableUtils: TableUtils = TableUtils(spark)

    val joinPath = "joins/team/example_join.v1"
    val confResource = getClass.getClassLoader.getResource(s"$joinPath")
    val src = Source.fromResource(joinPath)
    println(s"conf resource path for dir walker: ${confResource.getPath}")

    // Use standard resource loading instead of Bazel runfiles  
    val runFilesResource = getClass.getClassLoader.getResource("joins").getPath.replace("/joins", "")

    val expected = {
      try src.mkString
      finally src.close()
    }.replaceAll("\\s+", "")

    val acceptedEndPoints = List(MetadataEndPoint.ConfByKeyEndPointName)
    val inMemoryKvStore = OnlineUtils.buildInMemoryKVStore("FetcherTest")
    val singleFileDataSet = MetadataDataset + "_single_file_test"
    val singleFileMetadataStore = new fetcher.MetadataStore(FetchContext(inMemoryKvStore, singleFileDataSet))
    inMemoryKvStore.create(singleFileDataSet)
    // set the working directory to /chronon instead of $MODULE_DIR in configuration if Intellij fails testing
    val singleFileDirWalker = new MetadataDirWalker(runFilesResource, acceptedEndPoints)
    val singleFileKvMap = singleFileDirWalker.run
    val singleFilePut: Seq[Future[scala.collection.Seq[Boolean]]] = singleFileKvMap.toSeq.map { case (_, kvMap) =>
      singleFileMetadataStore.put(kvMap, singleFileDataSet)
    }
    singleFilePut.flatMap(putRequests => Await.result(putRequests, Duration.Inf))

    val joinKeyName = "joins/team.example_join.v1"

    val response = inMemoryKvStore.get(GetRequest(joinKeyName.getBytes(), singleFileDataSet))
    val res = Await.result(response, Duration.Inf)
    assertTrue(res.latest.isSuccess)
    val actual = new String(res.values.get.head.bytes)
    assertEquals(expected, actual.replaceAll("\\s+", ""))

    val directoryDataSetDataSet = MetadataDataset + "_directory_test"
    val directoryMetadataStore =
      new fetcher.MetadataStore(FetchContext(inMemoryKvStore, directoryDataSetDataSet))
    inMemoryKvStore.create(directoryDataSetDataSet)
    val directoryDataDirWalker =
      new MetadataDirWalker(runFilesResource, acceptedEndPoints)
    val directoryDataKvMap = directoryDataDirWalker.run
    val directoryPut = directoryDataKvMap.toSeq.map { case (_, kvMap) =>
      directoryMetadataStore.put(kvMap, directoryDataSetDataSet)
    }
    directoryPut.flatMap(putRequests => Await.result(putRequests, Duration.Inf))
    val dirResponse =
      inMemoryKvStore.get(GetRequest(joinKeyName.getBytes, directoryDataSetDataSet))
    val dirRes = Await.result(dirResponse, Duration.Inf)
    assertTrue(dirRes.latest.isSuccess)
    val dirActual = new String(dirRes.values.get.head.bytes)
    assertEquals(expected, dirActual.replaceAll("\\s+", ""))

    val emptyResponse =
      inMemoryKvStore.get(GetRequest("NoneExistKey".getBytes(), "NonExistDataSetName"))
    val emptyRes = Await.result(emptyResponse, Duration.Inf)
    assertFalse(emptyRes.latest.isSuccess)
  }

  it should "test join value infos" in {
    val namespace = "join_schema"
    val tableUtils: TableUtils = TableUtils(spark)
    val joinConf = FetcherTestUtil.generateEventOnlyData(namespace, tableUtils, spark)
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("FetcherTest")
    val inMemoryKvStore = kvStoreFunc()

    val endDs = "2021-04-10"
    val joinedDf = new ai.chronon.spark.Join(joinConf, endDs, tableUtils).computeJoin()
    val joinTable = s"$namespace.join_test_expected_${joinConf.metaData.cleanName}"
    joinedDf.save(joinTable)

    joinConf.joinParts.toScala.foreach(jp =>
      OnlineUtils.serve(tableUtils,
                        inMemoryKvStore,
                        kvStoreFunc,
                        namespace,
                        endDs,
                        jp.groupBy,
                        dropDsOnWrite = true,
                        tilingEnabled = true))

    val keys = joinConf.leftKeyCols
    val metadataStore = new fetcher.MetadataStore(FetchContext(inMemoryKvStore))
    inMemoryKvStore.create(MetadataDataset)
    metadataStore.putJoinConf(joinConf)

    val joinCodec = metadataStore.buildJoinCodec(joinConf, false)
    val expectedValueInfoKeys = joinCodec.valueInfos.flatMap(v => v.leftKeys).toSet
    // sanity check we are not missing any keys
    assertEquals(keys.toSet, expectedValueInfoKeys)
    // confirm the value columns match value schema
    val valueFields = joinCodec.valueSchema.fields.map(_.name).toSet
    val valueInfoFields = joinCodec.valueInfos.map(v => v.fullName).toSet
    assertEquals(valueFields, valueInfoFields)
  }
}
