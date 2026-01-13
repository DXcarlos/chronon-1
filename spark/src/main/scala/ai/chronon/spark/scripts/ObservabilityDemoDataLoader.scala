package ai.chronon.spark.scripts

import ai.chronon.api.ColorPrinter.ColorString
import ai.chronon.api.Constants
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.online.HTTPKVStore
import ai.chronon.online.KVStore
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.stats.drift.Summarizer
import ai.chronon.spark.stats.drift.SummaryUploader
import ai.chronon.spark.stats.drift.scripts.PrepareData
import ai.chronon.spark.submission.SparkSessionBuilder
import ai.chronon.online.InMemoryKvStore
import ai.chronon.spark.utils.MockApi
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.ScallopOption
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object ObservabilityDemoDataLoader {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def time(message: String)(block: => Unit): Unit = {
    logger.info(s"$message..".yellow)
    val start = System.currentTimeMillis()
    block
    val end = System.currentTimeMillis()
    logger.info(s"$message took ${end - start} ms".green)
  }

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val startDs: ScallopOption[String] = opt[String](
      name = "start-ds",
      default = Some("2023-01-01"),
      descr = "Start date in YYYY-MM-DD format"
    )

    val endDs: ScallopOption[String] = opt[String](
      name = "end-ds",
      default = Some("2023-03-01"),
      descr = "End date in YYYY-MM-DD format"
    )

    val rowCount: ScallopOption[Int] = opt[Int](
      name = "row-count",
      default = Some(700000),
      descr = "Number of rows to generate"
    )

    val namespace: ScallopOption[String] = opt[String](
      name = "namespace",
      default = Some("observability_demo"),
      descr = "Namespace for the demo"
    )

    verify()
  }

  def main(args: Array[String]): Unit = {

    val config = new Conf(args)
    val startDs = config.startDs()
    val endDs = config.endDs()
    val rowCount = config.rowCount()
    val namespace = config.namespace()

    val spark = SparkSessionBuilder.build(namespace, local = true)
    implicit val tableUtils: TableUtils = TableUtils(spark)
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")

    // generate anomalous data (join output)
    val prepareData = PrepareData(namespace)
    val join = prepareData.generateAnomalousFraudJoin

    time("Preparing data") {
      val df = prepareData.generateFraudSampleData(rowCount, startDs, endDs, join.metaData.loggedTable)
      df.show(10, truncate = false)
    }

    // mock api impl for online fetching and uploading
    val inMemKvStoreFunc: () => KVStore = () => {
      // cannot reuse the variable - or serialization error
      val result = InMemoryKvStore.build(namespace)
      result
    }
    val inMemoryApi = new MockApi(inMemKvStoreFunc, namespace)

    time("Summarizing data") {
      // compute summary table and packed table (for uploading)
      Summarizer.compute(inMemoryApi, join.metaData, ds = endDs, useLogs = true)
    }

    val packedTable = join.metaData.packedSummaryTable

    // create necessary tables in kvstore - we now publish to the HTTP KV store as we need this available to the Hub
    val httpKvStoreFunc: () => KVStore = () => {
      // cannot reuse the variable - or serialization error
      val result = new HTTPKVStore()
      result
    }
    val hubApi = new MockApi(httpKvStoreFunc, namespace)

    val kvStore = hubApi.genKvStore
    kvStore.create(Constants.MetadataDataset)
    kvStore.create(Constants.TiledSummaryDataset)

    // upload join conf
    hubApi.buildFetcher().metadataStore.putJoinConf(join)

    time("Uploading summaries") {
      val uploader = new SummaryUploader(tableUtils.loadTable(packedTable), hubApi)
      uploader.run()
    }

    println("Done uploading summaries! \uD83E\uDD73".green)
    // clean up spark session and force jvm exit
    spark.stop()
    System.exit(0)
  }
}
