package ai.chronon.integrations.cloud_gcp

import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.GroupBy
import ai.chronon.api.MetaData
import ai.chronon.integrations.cloud_gcp.BigTableKVStore.ColumnFamilyQualifierString
import ai.chronon.integrations.cloud_gcp.BigTableKVStore.ColumnFamilyString
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.submission.SparkSessionBuilder
import com.google.cloud.spark.bigquery.v2.Spark35BigQueryTableProvider
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.udf
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.ScallopOption

/** This Spark app handles loading data via Spark's BigTable connector (https://github.com/GoogleCloudDataproc/spark-bigtable-connector) into BigTable.
  * At the moment this uses the DF support in the BT connector. A limitation with this connector is that it does not support
  * setting the timestamp on the individual cells we write out. For GroupBy Uploads, this is fine as we can set the timestamp
  * to that of the endDs + span. If we need to tweak this behavior, we'll need to reach for the RDD version of these connector classes (BigtableRDD.writeRDD).
  */
object Spark2BigTableLoader {

  class Conf(args: Seq[String]) extends ScallopConf(args) {

    val dataset: ScallopOption[String] = opt[String](
      name = "dataset",
      descr = "Name of the dataset (e.g. GroupBy) that we are uploading",
      required = true
    )

    val endDs: ScallopOption[String] = opt[String](
      name = "end-ds",
      descr = "End date in YYYY-MM-DD format",
      required = true
    )

    val tableName: ScallopOption[String] = opt[String](
      name = "table-name",
      descr = "Input table name to load into BigTable",
      required = true
    )

    val projectId: ScallopOption[String] = opt[String](
      name = "project-id",
      descr = "Google Cloud project ID",
      required = true
    )

    val instanceId: ScallopOption[String] = opt[String](
      name = "instance-id",
      descr = "BigTable instance ID",
      required = true
    )

    verify()
  }

  def main(args: Array[String]): Unit = {
    val config = new Conf(args)

    val tableName = config.tableName()
    val projectId = config.projectId()
    val instanceId = config.instanceId()
    val endDate = config.endDs()
    val dataset = config.dataset()

    // table to host the data
    val allGroupByBatchTable = "GROUPBY_BATCH"

    val catalog: String =
      s"""{
         |"table":{"name":"$allGroupByBatchTable"},
         |"rowkey":"key",
         |"columns":{
         |"data_rowkey":{"cf":"rowkey", "col":"key", "type":"binary"},
         |"value_bytes":{"cf":"$ColumnFamilyString", "col":"$ColumnFamilyQualifierString", "type":"binary"}
         |}
         |}""".stripMargin

    val spark = SparkSessionBuilder.build(s"Spark2BigTableLoader-${tableName}")
    val tableUtils: TableUtils = TableUtils(spark)

    // filter to only include data for the specified end date
    val partitionFilter = s"WHERE ds = '$endDate'"

    // we use the endDs + span to indicate the timestamp of all the cell data we upload for endDs
    // this is used in the KV store multiget calls
    val endDsPlusOne = tableUtils.partitionSpec.epochMillis(endDate) + tableUtils.partitionSpec.spanMillis

    // we need to sanitize and append the batch suffix to the groupBy name as that's
    // what we use to look things up while fetching
    val groupBy = new GroupBy().setMetaData(new MetaData().setName(dataset))
    val datasetName = groupBy.batchDataset

    // row key is: dataset#key for batch IRs & GroupByServingInfo
    val buildRowKeyUDF = udf((keyBytes: Array[Byte], dataset: String) => {
      BigTableKVStore.buildRowKey(keyBytes, dataset)
    })

    // Use the BQ Spark connector to read the table directly — Spark SQL cannot resolve
    // fully-qualified BQ table names (project.dataset.table) without the connector catalog.
    val bqFormat = classOf[Spark35BigQueryTableProvider].getName
    val rawDf = spark.read
      .format(bqFormat)
      .option("filter", s"ds = '$endDate'")
      .load(tableName)

    val dataDf = rawDf
      .select(
        functions.col("key_bytes"),
        functions.col("value_bytes"),
        functions.lit(datasetName).as("dataset")
      )
    val finalDataDf =
      dataDf
        .withColumn("data_rowkey", buildRowKeyUDF(functions.col("key_bytes"), functions.col("dataset")))
        .drop("key_bytes", "dataset") // BT connector seems to not handle extra columns well

    finalDataDf.write
      .format("bigtable")
      .option("catalog", catalog)
      .option("spark.bigtable.project.id", projectId)
      .option("spark.bigtable.instance.id", instanceId)
      .option("spark.bigtable.create.new.table", false.toString) // we expect the table to be created already
      .option("spark.bigtable.write.timestamp.milliseconds",
              endDsPlusOne
      ) // the BT ingest sets timestamp of all cells to this
      .save()
  }
}
