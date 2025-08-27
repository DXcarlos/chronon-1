package ai.chronon.integrations.cloud_gcp

import ai.chronon.api.PartitionRange
import ai.chronon.spark.catalog.{FormatProvider, Iceberg, TableUtils}
import ai.chronon.spark.submission.SparkSessionBuilder
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.google.cloud.spark.bigquery.SparkBigQueryUtil
import org.apache.iceberg.gcp.bigquery.{BigQueryMetastoreCatalog => BQMSCatalog}
import org.apache.iceberg.gcp.gcs.GCSFileIO
import org.apache.iceberg.io.ResolvingFileIO
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.internal.SQLConf
import org.junit.Assert.{assertEquals, assertNotNull, assertTrue}
import org.objenesis.strategy.StdInstantiatorStrategy
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import scala.collection.JavaConverters._

class BigQueryCatalogTest extends AnyFlatSpec with MockitoSugar {

  lazy val spark: SparkSession = SparkSessionBuilder.build(
    "BigQuerySparkTest",
    local = true,
    additionalConfig = Some(
      Map(
        "spark.chronon.table.format_provider.class" -> classOf[GcpFormatProvider].getName,
        "hive.metastore.uris" -> "thrift://localhost:9083",
        "spark.chronon.partition.column" -> "ds",
        "spark.hadoop.fs.gs.impl" -> "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        "spark.hadoop.fs.AbstractFileSystem.gs.impl" -> "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        "spark.sql.catalogImplementation" -> "in-memory"

//        Uncomment to test
//        "spark.sql.defaultCatalog" -> "default_iceberg",
//        "spark.sql.catalog.default_iceberg" -> classOf[DelegatingBigQueryMetastoreCatalog].getName,
//        "spark.sql.catalog.default_iceberg.catalog-impl" -> classOf[BQMSCatalog].getName,
//        "spark.sql.catalog.default_iceberg.io-impl" -> classOf[ResolvingFileIO].getName,
//        "spark.sql.catalog.default_iceberg.warehouse" -> "gs://zipline-warehouse-canary/data/tables/",
//        "spark.sql.catalog.default_iceberg.gcp_location" -> "us-central1",
//        "spark.sql.catalog.default_iceberg.gcp_project" -> "canary-443022",
//        "spark.kryo.registrator" -> classOf[ChrononIcebergKryoRegistrator].getName,
//        "spark.sql.defaultUrlStreamHandlerFactory.enabled" -> false.toString,
//
//        "spark.sql.catalog.default_bigquery" -> classOf[BigQueryCatalog].getName,
      ))
  )
  lazy val tableUtils: TableUtils = TableUtils(spark)

  it should "works with views" ignore {
    val viewName = "data.purchases_native_view"
    val nativeName = "data.purchases"

    val viewParts =
      tableUtils.partitions(viewName,
                            partitionRange =
                              Option(PartitionRange("2023-11-01", "2023-11-30")(tableUtils.partitionSpec)))
    assertEquals(30, viewParts.size)
    val nativeParts =
      tableUtils.partitions(nativeName,
                            partitionRange =
                              Option(PartitionRange("2023-11-01", "2023-11-30")(tableUtils.partitionSpec)))
    assertEquals(30, nativeParts.size)

    assertEquals(nativeParts.toSet, viewParts.toSet)

  }

  it should "works with a partition range for views and tables" ignore {
    val viewName = "data.purchases_native_view"
    val nativeName = "data.purchases"
    val viewTruncated =
      tableUtils.partitions(viewName,
                            partitionRange =
                              Option(PartitionRange("2023-11-28", "2023-11-30")(tableUtils.partitionSpec)))
    assertEquals(3, viewTruncated.size)
    val nativeTruncated =
      tableUtils.partitions(nativeName,
                            partitionRange =
                              Option(PartitionRange("2023-11-28", "2023-11-30")(tableUtils.partitionSpec)))
    assertEquals(3, nativeTruncated.size)

    assertEquals(nativeTruncated.toSet, viewTruncated.toSet)

  }

  it should "verify dynamic classloading of GCP providers" in {
    assertEquals("thrift://localhost:9083", spark.sqlContext.getConf("hive.metastore.uris"))
    assertTrue(FormatProvider.from(spark).isInstanceOf[GcpFormatProvider])
  }

  it should "be consistent about parsing table names for spark and bigquery" in {
    val sparkTable = "`project-id`.dataset.table_name"

    val bTableId = SparkBQUtils.toTableId(sparkTable)(spark)

    assertEquals("table_name", bTableId.getTable)
    assertEquals("dataset", bTableId.getDataset)
    assertEquals("project-id", bTableId.getProject)

    val invalidSparkTableName = "project-id.dataset.table_name"
    assertThrows[ParseException] {
      val notReachable = spark.sessionState.sqlParser.parseMultipartIdentifier(invalidSparkTableName)
    }
  }

  it should "bigquery connector converts spark dates regardless of date setting" in {
    val input = spark.createDataFrame(Seq((1, "2021-01-01"))).toDF("id", "ds")
    spark.conf.set(SQLConf.DATETIME_JAVA8API_ENABLED.key, true)
    val java8Date = input.select(col("id"), to_date(col("ds"))).collect.take(1).head.get(1)
    assert(java8Date.isInstanceOf[java.time.LocalDate])
    SparkBigQueryUtil.sparkDateToBigQuery(java8Date)

    spark.conf.set(SQLConf.DATETIME_JAVA8API_ENABLED.key, false)
    val nonJava8Date = input.select(col("id"), to_date(col("ds"))).collect.take(1).head.get(1)
    assert(nonJava8Date.isInstanceOf[java.sql.Date])
    SparkBigQueryUtil.sparkDateToBigQuery(nonJava8Date)
  }

  it should "bigquery connector converts spark timestamp regardless of setting" in {
    val input = spark.createDataFrame(Seq((1, "2025-04-28 12:30:45"))).toDF("id", "ts")
    spark.conf.set(SQLConf.DATETIME_JAVA8API_ENABLED.key, true)
    val java8Timestamp = input.select(col("id"), col("ts").cast("timestamp")).collect.take(1).head.get(1)
    assert(java8Timestamp.isInstanceOf[java.time.Instant])
    SparkBigQueryUtil.sparkTimestampToBigQuery(java8Timestamp)

    spark.conf.set(SQLConf.DATETIME_JAVA8API_ENABLED.key, false)
    val nonJava8Timestamp = input.select(col("id"), col("ts").cast("timestamp")).collect.take(1).head.get(1)
    assert(nonJava8Timestamp.isInstanceOf[java.sql.Timestamp])
    SparkBigQueryUtil.sparkTimestampToBigQuery(nonJava8Timestamp)
  }

  it should "integration testing bigquery native table" ignore {
    val nativeTable = "data.checkouts"
    val table = tableUtils.loadTable(nativeTable)
    table.show
    // val database = tableUtils.createDatabase("test_database")
    val allParts = tableUtils.partitions(nativeTable)
    println(allParts)
  }

  it should "integration testing bigquery external table" ignore {
    val externalTable = "default_iceberg.data.checkouts_parquet"

    val table = tableUtils.loadTable(externalTable)
    table.show
    // val database = tableUtils.createDatabase("test_database")
    val allParts = tableUtils.partitions(externalTable)
    println(allParts)
  }

  it should "integration testing bigquery partition pushdown" ignore {
    import spark.implicits._
    val iceberg = "data.checkouts_native"

    val singleFilter = tableUtils.loadTable(iceberg, List("ds = '2023-11-30'"))
    val multiFilter = tableUtils.loadTable(iceberg, List("ds = '2023-11-30'", "ds = '2023-11-30'"))
    assertEquals(singleFilter.select("user_id", "ds").as[(String, String)].collect.toList,
                 multiFilter.select("user_id", "ds").as[(String, String)].collect.toList)
  }

  it should "integration testing formats" ignore {
    val externalTable = "default_iceberg.data.checkouts_parquet"
    val externalFormat = FormatProvider.from(spark).readFormat(externalTable)
    assertEquals(Some(BigQueryExternal), externalFormat)

    val externalTableNoCat = "data.checkouts_parquet"
    val externalFormatNoCat = FormatProvider.from(spark).readFormat(externalTableNoCat)
    assertEquals(Some(BigQueryExternal), externalFormatNoCat)

    val nativeTable = "default_iceberg.data.checkouts_native"
    val nativeFormat = FormatProvider.from(spark).readFormat(nativeTable)
    assertEquals(Some(BigQueryNative), nativeFormat)

    val nativeTableNoCat = "data.checkouts_native"
    val nativeFormatNoCat = FormatProvider.from(spark).readFormat(nativeTableNoCat)
    assertEquals(Some(BigQueryNative), nativeFormatNoCat)

    val icebergTable = "default_iceberg.data.quickstart_purchases_davidhan_v1_dev_davidhan"
    val icebergFormat = FormatProvider.from(spark).readFormat(icebergTable)
    assertEquals(Some(Iceberg), icebergFormat)

    val icebergTableNoCat = "data.quickstart_purchases_davidhan_v1_dev_davidhan"
    val icebergFormatNoCat = FormatProvider.from(spark).readFormat(icebergTableNoCat)
    assertEquals(Some(Iceberg), icebergFormatNoCat)

    val parts = icebergFormat.get.primaryPartitions(icebergTable, "ds", "")(spark)
    val partsNoCat = icebergFormat.get.primaryPartitions(icebergTableNoCat, "ds", "")(spark)
    assertEquals(parts, partsNoCat)

    val dneTable = "default_iceberg.data.dne"
    val dneFormat = FormatProvider.from(spark).readFormat(dneTable)
    assertTrue(dneFormat.isEmpty)
  }

  it should "integration testing bigquery partitions" ignore {
    // TODO(tchow): This test is ignored because it requires a running instance of the bigquery. Need to figure out stubbing locally.
    // to run, set `GOOGLE_APPLICATION_CREDENTIALS=<path_to_application_default_credentials.json>
    val externalPartitions = tableUtils.partitions("data.checkouts_parquet_partitioned")
    assertEquals(Seq("2023-11-30"), externalPartitions)
    val nativePartitions = tableUtils.partitions("data.purchases")
    assertEquals(
      Set(20231118, 20231122, 20231125, 20231102, 20231123, 20231119, 20231130, 20231101, 20231117, 20231110, 20231108,
          20231112, 20231115, 20231116, 20231113, 20231104, 20231103, 20231106, 20231121, 20231124, 20231128, 20231109,
          20231127, 20231129, 20231126, 20231114, 20231107, 20231111, 20231120, 20231105).map(_.toString),
      nativePartitions.toSet
    )

    val df = tableUtils.loadTable("`canary-443022.data`.purchases")
    df.show

    tableUtils.insertPartitions(df,
                                "data.tchow_test_iceberg",
                                Map("file_format" -> "PARQUET", "table_type" -> "iceberg"),
                                List("ds"))

    val icebergCols = spark.catalog.listColumns("data.tchow_test_iceberg")
    val externalCols = spark.catalog.listColumns("data.checkouts_parquet_partitioned")
    val nativeCols = spark.catalog.listColumns("data.purchases")

    val icebergPartitions = spark.sql("SELECT * FROM data.tchow_test_iceberg.partitions")

    val sqlDf = tableUtils.sql(s"""
        |SELECT ds FROM data.checkouts_parquet_partitioned -- external parquet
        |UNION ALL
        |SELECT ds FROM data.purchases -- bigquery native
        |UNION ALL
        |SELECT ds FROM data.tchow_test_iceberg -- external iceberg
        |""".stripMargin)
    sqlDf.show

  }

  it should "kryo serialization for ResolvingFileIO" in {
    val registrator = new ChrononIcebergKryoRegistrator()
    val kryo = new Kryo();
    kryo.setReferences(true);
    registrator.registerClasses(kryo)

    // Create an instance of ResolvingFileIO
    val original = new ResolvingFileIO();
    original.initialize(Map.empty[String, String].asJava)

    // Serialize the object
    val outputStream = new ByteArrayOutputStream();
    val output = new Output(outputStream);
    kryo.writeClassAndObject(output, original);
    output.close();

    // Deserialize the object
    val inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    val input = new Input(inputStream);
    val deserializedObj = kryo.readClassAndObject(input);
    input.close();

    assertNotNull("Deserialized object should not be null", deserializedObj);
    assertTrue("Deserialized object should be an instance of ResolvingFileIO",
               deserializedObj.isInstanceOf[ResolvingFileIO]);
  }

  it should "kryo serialization for GCSFileIO" in {
    val registrator = new ChrononIcebergKryoRegistrator()
    val kryo = new Kryo();
    kryo.setReferences(true);
    kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy))
    registrator.registerClasses(kryo)

    // Create an instance of GCSFileIO
    val original = new GCSFileIO();
    original.initialize(Map("k1" -> "v1").asJava)

    // Serialize the object
    val outputStream = new ByteArrayOutputStream();
    val output = new Output(outputStream);
    kryo.writeClassAndObject(output, original);
    output.close();

    // Deserialize the object
    val inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    val input = new Input(inputStream);
    val deserializedObj = kryo.readClassAndObject(input);
    input.close();

    assertNotNull("Deserialized object should not be null", deserializedObj);
    assertTrue("Deserialized object should be an instance of GCSFileIO", deserializedObj.isInstanceOf[GCSFileIO]);
    assertEquals(original.properties(), deserializedObj.asInstanceOf[GCSFileIO].properties())
  }
}
