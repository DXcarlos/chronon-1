package ai.chronon.integrations.aws

import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.submission.ChrononHudiKryoRegistrator
import ai.chronon.spark.submission.SparkSessionBuilder
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

class GlueCatalogTest extends AnyFlatSpec with MockitoSugar {

  lazy val spark: SparkSession = SparkSessionBuilder.build(
    classOf[GlueCatalogTest].getSimpleName,
    local = true,
    additionalConfig = Some(
      Map(
        "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        "spark.sql.extensions" -> "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
        "spark.kryo.registrator" -> classOf[ChrononHudiKryoRegistrator].getName,
        // Disable timeline server-based markers to avoid connection issues in tests
        "hoodie.write.markers.type" -> "DIRECT",
        "hoodie.embed.timeline.server" -> "false"
      ))
  )
  lazy val tableUtils: TableUtils = TableUtils(spark)

  "basic round trip hudi table" should "work with local metastore" in {
    import spark.implicits._

    val input = Set(1, 2, 3, 4)
    val sourceDF = spark.sparkContext.parallelize(input.toSeq).toDF("id")

    sourceDF.write
      .format("hudi")
      .mode(SaveMode.Overwrite)
      .saveAsTable("test_hudi_table")

    val back = spark.table("test_hudi_table").select("id").as[Int].collect()
    assertEquals(input, back.toSet)

  }
}
