package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark.catalog.Iceberg
import ai.chronon.spark.utils.SparkTestBase
import org.junit.Assert.assertEquals
import org.scalatestplus.mockito.MockitoSugar

class GcpFormatProviderTest extends SparkTestBase with MockitoSugar {

  override def sparkConfs: Map[String, String] = Map(
    "spark.chronon.table.format_provider.class" -> classOf[GcpFormatProvider].getName
  )

  it should "fall back to BigQueryNative when table is not found in any known format" in {
    val gcpFormatProvider = new GcpFormatProvider(spark)
    val result = gcpFormatProvider.readFormat("nonexistent_table")
    assertEquals(Some(BigQueryNative), result)
  }

  it should "return Iceberg for tables that exist as iceberg" in {
    val gcpFormatProvider = new GcpFormatProvider(spark)
    spark.sql("CREATE TABLE iceberg_format_test (id INT, ds STRING) USING iceberg PARTITIONED BY (ds)")
    try {
      val result = gcpFormatProvider.readFormat("iceberg_format_test")
      assertEquals(Some(Iceberg), result)
    } finally {
      spark.sql("DROP TABLE IF EXISTS iceberg_format_test")
    }
  }
}
