package ai.chronon.spark.catalog

import ai.chronon.spark.utils.SparkTestBase
import org.scalatest.matchers.should.Matchers

class IcebergTest extends SparkTestBase with Matchers {

  "Iceberg.tableProperties" should "contain expected commit retry and format properties" in {
    val props = Iceberg.tableProperties
    props("commit.retry.num-retries") shouldBe "20"
    props("commit.retry.min-wait-ms") shouldBe "10000"
    props("commit.retry.max-wait-ms") shouldBe "600000"
    props("commit.status-check.num-retries") shouldBe "20"
    props("commit.status-check.min-wait-ms") shouldBe "10000"
    props("commit.status-check.max-wait-ms") shouldBe "600000"
    props("write.merge.isolation-level") shouldBe "snapshot"
    props("format-version") shouldBe "2"
  }

  "Iceberg.supportSubPartitionsFilter" should "return false" in {
    Iceberg.supportSubPartitionsFilter shouldBe false
  }

  "qualifyWithCatalog" should "qualify a single-part name with catalog and current namespace" in {
    val result = Iceberg.qualifyWithCatalog("my_table")
    result shouldBe "`spark_catalog`.`default`.`my_table`"
  }

  it should "qualify a two-part name with the default catalog" in {
    val result = Iceberg.qualifyWithCatalog("my_ns.my_table")
    result shouldBe "`spark_catalog`.`my_ns`.`my_table`"
  }

  it should "re-quote a fully qualified three-part name" in {
    val result = Iceberg.qualifyWithCatalog("spark_catalog.my_ns.my_table")
    result shouldBe "`spark_catalog`.`my_ns`.`my_table`"
  }

  it should "properly quote identifiers with special characters" in {
    val result = Iceberg.qualifyWithCatalog("`my-ns`.`my-table`")
    result shouldBe "`spark_catalog`.`my-ns`.`my-table`"
  }

  it should "properly quote SQL reserved words" in {
    val result = Iceberg.qualifyWithCatalog("`select`.`table`")
    result shouldBe "`spark_catalog`.`select`.`table`"
  }

  it should "properly quote a three-part name with special characters" in {
    val result = Iceberg.qualifyWithCatalog("`my-catalog`.`my-ns`.`my-table`")
    result shouldBe "`my-catalog`.`my-ns`.`my-table`"
  }

  it should "handle identifiers containing backticks" in {
    // Spark parses ``a`b`` as the identifier a`b
    val result = Iceberg.qualifyWithCatalog("`a``b`.`c``d`")
    result shouldBe "`spark_catalog`.`a``b`.`c``d`"
  }

  "Iceberg.partitions" should "return partition values for an Iceberg table" in {
    val tableName = "default.iceberg_partitions_test"

    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        id INT,
        name STRING,
        ds STRING
      ) USING iceberg
      PARTITIONED BY (ds)
    """)

    spark.sql(s"""
      INSERT INTO $tableName VALUES
      (1, 'a', '2024-01-01'),
      (2, 'b', '2024-01-02'),
      (3, 'c', '2024-01-01')
    """)

    val parts = Iceberg.partitions(tableName, "")
    parts should contain theSameElementsAs List(
      Map("ds" -> "2024-01-01"),
      Map("ds" -> "2024-01-02")
    )
  }

  "Iceberg.primaryPartitions" should "return primary partition values" in {
    val tableName = "default.iceberg_primary_partitions_test"

    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        id INT,
        value DOUBLE,
        ds STRING
      ) USING iceberg
      PARTITIONED BY (ds)
    """)

    spark.sql(s"""
      INSERT INTO $tableName VALUES
      (1, 1.0, '2024-02-01'),
      (2, 2.0, '2024-02-02'),
      (3, 3.0, '2024-02-03')
    """)

    val parts = Iceberg.primaryPartitions(tableName, "ds", "")
    parts should contain theSameElementsAs List("2024-02-01", "2024-02-02", "2024-02-03")
  }

  it should "throw NotImplementedError when subPartitionsFilter is non-empty" in {
    a[NotImplementedError] should be thrownBy {
      Iceberg.primaryPartitions("any_table", "ds", "", subPartitionsFilter = Map("hr" -> "12"))
    }
  }

  it should "work with a single-part table name" in {
    val tableName = "iceberg_single_part_name_test"

    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        id INT,
        ds STRING
      ) USING iceberg
      PARTITIONED BY (ds)
    """)

    spark.sql(s"""INSERT INTO $tableName VALUES (1, '2024-03-01')""")

    val parts = Iceberg.primaryPartitions(tableName, "ds", "")
    parts shouldBe List("2024-03-01")
  }
}
