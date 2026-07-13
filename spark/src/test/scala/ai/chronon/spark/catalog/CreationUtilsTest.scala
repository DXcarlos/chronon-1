package ai.chronon.spark.catalog

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CreationUtilsTest extends AnyFlatSpec with Matchers {

  private val schema = StructType(
    Seq(
      StructField("user_id", StringType),
      StructField("amount", IntegerType),
      StructField("ds", StringType)
    ))

  "createTableSql" should "generate PARTITIONED BY when clusterByColumns is empty" in {
    val sql = CreationUtils.createTableSql("db.table", schema, List("ds"), Map.empty, "delta")
    sql should include("PARTITIONED BY")
    sql should not include "CLUSTER BY"
    // partition column excluded from the main column list, only appears in PARTITIONED BY
    sql should include("ds string")
  }

  it should "generate CLUSTER BY when clusterByColumns is non-empty" in {
    val sql = CreationUtils.createTableSql("db.table", schema, List("ds"), Map.empty, "delta", List("ds", "user_id"))
    sql should include("CLUSTER BY (\n    ds,\n    user_id\n)")
    sql should not include "PARTITIONED BY"
  }

  it should "keep the clustering column as a regular data column, not extract it" in {
    val sql = CreationUtils.createTableSql("db.table", schema, List("ds"), Map.empty, "delta", List("ds"))
    // ds appears in the main column DDL (schema.toDDL), unlike the partitioned path which
    // excludes partition columns from the main column list.
    sql should include("ds STRING")
  }

  it should "fall back to PARTITIONED BY when clusterByColumns is null" in {
    val sql = CreationUtils.createTableSql("db.table", schema, List("ds"), Map.empty, "delta", null)
    sql should include("PARTITIONED BY")
  }
}
