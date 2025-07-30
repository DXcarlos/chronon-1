package ai.chronon.spark.test.udafs

import ai.chronon.spark.submission.SparkSessionBuilder
import ai.chronon.spark.udafs.ApproxDistinct
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ApproxDistinctTest extends AnyFlatSpec with Matchers {

  val spark: SparkSession = SparkSessionBuilder.build("ApproxDistinctTest", local = true)
  import spark.implicits._

  "ApproxDistinct.columnCardinality" should "correctly estimate cardinality for various column types" in {
    // Create a sample DataFrame with different column types
    val df: DataFrame = Seq(
      (1, "A", 1.0, Seq(1, 2, 3), Map("key1" -> "value1")),
      (2, "B", 2.0, Seq(2, 3, 4), Map("key2" -> "value2")),
      (3, "C", 3.0, Seq(3, 4, 5), Map("key3" -> "value3")),
      (1, "A", 1.0, Seq(1, 2, 3), Map("key1" -> "value1")),
      (4, "D", 4.0, Seq(4, 5, 6), Map("key4" -> "value4"))
    ).toDF("int_col", "string_col", "double_col", "array_col", "map_col")

    val result = ApproxDistinct.columnCardinality(df)

    // Check if all columns are present in the result
    result.keySet should contain allOf ("int_col", "string_col", "double_col", "array_col", "map_col")

    // Check if the cardinality estimates are reasonable
    result("int_col") should be(4L +- 1L) // Exact: 4
    result("string_col") should be(4L +- 1L) // Exact: 4
    result("double_col") should be(4L +- 1L) // Exact: 4
    result("array_col") should be(6L +- 1L) // Exact: 6 (unique elements in all arrays)
    result("map_col") should be(4L +- 1L) // Exact: 4 (unique values in all maps)
  }

  it should "handle null values correctly" in {
    val schema = types.StructType(
      Seq(
        types.StructField("int_col", types.IntegerType, nullable = true),
        types.StructField("string_col", types.StringType, nullable = true),
        types.StructField("double_col", types.DoubleType, nullable = true)
      ))

    val data = Seq(
      Row(1, "A", null),
      Row(2, null, 2.0),
      Row(null, "C", 3.0),
      Row(1, "A", null),
      Row(4, "D", 4.0)
    )

    val rdd = spark.sparkContext.parallelize(data)
    val df = spark.createDataFrame(rdd, schema)
    val result = ApproxDistinct.columnCardinality(df)

    result("int_col") should be(3L +- 1L) // Exact: 3 (null is not counted)
    result("string_col") should be(3L +- 1L) // Exact: 3 (null is not counted)
    result("double_col") should be(3L +- 1L) // Exact: 3 (null is not counted)
  }

  it should "handle empty DataFrame" in {
    val emptyDf: DataFrame = Seq.empty[(Int, String)].toDF("int_col", "string_col")
    val result = ApproxDistinct.columnCardinality(emptyDf)

    result.size should be(2)
    result("int_col") should be(0L)
    result("string_col") should be(0L)
  }
}
