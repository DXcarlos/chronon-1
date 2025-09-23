package ai.chronon.spark.udafs

import ai.chronon.spark.submission.SparkSessionBuilder
import ai.chronon.spark.udafs.ArrayStringHistogramAggregator
import ai.chronon.spark.udafs.HistogramAggregator
import ai.chronon.spark.udafs.MapHistogramAggregator
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udaf
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HistogramTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  val spark: SparkSession = SparkSessionBuilder.build("HistogramTest", local = true)

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Register UDAFs
    spark.udf.register("map_histogram", udaf(MapHistogramAggregator))
    spark.udf.register("string_histogram", udaf(HistogramAggregator))
    spark.udf.register("array_string_histogram", udaf(ArrayStringHistogramAggregator))

    // Create and register sample data with nulls
    val mapData = Seq(
      Row("group1", Map("a" -> 2L, "b" -> 3L)),
      Row("group1", Map("b" -> 1L, "c" -> 4L)),
      Row("group2", Map("a" -> 1L, "c" -> 2L)),
      Row("group2", null),
      Row("group3", null)
    )
    val mapSchema = StructType(
      Seq(
        StructField("group", StringType, nullable = false),
        StructField("data", MapType(StringType, LongType), nullable = true)
      ))
    val mapDF = spark.createDataFrame(spark.sparkContext.parallelize(mapData), mapSchema)
    mapDF.createOrReplaceTempView("map_data")

    val stringData = Seq(
      Row("group1", "a"),
      Row("group1", "b"),
      Row("group1", "a"),
      Row("group2", "b"),
      Row("group2", "c"),
      Row("group2", "c"),
      Row("group2", null),
      Row("group3", null)
    )
    val stringSchema = StructType(
      Seq(
        StructField("group", StringType, nullable = false),
        StructField("data", StringType, nullable = true)
      ))
    val stringDF = spark.createDataFrame(spark.sparkContext.parallelize(stringData), stringSchema)
    stringDF.createOrReplaceTempView("string_data")

    val arrayData = Seq(
      Row("group1", Seq("a", "b", "a")),
      Row("group1", Seq("b", "c", null)),
      Row("group1", null),
      Row("group2", Seq("a", "c", "c", null)),
      Row("group3", null)
    )
    val arraySchema = StructType(
      Seq(
        StructField("group", StringType, nullable = false),
        StructField("data", ArrayType(StringType), nullable = true)
      ))
    val arrayDF = spark.createDataFrame(spark.sparkContext.parallelize(arrayData), arraySchema)
    arrayDF.createOrReplaceTempView("array_data")
  }

  "MapHistogramAggregator" should "work correctly in SQL queries and handle nulls" in {
    val result = spark
      .sql("""
      SELECT group, map_histogram(data) as histogram
      FROM map_data
      GROUP BY group
      ORDER BY group
    """)
      .collect()

    result should have length 3
    result(0).getAs[String]("group") shouldBe "group1"
    result(0).getAs[Map[String, Long]]("histogram") shouldBe Map("a" -> 2L, "b" -> 4L, "c" -> 4L)
    result(1).getAs[String]("group") shouldBe "group2"
    result(1).getAs[Map[String, Long]]("histogram") shouldBe Map("a" -> 1L, "c" -> 2L)
    result(2).getAs[String]("group") shouldBe "group3"
    result(2).getAs[Map[String, Long]]("histogram") shouldBe null
  }

  "HistogramAggregator" should "work correctly in SQL queries and handle nulls" in {
    val result = spark
      .sql("""
      SELECT group, string_histogram(data) as histogram
      FROM string_data
      GROUP BY group
      ORDER BY group
    """)
      .collect()

    result should have length 3
    result(0).getAs[String]("group") shouldBe "group1"
    result(0).getAs[Map[String, Long]]("histogram") shouldBe Map("a" -> 2L, "b" -> 1L)
    result(1).getAs[String]("group") shouldBe "group2"
    result(1).getAs[Map[String, Long]]("histogram") shouldBe Map("b" -> 1L, "c" -> 2L, "NULL" -> 1)
    result(2).getAs[String]("group") shouldBe "group3"
    result(2).getAs[Map[String, Long]]("histogram") shouldBe Map("NULL" -> 1)
  }

  "ArrayStringHistogramAggregator" should "work correctly in SQL queries and handle nulls" in {
    val result = spark
      .sql("""
      SELECT group, array_string_histogram(data) as histogram
      FROM array_data
      GROUP BY group
      ORDER BY group
    """)
      .collect()

    result should have length 3
    result(0).getAs[String]("group") shouldBe "group1"
    result(0).getAs[Map[String, Long]]("histogram") shouldBe Map("a" -> 2L, "b" -> 2L, "c" -> 1L, "NULL" -> 1L)
    result(1).getAs[String]("group") shouldBe "group2"
    result(1).getAs[Map[String, Long]]("histogram") shouldBe Map("a" -> 1L, "c" -> 2L, "NULL" -> 1)
    result(2).getAs[String]("group") shouldBe "group3"
    result(2).getAs[Map[String, Long]]("histogram") shouldBe Map.empty[String, Long]
  }
}
