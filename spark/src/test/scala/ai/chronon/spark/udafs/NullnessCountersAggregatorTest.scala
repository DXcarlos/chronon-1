package ai.chronon.spark.udafs

import ai.chronon.spark.submission.SparkSessionBuilder
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class NullnessCountersAggregatorTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  val spark: SparkSession = SparkSessionBuilder.build("NullnessCountersAggregatorTest", local = true)

  override def beforeAll(): Unit = {
    super.beforeAll()

    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("string_array", ArrayType(StringType, containsNull = true), nullable = true)
      ))

    val data = Seq(
      Row(1, Array("a", null, "c", null)),
      Row(2, Array(null, "b", "c", "d")),
      Row(3, null),
      Row(4, Array("e", "f", null, "h"))
    )

    val testData: DataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )
    testData.createOrReplaceTempView("test_data")
  }

  "ContainerNullCountAggregator" should "handle null inputs in array of strings, counting nulls efficiently" in {
    val innerDf = spark.sql(
      """
        |SELECT
        |   id,
        |   aggregate(
        |     string_array,
        |     0L,
        |     (acc, x) -> CASE WHEN x IS NULL THEN acc + 1 ELSE acc END
        |   ) AS nulls,
        |   IF(isNull(string_array), NULL, size(string_array)) AS size
        |   FROM test_data
        |""".stripMargin
    )
    innerDf.show()
    val resultDf =
      spark.sql("""
      WITH array_counts AS (
        SELECT
          id,
          aggregate(
              string_array,
              0L,
              (acc, x) -> CASE WHEN x IS NULL THEN acc + 1 ELSE acc END
          ) AS nulls,
          IF(isNull(string_array), NULL, size(string_array)) AS size
        FROM test_data
      )
      SELECT sum(nulls) as nulls, sum(size), approx_percentile(size, array(0.5, 0.95), 10000) as total
      FROM array_counts
    """)

    println("Result Schema:")
    resultDf.show()
    resultDf.printSchema()

    val result = resultDf.collect().head
    result.getLong(0) shouldBe 4 // Total nulls
    result.getLong(1) shouldBe 12 // Total size (including nulls)
  }
}
