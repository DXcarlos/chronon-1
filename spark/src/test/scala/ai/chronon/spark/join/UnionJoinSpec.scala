package ai.chronon.spark.join

import ai.chronon.spark.join.UnionJoin
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers

import scala.collection.{Seq, immutable}

class UnionJoinSpec extends BaseJoinTest with Matchers {

  import spark.implicits._

  "UnionJoin" should "join two dataframes with different schemas" in {
    // Create test data
    val leftDF = Seq(
      (1, "A", 100, 1000),
      (1, "B", 200, 2000),
      (2, "C", 300, 3000)
    ).toSeq.toDF("id", "name", "value", "timestamp")

    val rightDF = Seq(
      (1, "X", 50.5, 1500),
      (1, "Y", 60.5, 2500),
      (3, "Z", 70.5, 3500)
    ).toSeq.toDF("user_id", "category", "score", "event_time")

    // Define key mapping
    val keyMapping = Map("id" -> "user_id")

    // Call the unionJoin function
    val joinedDF = UnionJoin
      .unionJoin(
        leftDF,
        rightDF,
        keyMapping,
        "timestamp",
        "event_time"
      )
      .df

    // Check result schema
    joinedDF.schema.fields.length shouldBe 3
    joinedDF.schema.fields(0).name shouldBe "id"
    joinedDF.schema.fields(1).name shouldBe "left_data_array"
    joinedDF.schema.fields(2).name shouldBe "right_data_array"

    // Check that left_data_array and right_data_array are array types of structs
    joinedDF.schema.fields(1).dataType.isInstanceOf[ArrayType] shouldBe true
    joinedDF.schema.fields(2).dataType.isInstanceOf[ArrayType] shouldBe true

    // Convert to rows for easier testing
    val results = joinedDF.collect()

    // Should have 3 rows (ids 1, 2, 3)
    results.length shouldBe 3

    // Check id = 1 (should have data from both left and right)
    val row1 = results.find(_.getInt(0) == 1).get
    val leftDataArray1 = row1.getAs[Seq[Row]](1)
    val rightDataArray1 = row1.getAs[Seq[Row]](2)

    // Id 1 should have 2 entries in both left and right
    leftDataArray1.length shouldBe 2
    rightDataArray1.length shouldBe 2

    // Check sorting - left data should be sorted by timestamp
    leftDataArray1(0).getInt(2) shouldBe 1000 // First entry should have timestamp 1000
    leftDataArray1(1).getInt(2) shouldBe 2000 // Second entry should have timestamp 2000

    // Right data should be sorted by event_time
    rightDataArray1(0).getDouble(1) shouldBe 50.5 // First entry should have score 50.5 (event_time 1500)
    rightDataArray1(1).getDouble(1) shouldBe 60.5 // Second entry should have score 60.5 (event_time 2500)

    // Check id = 2 (should only have data from left)
    val row2 = results.find(_.getInt(0) == 2).get
    val leftDataArray2 = row2.getAs[Seq[Row]](1)
    val rightDataArray2 = row2.getAs[Seq[Row]](2)

    leftDataArray2.length shouldBe 1
    rightDataArray2.length shouldBe 0

    // Check id = 3 (should only have data from right)
    val row3 = results.find(_.getInt(0) == 3).get
    val leftDataArray3 = row3.getAs[Seq[Row]](1)
    val rightDataArray3 = row3.getAs[Seq[Row]](2)

    leftDataArray3.length shouldBe 0
    rightDataArray3.length shouldBe 1
  }

  it should "handle multiple key columns correctly" in {
    // Create test data with composite keys
    val leftDF = Seq(
      (1, "A", "left1", 1000),
      (1, "B", "left2", 2000),
      (2, "B", "left3", 3000)
    ).toSeq.toDF("id1", "id2", "data", "timestamp")

    val rightDF = Seq(
      (1, "A", "right1", 1500),
      (1, "B", "right2", 2500),
      (2, "C", "right3", 3500)
    ).toSeq.toDF("key1", "key2", "info", "event_time")

    // Define key mapping for multiple keys
    val keyMapping = Map("id1" -> "key1", "id2" -> "key2")

    // Call the unionJoin function
    val joinedDF = UnionJoin
      .unionJoin(
        leftDF,
        rightDF,
        keyMapping,
        "timestamp",
        "event_time"
      )
      .df

    // Check result has the expected number of rows
    // Should have 3 rows: (1,A), (1,B), (2,B), and (2,C)
    joinedDF.count() shouldBe 4

    // Convert to rows for easier testing
    val results = joinedDF.collect()

    // Find the row for id1=1, id2=A
    val row1A = results.find(r => r.getInt(0) == 1 && r.getString(1) == "A").get
    val leftDataArray1A = row1A.getAs[Seq[Row]](2)
    val rightDataArray1A = row1A.getAs[Seq[Row]](3)

    leftDataArray1A.length shouldBe 1
    rightDataArray1A.length shouldBe 1

    // Find the row for id1=1, id2=B
    val row1B = results.find(r => r.getInt(0) == 1 && r.getString(1) == "B").get
    val leftDataArray1B = row1B.getAs[Seq[Row]](2)
    val rightDataArray1B = row1B.getAs[Seq[Row]](3)

    leftDataArray1B.length shouldBe 1
    rightDataArray1B.length shouldBe 1

    // Find the row for id1=2, id2=B
    val row2B = results.find(r => r.getInt(0) == 2 && r.getString(1) == "B").get
    val leftDataArray2B = row2B.getAs[Seq[Row]](2)
    val rightDataArray2B = row2B.getAs[Seq[Row]](3)

    leftDataArray2B.length shouldBe 1
    rightDataArray2B.length shouldBe 0

    // Find the row for id1=2, id2=C
    val row2C = results.find(r => r.getInt(0) == 2 && r.getString(1) == "C").get
    val leftDataArray2C = row2C.getAs[Seq[Row]](2)
    val rightDataArray2C = row2C.getAs[Seq[Row]](3)

    leftDataArray2C.length shouldBe 0
    rightDataArray2C.length shouldBe 1
  }

  it should "filter out null timestamp values" in {
    import org.apache.spark.sql.types._

    // Define schema for left DataFrame
    val leftSchema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("value", IntegerType),
        StructField("timestamp", IntegerType)
      ).toSeq
    )

    // Create test data with some null timestamps
    val leftData = Seq(
      Row(1, "A", 100, 1000),
      Row(1, "B", 200, null),
      Row(2, "C", 300, 3000)
    )
    val leftDF = spark.createDataFrame(
      spark.sparkContext.parallelize(leftData.toSeq),
      leftSchema
    )

    // Define schema for right DataFrame
    val rightSchema = StructType(
      Seq(
        StructField("user_id", IntegerType),
        StructField("category", StringType),
        StructField("score", DoubleType),
        StructField("event_time", IntegerType)
      ).toSeq
    )

    // Create test data with some null timestamps
    val rightData = Seq(
      Row(1, "X", 50.5, 1500),
      Row(1, "Y", 60.5, null),
      Row(2, "Z", 70.5, 2500)
    )
    val rightDF = spark.createDataFrame(
      spark.sparkContext.parallelize(rightData.toSeq),
      rightSchema
    )

    // Define key mapping
    val keyMapping = Map("id" -> "user_id")

    // Call the unionJoin function
    val joinedDF = UnionJoin
      .unionJoin(
        leftDF,
        rightDF,
        keyMapping,
        "timestamp",
        "event_time"
      )
      .df

    // Convert to rows for easier testing
    val results = joinedDF.collect()

    // Find row with id = 1
    val row1 = results.find(_.getInt(0) == 1).get
    val leftDataArray1 = row1.getAs[Seq[Row]](1)
    val rightDataArray1 = row1.getAs[Seq[Row]](2)

    // Should have filtered out null timestamps
    leftDataArray1.length shouldBe 1 // Only the row with timestamp 1000
    rightDataArray1.length shouldBe 1 // Only the row with event_time 1500

    // Find row with id = 2
    val row2 = results.find(_.getInt(0) == 2).get
    val leftDataArray2 = row2.getAs[Seq[Row]](1)
    val rightDataArray2 = row2.getAs[Seq[Row]](2)

    leftDataArray2.length shouldBe 1
    rightDataArray2.length shouldBe 1
  }

  it should "handle timestamp sorting correctly" in {
    // Create test data with timestamps in non-sorted order
    val leftDF = Seq(
      (1, "A", 100, 3000),
      (1, "B", 200, 1000),
      (1, "C", 300, 2000)
    ).toSeq.toDF("id", "name", "value", "timestamp")

    val rightDF = Seq(
      (1, "X", 50.5, 3500),
      (1, "Y", 60.5, 1500),
      (1, "Z", 70.5, 2500)
    ).toSeq.toDF("user_id", "category", "score", "event_time")

    // Define key mapping
    val keyMapping = Map("id" -> "user_id")

    // Call the unionJoin function
    val joinedDF = UnionJoin
      .unionJoin(
        leftDF,
        rightDF,
        keyMapping,
        "timestamp",
        "event_time"
      )
      .df

    // Convert to rows for easier testing
    val results = joinedDF.collect()

    // Find row with id = 1
    val row1 = results.find(_.getInt(0) == 1).get
    val leftDataArray1 = row1.getAs[Seq[Row]](1)
    val rightDataArray1 = row1.getAs[Seq[Row]](2)

    // Verify left array is sorted by timestamp (should be B, C, A)
    leftDataArray1.length shouldBe 3
    leftDataArray1(0).getString(0) shouldBe "B" // timestamp 1000
    leftDataArray1(1).getString(0) shouldBe "C" // timestamp 2000
    leftDataArray1(2).getString(0) shouldBe "A" // timestamp 3000

    // Verify right array is sorted by event_time (should be Y, Z, X)
    rightDataArray1.length shouldBe 3
    rightDataArray1(0).getString(0) shouldBe "Y" // event_time 1500
    rightDataArray1(1).getString(0) shouldBe "Z" // event_time 2500
    rightDataArray1(2).getString(0) shouldBe "X" // event_time 3500
  }
}
