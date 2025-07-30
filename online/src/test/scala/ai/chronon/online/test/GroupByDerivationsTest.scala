package ai.chronon.online.test

import ai.chronon.api.ScalaJavaConversions.ListOps

import java.util
import ai.chronon.api.{
  Accuracy,
  Builders,
  DoubleType,
  GroupBy,
  GroupByServingInfo,
  IntType,
  LongType,
  Operation,
  StringType,
  StructField,
  StructType,
  TimeUnit,
  Window
}
import ai.chronon.online.GroupByServingInfoParsed
import ai.chronon.online.OnlineDerivationUtil.applyDeriveFunc
import ai.chronon.online.fetcher.Fetcher.Request
import ai.chronon.online.serde.AvroConversions
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class GroupByDerivationsTest extends AnyFlatSpec {
  it should "parse and evaluate a groupBy with derivations" in {

    def makeArrayList(vals: Any*): util.ArrayList[Any] =
      new util.ArrayList[Any](util.Arrays.asList(vals: _*))

    val groupByServingInfoParsed = GroupByDerivationsTest.makeTestGroupByServingInfoParsed()
    groupByServingInfoParsed should not be null

    // Verify the derivation function can be obtained
    val deriveFn = groupByServingInfoParsed.deriveFunc
    deriveFn should not be null

    // Create a Request object with the key from the GroupBy
    val request = Request(
      name = "derivations_test_group_by",
      keys = Map("id" -> "test_user_123"),
      atMillis = Some(System.currentTimeMillis()),
      context = None
    )

    // Create a base feature value map with sample aggregated values
    // In some cases we skip the int_val and double_val fields
    val namedStructMap1 = Map("id" -> "test_user_123")
    val namedStructMap2 = Map("id" -> "test_user_123", "int_val" -> java.lang.Integer.valueOf(100))
    val anotherNamedStructMap1 = Map("id" -> "test_user_123")
    val anotherNamedStructMap2 = Map("id" -> "test_user_123", "double_val" -> java.lang.Double.valueOf(42.5))

    val baseFeatureMap: Map[String, AnyRef] = Map(
      "double_val_sum_1d" -> java.lang.Double.valueOf(42.5),
      "named_struct_last2_1d" -> makeArrayList(namedStructMap1, namedStructMap2),
      "named_struct_last2_2d" -> makeArrayList(namedStructMap1, namedStructMap2),
      "another_named_struct_last2_1d" -> makeArrayList(anotherNamedStructMap2, anotherNamedStructMap1),
      "another_named_struct_last2_2d" -> makeArrayList(anotherNamedStructMap1, anotherNamedStructMap2),
      "int_val_last_1d" -> java.lang.Integer.valueOf(100)
    )

    // Invoke the deriveFn with the Request and feature value map
    val derivedValues = applyDeriveFunc(deriveFn, request, baseFeatureMap)

    // Verify the derived values match expectations
    derivedValues should contain key ("int_val")
    derivedValues("int_val") shouldEqual 100

    derivedValues should contain key ("id_last2_1d")
    val idLast2Values = derivedValues("id_last2_1d").asInstanceOf[util.List[String]]
    idLast2Values.toScala shouldEqual List("test_user_123", "test_user_123")

    derivedValues should contain key ("id2_last2_1d")
    val id2Last2Values = derivedValues("id2_last2_1d").asInstanceOf[util.List[String]]
    id2Last2Values.toScala shouldEqual List("test_user_123", "test_user_123")

    // make another feature map which represents structs as Array[Any] instead of Maps
    val namedStructArray1 = Array[Any]("test_user_123", java.lang.Integer.valueOf(100))
    val namedStructArray2 = Array[Any]("test_user_123", null)
    val anotherNamedStructArray1 = Array[Any]("test_user_123", java.lang.Double.valueOf(42.5))
    val anotherNamedStructArray2 = Array[Any]("test_user_123", null)

    val baseFeatureMap2: Map[String, AnyRef] = Map(
      "double_val_sum_1d" -> java.lang.Double.valueOf(42.5),
      "named_struct_last2_1d" -> makeArrayList(namedStructArray1, namedStructArray2),
      "named_struct_last2_2d" -> makeArrayList(namedStructArray1, namedStructArray2),
      "another_named_struct_last2_1d" -> makeArrayList(anotherNamedStructArray1, anotherNamedStructArray2),
      "another_named_struct_last2_2d" -> makeArrayList(anotherNamedStructArray2, anotherNamedStructArray1),
      "int_val_last_1d" -> java.lang.Integer.valueOf(100)
    )

    val derivedValues2 = applyDeriveFunc(deriveFn, request, baseFeatureMap2)
    derivedValues2 should contain key ("id_last2_1d")
    val idLast2Values2 = derivedValues2("id_last2_1d").asInstanceOf[util.List[String]]
    idLast2Values2.toScala shouldEqual List("test_user_123", "test_user_123")
  }
}

object GroupByDerivationsTest {
  def makeTestGroupByServingInfoParsed(): GroupByServingInfoParsed = {
    val groupBy = makeGroupBy()
    val groupByServingInfo = new GroupByServingInfo()
    groupByServingInfo.setGroupBy(groupBy)

    // Set input avro schema for groupByServingInfo
    val inputSchema = StructType(
      "Input",
      Array(
        StructField("id", StringType),
        StructField("int_val", IntType),
        StructField("double_val", DoubleType),
        StructField("ts", LongType)
      )
    )

    groupByServingInfo.setInputAvroSchema(
      AvroConversions.fromChrononSchema(inputSchema).toString(true)
    )

    // Set key avro schema for groupByServingInfo
    val keySchema = StructType(
      "Key",
      Array(
        StructField("id", StringType)
      )
    )
    groupByServingInfo.setKeyAvroSchema(AvroConversions.fromChrononSchema(keySchema).toString(true))

    // set selected schema
    val selectedSchema = StructType(
      "Selected",
      Array(
        StructField("id", StringType),
        StructField("int_val", IntType),
        StructField("double_val", DoubleType),
        StructField("named_struct",
                    StructType("named_struct", Array(StructField("id", StringType), StructField("int_val", IntType)))),
        StructField("another_named_struct",
                    StructType("another_named_struct",
                               Array(StructField("id", StringType), StructField("double_val", DoubleType))))
      )
    )
    groupByServingInfo.setSelectedAvroSchema(AvroConversions.fromChrononSchema(selectedSchema).toString(true))

    groupByServingInfo.setBatchEndDate("2023-11-06")
    groupByServingInfo.setDateFormat("yyyy-MM-dd")
    new GroupByServingInfoParsed(groupByServingInfo)
  }

  def makeGroupBy(): GroupBy =
    Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = "events.my_stream_raw",
          topic = "events.my_stream",
          query = Builders.Query(
            selects = Map(
              "id" -> "id",
              "int_val" -> "int_val",
              "double_val" -> "double_val",
              "named_struct" -> "IF(id IS NOT NULL NAMED_STRUCT('id', id, 'int_val', int_val), NULL)",
              "another_named_struct" -> "IF(id IS NOT NULL NAMED_STRUCT('id', id, 'double_val', double_val), NULL)"
            ),
            wheres = Seq.empty,
            timeColumn = "ts",
            startPartition = "20231106"
          )
        )
      ),
      keyColumns = Seq("id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "double_val",
          windows = Seq(
            new Window(1, TimeUnit.DAYS)
          )
        ),
        Builders.Aggregation(
          operation = Operation.LAST_K,
          inputColumn = "named_struct",
          windows = Seq(
            new Window(1, TimeUnit.DAYS),
            new Window(2, TimeUnit.DAYS)
          ),
          argMap = Map("k" -> "2")
        ),
        Builders.Aggregation(
          operation = Operation.LAST_K,
          inputColumn = "another_named_struct",
          windows = Seq(
            new Window(1, TimeUnit.DAYS),
            new Window(2, TimeUnit.DAYS)
          ),
          argMap = Map("k" -> "2")
        ),
        Builders.Aggregation(
          operation = Operation.LAST,
          inputColumn = "int_val",
          windows = Seq(
            new Window(1, TimeUnit.DAYS)
          )
        )
      ),
      metaData = Builders.MetaData(
        name = "derivations_test_group_by"
      ),
      accuracy = Accuracy.TEMPORAL,
      derivations = Seq(
        Builders.Derivation(
          name = "int_val",
          expression = "int_val_last_1d"
        ),
        Builders.Derivation(
          name = "id_last2_1d",
          expression = "transform(named_struct_last2_1d, x -> x.id)"
        ),
        Builders.Derivation(
          name = "id2_last2_1d",
          expression = "transform(another_named_struct_last2_1d, x -> x.id)"
        )
      )
    )
}
