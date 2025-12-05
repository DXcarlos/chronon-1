/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.spark

import ai.chronon.api
import ai.chronon.online.serde.AvroConversions
import ai.chronon.online.serde.SparkConversions
import ai.chronon.spark.Extensions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object GenericRowHandler {
  val func: Any => Array[Any] = {
    case x: GenericRowWithSchema => {
      val result = new Array[Any](x.length)
      var i = 0
      while (i < x.length) {
        result.update(i, x.get(i))
        i += 1
      }
      result
    }
    case x: Array[_] => x.asInstanceOf[Array[Any]] // Handle plain arrays directly
  }
}

sealed trait BaseKvRdd {

  def keySchema: StructType
  def valueSchema: StructType
  def withTime: Boolean
  val timeField: StructField = StructField(api.Constants.TimeColumn, LongType)
  val keyZSchema: api.StructType = keySchema.toChrononSchema("Key")
  val valueZSchema: api.StructType = valueSchema.toChrononSchema("Value")
  val baseFlatSchema: StructType = StructType(keySchema ++ valueSchema)
  def flatSchema: StructType = if (withTime) StructType(baseFlatSchema :+ timeField) else baseFlatSchema
  def flatZSchema: api.StructType = flatSchema.toChrononSchema("Flat")
  lazy val keyToBytes: Any => Array[Byte] = AvroConversions.encodeBytes(keyZSchema, GenericRowHandler.func)
  lazy val valueToBytes: Any => Array[Byte] = AvroConversions.encodeBytes(valueZSchema, GenericRowHandler.func)
  lazy val keyToJson: Any => String = AvroConversions.encodeJson(keyZSchema, GenericRowHandler.func)
  lazy val valueToJson: Any => String = AvroConversions.encodeJson(valueZSchema, GenericRowHandler.func)
  private val baseRowSchema = StructType(
    Seq(
      StructField("key_bytes", BinaryType),
      StructField("value_bytes", BinaryType),
      StructField("key_json", StringType),
      StructField("value_json", StringType)
    )
  )
  def rowSchema: StructType = if (withTime) StructType(baseRowSchema :+ timeField) else baseRowSchema

  def toFlatDf: DataFrame
}

case class KvRdd(data: RDD[(Array[Any], Array[Any])], keySchema: StructType, valueSchema: StructType)(implicit
    sparkSession: SparkSession)
    extends BaseKvRdd {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  val withTime = false

  def toAvroDf(jsonPercent: Int = 1): DataFrame = {
    val avroRdd: RDD[Row] = data.map { case (keys: Array[Any], values: Array[Any]) =>
      // json encoding is very expensive (50% of entire job).
      // We only do it for a specified fraction to retain debuggability.
      val (keyJson, valueJson) = if (math.random < jsonPercent.toDouble / 100) {
        (keyToJson(keys), valueToJson(values))
      } else {
        (null, null)
      }
      val result: Array[Any] = Array(keyToBytes(keys), valueToBytes(values), keyJson, valueJson)
      new GenericRow(result)
    }

    logger.info(s"""
          |key schema:
          |  ${AvroConversions.fromChrononSchema(keyZSchema).toString(true)}
          |value schema:
          |  ${AvroConversions.fromChrononSchema(valueZSchema).toString(true)}
          |""".stripMargin)

    sparkSession.createDataFrame(avroRdd, rowSchema)
  }

  override def toFlatDf: DataFrame = {
    val flatRdd: RDD[Row] = data.map { case (keys: Array[Any], values: Array[Any]) =>
      val result = new Array[Any](keys.length + values.length)
      System.arraycopy(keys, 0, result, 0, keys.length)
      System.arraycopy(values, 0, result, keys.length, values.length)
      SparkConversions.toSparkRow(result, flatZSchema, GenericRowHandler.func).asInstanceOf[GenericRow]
    }
    sparkSession.createDataFrame(flatRdd, flatSchema)
  }
}

case class TimedKvRdd(data: RDD[(Array[Any], Array[Any], Long)],
                      keySchema: StructType,
                      valueSchema: StructType,
                      storeSchemasPrefix: Option[String] = None,
                      metadata: Option[Map[String, String]] = None)(implicit sparkSession: SparkSession)
    extends BaseKvRdd {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  val withTime = true

  // TODO make json percent configurable
  def toAvroDf: DataFrame = {
    val avroRdd: RDD[Row] = data.map { case (keys, values, ts) =>
      val (keyJson, valueJson) = if (math.random < 0.01) {
        (keyToJson(keys), valueToJson(values))
      } else {
        (null, null)
      }
      val result: Array[Any] = Array(keyToBytes(keys), valueToBytes(values), keyJson, valueJson, ts)
      new GenericRow(result)
    }

    val schemasStr = Seq(keyZSchema, valueZSchema).map(AvroConversions.fromChrononSchema(_).toString(true))
    logger.info(s"""
         |key schema:
         |  ${schemasStr(0)}
         |value schema:
         |  ${schemasStr(1)}
         |""".stripMargin)
    val dataDf = sparkSession.createDataFrame(avroRdd, rowSchema)
    if (storeSchemasPrefix.isDefined) {
      val ts = System.currentTimeMillis()
      val schemaPrefix = storeSchemasPrefix.get
      logger.info(s"Using schema prefix: $schemaPrefix")
      val keyStr = s"$schemaPrefix${api.Constants.TimedKvRDDKeySchemaKey}"
      val valStr = s"$schemaPrefix${api.Constants.TimedKvRDDValueSchemaKey}"
      val schemaRows: Seq[Array[Any]] = Seq(
        Array(
          keyStr.getBytes(api.Constants.UTF8),
          schemasStr(0).getBytes(api.Constants.UTF8),
          keyStr,
          schemasStr(0),
          ts
        ),
        Array(
          valStr.getBytes(api.Constants.UTF8),
          schemasStr(1).getBytes(api.Constants.UTF8),
          valStr,
          schemasStr(1),
          ts
        )
      )

      // Add metadata rows if provided
      val metadataRows: Seq[Array[Any]] = metadata
        .map { metaMap =>
          metaMap.map { case (key, value) =>
            val metaKey = s"$schemaPrefix/$key"
            Array(
              metaKey.getBytes(api.Constants.UTF8),
              value.getBytes(api.Constants.UTF8),
              metaKey,
              value,
              ts
            )
          }.toSeq
        }
        .getOrElse(Seq.empty)

      val allRows = schemaRows ++ metadataRows
      val allRowsRdd: RDD[Row] = sparkSession.sparkContext.parallelize(allRows.map(new GenericRow(_)))
      val allRowsDf = sparkSession.createDataFrame(allRowsRdd, rowSchema)
      return dataDf.union(allRowsDf)
    }
    dataDf
  }

  override def toFlatDf: DataFrame = {
    val flatRdd: RDD[Row] = data.map { case (keys, values, ts) =>
      val result = new Array[Any](keys.length + values.length + 1)
      System.arraycopy(keys, 0, result, 0, keys.length)
      System.arraycopy(values, 0, result, keys.length, values.length)
      result(result.length - 1) = ts
      SparkConversions.toSparkRow(result, flatZSchema, GenericRowHandler.func).asInstanceOf[GenericRow]
    }
    sparkSession.createDataFrame(flatRdd, flatSchema)
  }
}
