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
import ai.chronon.api.Constants
import ai.chronon.online.serde.{AvroConversions, SparkConversions}
import ai.chronon.spark.Extensions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

/** Utility for encoding flat DataFrames into Avro key/value byte format,
  * replacing KvRdd.toAvroDf and TimedKvRdd.toAvroDf.
  */
object AvroKvEncoder {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private val baseRowSchema = StructType(
    Seq(
      StructField("key_bytes", BinaryType),
      StructField("value_bytes", BinaryType),
      StructField("key_json", StringType),
      StructField("value_json", StringType)
    )
  )

  private val timedRowSchema = StructType(baseRowSchema :+ StructField(Constants.TimeColumn, LongType))

  /** Encode a flat DataFrame (with individual key/value columns) into an Avro DataFrame
    * with key_bytes, value_bytes, key_json, value_json columns.
    *
    * @param flatDf      The source DataFrame with individual columns
    * @param keyColumns  Names of columns that form the key
    * @param valueColumns Names of columns that form the value
    * @param jsonPercent Percentage of rows to include JSON encoding (1 = 1%)
    */
  def encode(
      flatDf: DataFrame,
      keyColumns: Seq[String],
      valueColumns: Seq[String],
      jsonPercent: Int = 1
  )(implicit sparkSession: SparkSession): DataFrame = {
    val keySparkSchema = StructType(keyColumns.map(flatDf.schema(_)))
    val valueSparkSchema = StructType(valueColumns.map(flatDf.schema(_)))
    val keyZSchema = keySparkSchema.toChrononSchema("Key")
    val valueZSchema = valueSparkSchema.toChrononSchema("Value")

    val keyToBytes = AvroConversions.encodeBytes(keyZSchema, GenericRowHandler.func)
    val valueToBytes = AvroConversions.encodeBytes(valueZSchema, GenericRowHandler.func)
    val keyToJson = AvroConversions.encodeJson(keyZSchema, GenericRowHandler.func)
    val valueToJson = AvroConversions.encodeJson(valueZSchema, GenericRowHandler.func)

    val keyIndices = keyColumns.map(flatDf.schema.fieldIndex).toArray
    val valueIndices = valueColumns.map(flatDf.schema.fieldIndex).toArray
    val jsonPct = jsonPercent.toDouble / 100

    logger.info(
      s"""
         |key schema:
         |  ${AvroConversions.fromChrononSchema(keyZSchema).toString(true)}
         |value schema:
         |  ${AvroConversions.fromChrononSchema(valueZSchema).toString(true)}
         |""".stripMargin
    )

    val rowEncoder = ExpressionEncoder(baseRowSchema)
    flatDf
      .map { row =>
        val keys = keyIndices.map(row.get)
        val values = valueIndices.map(row.get)
        val (keyJson, valueJson) = if (math.random < jsonPct) {
          (keyToJson(keys), valueToJson(values))
        } else {
          (null, null)
        }
        Row(keyToBytes(keys), valueToBytes(values), keyJson, valueJson)
      }(rowEncoder)
      .toDF()
  }

  /** Encode a flat DataFrame with a timestamp column into a timed Avro DataFrame.
    * Optionally includes schema metadata rows for KV store upload.
    *
    * @param flatDf              The source DataFrame
    * @param keyColumns          Names of key columns
    * @param valueColumns        Names of value columns
    * @param tsColumn            Name of the timestamp column
    * @param storeSchemasPrefix  If set, adds schema rows for KV store
    * @param metadata            Additional metadata key-value pairs to include as rows
    * @param jsonPercent         Percentage of rows to include JSON (default 1%)
    */
  def encodeTimed(
      flatDf: DataFrame,
      keyColumns: Seq[String],
      valueColumns: Seq[String],
      tsColumn: String = Constants.TimeColumn,
      storeSchemasPrefix: Option[String] = None,
      metadata: Option[Map[String, String]] = None,
      jsonPercent: Int = 1
  )(implicit sparkSession: SparkSession): DataFrame = {
    val keySparkSchema = StructType(keyColumns.map(flatDf.schema(_)))
    val valueSparkSchema = StructType(valueColumns.map(flatDf.schema(_)))
    val keyZSchema = keySparkSchema.toChrononSchema("Key")
    val valueZSchema = valueSparkSchema.toChrononSchema("Value")

    val keyToBytes = AvroConversions.encodeBytes(keyZSchema, GenericRowHandler.func)
    val valueToBytes = AvroConversions.encodeBytes(valueZSchema, GenericRowHandler.func)
    val keyToJson = AvroConversions.encodeJson(keyZSchema, GenericRowHandler.func)
    val valueToJson = AvroConversions.encodeJson(valueZSchema, GenericRowHandler.func)

    val keyIndices = keyColumns.map(flatDf.schema.fieldIndex).toArray
    val valueIndices = valueColumns.map(flatDf.schema.fieldIndex).toArray
    val tsIndex = flatDf.schema.fieldIndex(tsColumn)
    val jsonPct = jsonPercent.toDouble / 100

    val schemasStr = Seq(keyZSchema, valueZSchema).map(AvroConversions.fromChrononSchema(_).toString(true))
    logger.info(
      s"""
         |key schema:
         |  ${schemasStr(0)}
         |value schema:
         |  ${schemasStr(1)}
         |""".stripMargin
    )

    val timedRowEncoder = ExpressionEncoder(timedRowSchema)
    val dataDf = flatDf
      .map { row =>
        val keys = keyIndices.map(row.get)
        val values = valueIndices.map(row.get)
        val ts = row.getLong(tsIndex)
        val (keyJson, valueJson) = if (math.random < jsonPct) {
          (keyToJson(keys), valueToJson(values))
        } else {
          (null, null)
        }
        Row(keyToBytes(keys), valueToBytes(values), keyJson, valueJson, ts)
      }(timedRowEncoder)
      .toDF()

    if (storeSchemasPrefix.isDefined) {
      val ts = System.currentTimeMillis()
      val schemaPrefix = storeSchemasPrefix.get
      logger.info(s"Using schema prefix: $schemaPrefix")
      val keyStr = s"$schemaPrefix${Constants.TimedKvRDDKeySchemaKey}"
      val valStr = s"$schemaPrefix${Constants.TimedKvRDDValueSchemaKey}"
      val schemaRows: Seq[Array[Any]] = Seq(
        Array(
          keyStr.getBytes(Constants.UTF8),
          schemasStr(0).getBytes(Constants.UTF8),
          keyStr,
          schemasStr(0),
          ts
        ),
        Array(
          valStr.getBytes(Constants.UTF8),
          schemasStr(1).getBytes(Constants.UTF8),
          valStr,
          schemasStr(1),
          ts
        )
      )

      val metadataRows: Seq[Array[Any]] = metadata
        .map { metaMap =>
          metaMap.map { case (key, value) =>
            val metaKey = s"$schemaPrefix/$key"
            Array(
              metaKey.getBytes(Constants.UTF8),
              value.getBytes(Constants.UTF8),
              metaKey,
              value,
              ts
            )
          }.toSeq
        }
        .getOrElse(Seq.empty)

      val allRows = schemaRows ++ metadataRows
      val allRowsDf = sparkSession.createDataFrame(
        java.util.Arrays.asList(allRows.map(new GenericRow(_): Row): _*),
        timedRowSchema
      )
      dataDf.union(allRowsDf)
    } else {
      dataDf
    }
  }

  /** Create a flat DataFrame from arrays of key/value data.
    * Replaces KvRdd.toFlatDf.
    */
  def createFlatDf(
      data: Seq[(Array[Any], Array[Any])],
      keySchema: StructType,
      valueSchema: StructType
  )(implicit sparkSession: SparkSession): DataFrame = {
    val flatSchema = StructType(keySchema ++ valueSchema)
    val flatZSchema = flatSchema.toChrononSchema("Flat")

    val rows = data.map { case (keys, values) =>
      val result = new Array[Any](keys.length + values.length)
      System.arraycopy(keys, 0, result, 0, keys.length)
      System.arraycopy(values, 0, result, keys.length, values.length)
      SparkConversions.toSparkRow(result, flatZSchema, GenericRowHandler.func).asInstanceOf[GenericRow]: Row
    }
    sparkSession.createDataFrame(java.util.Arrays.asList(rows: _*), flatSchema)
  }
}
