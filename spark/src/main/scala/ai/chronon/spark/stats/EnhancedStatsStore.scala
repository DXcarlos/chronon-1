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

package ai.chronon.spark.stats

import ai.chronon.aggregator.row.{RowAggregator, StatsGenerator}
import ai.chronon.api
import ai.chronon.api.{Constants, SerdeUtils, StringType, StructType}
import ai.chronon.online.{Api, KVStore}
import ai.chronon.online.KVStore.{GetRequest, PutRequest}
import ai.chronon.online.serde.{AvroCodec, AvroConversions}
import ai.chronon.spark.catalog.TableUtils
import org.apache.avro.generic
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types
import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.Charset
import java.util.concurrent.TimeUnit
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

/** Utilities for uploading and fetching enhanced statistics from KV Store.
  *
  * Handles:
  * - Uploading Avro-encoded DataFrames (daily/hourly tiles with IRs) to KV Store
  * - Fetching IRs for a given time range
  * - Merging IRs across time ranges using RowAggregator
  * - Normalizing merged IRs to final statistics
  *
  * @param api The API instance for accessing KV stores
  * @param tableBaseName The base table name for the BigTable enhanced stats table (default: "ENHANCED_STATS")
  * @param datasetName The dataset name within the KV store (default: Constants.EnhancedStatsDataset)
  */
class EnhancedStatsStore(api: Api,
                         tableBaseName: String = "ENHANCED_STATS",
                         datasetName: String = Constants.EnhancedStatsDataset)(implicit tu: TableUtils)
    extends Serializable {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  // Use the specialized enhanced stats KV store for efficient BigTable time-series operations
  @transient lazy val kvStore: KVStore = api.genEnhancedStatsKvStore(tableBaseName)

  /** Upload Avro DataFrame to KV Store.
    *
    * @param avroDf The Avro-encoded DataFrame (key_bytes, value_bytes, key_json, value_json, ts)
    * @param putsPerRequest Number of puts to batch per request (default: 100)
    */
  def upload(avroDf: DataFrame, putsPerRequest: Int = 100): Unit = {

    // Debug logging to inspect the DataFrame
    val totalRows = avroDf.count()
    logger.info(s"Total rows in avroDf: $totalRows")

    // Sample a few rows to inspect key_bytes
    val sampleRows = avroDf.limit(5).collect()
    logger.info(s"Sample rows from avroDf:")
    sampleRows.zipWithIndex.foreach { case (row, idx) =>
      val keyBytes = if (row.isNullAt(0)) "NULL" else s"[${row.getAs[Array[Byte]](0).length} bytes]"
      val valueBytes = if (row.isNullAt(1)) "NULL" else s"[${row.getAs[Array[Byte]](1).length} bytes]"
      val ts = if (row.length > 4 && !row.isNullAt(4)) row.getLong(4) else "N/A"
      logger.info(s"  Row $idx: key_bytes=$keyBytes, value_bytes=$valueBytes, ts=$ts")
    }

    // Validate schema
    val requiredColumns = Seq("key_bytes", "value_bytes", Constants.TimeColumn)
    val missingColumns = requiredColumns.filterNot(avroDf.columns.contains)
    require(missingColumns.isEmpty, s"Missing required columns: ${missingColumns.mkString(", ")}")

    // Create dataset if it doesn't exist
    try {
      kvStore.create(datasetName)
      logger.info(s"Created KV Store dataset: $datasetName")
    } catch {
      case e: Exception =>
        logger.debug(s"Dataset $datasetName may already exist: ${e.getMessage}")
    }

    // Collect all rows to driver (safe for stats data which is small aggregated tiles)
    val allRows = avroDf.collect()
    logger.info(s"Collected ${allRows.length} rows to driver for upload")

    val keyIndex = avroDf.schema.fieldIndex("key_bytes")
    val valueIndex = avroDf.schema.fieldIndex("value_bytes")
    val timestampIndex = avroDf.schema.fieldIndex(Constants.TimeColumn)

    // Convert rows to PutRequests
    val putRequests = allRows.zipWithIndex.flatMap { case (row, idx) =>
      if (row.isNullAt(keyIndex)) {
        logger.warn(s"Row $idx has NULL key_bytes - skipping")
        None
      } else {
        val keyBytes = row.getAs[Array[Byte]](keyIndex)
        val valueBytes = if (row.isNullAt(valueIndex)) Array.empty[Byte] else row.getAs[Array[Byte]](valueIndex)
        val timestamp =
          if (timestampIndex < 0 || row.isNullAt(timestampIndex)) None
          else Some(row.getAs[Long](timestampIndex))
        logger.info(
          s"Row $idx: keyBytes=${keyBytes.length} bytes, valueBytes=${valueBytes.length} bytes, ts=$timestamp")
        Some(PutRequest(keyBytes, valueBytes, datasetName, timestamp))
      }
    }

    logger.info(s"Created ${putRequests.length} PutRequests, uploading in batches of $putsPerRequest")

    // Upload in batches from driver using the specialized enhanced stats KV store
    val batches = putRequests.grouped(putsPerRequest).toSeq

    batches.zipWithIndex.foreach { case (batch, batchIdx) =>
      logger.info(s"Uploading batch ${batchIdx + 1}/${batches.size} with ${batch.length} requests")
      val uploadFuture = kvStore.multiPut(batch)
      val results = Await.result(uploadFuture, Duration(30L, TimeUnit.SECONDS))

      val successCount = results.count(_ == true)
      val failureCount = results.count(_ == false)
      logger.info(s"Batch ${batchIdx + 1} complete: $successCount succeeded, $failureCount failed")

      if (failureCount > 0) {
        logger.error(s"Some uploads failed in batch ${batchIdx + 1}")
      }
    }

    logger.info(s"Upload complete for dataset: $datasetName")
  }
}
