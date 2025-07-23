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

package ai.chronon.spark.test.fetcher

import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.{Logger, LoggerFactory}

import java.util.TimeZone

class FetcherGeneratedTest extends AnyFlatSpec {

  import ai.chronon.spark.submission

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  val sessionName = "FetcherGeneratedTest"
  val spark: SparkSession = submission.SparkSessionBuilder.build(sessionName, local = true)
  private val tableUtils = TableUtils(spark)
  private val topic = "test_topic"
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val yesterday = tableUtils.partitionSpec.before(today)

  it should "test temporal fetch join generated" in {
    val namespace = "generated_fetch"
    val joinConf = FetcherTestUtil.generateRandomData(namespace, tableUtils, spark, topic, today, yesterday)
    FetcherTestUtil.compareTemporalFetch(joinConf,
                                         tableUtils.partitionSpec.at(System.currentTimeMillis()),
                                         namespace,
                                         consistencyCheck = true,
                                         dropDsOnWrite = false)(spark)
  }
}
