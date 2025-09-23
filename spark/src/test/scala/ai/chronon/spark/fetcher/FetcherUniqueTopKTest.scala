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

package ai.chronon.spark.fetcher

import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.{Logger, LoggerFactory}

import java.util.TimeZone

class FetcherUniqueTopKTest extends AnyFlatSpec {

  import ai.chronon.spark.submission

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  val sessionName = "FetcherUniqueTopKTest"
  val spark: SparkSession = submission.SparkSessionBuilder.build(sessionName, local = true)
  private val tableUtils = TableUtils(spark)
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  it should "test temporal fetch join deterministic with UniqueTopK struct" in {
    val namespace = "deterministic_unique_topk_fetch"
    val joinConf = FetcherTestUtil.generateMutationDataWithUniqueTopK(namespace, tableUtils, spark)
    FetcherTestUtil.compareTemporalFetch(joinConf,
                                         "2021-04-10",
                                         namespace,
                                         consistencyCheck = false,
                                         dropDsOnWrite = true)(spark)
  }
}
