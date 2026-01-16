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

package ai.chronon.spark.groupby

import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.utils.SparkTestBase
import org.scalatest.matchers.should.Matchers

class DisableCombineGroupByUploadTest extends SparkTestBase with Matchers {

  override def sparkConfs: Map[String, String] = Map(
    "spark.chronon.group_by.upload.combine" -> "false",
  )
  private val tableUtils = TableUtils(spark)

  it should "produce valid batch data for temporal events case with spark.chronon.group_by.upload.combine disabled" in {
    val eventsTable = "my_events_check_temporal_combine_disabled"
    val namespace = this.getClass.getSimpleName
    createDatabase(namespace)
    GroupByUploadTest.runAndValidateActualTemporalBatchData(namespace=namespace, sparkSession = spark, tableUtils = tableUtils, eventsTable = eventsTable)
  }
}