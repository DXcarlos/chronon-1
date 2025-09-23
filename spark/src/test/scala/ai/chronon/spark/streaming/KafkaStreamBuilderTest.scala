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

package ai.chronon.spark.streaming

import ai.chronon.online.TopicInfo
import ai.chronon.spark.streaming.KafkaStreamBuilder
import ai.chronon.spark.submission.SparkSessionBuilder
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class KafkaStreamBuilderTest extends AnyFlatSpec {

  private val spark: SparkSession = SparkSessionBuilder.build("KafkaStreamBuilderTest", local = true)

  it should "throw when kafka stream does not exist" in {
    val topicInfo = TopicInfo.parse("kafka://test_topic/schema=my_schema/host=X/port=Y")
    intercept[RuntimeException] {
      KafkaStreamBuilder.from(topicInfo)(spark, Map.empty)
    }
  }
}
