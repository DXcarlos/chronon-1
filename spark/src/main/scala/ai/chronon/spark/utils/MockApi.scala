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

package ai.chronon.spark.utils

import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.Extensions.SourceOps
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.online.fetcher.Fetcher
import ai.chronon.online.fetcher.Fetcher.Response
import ai.chronon.online._
import ai.chronon.online.serde._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import java.util
import java.util.Base64
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.Seq
import scala.concurrent.Future
import scala.util.Success

class MockStreamBuilder extends StreamBuilder {
  override def from(topicInfo: TopicInfo)(implicit session: SparkSession, props: Map[String, String]): DataStream = {
    val tableUtils = TableUtils(session)
    println(s"""building stream from topic: ${topicInfo.name}""")
    val ds = topicInfo.params("ds")
    val df = tableUtils.sql(s"select * from ${topicInfo.name} where ds >= '$ds'")
    val encodedDf = (new InMemoryStream).getContinuousStreamDF(session, df.drop("ds"))
    // table name should be same as topic name
    DataStream(encodedDf, 1, topicInfo)
  }
}

class MockApi(kvStore: () => KVStore, val namespace: String) extends Api(null) {
  class PlusOneExternalHandler extends ExternalSourceHandler {
    override def fetch(requests: collection.Seq[Fetcher.Request]): Future[collection.Seq[Fetcher.Response]] = {
      Future(
        requests.map(req =>
          Response(req,
                   Success(req.keys.mapValues(_.asInstanceOf[Integer] + 1).mapValues(_.asInstanceOf[AnyRef]).toMap))))
    }
  }

  class AlwaysFailsHandler extends JavaExternalSourceHandler {
    override def fetchJava(requests: util.List[JavaRequest]): CompletableFuture[util.List[JavaResponse]] = {
      CompletableFuture.completedFuture[util.List[JavaResponse]](
        requests
          .iterator()
          .toScala
          .map(req =>
            new JavaResponse(
              req,
              JTry.failure(
                new RuntimeException("This handler always fails things")
              )
            ))
          .toList
          .toJava
      )
    }
  }

  class JavaPlusOneExternalHandler extends JavaExternalSourceHandler {
    override def fetchJava(requests: util.List[JavaRequest]): CompletableFuture[util.List[JavaResponse]] = {
      CompletableFuture.completedFuture(
        requests
          .iterator()
          .toScala
          .map { req =>
            new JavaResponse(req,
                             JTry.success(
                               req.keys
                                 .entrySet()
                                 .iterator()
                                 .toScala
                                 .map(e => e.getKey -> (e.getValue.asInstanceOf[Integer] + 1).asInstanceOf[AnyRef])
                                 .toMap
                                 .toJava
                             ))
          }
          .toSeq
          .toJava)
    }
  }

  val loggedResponseList: ConcurrentLinkedQueue[LoggableResponseBase64] =
    new ConcurrentLinkedQueue[LoggableResponseBase64]

  override def streamDecoder(parsedInfo: GroupByServingInfoParsed): SerDe = {
    println(
      s"decoding stream ${parsedInfo.groupBy.streamingSource.get.topic} with " +
        s"schema: ${SparkConversions.fromChrononSchema(parsedInfo.streamChrononSchema).catalogString}")
    val streamChrononSchema = parsedInfo.streamChrononSchema
    new AvroSerDe(AvroConversions.fromChrononSchema(streamChrononSchema))
  }

  override def genKvStore: KVStore = {
    kvStore()
  }

  override def logResponse(loggableResponse: LoggableResponse): Unit =
    loggedResponseList.add(
      LoggableResponseBase64(
        keyBase64 = Base64.getEncoder.encodeToString(loggableResponse.keyBytes),
        valueBase64 = Base64.getEncoder.encodeToString(loggableResponse.valueBytes),
        name = loggableResponse.joinName,
        tsMillis = loggableResponse.tsMillis,
        schemaHash = loggableResponse.schemaHash
      ))

  val logTable: String = s"$namespace.mock_log_table"
  val schemaTable: String = s"$namespace.mock_schema_table"

  def flushLoggedValues: Seq[LoggableResponseBase64] = {
    val loggedValues = loggedResponseList.iterator().toScala.toArray
    loggedResponseList.clear()
    loggedValues
  }

  def loggedValuesToDf(loggedValues: Seq[LoggableResponseBase64], session: SparkSession): DataFrame = {
    val df = session.sqlContext.createDataFrame(session.sparkContext.parallelize(loggedValues.toSeq))
    df.withTimeBasedColumn("ds", "tsMillis").camelToSnake
  }

  override def externalRegistry: ExternalSourceRegistry = {
    val registry = new ExternalSourceRegistry
    registry.add("plus_one", new PlusOneExternalHandler)
    registry.add("always_fails", new AlwaysFailsHandler)
    registry.add("java_plus_one", new JavaPlusOneExternalHandler)
    registry
  }
  override def generateStreamBuilder(streamType: String): StreamBuilder = {
    new MockStreamBuilder()
  }
}
