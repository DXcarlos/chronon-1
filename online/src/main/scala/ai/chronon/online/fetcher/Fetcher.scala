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

package ai.chronon.online.fetcher

import ai.chronon.api
import ai.chronon.api.Constants.UTF8
import ai.chronon.api.Extensions.{ExternalPartOps, JoinOps, StringOps, ThrowableOps}
import ai.chronon.api._
import ai.chronon.online.OnlineDerivationUtil.applyDeriveFunc
import ai.chronon.online._
import ai.chronon.online.fetcher.Fetcher.{AvroResponseValue, BaseResponse, JoinSchemaResponse, Request, Response, ResponseV2, ResponseWithContext}
import ai.chronon.online.fetcher.ResponseType.ResponseType
import ai.chronon.online.metrics.{Metrics, TTLCache}
import ai.chronon.online.serde._
import com.google.gson.Gson
import org.apache.avro.generic.GenericRecord
import org.slf4j.{Logger, LoggerFactory}

import java.util.function.Consumer
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.{Seq, mutable}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object ResponseType extends Enumeration {
  type ResponseType = Value
  val Map, WithAvroBytes, WithAvroString = Value
}

object Fetcher {

  import ai.chronon.online.metrics

  case class Request(name: String,
                     keys: Map[String, AnyRef],
                     atMillis: Option[Long] = None,
                     context: Option[metrics.Metrics.Context] = None)

  case class PrefixedRequest(prefix: String, request: Request)

  trait BaseResponse

  case class Response(request: Request, values: Try[Map[String, AnyRef]]) extends BaseResponse
  case class ResponseWithContext(request: Request,
                                 derivedValues: Map[String, AnyRef],
                                 baseValues: Map[String, AnyRef]) {
    def combinedValues: Map[String, AnyRef] = baseValues ++ derivedValues
  }

  sealed trait AvroResponseValue
  object AvroResponseValue {
    case class Map(value: Try[scala.collection.immutable.Map[String, AnyRef]]) extends AvroResponseValue
    case class AvroBytes(value: Try[Array[Byte]]) extends AvroResponseValue
    case class AvroString(value: Try[String]) extends AvroResponseValue
  }

  case class ResponseV2(request: Request, value: AvroResponseValue) extends BaseResponse {
    def valuesMap: Try[Map[String, AnyRef]] = value match {
      case AvroResponseValue.Map(v) => v
    }

    def valuesAvroBytes: Try[Array[Byte]] = value match {
      case AvroResponseValue.AvroBytes(v) => v
    }

    def valuesAvroString: Try[String] = value match {
      case AvroResponseValue.AvroString(v) => v
    }

    def getResponseValueType: ResponseType = value match {
      case AvroResponseValue.Map(_)        => ResponseType.Map
      case AvroResponseValue.AvroBytes(_)  => ResponseType.WithAvroBytes
      case AvroResponseValue.AvroString(_) => ResponseType.WithAvroString
    }
  }



  case class ColumnSpec(groupByName: String,
                        columnName: String,
                        prefix: Option[String],
                        keyMapping: Option[Map[String, AnyRef]])

  def logResponseStats(response: Response, context: metrics.Metrics.Context): Unit = {
    import ai.chronon.online.metrics
    val responseMap = response.values.get
    var exceptions = 0
    var nulls = 0
    responseMap.foreach { case (_, v) =>
      if (v == null) nulls += 1
      else if (v.isInstanceOf[Throwable]) exceptions += 1
    }
    context.distribution(metrics.Metrics.Name.FetchNulls, nulls)
    context.distribution(metrics.Metrics.Name.FetchExceptions, exceptions)
    context.distribution(metrics.Metrics.Name.FetchCount, responseMap.size)
  }

  def logFeatureNullRates(responseMap: Map[String, AnyRef], context: metrics.Metrics.Context): Unit = {
    import ai.chronon.online.metrics

    responseMap.foreach { case (featureName, value) =>
      if (!featureName.endsWith("_exception")) {
        if (value == null)
          context.increment(
            metrics.Metrics.Name.FeatureNulls,
            Map(metrics.Metrics.Tag.Feature -> featureName)
          )
        context.increment(
          metrics.Metrics.Name.FeatureCount,
          Map(metrics.Metrics.Tag.Feature -> featureName)
        )
      }
    }
  }

  /** Response for a join schema request
    * @param joinName - Name of the join
    * @param keySchema - Avro schema string for the key
    * @param valueSchema - Avro schema string for the value
    * @param schemaHash - Hash of the join schema payload (used to track updates to key / value schema fields or types)
    * @param valueInfos - Per feature column metadata (e.g. group name, corresponding left lookup keys, ..)
    */
  case class JoinSchemaResponse(joinName: String,
                                keySchema: String,
                                valueSchema: String,
                                schemaHash: String,
                                valueInfos: Array[JoinCodec.ValueInfo])
}

//private[online] case class FetcherResponseWithTs(responses: Seq[Fetcher.Response], endTs: Long)

private[online] case class FetcherResponseWithTs[T <: BaseResponse](responses: Seq[T], endTs: Long)


// BaseFetcher + Logging + External service calls
class Fetcher(val kvStore: KVStore,
              metaDataSet: String,
              timeoutMillis: Long = 10000,
              logFunc: Consumer[LoggableResponse] = null,
              debug: Boolean = false,
              val externalSourceRegistry: ExternalSourceRegistry = null,
              callerName: String = null,
              flagStore: FlagStore = null,
              disableErrorThrows: Boolean = false,
              executionContextOverride: ExecutionContext = null) {

  @transient implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private val fetchContext: FetchContext =
    FetchContext(kvStore, metaDataSet, timeoutMillis, debug, flagStore, disableErrorThrows, executionContextOverride)

  implicit private val executionContext: ExecutionContext = fetchContext.getOrCreateExecutionContext
  val metadataStore: MetadataStore = new MetadataStore(fetchContext)
  private val joinPartFetcher = new JoinPartFetcher(fetchContext, metadataStore)

  lazy val joinCodecCache: TTLCache[String, Try[JoinCodec]] = metadataStore.buildJoinCodecCache(
    Some(logControlEvent)
  )
  // Generic withTs method that works with any TimestampableResponse
  private[online] def withTs[T <: BaseResponse](responses: Future[Seq[T]]): Future[FetcherResponseWithTs[T]] = {
    responses.map { response =>
      FetcherResponseWithTs(response, System.currentTimeMillis())
    }
  }

//
//  private[online] def withTs(responses: Future[Seq[Response]]): Future[FetcherResponseWithTs] = {
//    responses.map { response =>
//      FetcherResponseWithTs(response, System.currentTimeMillis())
//    }
//  }

  def fetchGroupBys(requests: Seq[Request]): Future[Seq[Response]] = {
    joinPartFetcher.fetchGroupBys(requests)
  }

  def fetchJoin(requests: Seq[Request], joinConf: Option[api.Join] = None): Future[Seq[Response]] = {
    val ts = System.currentTimeMillis()
    val internalResponsesF = joinPartFetcher.fetchJoins(requests, joinConf)
    val externalResponsesF = fetchExternal(requests)
    val combinedResponsesF =
      internalResponsesF.zip(externalResponsesF).map { case (internalResponses, externalResponses) =>
        val zipped = if (externalResponses == null) {
          internalResponses.map(_ -> null)
        } else {
          internalResponses.zip(externalResponses)
        }

        val derivedResults = zipped.map { case (internalResponse, externalResponse) =>
          val cleanInternalRequest = internalResponse.request.copy(context = None)
          val internalMap = internalResponse.values.getOrElse(
            Map("join_part_fetch_exception" -> internalResponse.values.failed.get.traceString))

          val baseMap = if (externalResponse != null) {

            assert(
              cleanInternalRequest == externalResponse.request,
              s"""
                   |Logic error. Responses are not aligned to requests
                   |mismatching requests:  $cleanInternalRequest, ${externalResponse.request}
                   |  requests:            ${requests.map(_.name)}
                   |  internalResponses:   ${internalResponses.map(_.request.name)}
                   |  externalResponses:   ${externalResponses.map(_.request.name)}""".stripMargin
            )

            val externalMap = externalResponse.values.getOrElse(
              Map("external_part_fetch_exception" -> externalResponse.values.failed.get.traceString))

            internalMap ++ externalMap
          } else {
            internalMap
          }

          applyDerivations(ts, internalResponse.request, baseMap)
        }

        val ctx = Metrics.Context(Metrics.Environment.JoinFetching)
        ctx.distribution("overall.latency.millis", System.currentTimeMillis() - ts)
        derivedResults
      }

    combinedResponsesF
      .map(_.iterator.map(logResponse(_, ts)).toSeq)
  }

  private def convertJoinFeaturesResponseToAvroBytes(features: Map[String, AnyRef],
                                                     joinName: String): Try[Array[Byte]] = {
    val startTime = System.currentTimeMillis()
    val ctx =
      Metrics.Context(Metrics.Environment.JoinSchemaFetching, join = joinName)
    val joinCodecTry = joinCodecCache(joinName)

    joinCodecTry.flatMap { joinCodec =>
      Try {
        val response = encode(joinCodec.valueSchema, joinCodec.valueCodec, features)
        ctx.distribution("avroconversionbytes.latency.millis", System.currentTimeMillis() - startTime)
        response
      }.recover { case exception =>
        logger.error(s"Failed to convert features to avro for $joinName", exception)
        throw exception
      }
    }
  }

  private def convertJoinFeaturesResponseToAvroString(features: Map[String, AnyRef], joinName: String): Try[String] = {
    val startTime = System.currentTimeMillis()
    val ctx =
      Metrics.Context(Metrics.Environment.JoinSchemaFetching, join = joinName)
    val joinCodecTry = joinCodecCache(joinName)

    joinCodecTry.flatMap { joinCodec =>
      Try {
        val avroBytes = encode(joinCodec.valueSchema, joinCodec.valueCodec, features)
        val avroString = java.util.Base64.getEncoder.encodeToString(avroBytes)
        ctx.distribution("avroconversionstring.latency.millis", System.currentTimeMillis() - startTime)
        avroString
      }.recover { case exception =>
        logger.error(s"Failed to convert features to avro for $joinName", exception)
        throw exception
      }
    }
  }


  private def convertResponseToResponseWithAvroBytes(response: Response): ResponseV2 = {
    ResponseV2(response.request, AvroResponseValue.AvroBytes(response.values.flatMap(
      v => {
        convertJoinFeaturesResponseToAvroBytes(v, response.request.name)
      }
    )))
  }

  private def convertResponseToResponseWithAvroString(response: Response): ResponseV2 = {
    ResponseV2(response.request, AvroResponseValue.AvroString(response.values.flatMap(
      v => {
        convertJoinFeaturesResponseToAvroString(v, response.request.name)
      }
    )))
  }
  def fetchJoinV2(requests: Seq[Request],
                  joinConf: Option[api.Join] = None,
                  responseType: ResponseType = ResponseType.Map): Future[Seq[ResponseV2]] = {
    val rawResponse = fetchJoin(requests, joinConf)

    responseType match {
      case ResponseType.WithAvroBytes => {
        rawResponse.map(_.iterator.map(convertResponseToResponseWithAvroBytes).toSeq)
      }
      case ResponseType.WithAvroString =>
        rawResponse.map(_.iterator.map(convertResponseToResponseWithAvroString).toSeq)
    }
  }

  private def applyDerivations(ts: Long, request: Request, baseMap: Map[String, AnyRef]): ResponseWithContext = {

    val derivationStartTs = System.currentTimeMillis()
    val joinName = request.name
    val ctx = Metrics.Context(Metrics.Environment.JoinFetching, join = joinName)
    val joinCodecTry = joinCodecCache(request.name)

    joinCodecTry match {
      case Success(joinCodec) =>
        ctx.distribution("derivation_codec.latency.millis", System.currentTimeMillis() - derivationStartTs)

        val derivedMapTry: Try[Map[String, AnyRef]] = Try {
          applyDeriveFunc(joinCodec.deriveFunc, request, baseMap)
        }

        val derivedMap: Map[String, AnyRef] = derivedMapTry match {
          case Success(derivedMap) => derivedMap
          case Failure(exception) =>
            ctx.incrementException(exception)

            val renameOnlyDerivedMapTry: Try[Map[String, AnyRef]] = Try {
              joinCodec
                .renameOnlyDeriveFunc(request.keys, baseMap)
                .mapValues(_.asInstanceOf[AnyRef])
                .toMap
            }

            val renameOnlyDerivedMap: Map[String, AnyRef] =
              renameOnlyDerivedMapTry match {
                case Success(renameOnlyDerivedMap) =>
                  renameOnlyDerivedMap
                case Failure(exception) =>
                  ctx.incrementException(exception)
                  Map(
                    "derivation_rename_exception" -> exception.traceString
                      .asInstanceOf[AnyRef])
              }

            val derivedExceptionMap: Map[String, AnyRef] =
              Map(
                "derivation_fetch_exception" -> exception.traceString
                  .asInstanceOf[AnyRef])

            renameOnlyDerivedMap ++ derivedExceptionMap
        }

        // Preserve exceptions from baseMap
        val baseMapExceptions = baseMap.filter(_._1.endsWith("_exception"))
        val finalizedDerivedMap = derivedMap ++ baseMapExceptions
        val requestEndTs = System.currentTimeMillis()
        ctx.distribution("derivation.latency.millis", requestEndTs - derivationStartTs)
        ctx.distribution("request.latency.millis", requestEndTs - ts)

        val response = ResponseWithContext(request, finalizedDerivedMap, baseMap)
        // Refresh joinCodec if it has partial failure
        if (joinCodec.hasPartialFailure) {
          joinCodecCache.refresh(joinName)
        }
        response

      case Failure(exception) =>
        // more validation logic will be covered in compile.py to avoid this case
        joinCodecCache.refresh(joinName)
        ctx.incrementException(exception)
        ResponseWithContext(request, Map("join_codec_fetch_exception" -> exception.traceString), Map.empty)

    }
  }

  private def encode(schema: StructType,
                     codec: AvroCodec,
                     dataMap: Map[String, AnyRef],
                     cast: Boolean = false,
                     tries: Int = 3): Array[Byte] = {
    def encodeOnce(schema: StructType,
                   codec: AvroCodec,
                   dataMap: Map[String, AnyRef],
                   cast: Boolean = false): Array[Byte] = {
      val data = schema.castArr(dataMap)
      val avroRecord =
        AvroConversions.fromChrononRow(data, schema, codec.schema).asInstanceOf[GenericRecord]
      codec.encodeBinary(avroRecord)
    }

    @tailrec
    def tryOnce(lastTry: Try[Array[Byte]], tries: Int): Try[Array[Byte]] = {

      if (tries == 0 || (lastTry != null && lastTry.isSuccess))
        return lastTry

      val binary = encodeOnce(schema, codec, dataMap, cast)

      tryOnce(Try(codec.decodeRow(binary)).map(_ => binary), tries - 1)
    }

    tryOnce(null, tries).get
  }

  private def logResponse(resp: ResponseWithContext, ts: Long): Response = {

    val joinCodecTry = joinCodecCache(resp.request.name)

    val loggingTry: Try[Unit] = joinCodecTry
      .map(codec => {
        val metaData = codec.conf.join.metaData
        val samplePercent = if (metaData.isSetSamplePercent) metaData.getSamplePercent else 0

        if (samplePercent > 0)
          encodeAndPublishLog(resp, ts, codec, samplePercent)

      })

    loggingTry.failed.map { exception =>
      // to handle GroupByServingInfo staleness that results in encoding failure
      joinCodecCache.refresh(resp.request.name)

      resp.request.context.foreach(
        _.incrementException(new RuntimeException(s"Logging failed due to: ${exception.traceString}", exception)))
    }

    if (joinCodecTry.isSuccess && joinCodecTry.get.hasPartialFailure) {
      joinCodecCache.refresh(resp.request.name)
    }

    Response(resp.request, Success(resp.derivedValues))
  }

  private def encodeAndPublishLog(resp: ResponseWithContext,
                                  ts: Long,
                                  codec: JoinCodec,
                                  samplePercent: Double): Unit = {

    val loggingStartTs = System.currentTimeMillis()
    val loggingTs = resp.request.atMillis.getOrElse(ts)

    val keyBytes = encode(codec.keySchema, codec.keyCodec, resp.request.keys, cast = true)

    val hash = if (samplePercent > 0) {
      Math.abs(HashUtils.md5Long(keyBytes))
    } else {
      -1
    }

    val shouldPublishLog = (hash > 0) && ((hash % (100 * 1000)) <= (samplePercent * 1000))

    if (shouldPublishLog || debug) {
      val values = if (codec.conf.join.logFullValues) {
        resp.combinedValues
      } else {
        resp.derivedValues
      }

      if (debug) {
        logger.info(s"Logging ${resp.request.keys} : ${hash % 100000}: $samplePercent")
        val gson = new Gson()
        val valuesFormatted =
          values.map { case (k, v) => s"$k -> ${gson.toJson(v)}" }.mkString(", ")
        logger.info(s"""Sampled join fetch
               |Key Map: ${resp.request.keys}
               |Value Map: [$valuesFormatted]
               |""".stripMargin)
      }

      val valueBytes = encode(codec.valueSchema, codec.valueCodec, values)

      val loggableResponse = LoggableResponse(
        keyBytes,
        valueBytes,
        resp.request.name,
        loggingTs,
        codec.loggingSchemaHash
      )

      if (logFunc != null) {
        logFunc.accept(loggableResponse)

        val joinContext = resp.request.context

        joinContext.foreach(context => context.increment("logging_request.count"))
        joinContext.foreach(context =>
          context.distribution("logging_request.latency.millis", System.currentTimeMillis() - loggingStartTs))
        joinContext.foreach(context =>
          context.distribution("logging_request.overall.latency.millis", System.currentTimeMillis() - ts))

        if (debug) {
          logger.info(s"Logged data with schema_hash ${codec.loggingSchemaHash}")
        }
      }
    }
  }

  // Pulling external features in a batched fashion across services in-parallel
  private def fetchExternal(joinRequests: Seq[Request]): Future[Seq[Response]] = {

    val startTime = System.currentTimeMillis()
    val resultMap = new mutable.LinkedHashMap[Request, Try[mutable.HashMap[String, Any]]]
    var invalidCount = 0
    val validRequests = new ListBuffer[Request]

    // step-1 handle invalid requests and collect valid ones
    joinRequests.foreach { request =>
      val joinName = request.name
      val joinConfTry: Try[JoinOps] = metadataStore.getJoinConf(request.name)
      if (joinConfTry.isFailure) {
        metadataStore.getJoinConf.refresh(request.name)
        resultMap.update(
          request,
          Failure(
            new IllegalArgumentException(
              s"Failed to fetch join conf for $joinName. Please ensure metadata upload succeeded",
              joinConfTry.failed.get))
        )
        invalidCount += 1
      } else if (joinConfTry.get.join.onlineExternalParts == null) {
        resultMap.update(request, Success(mutable.HashMap.empty[String, Any]))
      } else {
        resultMap.update(request, Success(mutable.HashMap.empty[String, Any]))
        validRequests.append(request)
      }
    }

    // early exit if no external requests detected
    if (validRequests.isEmpty) { return Future.successful(null) }

    // step-2 dedup external requests across joins
    val externalToJoinRequests: Seq[ExternalToJoinRequest] = validRequests
      .flatMap { joinRequest =>
        val joinConf = metadataStore.getJoinConf(joinRequest.name)
        if (joinConf.isFailure) {
          metadataStore.getJoinConf.refresh(joinRequest.name)
        }
        val parts =
          metadataStore
            .getJoinConf(joinRequest.name)
            .get
            .join
            .onlineExternalParts // cheap since it is cached, valid since step-1

        parts.iterator().asScala.map { part =>
          val externalRequest = Try(part.applyMapping(joinRequest.keys)) match {
            case Success(mappedKeys)                     => Left(Request(part.source.metadata.name, mappedKeys))
            case Failure(exception: KeyMissingException) => Right(exception)
            case Failure(otherException)                 => throw otherException
          }
          ExternalToJoinRequest(externalRequest, joinRequest, part)
        }

      }

    val validExternalRequestToJoinRequestMap = externalToJoinRequests
      .filter(_.externalRequest.isLeft)
      .groupBy(_.externalRequest.left.get)
      .mapValues(_.toSeq)
      .toMap

    val context =
      Metrics.Context(
        environment = Metrics.Environment.JoinFetching,
        join = validRequests.iterator.map(_.name.sanitize).toSeq.distinct.mkString(",")
      )
    context.distribution("response.external_pre_processing.latency", System.currentTimeMillis() - startTime)
    context.count("response.external_invalid_joins.count", invalidCount)
    val responseFutures =
      externalSourceRegistry.fetchRequests(validExternalRequestToJoinRequestMap.keys.toSeq, context)

    // step-3 walk the response, find all the joins to update and the result map
    responseFutures.map { responses =>
      responses.foreach { response =>
        val responseTry: Try[Map[String, Any]] = response.values
        val joinsToUpdate: Seq[ExternalToJoinRequest] =
          validExternalRequestToJoinRequestMap(response.request)

        joinsToUpdate.foreach { externalToJoin =>
          val resultValueMap: mutable.HashMap[String, Any] =
            resultMap(externalToJoin.joinRequest).get
          val prefix = externalToJoin.part.fullName + "_"
          responseTry match {
            case Failure(exception) =>
              resultValueMap.update(prefix + "exception", exception)
              externalToJoin.context.incrementException(exception)
            case Success(responseMap) =>
              externalToJoin.context.count("response.value_count", responseMap.size)
              responseMap.foreach { case (name, value) =>
                resultValueMap.update(prefix + name, value)
              }
          }
        }
      }

      externalToJoinRequests
        .filter(_.externalRequest.isRight)
        .foreach(externalToJoin => {

          val resultValueMap: mutable.HashMap[String, Any] =
            resultMap(externalToJoin.joinRequest).get
          val KeyMissingException = externalToJoin.externalRequest.right.get
          resultValueMap.update(externalToJoin.part.fullName + "_" + "exception", KeyMissingException)
          externalToJoin.context.incrementException(KeyMissingException)

        })

      // step-4 convert the resultMap into Responses
      joinRequests.map { req =>
        Metrics
          .Context(Metrics.Environment.JoinFetching, join = req.name)
          .distribution("external.latency.millis", System.currentTimeMillis() - startTime)
        Response(req, resultMap(req).map(_.mapValues(_.asInstanceOf[AnyRef]).toMap))
      }
    }
  }

  def fetchJoinSchema(joinName: String): Try[JoinSchemaResponse] = {
    val startTime = System.currentTimeMillis()
    val ctx =
      Metrics.Context(Metrics.Environment.JoinSchemaFetching, join = joinName)

    val joinCodecTry = joinCodecCache(joinName)

    val joinSchemaResponse = joinCodecTry
      .map { joinCodec =>
        val response = JoinSchemaResponse(joinName,
                                          joinCodec.keyCodec.schemaStr,
                                          joinCodec.valueCodec.schemaStr,
                                          joinCodec.loggingSchemaHash,
                                          joinCodec.valueInfos.toArray)
        if (joinCodec.hasPartialFailure) {
          joinCodecCache.refresh(joinName)
        }
        ctx.distribution("response.latency.millis", System.currentTimeMillis() - startTime)
        response
      }
      .recover { case exception =>
        logger.error(s"Failed to fetch join schema for $joinName", exception)
        ctx.incrementException(exception)
        throw exception
      }

    joinSchemaResponse
  }

  private def logControlEvent(encTry: Try[JoinCodec]): Unit = {
    if (encTry.isFailure) return

    val enc = encTry.get
    val ts = System.currentTimeMillis()
    val controlEvent = LoggableResponse(
      enc.loggingSchemaHash.getBytes(UTF8),
      enc.loggingSchema.getBytes(UTF8),
      Constants.SchemaPublishEvent,
      ts,
      null
    )
    if (logFunc != null) {
      logFunc.accept(controlEvent)
      if (debug) {
        logger.info(s"schema data logged successfully with schema_hash ${enc.loggingSchemaHash}")
      }
    }
  }

  private case class ExternalToJoinRequest(externalRequest: Either[Request, KeyMissingException],
                                           joinRequest: Request,
                                           part: ExternalPart) {

    lazy val context: Metrics.Context =
      Metrics.Context(Metrics.Environment.JoinFetching, join = joinRequest.name, groupBy = part.fullName)
  }
}
