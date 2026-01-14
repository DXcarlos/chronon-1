package ai.chronon.spark.submission

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.beans.BeanProperty

/** Authentication configuration for Kyuubi REST API */
sealed trait KyuubiAuth

object KyuubiAuth {
  case object NoAuth extends KyuubiAuth
  case class BasicAuth(username: String, password: String) extends KyuubiAuth
  case class BearerToken(token: String) extends KyuubiAuth
  case class CustomHeader(headerName: String, headerValue: String) extends KyuubiAuth
}

/** Request body for batch job submission to Kyuubi REST API.
  *
  * Maps to POST /api/v1/batches request body.
  */
@JsonIgnoreProperties(ignoreUnknown = true)
case class BatchSubmitRequest(
    @BeanProperty @JsonProperty("batchType") batchType: String,
    @BeanProperty @JsonProperty("resource") resource: String,
    @BeanProperty @JsonProperty("className") className: String,
    @BeanProperty @JsonProperty("name") name: Option[String] = None,
    @BeanProperty @JsonProperty("args") args: Seq[String] = Seq.empty,
    @BeanProperty @JsonProperty("conf") conf: Map[String, String] = Map.empty,
    @BeanProperty @JsonProperty("proxyUser") proxyUser: Option[String] = None
)

/** Response from batch job submission.
  *
  * Maps to POST /api/v1/batches response body.
  */
@JsonIgnoreProperties(ignoreUnknown = true)
case class BatchSubmitResponse(
    @BeanProperty @JsonProperty("id") id: String,
    @BeanProperty @JsonProperty("user") user: Option[String],
    @BeanProperty @JsonProperty("batchType") batchType: String,
    @BeanProperty @JsonProperty("name") name: Option[String],
    @BeanProperty @JsonProperty("state") state: String,
    @BeanProperty @JsonProperty("createTime") createTime: Long,
    @BeanProperty @JsonProperty("endTime") endTime: Option[Long]
)

/** Response from batch status query.
  *
  * Maps to GET /api/v1/batches/{batchId} response body.
  * Extends BatchSubmitResponse with additional application-specific fields.
  */
@JsonIgnoreProperties(ignoreUnknown = true)
case class BatchStatusResponse(
    @BeanProperty @JsonProperty("id") id: String,
    @BeanProperty @JsonProperty("user") user: Option[String],
    @BeanProperty @JsonProperty("batchType") batchType: String,
    @BeanProperty @JsonProperty("name") name: Option[String],
    @BeanProperty @JsonProperty("state") state: String,
    @BeanProperty @JsonProperty("createTime") createTime: Long,
    @BeanProperty @JsonProperty("endTime") endTime: Option[Long],
    @BeanProperty @JsonProperty("appId") appId: Option[String],
    @BeanProperty @JsonProperty("appUrl") appUrl: Option[String],
    @BeanProperty @JsonProperty("appState") appState: Option[String],
    @BeanProperty @JsonProperty("appDiagnostic") appDiagnostic: Option[String]
)

/** Summary information for a batch job.
  *
  * Used in list responses.
  */
@JsonIgnoreProperties(ignoreUnknown = true)
case class BatchSummary(
    @BeanProperty @JsonProperty("id") id: String,
    @BeanProperty @JsonProperty("user") user: Option[String],
    @BeanProperty @JsonProperty("batchType") batchType: String,
    @BeanProperty @JsonProperty("name") name: Option[String],
    @BeanProperty @JsonProperty("state") state: String,
    @BeanProperty @JsonProperty("createTime") createTime: Long
)

/** Response from list batches query.
  *
  * Maps to GET /api/v1/batches response body.
  */
@JsonIgnoreProperties(ignoreUnknown = true)
case class BatchListResponse(
    @BeanProperty @JsonProperty("from") from: Int,
    @BeanProperty @JsonProperty("total") total: Int,
    @BeanProperty @JsonProperty("batches") batches: Seq[BatchSummary]
)

/** Shared Jackson ObjectMapper for JSON serialization/deserialization */
object KyuubiJsonMapper {
  val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m
  }

  def toJson[T](obj: T): String = mapper.writeValueAsString(obj)

  def fromJson[T](json: String, clazz: Class[T]): T = mapper.readValue(json, clazz)
}
