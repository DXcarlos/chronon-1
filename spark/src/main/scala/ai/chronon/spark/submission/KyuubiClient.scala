package ai.chronon.spark.submission

import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.{JsonArray, JsonObject}
import io.vertx.ext.web.client.{HttpResponse, WebClient, WebClientOptions}
import org.slf4j.{Logger, LoggerFactory}

import java.util.Base64
import java.util.concurrent.{CompletableFuture, TimeUnit}
import scala.jdk.CollectionConverters._

/** Exception thrown when Kyuubi API calls fail */
case class KyuubiApiException(message: String, statusCode: Option[Int] = None, cause: Throwable = null)
    extends RuntimeException(message, cause)

/** HTTP client for Kyuubi REST API interactions using Vertx WebClient.
  *
  * @param baseUrl The base URL of the Kyuubi server (e.g., "http://kyuubi-server:10099")
  * @param auth Authentication configuration
  * @param client Optional custom WebClient instance
  */
class KyuubiClient(
    val baseUrl: String,
    val auth: KyuubiAuth = KyuubiAuth.NoAuth,
    client: WebClient
) {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private val apiBasePath = "/api/v1"
  private val timeoutSeconds = 5L // 5 minute timeout for operations

  /** Submit a batch job to Kyuubi.
    *
    * @param request The batch submit request
    * @return The batch submit response containing the job ID
    * @throws KyuubiApiException if the submission fails
    */
  def submitBatch(request: BatchSubmitRequest): BatchSubmitResponse = {
    val uri = s"$apiBasePath/batches"
    val requestBody = toVertxJson(request)

    logger.info(
      s"Submitting batch job to Kyuubi: ${request.name.getOrElse("unnamed")} " +
        s"type=${request.batchType} class=${request.className}"
    )
    logger.debug(s"Request body: ${requestBody.encode()}")

    val future = new CompletableFuture[BatchSubmitResponse]()

    val httpRequest = client.postAbs(s"$baseUrl$uri")
    applyAuth(httpRequest)
      .putHeader("Content-Type", "application/json")
      .sendJsonObject(
        requestBody,
        ar => {
          if (ar.succeeded()) {
            val response = ar.result()
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
              val batchResponse = parseResponse(response.bodyAsString(), classOf[BatchSubmitResponse])
              logger.info(s"Batch job submitted successfully. ID: ${batchResponse.id}, State: ${batchResponse.state}")
              future.complete(batchResponse)
            } else {
              val errorMsg =
                s"Failed to submit batch job: HTTP ${response.statusCode()} - ${response.bodyAsString()}"
              logger.error(errorMsg)
              future.completeExceptionally(KyuubiApiException(errorMsg, Some(response.statusCode())))
            }
          } else {
            val errorMsg = s"Failed to submit batch job: ${ar.cause().getMessage}"
            logger.error(errorMsg, ar.cause())
            future.completeExceptionally(KyuubiApiException(errorMsg, cause = ar.cause()))
          }
        }
      )

    try {
      future.get(timeoutSeconds, TimeUnit.SECONDS)
    } catch {
      case e: java.util.concurrent.ExecutionException =>
        throw e.getCause match {
          case ke: KyuubiApiException => ke
          case other                  => KyuubiApiException(s"Request failed: ${other.getMessage}", cause = other)
        }
      case e: java.util.concurrent.TimeoutException =>
        throw KyuubiApiException(s"Request timed out after $timeoutSeconds seconds", cause = e)
    }
  }

  /** Get the status of a batch job.
    *
    * @param batchId The batch job ID
    * @return The batch status response
    * @throws KyuubiApiException if the status query fails
    */
  def getBatchStatus(batchId: String): BatchStatusResponse = {
    val uri = s"$apiBasePath/batches/$batchId"

    logger.debug(s"Getting status for batch job: $batchId")

    val future = new CompletableFuture[BatchStatusResponse]()

    val httpRequest = client.getAbs(s"$baseUrl$uri")
    applyAuth(httpRequest)
      .send(ar => {
        if (ar.succeeded()) {
          val response = ar.result()
          if (response.statusCode() >= 200 && response.statusCode() < 300) {
            val statusResponse = parseResponse(response.bodyAsString(), classOf[BatchStatusResponse])
            logger.debug(s"Batch $batchId state: ${statusResponse.state}")
            future.complete(statusResponse)
          } else {
            val errorMsg =
              s"Failed to get batch status for $batchId: HTTP ${response.statusCode()} - ${response.bodyAsString()}"
            logger.error(errorMsg)
            future.completeExceptionally(KyuubiApiException(errorMsg, Some(response.statusCode())))
          }
        } else {
          val errorMsg = s"Failed to get batch status: ${ar.cause().getMessage}"
          logger.error(errorMsg, ar.cause())
          future.completeExceptionally(KyuubiApiException(errorMsg, cause = ar.cause()))
        }
      })

    try {
      future.get(timeoutSeconds, TimeUnit.SECONDS)
    } catch {
      case e: java.util.concurrent.ExecutionException =>
        throw e.getCause match {
          case ke: KyuubiApiException => ke
          case other                  => KyuubiApiException(s"Request failed: ${other.getMessage}", cause = other)
        }
      case e: java.util.concurrent.TimeoutException =>
        throw KyuubiApiException(s"Request timed out after $timeoutSeconds seconds", cause = e)
    }
  }

  /** Delete/kill a batch job.
    *
    * @param batchId The batch job ID to kill
    * @throws KyuubiApiException if the delete operation fails
    */
  def deleteBatch(batchId: String): Unit = {
    val uri = s"$apiBasePath/batches/$batchId"

    logger.info(s"Deleting/killing batch job: $batchId")

    val future = new CompletableFuture[Unit]()

    val httpRequest = client.deleteAbs(s"$baseUrl$uri")
    applyAuth(httpRequest)
      .send(ar => {
        if (ar.succeeded()) {
          val response = ar.result()
          if (response.statusCode() >= 200 && response.statusCode() < 300) {
            logger.info(s"Batch $batchId deleted successfully")
            future.complete(())
          } else {
            val errorMsg =
              s"Failed to delete batch $batchId: HTTP ${response.statusCode()} - ${response.bodyAsString()}"
            logger.error(errorMsg)
            future.completeExceptionally(KyuubiApiException(errorMsg, Some(response.statusCode())))
          }
        } else {
          val errorMsg = s"Failed to delete batch: ${ar.cause().getMessage}"
          logger.error(errorMsg, ar.cause())
          future.completeExceptionally(KyuubiApiException(errorMsg, cause = ar.cause()))
        }
      })

    try {
      future.get(timeoutSeconds, TimeUnit.SECONDS)
    } catch {
      case e: java.util.concurrent.ExecutionException =>
        throw e.getCause match {
          case ke: KyuubiApiException => ke
          case other                  => KyuubiApiException(s"Request failed: ${other.getMessage}", cause = other)
        }
      case e: java.util.concurrent.TimeoutException =>
        throw KyuubiApiException(s"Request timed out after $timeoutSeconds seconds", cause = e)
    }
  }

  /** List batch jobs with optional filters.
    *
    * @param batchType Optional filter by batch type (SPARK, FLINK)
    * @param batchState Optional filter by batch state (PENDING, RUNNING, FINISHED, ERROR, KILLED)
    * @param batchUser Optional filter by user
    * @param batchName Optional filter by name
    * @param from Starting index for pagination
    * @param size Number of results to return
    * @return The batch list response
    * @throws KyuubiApiException if the list operation fails
    */
  def listBatches(
      batchType: Option[String] = None,
      batchState: Option[String] = None,
      batchUser: Option[String] = None,
      batchName: Option[String] = None,
      from: Int = 0,
      size: Int = 100
  ): BatchListResponse = {
    val queryParams = Seq(
      Some(s"from=$from"),
      Some(s"size=$size"),
      batchType.map(t => s"batchType=$t"),
      batchState.map(s => s"batchState=$s"),
      batchUser.map(u => s"batchUser=$u"),
      batchName.map(n => s"batchName=$n")
    ).flatten.mkString("&")

    val uri = s"$apiBasePath/batches?$queryParams"

    logger.debug(s"Listing batches with filters: type=$batchType, state=$batchState, user=$batchUser, name=$batchName")

    val future = new CompletableFuture[BatchListResponse]()

    val httpRequest = client.getAbs(s"$baseUrl$uri")
    applyAuth(httpRequest)
      .send(ar => {
        if (ar.succeeded()) {
          val response = ar.result()
          if (response.statusCode() >= 200 && response.statusCode() < 300) {
            val listResponse = parseResponse(response.bodyAsString(), classOf[BatchListResponse])
            logger.debug(s"Found ${listResponse.total} batches")
            future.complete(listResponse)
          } else {
            val errorMsg = s"Failed to list batches: HTTP ${response.statusCode()} - ${response.bodyAsString()}"
            logger.error(errorMsg)
            future.completeExceptionally(KyuubiApiException(errorMsg, Some(response.statusCode())))
          }
        } else {
          val errorMsg = s"Failed to list batches: ${ar.cause().getMessage}"
          logger.error(errorMsg, ar.cause())
          future.completeExceptionally(KyuubiApiException(errorMsg, cause = ar.cause()))
        }
      })

    try {
      future.get(timeoutSeconds, TimeUnit.SECONDS)
    } catch {
      case e: java.util.concurrent.ExecutionException =>
        throw e.getCause match {
          case ke: KyuubiApiException => ke
          case other                  => KyuubiApiException(s"Request failed: ${other.getMessage}", cause = other)
        }
      case e: java.util.concurrent.TimeoutException =>
        throw KyuubiApiException(s"Request timed out after $timeoutSeconds seconds", cause = e)
    }
  }

  /** Apply authentication headers to a request.
    *
    * @param request The HTTP request
    * @return The request with authentication applied
    */
  private def applyAuth(
      request: io.vertx.ext.web.client.HttpRequest[Buffer]
  ): io.vertx.ext.web.client.HttpRequest[Buffer] = {
    auth match {
      case KyuubiAuth.NoAuth => request
      case KyuubiAuth.BasicAuth(username, password) =>
        val credentials = Base64.getEncoder.encodeToString(s"$username:$password".getBytes("UTF-8"))
        request.putHeader("Authorization", s"Basic $credentials")
      case KyuubiAuth.BearerToken(token) =>
        request.putHeader("Authorization", s"Bearer $token")
      case KyuubiAuth.CustomHeader(headerName, headerValue) =>
        request.putHeader(headerName, headerValue)
    }
  }

  /** Convert a BatchSubmitRequest to Vertx JsonObject */
  private def toVertxJson(request: BatchSubmitRequest): JsonObject = {
    val json = new JsonObject()
      .put("batchType", request.batchType)
      .put("resource", request.resource)
      .put("className", request.className)

    request.name.foreach(n => json.put("name", n))
    request.proxyUser.foreach(u => json.put("proxyUser", u))

    if (request.args.nonEmpty) {
      val argsArray = new JsonArray()
      request.args.foreach(arg => argsArray.add(arg))
      json.put("args", argsArray)
    }

    if (request.conf.nonEmpty) {
      val confObj = new JsonObject()
      request.conf.foreach { case (k, v) => confObj.put(k, v) }
      json.put("conf", confObj)
    }

    json
  }

  /** Parse JSON response to a case class */
  private def parseResponse[T](json: String, clazz: Class[T]): T = {
    KyuubiJsonMapper.fromJson(json, clazz)
  }

  /** Close the client and release resources. */
  def close(): Unit = {
    logger.debug("KyuubiClient close() called")
    client.close()
  }
}

object KyuubiClient {

  /** Factory method to create a KyuubiClient.
    *
    * @param baseUrl The base URL of the Kyuubi server
    * @param auth Authentication configuration
    * @return A new KyuubiClient instance
    */
  def apply(baseUrl: String, auth: KyuubiAuth = KyuubiAuth.NoAuth): KyuubiClient = {
    val vertx = Vertx.vertx()
    val options = new WebClientOptions()
      .setConnectTimeout(30000) // 30 second connection timeout
      .setIdleTimeout(300) // 5 minute idle timeout
    val client = WebClient.create(vertx, options)
    new KyuubiClient(baseUrl, auth, client)
  }

  /** Create a KyuubiClient with an existing WebClient.
    *
    * @param baseUrl The base URL of the Kyuubi server
    * @param auth Authentication configuration
    * @param client The WebClient instance to use
    * @return A new KyuubiClient instance
    */
  def apply(baseUrl: String, auth: KyuubiAuth, client: WebClient): KyuubiClient = {
    new KyuubiClient(baseUrl, auth, client)
  }

}
