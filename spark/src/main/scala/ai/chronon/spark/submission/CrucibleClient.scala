package ai.chronon.spark.submission

import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.{WebClient, WebClientOptions}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.{CompletableFuture, TimeUnit}

/** Exception thrown when Crucible API calls fail */
case class CrucibleApiException(message: String, statusCode: Option[Int] = None, cause: Throwable = null)
    extends RuntimeException(message, cause)

/** HTTP client for Crucible REST API.
  *
  * @param baseUrl The base URL of the Crucible gateway (e.g., "http://crucible.crucible-system:8080")
  * @param namespace The Crucible namespace to submit jobs into
  */
class CrucibleClient(val baseUrl: String, val namespace: String) {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private val timeoutSeconds = 60L

  private val client: WebClient = {
    val vertx = Vertx.vertx()
    val options = new WebClientOptions()
      .setConnectTimeout(10000)
      .setIdleTimeout(60)
    WebClient.create(vertx, options)
  }

  private val apiBase = s"/api/v1/namespaces/$namespace"

  /** Submit a job to Crucible.
    *
    * @param body JSON request body
    * @return The job ID from the response
    */
  def submitJob(body: JsonObject): String = {
    val uri = s"$apiBase/jobs"
    logger.info(s"Submitting job to Crucible: ${body.getString("name", "unnamed")} type=${body.getString("type")}")

    val future = new CompletableFuture[String]()

    client
      .postAbs(s"$baseUrl$uri")
      .putHeader("Content-Type", "application/json")
      .sendJsonObject(
        body,
        ar => {
          if (ar.succeeded()) {
            val response = ar.result()
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
              val respBody = new JsonObject(response.bodyAsString())
              val jobId = respBody.getString("id")
              logger.info(s"Job submitted successfully. ID: $jobId")
              future.complete(jobId)
            } else {
              val errorMsg = s"Failed to submit job: HTTP ${response.statusCode()} - ${response.bodyAsString()}"
              logger.error(errorMsg)
              future.completeExceptionally(CrucibleApiException(errorMsg, Some(response.statusCode())))
            }
          } else {
            val errorMsg = s"Failed to submit job: ${ar.cause().getMessage}"
            logger.error(errorMsg, ar.cause())
            future.completeExceptionally(CrucibleApiException(errorMsg, cause = ar.cause()))
          }
        }
      )

    try {
      future.get(timeoutSeconds, TimeUnit.SECONDS)
    } catch {
      case e: java.util.concurrent.ExecutionException =>
        throw Option(e.getCause).getOrElse(e)
    }
  }

  /** Get job status from Crucible.
    *
    * @param jobId The job ID
    * @return (status string, HTTP status code)
    */
  def getJobStatus(jobId: String): (String, Int) = {
    val uri = s"$apiBase/jobs/$jobId"
    val future = new CompletableFuture[(String, Int)]()

    client
      .getAbs(s"$baseUrl$uri")
      .send(ar => {
        if (ar.succeeded()) {
          val response = ar.result()
          if (response.statusCode() == 200) {
            val respBody = new JsonObject(response.bodyAsString())
            val status = respBody.getString("status", "UNKNOWN")
            future.complete((status, 200))
          } else {
            future.complete(("NOT_FOUND", response.statusCode()))
          }
        } else {
          val errorMsg = s"Failed to get job status: ${ar.cause().getMessage}"
          logger.error(errorMsg, ar.cause())
          future.completeExceptionally(CrucibleApiException(errorMsg, cause = ar.cause()))
        }
      })

    try {
      future.get(timeoutSeconds, TimeUnit.SECONDS)
    } catch {
      case e: java.util.concurrent.ExecutionException =>
        throw Option(e.getCause).getOrElse(e)
    }
  }

  /** Kill a job via Crucible.
    *
    * @param jobId The job ID
    */
  def killJob(jobId: String): Unit = {
    val uri = s"$apiBase/jobs/$jobId"
    logger.info(s"Killing job $jobId")

    val future = new CompletableFuture[Unit]()

    client
      .deleteAbs(s"$baseUrl$uri")
      .send(ar => {
        if (ar.succeeded()) {
          logger.info(s"Kill request sent for job $jobId: HTTP ${ar.result().statusCode()}")
          future.complete(())
        } else {
          val errorMsg = s"Failed to kill job: ${ar.cause().getMessage}"
          logger.error(errorMsg, ar.cause())
          future.completeExceptionally(CrucibleApiException(errorMsg, cause = ar.cause()))
        }
      })

    try {
      future.get(timeoutSeconds, TimeUnit.SECONDS)
    } catch {
      case e: java.util.concurrent.ExecutionException =>
        throw Option(e.getCause).getOrElse(e)
    }
  }

  def close(): Unit = {
    client.close()
  }
}
