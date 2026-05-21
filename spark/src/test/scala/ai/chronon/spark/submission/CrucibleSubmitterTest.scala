package ai.chronon.spark.submission

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

class CrucibleSubmitterTest extends AnyFlatSpec with Matchers {

  it should "return Spark history URL from Crucible job status when available" in {
    withCrucibleJobResponse(
      """{"status":"COMPLETED","sparkApplicationId":"spark-app-1","historyUrl":"https://crucible.example.com/spark-history/history/spark-app-1/jobs/"}"""
    ) { baseUrl =>
      val submitter = new CrucibleSubmitter(
        baseUrl = baseUrl,
        namespace = "test-ns",
        sparkImage = "spark-image",
        flinkImage = "flink-image"
      )
      try {
        submitter.getSparkUrl("job-1") shouldBe Some(
          "https://crucible.example.com/spark-history/history/spark-app-1/jobs/")
      } finally {
        submitter.close()
      }
    }
  }

  it should "fall back to Crucible live Spark UI URL when Spark history URL is unavailable" in {
    withCrucibleJobResponse("""{"status":"RUNNING"}""") { baseUrl =>
      val submitter = new CrucibleSubmitter(
        baseUrl = baseUrl,
        namespace = "test-ns",
        sparkImage = "spark-image",
        flinkImage = "flink-image"
      )
      try {
        submitter.getSparkUrl("job-1") shouldBe Some(s"$baseUrl/jobs/test-ns/job-1/ui")
      } finally {
        submitter.close()
      }
    }
  }

  it should "return Crucible live Flink UI URL under the configured base URL" in {
    val submitter = new CrucibleSubmitter(
      baseUrl = "https://crucible.example.com/spark-history",
      namespace = "test-ns",
      sparkImage = "spark-image",
      flinkImage = "flink-image"
    )

    submitter.getFlinkUrl("flink-job-1") shouldBe Some(
      "https://crucible.example.com/spark-history/flink/test-ns/flink-job-1/ui")
  }

  private def withCrucibleJobResponse(responseJson: String)(test: String => Unit): Unit = {
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext(
      "/api/v1/namespaces/test-ns/jobs/job-1",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          val response = responseJson.getBytes(StandardCharsets.UTF_8)
          exchange.getResponseHeaders.add("Content-Type", "application/json")
          exchange.sendResponseHeaders(200, response.length.toLong)
          val body = exchange.getResponseBody
          try {
            body.write(response)
          } finally {
            body.close()
          }
        }
      }
    )
    server.start()
    try {
      test(s"http://127.0.0.1:${server.getAddress.getPort}")
    } finally {
      server.stop(0)
    }
  }
}
