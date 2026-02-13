package ai.chronon.integrations.aws.dynamo_load

import ai.chronon.integrations.aws.AwsApiImpl
import ai.chronon.online.KVStore
import ai.chronon.online.kv_load.{DataLoader, PerfTestConstants, TrafficDriver}
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.{Logger, LoggerFactory}

/** ScalaTest entry point for the DynamoDB perf-test framework.
  *
  * Both test cases are gated behind CHRONON_PERF_TEST_ENABLED=true; when that
  * env var is absent the tests are *canceled* (not failed), so a normal
  * `./mill cloud_aws.test` run is unaffected.
  *
  * Environment variables:
  *   CHRONON_PERF_TEST_ENABLED   must be "true" to run
  *   AWS_DEFAULT_REGION          DynamoDB region (falls back to SDK default chain)
  *   DYNAMO_ENDPOINT             optional endpoint override (e.g. for local testing)
  *   PERF_TILE_SIZE_MS           300000 (5 min) or 3600000 (1 hr).  Default: 3600000
  *   PERF_SKIP_LOAD              set to "true" to skip phase 1 (data already present)
  *   PERF_DURATION_MINUTES       minutes of traffic per time range.  Default: 15
  *   PERF_NUM_THREADS            concurrent multiGet threads per time range.  Default: 4
  */
class DynamoDBPerfTestHarness extends AnyFlatSpec {

  private val logger: Logger = LoggerFactory.getLogger(classOf[DynamoDBPerfTestHarness])

  // --- Opt-in gate ---------------------------------------------------------
  private val perfTestEnabled: Boolean =
    sys.env.getOrElse(PerfTestConstants.ENABLED_ENV_VAR, "false").toLowerCase == "true"

  private lazy val awsApi: AwsApiImpl = new AwsApiImpl(Map.empty)

  private lazy val kvStore: KVStore = awsApi.genKvStore

  // --- Configurable parameters ---------------------------------------------
  private val tileSizeMs: Long =
    sys.env.getOrElse("PERF_TILE_SIZE_MS", "3600000").toLong

  private val skipLoad: Boolean =
    sys.env.getOrElse("PERF_SKIP_LOAD", "false").toLowerCase == "true"

  private val durationMinutes: Int =
    sys.env.getOrElse("PERF_DURATION_MINUTES", "5").toInt

  private val numThreads: Int =
    sys.env.getOrElse("PERF_NUM_THREADS", "10").toInt

  // --- Phase 1: data load --------------------------------------------------
  // Declared first — AnyFlatSpec runs tests in declaration order.
  "DynamoDB perf test" should "load test data" in {
    assume(perfTestEnabled, "Set CHRONON_PERF_TEST_ENABLED=true to run perf tests")
    if (skipLoad) {
      logger.info("PERF_SKIP_LOAD=true — skipping data load phase")
    } else {
      val loader = new DataLoader(kvStore, tileSizeMs)
      loader.run()
    }
  }

  // --- Phase 2: traffic driver ---------------------------------------------
  it should "drive read traffic and report latencies" in {
    assume(perfTestEnabled, "Set CHRONON_PERF_TEST_ENABLED=true to run perf tests")
    val driver = new TrafficDriver(kvStore, tileSizeMs, durationMinutes, numThreads)
    driver.run()
  }
}
