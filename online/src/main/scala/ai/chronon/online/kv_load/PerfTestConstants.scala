package ai.chronon.online.kv_load

import java.nio.charset.StandardCharsets
import java.time.Instant

object PerfTestConstants {

  // _STREAMING suffix causes most KVStore implementations to treat this as a tiled streaming table.
  val DATASET: String = "PERF_TEST_STREAMING"

  // DataLoader and TrafficDriver index into the same sequence so partition-key bytes are identical on write/read.
  val ENTITY_KEYS: Seq[Array[Byte]] =
    (1 to 100).map(i => s"user_id=$i".getBytes(StandardCharsets.UTF_8))

  val RANGE_START_MS: Long = Instant.parse("2026-02-01T00:00:00Z").toEpochMilli
  val RANGE_END_MS: Long = Instant.parse("2026-02-06T00:00:00Z").toEpochMilli

  val TILE_SIZE_5MIN: Long = 300000L
  val TILE_SIZE_1HR: Long = 3600000L

  // All windows open at 2026-02-04 00:00 UTC (day 4 of the loaded dataset).
  // Iteration order matters for the report.
  private val TRAFFIC_START_MS: Long = Instant.parse("2026-02-04T00:00:00Z").toEpochMilli

  val TRAFFIC_RANGES: Seq[(String, (Long, Long))] = Seq(
    "6h" -> (TRAFFIC_START_MS, Instant.parse("2026-02-04T06:00:00Z").toEpochMilli),
    "12h" -> (TRAFFIC_START_MS, Instant.parse("2026-02-04T12:00:00Z").toEpochMilli),
    "18h" -> (TRAFFIC_START_MS, Instant.parse("2026-02-04T18:00:00Z").toEpochMilli),
    "24h" -> (TRAFFIC_START_MS, Instant.parse("2026-02-05T00:00:00Z").toEpochMilli),
    "30h" -> (TRAFFIC_START_MS, Instant.parse("2026-02-05T06:00:00Z").toEpochMilli)
  )

  val PUT_BATCH_SIZE: Int = 200

  // Tests are canceled (not failed) when this env var is absent.
  val ENABLED_ENV_VAR: String = "CHRONON_PERF_TEST_ENABLED"
}
