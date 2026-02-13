package ai.chronon.online.kv_load

import ai.chronon.api.TilingUtils
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.PutRequest
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.Random

class DataLoader(kvStore: KVStore, tileSizeMs: Long) {

  private val logger: Logger = LoggerFactory.getLogger(classOf[DataLoader])

  def run(): Unit = {
    createTable()
    loadTiles()
  }

  private def createTable(): Unit = {
    logger.info(s"Creating table ${PerfTestConstants.DATASET} (idempotent)")
    kvStore.create(PerfTestConstants.DATASET, Map("is-time-sorted" -> "true"))
    logger.info("Table ready")
  }

  private def loadTiles(): Unit = {
    // Deterministic seed for repeatable payloads across runs (enables before/after comparisons).
    val rng = new Random(42)

    val tileStarts: Seq[Long] =
      (PerfTestConstants.RANGE_START_MS until PerfTestConstants.RANGE_END_MS by tileSizeMs).toSeq

    val totalPossiblePuts = PerfTestConstants.ENTITY_KEYS.size * tileStarts.size
    val is5MinTiles = tileSizeMs == PerfTestConstants.TILE_SIZE_5MIN

    logger.info(
      s"Starting load: ${PerfTestConstants.ENTITY_KEYS.size} entities x ${tileStarts.size} tiles " +
        s"= $totalPossiblePuts ${if (is5MinTiles) "possible puts (5min tiles: ~20% will be empty/skipped, 100-1000 byte payloads)"
          else "total puts (1hr tiles: 500-2000 byte payloads)"} " +
        s"(tileSizeMs=$tileSizeMs)"
    )

    val loadStart = System.currentTimeMillis()
    var putCount = 0
    var skippedCount = 0

    for (entityKeyBytes <- PerfTestConstants.ENTITY_KEYS) {
      // Keeping writes for a single entity contiguous is friendlier to KVStore caching.
      val requests: Seq[PutRequest] = tileStarts.flatMap { tileStartMs =>
        if (is5MinTiles && rng.nextDouble() < 0.2) {
          skippedCount += 1
          None
        } else {
          // WRITE TileKey: both tileSizeMs and tileStartTs set.
          // multiPut extracts tileStartTs as sort key, builds partition key = entityKeyBytes ++ "#<tileSizeMs>".
          val tileKey =
            TilingUtils.buildTileKey(PerfTestConstants.DATASET, entityKeyBytes, Some(tileSizeMs), Some(tileStartMs))
          val tileKeyBytes = TilingUtils.serializeTileKey(tileKey)

          val payloadSize = if (is5MinTiles) {
            100 + rng.nextInt(901)
          } else {
            500 + rng.nextInt(1501)
          }
          val valueBytes = new Array[Byte](payloadSize)
          rng.nextBytes(valueBytes)

          // tsMillis ignored by multiPut for streaming tables (uses tileStartTimestampMillis from TileKey),
          // but included to match canonical pattern.
          Some(PutRequest(tileKeyBytes, valueBytes, PerfTestConstants.DATASET, Some(tileStartMs)))
        }
      }

      requests.grouped(PerfTestConstants.PUT_BATCH_SIZE).foreach { batch =>
        val results = Await.result(kvStore.multiPut(batch), 5.minutes)
        val failures = results.count(!_)
        if (failures > 0) {
          logger.warn(s"  $failures failures in batch of ${batch.size}")
        }
        putCount += batch.size
        if (putCount % 1000 == 0) {
          val progress = if (is5MinTiles) {
            s"Loaded $putCount puts, skipped $skippedCount empty tiles"
          } else {
            s"Loaded $putCount / $totalPossiblePuts (${(putCount * 100L) / totalPossiblePuts}%)"
          }
          logger.info(s"  $progress")
        }
      }
    }

    val elapsedMs = System.currentTimeMillis() - loadStart
    val summary = if (is5MinTiles) {
      s"$putCount puts, $skippedCount empty tiles skipped"
    } else {
      s"$putCount puts"
    }
    logger.info(s"Load complete: $summary in ${elapsedMs}ms")
  }
}
