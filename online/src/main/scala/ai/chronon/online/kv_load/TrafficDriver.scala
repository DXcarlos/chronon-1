package ai.chronon.online.kv_load

import ai.chronon.api.TilingUtils
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.GetRequest
import org.slf4j.{Logger, LoggerFactory}

import java.util.ArrayList
import java.util.concurrent.{Callable, Executors}
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.Random

class TrafficDriver(kvStore: KVStore, tileSizeMs: Long, durationPerRangeMinutes: Int = 15, numThreads: Int = 4) {

  private val logger: Logger = LoggerFactory.getLogger(classOf[TrafficDriver])

  def run(): Unit = {
    val allStats: Seq[(String, RangeStats)] =
      PerfTestConstants.TRAFFIC_RANGES.map { case (label, (startMs, endMs)) =>
        logger.info(
          s"[TrafficDriver] Starting range=$label, duration=${durationPerRangeMinutes}min, threads=$numThreads")
        val stats = driveRange(startMs, endMs, durationPerRangeMinutes)
        logger.info(s"[TrafficDriver] Finished range=$label: ${stats.requestCount} requests")
        (label, stats)
      }

    printReport(allStats)
  }

  // READ TileKey construction: tileSizeMs set, tileStartTs deliberately None (matches Fetcher behavior).
  // multiGet extracts partition key as entityKeyBytes ++ "#<tileSizeMs>"; time window for sort-key
  // BETWEEN clause comes from GetRequest.startTsMillis/endTsMillis.
  private def driveRange(startMs: Long, endMs: Long, durationMinutes: Int): RangeStats = {
    val executor = Executors.newFixedThreadPool(numThreads)
    val taskList = new ArrayList[Callable[RangeStats]](numThreads)

    (0 until numThreads).foreach { threadIdx =>
      taskList.add(new Callable[RangeStats] {
        override def call(): RangeStats = {
          val rng = new Random(threadIdx.toLong)
          val deadlineNs = System.nanoTime() + durationMinutes.toLong * 60L * 1000000000L
          var latencies = List.empty[Long]
          var tileCounts = List.empty[Int]

          while (System.nanoTime() < deadlineNs) {
            val entityKeyBytes = PerfTestConstants.ENTITY_KEYS(rng.nextInt(PerfTestConstants.ENTITY_KEYS.size))
            val readTileKey =
              TilingUtils.buildTileKey(PerfTestConstants.DATASET, entityKeyBytes, Some(tileSizeMs), None)
            val readKeyBytes = TilingUtils.serializeTileKey(readTileKey)
            val getRequest = GetRequest(readKeyBytes, PerfTestConstants.DATASET, Some(startMs), Some(endMs))

            val beforeNs = System.nanoTime()
            val responses = Await.result(kvStore.multiGet(Seq(getRequest)), 30.seconds)
            val afterNs = System.nanoTime()

            latencies = (afterNs - beforeNs) :: latencies
            val tileCount = responses.head.values match {
              case scala.util.Success(tvs) => tvs.size
              case scala.util.Failure(_)   => 0
            }
            tileCounts = tileCount :: tileCounts
          }

          RangeStats(latencies, tileCounts)
        }
      })
    }

    val futures = executor.invokeAll(taskList)
    executor.shutdown()

    // Ordering across threads doesn't matter for percentile/mean computation.
    val threadResults = (0 until futures.size).map(i => futures.get(i).get())
    RangeStats(
      latenciesNs = threadResults.flatMap(_.latenciesNs).toList,
      tileCounts = threadResults.flatMap(_.tileCounts).toList
    )
  }

  private def printReport(allStats: Seq[(String, RangeStats)]): Unit = {
    val header =
      "%-8s %10s %10s %12s %12s %10s".format("Range", "Requests", "P50(ms)", "P99(ms)", "Mean(ms)", "AvgTiles")
    val sep = "-" * header.length
    logger.info(sep)
    logger.info(header)
    logger.info(sep)
    for ((label, stats) <- allStats) {
      logger.info(
        "%-8s %10d %10.2f %12.2f %12.2f %10.2f".format(
          label,
          stats.requestCount,
          stats.p50Ms,
          stats.p99Ms,
          stats.meanLatencyMs,
          stats.meanTiles
        ))
    }
    logger.info(sep)
  }
}
