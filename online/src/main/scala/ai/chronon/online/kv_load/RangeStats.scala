package ai.chronon.online.kv_load

// Latencies stored in nanoseconds, converted to milliseconds only at report time to preserve precision.
case class RangeStats(latenciesNs: List[Long], tileCounts: List[Int]) {
  def requestCount: Int = latenciesNs.size
  def p50Ms: Double = percentileMs(0.50)
  def p99Ms: Double = percentileMs(0.99)
  def meanLatencyMs: Double = if (latenciesNs.isEmpty) 0.0 else latenciesNs.sum.toDouble / latenciesNs.size / 1000000.0
  def meanTiles: Double = if (tileCounts.isEmpty) 0.0 else tileCounts.sum.toDouble / tileCounts.size

  private def percentileMs(p: Double): Double = {
    if (latenciesNs.isEmpty) return 0.0
    val sorted = latenciesNs.sorted
    val idx = Math.min((p * sorted.size).toInt, sorted.size - 1)
    sorted(idx).toDouble / 1000000.0
  }
}
