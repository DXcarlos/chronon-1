package ai.chronon.spark.catalog

import org.apache.spark.sql.SparkSession

/** Helper to read spark.chronon.* configs from SparkContext.
  *
  * Reads from SparkContext.getConf first, then falls back to session.conf.
  * In most Spark environments session.conf should reflect SparkConf entries,
  * but reading from SparkContext directly is a defensive measure against
  * environments where the propagation is incomplete.
  *
  * See: https://github.com/apache/spark/blob/v3.5.8/sql/core/src/main/scala/org/apache/spark/sql/RuntimeConfig.scala
  */
object ChrononSparkConf {

  def get(session: SparkSession, key: String, default: String): String = {
    try {
      session.sparkContext.getConf.get(key, default)
    } catch {
      case _: Exception =>
        try { session.conf.get(key, default) }
        catch { case _: Exception => default }
    }
  }

  def get(session: SparkSession, key: String): String = {
    try {
      session.sparkContext.getConf.get(key)
    } catch {
      case _: java.util.NoSuchElementException =>
        session.conf.get(key) // may throw NoSuchElementException
    }
  }
}
