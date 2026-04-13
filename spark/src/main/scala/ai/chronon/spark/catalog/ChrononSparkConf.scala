package ai.chronon.spark.catalog

import org.apache.spark.sql.SparkSession

/** Helper to read spark.chronon.* configs from SparkContext.
  *
  * SQLConf (session.conf) only contains SQL-level configs. Custom keys
  * like spark.chronon.* set via spark-submit --conf are on SparkConf
  * but NOT propagated to SQLConf. This helper reads from SparkContext
  * first, then falls back to session.conf.
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
