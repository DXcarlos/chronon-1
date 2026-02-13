package ai.chronon.integrations.cloud_gcp

// Tracks additional Jars that need to be specified while submitting Dataproc Flink jobs as we build
// thin jars with mill and these additional deps need to be augmented during job submission
object DataprocAdditionalJars {

  private val defaultFlinkJarsBasePath = "gs://zipline-spark-libs/spark-3.5.3/libs/"

  private val defaultFlinkJarsList = Array(
    "commons-collections4-4.4.jar",
    "commons-compiler-3.1.9.jar",
    "janino-3.1.9.jar",
    "json4s-ast_2.12-3.7.0-M11.jar",
    "json4s-core_2.12-3.7.0-M11.jar",
    "kryo-shaded-4.0.2.jar",
    "metrics-core-4.2.19.jar",
    "metrics-json-4.2.19.jar",
    "spark-catalyst_2.12-3.5.3.jar",
    "spark-common-utils_2.12-3.5.3.jar",
    "spark-core_2.12-3.5.3.jar",
    "spark-kvstore_2.12-3.5.3.jar",
    "spark-launcher_2.12-3.5.3.jar",
    "spark-hive_2.12-3.5.3.jar",
    "spark-network-common_2.12-3.5.3.jar",
    "spark-network-shuffle_2.12-3.5.3.jar",
    "spark-sql-api_2.12-3.5.3.jar",
    "spark-sql_2.12-3.5.3.jar",
    "spark-unsafe_2.12-3.5.3.jar",
    "xbean-asm9-shaded-4.23.jar"
  )

  // Need a lot of the Spark jars for Flink as Flink leverages Spark's catalyst / sql components for Spark expr eval.
  // This list can be trimmed down, but a lot of these jars are needed for a class or two that if absent, results in a CNF exception.
  // The base path can be configured via FLINK_JARS_URI environment variable, which should point to a directory containing these jars.
  def additionalFlinkJobJars(flinkJarsBasePath: Option[String] = None): Array[String] = {
    val basePath = flinkJarsBasePath.getOrElse(defaultFlinkJarsBasePath)
    val normalizedBasePath = if (basePath.endsWith("/")) basePath else basePath + "/"
    defaultFlinkJarsList.map(jar => normalizedBasePath + jar)
  }
}
