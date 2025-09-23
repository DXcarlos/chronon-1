package ai.chronon.integrations.cloud_gcp

// Tracks additional Jars that need to be specified while submitting Dataproc Spark / Flink jobs as we build
// thin jars with mill and these additional deps need to be augmented during job submission
object DataprocAdditionalJars {
  val additionalSparkJobJars: Array[String] = Array(
    // todo(tchow): upgrade when https://github.com/apache/iceberg/pull/14113 is released
    // "gs://zipline-spark-libs/iceberg/iceberg-bigquery-1.10.0.jar"
  )

  // Need a lot of the Spark jars for Flink as Flink leverages Spark's catalyst / sql components for Spark expr eval.
  // This list can be trimmed down, but a lot of these jars are needed for a class or two that if absent, results in a CNF exception.
  val additionalFlinkJobJars = Array(
    "gs://zipline-spark-libs/spark-3.5.3/libs/commons-collections4-4.4.jar",
    "gs://zipline-spark-libs/spark-3.5.3/libs/commons-compiler-3.1.9.jar",
    "gs://zipline-spark-libs/spark-3.5.3/libs/janino-3.1.9.jar",
    "gs://zipline-spark-libs/spark-3.5.3/libs/json4s-ast_2.12-3.7.0-M11.jar",
    "gs://zipline-spark-libs/spark-3.5.3/libs/json4s-core_2.12-3.7.0-M11.jar",
    "gs://zipline-spark-libs/spark-3.5.3/libs/kryo-shaded-4.0.2.jar",
    "gs://zipline-spark-libs/spark-3.5.3/libs/metrics-core-4.2.19.jar",
    "gs://zipline-spark-libs/spark-3.5.3/libs/metrics-json-4.2.19.jar",
    "gs://zipline-spark-libs/spark-3.5.3/libs/spark-catalyst_2.12-3.5.3.jar",
    "gs://zipline-spark-libs/spark-3.5.3/libs/spark-common-utils_2.12-3.5.3.jar",
    "gs://zipline-spark-libs/spark-3.5.3/libs/spark-core_2.12-3.5.3.jar",
    "gs://zipline-spark-libs/spark-3.5.3/libs/spark-kvstore_2.12-3.5.3.jar",
    "gs://zipline-spark-libs/spark-3.5.3/libs/spark-launcher_2.12-3.5.3.jar",
    "gs://zipline-spark-libs/spark-3.5.3/libs/spark-hive_2.12-3.5.3.jar",
    "gs://zipline-spark-libs/spark-3.5.3/libs/spark-network-common_2.12-3.5.3.jar",
    "gs://zipline-spark-libs/spark-3.5.3/libs/spark-network-shuffle_2.12-3.5.3.jar",
    "gs://zipline-spark-libs/spark-3.5.3/libs/spark-sql-api_2.12-3.5.3.jar",
    "gs://zipline-spark-libs/spark-3.5.3/libs/spark-sql_2.12-3.5.3.jar",
    "gs://zipline-spark-libs/spark-3.5.3/libs/spark-unsafe_2.12-3.5.3.jar",
    "gs://zipline-spark-libs/spark-3.5.3/libs/xbean-asm9-shaded-4.23.jar"
  )
}
