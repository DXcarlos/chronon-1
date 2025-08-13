load("//tools/build_rules/dependencies:defs.bzl", "get_jars_for_repository")
load("//tools/build_rules/dependencies:maven_repository.bzl", "MAVEN_REPOSITORY_NAME")

package(default_visibility = ["//visibility:public"])

SPARK_JARS = [
    scala_jar(
        name = "spark-core",
        org = "org.apache.spark",
    ),
    scala_jar(
        name = "spark-catalyst",
        org = "org.apache.spark",
    ),
    scala_jar(
        name = "spark-sql",
        org = "org.apache.spark",
    ),
    #    scala_jar(
    #        name = "spark-hive",
    #        org = "org.apache.spark",
    #    ),
    #    scala_jar(
    #        name = "spark-avro",
    #        org = "org.apache.spark",
    #    ),
    scala_jar(
        name = "spark-sketch",
        org = "org.apache.spark",
    ),
    scala_jar(
        name = "spark-streaming",
        org = "org.apache.spark",
    ),
    scala_jar(
        name = "spark-tags",
        org = "org.apache.spark",
    ),
    scala_jar(
        name = "spark-unsafe",
        org = "org.apache.spark",
    ),
    jar(
        name = "hadoop-common",
        org = "org.apache.hadoop",
    ),
    jar(
        name = "hadoop-mapreduce-client-core",
        org = "org.apache.hadoop",
    ),
    jar(
        name = "hadoop-hdfs",
        org = "org.apache.hadoop",
    ),
    #    jar(
    #        name = "hive-metastore",
    #        org = "org.apache.hive",
    #    ),
    jar(
        name = "kryo_shaded",
        org = "com.esotericsoftware",
    ),
]

SPARK_3_5_JARS = SPARK_JARS + [
    scala_jar(
        name = "spark-common-utils",
        org = "org.apache.spark",
    ),
    scala_jar(
        name = "spark-sql-api",
        org = "org.apache.spark",
    ),
]

java_library(
    name = "spark-exec",
    visibility = ["//visibility:public"],
    exports = get_jars_for_repository(MAVEN_REPOSITORY_NAME, SPARK_3_5_JARS),
)

# To simulate the spark runtime environment with dependencies we want to exclude from our final deployment jar
# We would need to specify this as 'deploy_env' for our final build target.
java_binary(
    name = "spark",
    main_class = "None",  #hack
    runtime_deps = [
        # Exclude all spark related dependencies as they are already available in our cluster runtime environment
        ":spark-exec",
    ],
)
