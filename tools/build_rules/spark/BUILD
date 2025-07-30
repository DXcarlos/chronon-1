package(default_visibility = ["//visibility:public"])

java_library(
    name = "spark-exec",
    visibility = ["//visibility:public"],
    exports = [
        "@maven//:org_apache_spark_spark_core_2_12",
        "@maven//:org_apache_spark_spark_sql_2_12", 
        "@maven//:org_apache_spark_spark_catalyst_2_12",
        "@maven//:org_apache_spark_spark_hive_2_12",
        "@maven//:org_apache_spark_spark_sql_api_2_12",
        "@maven//:org_apache_spark_spark_common_utils_2_12",
        "@maven//:org_apache_spark_spark_unsafe_2_12",
        "@maven//:org_apache_spark_spark_sketch_2_12",
        "@maven//:org_apache_spark_spark_tags_2_12",
        "@maven//:com_esotericsoftware_kryo_shaded",
        "@maven//:org_apache_hive_hive_metastore",
    ],
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
