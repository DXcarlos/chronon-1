load("@rules_jvm_external//:specs.bzl", "maven")
load(":defs.bzl", "repository", "versioned_artifacts")

MAVEN_REPOSITORY_NAME = "maven"

HADOOP_DEPS = [
    "org.apache.hadoop:hadoop-client",
    "org.apache.hadoop:hadoop-common",
    "org.apache.hadoop:hadoop-hdfs",
    "org.apache.hadoop:hadoop-mapreduce-client-core",
]

maven_repository = repository(
    name = MAVEN_REPOSITORY_NAME,
    pinned = False,
    maven_install_json = "//:maven_install.json",
    artifacts = [
        # Scala 2.12 libraries
        "org.scala-lang.modules:scala-collection-compat_2.12:2.6.0",
        "org.scala-lang.modules:scala-parser-combinators_2.12:2.3.0",
        "org.scala-lang.modules:scala-java8-compat_2.12:1.0.2",

        # Unit testing
        "junit:junit:4.13.2",
        "org.junit.jupiter:junit-jupiter-api:5.10.5",
        "org.junit.platform:junit-platform-launcher:1.10.5",
        "org.junit.platform:junit-platform-reporting:1.10.5",
        "com.novocode:junit-interface:0.11",
        "org.mockito:mockito-core:5.12.0",
        "org.objenesis:objenesis:3.4",

        # Unit testing - for scala 2.12
        "org.scalatestplus:mockito-3-4_2.12:3.2.10.0",
        "org.mockito:mockito-scala_2.12:1.17.0",
        "org.scalatest:scalatest_2.12:3.2.15",
        "org.scalatest:scalatest-shouldmatchers_2.12:3.2.15",
        "org.scalatest:scalatest-matchers-core_2.12:3.2.15",
        "org.scalactic:scalactic_2.12:3.2.15",
        "org.mockito:mockito-core:5.12.0",

        # Add other dependencies
        "org.slf4j:slf4j-api:2.0.12",
        "org.apache.logging.log4j:log4j-slf4j-impl:2.20.0",
        "org.apache.logging.log4j:log4j-core:2.20.0",
        "org.apache.datasketches:datasketches-memory:3.0.2",
        "org.apache.datasketches:datasketches-java:6.1.1",
        "com.fasterxml.jackson.core:jackson-core:2.15.2",
        "com.fasterxml.jackson.core:jackson-databind:2.15.2",
        "com.fasterxml.jackson.module:jackson-module-afterburner:2.15.2",
        "com.google.code.gson:gson:2.10.1",
        "javax.annotation:javax.annotation-api:1.3.2",
        "com.datadoghq:java-dogstatsd-client:4.4.1",
        "net.jodah:typetools:0.6.3",
        "com.github.ben-manes.caffeine:caffeine:3.1.8",
        "jakarta.servlet:jakarta.servlet-api:4.0.3",
        "com.google.guava:guava:33.3.1-jre",
        "org.yaml:snakeyaml:2.3",
        #        "commons-lang:commons-lang:2.6",
        "io.netty:netty-all:4.1.118.Final",
        "ch.qos.reload4j:reload4j:1.2.25",
        "ch.qos.logback:logback-classic:1.5.13",
        "com.typesafe:config:1.4.3",
        "io.micrometer:micrometer-registry-statsd:1.13.6",
        "io.micrometer:micrometer-registry-otlp:1.13.6",
        # use an older version of micrometer-registry-prometheus to avoid
        # running into issues where they've moved classes from io.micrometer.prometheus to
        # io.micrometer.prometheusmetrics and Vert.x metrics haven't picked this up
        # Can revisit when we bump Vert.x to 5.x
        "io.micrometer:micrometer-registry-prometheus:1.10.13",
        "net.sf.py4j:py4j:0.10.9.9",
        "commons-io:commons-io:2.20.0",
        "org.apache.commons:commons-math3:3.6.1",
        "org.apache.commons:commons-pool2:2.12.1",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",

        # Add other dependencies - for scala 2.12
        "org.apache.logging.log4j:log4j-api-scala_2.12:13.1.0",
        "com.fasterxml.jackson.module:jackson-module-scala_2.12:2.15.2",
        "org.rogach:scallop_2.12:5.1.0",
        "com.softwaremill.sttp.client3:core_2.12:3.9.7",
        "org.json4s:json4s-jackson_2.12:3.7.0-M11",
        "org.json4s:json4s-core_2.12:3.7.0-M11",
        "org.json4s:json4s-ast_2.12:3.7.0-M11",
        "io.delta:delta-spark_2.12:3.2.0",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
        "org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.0",

        # grpc
        "io.grpc:grpc-core:1.69.0",
        "io.grpc:grpc-stub:1.69.0",
        "io.grpc:grpc-inprocess:1.69.0",

        # Kafka
        "org.apache.kafka:kafka-clients:3.8.1",
        "io.confluent:kafka-schema-registry-client:7.8.0",
        "io.confluent:kafka-protobuf-provider:7.8.0",
        "com.google.protobuf:protobuf-java-util:3.25.1",
        "com.google.protobuf:protobuf-java:3.25.1",

        # Avro
        "org.apache.avro:avro:1.11.4",
        "com.linkedin.avroutil1:avro-fastserde:0.4.25",

        # AWS
        versioned_artifacts(
            group = "software.amazon.awssdk",
            version = "2.30.13",
            artifacts = [
                "dynamodb",
                "regions",
                "aws-core",
                "sdk-core",
                "utils",
                "auth",
                "url-connection-client",
                "identity-spi",
                "emr",
            ],
            exclusions = HADOOP_DEPS,
        ),
        "software.amazon.dynamodb:DynamoDBLocal:3.0.0",

        # Google Cloud
        "com.google.cloud:google-cloud-bigquery:2.42.0",
        "com.google.cloud:google-cloud-bigtable:2.57.1",
        "com.google.cloud:google-cloud-pubsub:1.131.0",
        "com.google.cloud:google-cloud-dataproc:4.52.0",

        # spark related
        # Have to specify in group:artifact:packaging:version format if version doesn't start with a digit
        # Code reference: https://github.com/bazel-contrib/rules_jvm_external/blob/master/private/lib/coordinates.bzl#L44
        "com.google.cloud.bigdataoss:gcs-connector:jar:hadoop3-2.2.26",
        "com.google.cloud.bigdataoss:gcsio:2.2.26",
        "com.google.cloud.bigdataoss:util-hadoop:jar:hadoop3-2.2.26",
        "com.google.cloud.bigdataoss:util:2.2.26",
        "com.google.cloud.spark:spark-3.5-bigquery:0.42.0",

        # Managed kafka
        "com.google.cloud.hosted.kafka:managed-kafka-auth-login-handler:1.0.3",

        # Google Cloud
        "com.google.cloud:google-cloud-spanner:6.86.0",
        "com.google.cloud:google-cloud-storage:2.52.1",
        "com.google.cloud:google-cloud-core:2.57.0",
        "com.google.cloud:google-cloud-bigtable-emulator:0.178.0",

        # Google api
        "com.google.api:api-common:2.46.1",
        "com.google.api:gax:2.60.0",
        "com.google.api:gax-grpc:2.60.0",

        # Google api grpc
        "com.google.api.grpc:proto-google-cloud-pubsub-v1:1.120.0",
        "com.google.api.grpc:proto-google-longrunning-v1:0.1.24",  # todo fix vulns or remove
        "com.google.api.grpc:proto-google-cloud-bigtable-v2:2.57.1",
        "com.google.api.grpc:proto-google-cloud-bigtable-admin-v2:2.57.1",
        "com.google.api.grpc:grpc-google-cloud-bigtable-v2:2.57.1",

        # Flink
        "org.apache.flink:flink-metrics-dropwizard:1.17.0",
        "org.apache.flink:flink-metrics-prometheus:1.17.0",
        "org.apache.flink:flink-clients:1.17.0",
        "org.apache.flink:flink-yarn:1.17.0",
        "org.apache.flink:flink-runtime:1.17.0",
        "org.apache.flink:flink-connector-kafka:1.17.0",
        "org.apache.flink:flink-connector-files:1.17.0",
        "org.apache.flink:flink-avro:1.17.0",
        "org.apache.flink:flink-runtime:1.17.0:tests",
        "org.apache.flink:flink-test-utils:1.17.0",

        # Flink Connectors
        "org.apache.flink:flink-connector-gcp-pubsub:3.0.2-1.17",

        # Vertx
        "io.vertx:vertx-core:4.5.10",
        "io.vertx:vertx-web:4.5.10",
        "io.vertx:vertx-web-client:4.5.10",
        "io.vertx:vertx-config:4.5.10",
        "io.vertx:vertx-micrometer-metrics:4.5.10",
        "io.vertx:vertx-junit5:4.5.10",
        "io.vertx:vertx-unit:4.5.10",
        "io.vertx:vertx-unit:4.5.10",

        # Postgres SQL
        "org.postgresql:postgresql:42.7.5",
        "org.testcontainers:postgresql:1.20.4",

        # Spark artifacts - for scala 2.12
        "org.apache.spark:spark-sql_2.12:3.5.3",
        #        "org.apache.spark:spark-hive_2.12:3.5.3",
        "org.apache.spark:spark-streaming_2.12:3.5.3",
        #        "org.apache.spark:spark-avro_2.12:3.5.3",

        # Circe - for scala 2.12
        "io.circe:circe-core_2.12:0.14.9",
        "io.circe:circe-generic_2.12:0.14.9",
        "io.circe:circe-parser_2.12:0.14.9",
        "com.chuusai:shapeless_2.12:2.3.12",

        # Slick - for scala 2.12
        "com.typesafe.slick:slick_2.12:3.3.3",

        # for database connection pooling
        "com.zaxxer:HikariCP:7.0.0",

        # Temporal
        "io.temporal:temporal-sdk:1.28.0",
        "io.temporal:temporal-testing:1.28.0",

        # OpenTelemetry
        "io.opentelemetry:opentelemetry-api:1.49.0",
        "io.opentelemetry:opentelemetry-sdk:1.49.0",
        "io.opentelemetry:opentelemetry-sdk-metrics:1.49.0",
        "io.opentelemetry:opentelemetry-exporter-otlp:1.49.0",
        "io.opentelemetry:opentelemetry-exporter-prometheus:1.49.0-alpha",
        "io.opentelemetry:opentelemetry-sdk-extension-autoconfigure:1.49.0",

        # liquibase
        "org.liquibase:liquibase-core:4.33.0",

        # vulnerable dependency fixes
        "com.fasterxml.woodstox:woodstox-core:5.4.0",
        "org.apache.commons:commons-configuration2:2.12.0",
        "org.codehaus.jettison:jettison:1.5.4",
        "org.apache.commons:commons-lang3:3.18.0",
        "org.eclipse.jetty:jetty-util:9.4.57.v20241219",  # latest version that is still built on jdk 11 and not 17.
        "org.eclipse.jetty:jetty-server:9.4.57.v20241219",
        "org.eclipse.jetty:jetty-xml:9.4.57.v20241219",
        "org.eclipse.jetty:jetty-http:12.0.12",

        # hadoop deps
        #        "org.apache.hadoop:hadoop-common:3.4.1",
        #        "org.apache.hadoop:hadoop-mapreduce-client-core:3.4.1",
        #        "org.apache.hadoop:hadoop-hdfs:3.4.1",
        #        "org.apache.hadoop:hadoop-client:3.4.1",
        versioned_artifacts(
            group = "org.apache.hadoop",
            version = "3.3.4",
            artifacts = [
                "hadoop-yarn-api",
                "hadoop-yarn-client",
                "hadoop-client",
                "hadoop-common",
                "hadoop-auth",
                "hadoop-hdfs",
            ],
        ),
    ],
    excluded_artifacts = [
        "org.apache.beam:beam-sdks-java-io-hadoop-common",
        "org.pentaho:pentaho-aggdesigner-algorithm",
        # Exclude Hadoop from the assembled JAR
        # Else we hit an error - IllegalAccessError: class org.apache.hadoop.hdfs.web.HftpFileSystem cannot access its
        # superinterface org.apache.hadoop.hdfs.web.TokenAspect$TokenManagementDelegator
        # Note: Only excluding them from a specific module is getting tricky
        # so we ended up removing these from our entire repo as they are required across our project
        "org.apache.hadoop:hadoop-annotations",
        "org.apache.hadoop:hadoop-auth",
        "org.apache.hadoop:hadoop-hdfs-client",
        #        "org.apache.hadoop:hadoop-hdfs",
        #        "org.apache.hadoop:hadoop-mapreduce-client-core",
        "org.apache.hadoop:hadoop-yarn-api",
        "org.apache.hadoop:hadoop-yarn-client",
        "org.apache.hadoop:hadoop-client-runtime",
        #        "org.apache.hadoop:hadoop-common",
        "org.apache.zookeeper:zookeeper",
        "org.apache.hadoop:hadoop-client-api",
        # Exclude old parquet-hadoop-bundle that includes vulnerable parquet-avro 1.13.1
        "org.apache.parquet:parquet-hadoop-bundle",
        "org.apache.parquet:parquet-hadoop",
        # Exclude rocksdb from the assembled JARs that pull this in (e.g. flink, cloud_gcp) as we want to exclude
        # the rockdb library and rely on those part of the dist / env
        # Else we hit an error - NoSuchMethodError: 'void org.rocksdb.WriteBatch.remove
        "org.rocksdb:rocksdbjni",
        # Exclude scala artifacts as right versions are pulled from scala repository
        "org.scala-lang:scala-library",
        "org.scala-lang:scala-reflect",
        "log4j:log4j",
    ],
    overrides = {
        # Force reload4j as a replacement for vulnerable log4j 1.2.17
        "log4j:log4j": "ch.qos.reload4j:reload4j:1.2.25",
    },
)
