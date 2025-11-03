from enum import Enum


class RunMode(str, Enum):
    def __str__(self):
        return self.value

    BACKFILL = "backfill"
    BACKFILL_LEFT = "backfill-left"
    BACKFILL_FINAL = "backfill-final"
    DEPLOY = "deploy"
    UPLOAD = "upload"
    UPLOAD_TO_KV = "upload-to-kv"
    STATS_SUMMARY = "stats-summary"
    LOG_SUMMARY = "log-summary"
    ANALYZE = "analyze"
    STREAMING = "streaming"
    METADATA_UPLOAD = "metadata-upload"
    FETCH = "fetch"
    CONSISTENCY_METRICS_COMPUTE = "consistency-metrics-compute"
    COMPARE = "compare"
    LOCAL_STREAMING = "local-streaming"
    LOG_FLATTENER = "log-flattener"
    METADATA_EXPORT = "metadata-export"
    LABEL_JOIN = "label-join"
    STREAMING_CLIENT = "streaming-client"
    SOURCE_JOB = "source-job"
    JOIN_PART_JOB = "join-part-job"
    MERGE_JOB = "merge-job"
    METASTORE = "metastore"
    INFO = "info"


ONLINE_ARGS = "--online-jar={online_jar} --online-class={online_class} "
OFFLINE_ARGS = "--conf-path={conf_path} --end-date={ds} "
ONLINE_WRITE_ARGS = "--conf-path={conf_path} " + ONLINE_ARGS

ONLINE_OFFLINE_WRITE_ARGS = OFFLINE_ARGS + ONLINE_ARGS
ONLINE_MODES = [
    RunMode.STREAMING,
    RunMode.METADATA_UPLOAD,
    RunMode.FETCH,
    RunMode.LOCAL_STREAMING,
    RunMode.STREAMING_CLIENT,
]
SPARK_MODES = [
    RunMode.BACKFILL,
    RunMode.BACKFILL_LEFT,
    RunMode.BACKFILL_FINAL,
    RunMode.UPLOAD,
    RunMode.UPLOAD_TO_KV,
    RunMode.STREAMING,
    RunMode.STREAMING_CLIENT,
    RunMode.CONSISTENCY_METRICS_COMPUTE,
    RunMode.COMPARE,
    RunMode.ANALYZE,
    RunMode.STATS_SUMMARY,
    RunMode.LOG_SUMMARY,
    RunMode.LOG_FLATTENER,
    RunMode.METADATA_EXPORT,
    RunMode.LABEL_JOIN,
    RunMode.SOURCE_JOB,
    RunMode.JOIN_PART_JOB,
    RunMode.MERGE_JOB,
]
MODES_USING_EMBEDDED = [
    RunMode.METADATA_UPLOAD,
    RunMode.FETCH,
    RunMode.LOCAL_STREAMING,
]

# Constants for supporting multiple spark versions.
SUPPORTED_SPARK = ["2.4.0", "3.1.1", "3.2.1", "3.5.1"]
SCALA_VERSION_FOR_SPARK = {
    "2.4.0": "2.11",
    "3.1.1": "2.12",
    "3.2.1": "2.13",
    "3.5.1": "2.12",
}

MODE_ARGS = {
    RunMode.BACKFILL: OFFLINE_ARGS,
    RunMode.BACKFILL_LEFT: OFFLINE_ARGS,
    RunMode.BACKFILL_FINAL: OFFLINE_ARGS,
    RunMode.UPLOAD: OFFLINE_ARGS,
    RunMode.UPLOAD_TO_KV: ONLINE_WRITE_ARGS,
    RunMode.STATS_SUMMARY: OFFLINE_ARGS,
    RunMode.LOG_SUMMARY: OFFLINE_ARGS,
    RunMode.ANALYZE: OFFLINE_ARGS,
    RunMode.STREAMING: ONLINE_WRITE_ARGS,
    RunMode.METADATA_UPLOAD: ONLINE_WRITE_ARGS,
    RunMode.FETCH: ONLINE_ARGS,
    RunMode.CONSISTENCY_METRICS_COMPUTE: OFFLINE_ARGS,
    RunMode.COMPARE: OFFLINE_ARGS,
    RunMode.LOCAL_STREAMING: ONLINE_WRITE_ARGS + " -d",
    RunMode.LOG_FLATTENER: OFFLINE_ARGS,
    RunMode.METADATA_EXPORT: OFFLINE_ARGS,
    RunMode.LABEL_JOIN: OFFLINE_ARGS,
    RunMode.STREAMING_CLIENT: ONLINE_WRITE_ARGS,
    RunMode.SOURCE_JOB: OFFLINE_ARGS,
    RunMode.JOIN_PART_JOB: OFFLINE_ARGS,
    RunMode.MERGE_JOB: OFFLINE_ARGS,
    RunMode.METASTORE: "", # purposely left blank. we'll handle this specifically
    RunMode.INFO: "",
}

ROUTES = {
    "group_bys": {
        RunMode.UPLOAD: "group-by-upload",
        RunMode.UPLOAD_TO_KV: "group-by-upload-bulk-load",
        RunMode.BACKFILL: "group-by-backfill",
        RunMode.STREAMING: "group-by-streaming",
        RunMode.METADATA_UPLOAD: "metadata-upload",
        RunMode.LOCAL_STREAMING: "group-by-streaming",
        RunMode.FETCH: "fetch",
        RunMode.ANALYZE: "analyze",
        RunMode.METADATA_EXPORT: "metadata-export",
        RunMode.STREAMING_CLIENT: "group-by-streaming",
    },
    "joins": {
        RunMode.BACKFILL: "join",
        RunMode.BACKFILL_LEFT: "join-left",
        RunMode.BACKFILL_FINAL: "join-final",
        RunMode.METADATA_UPLOAD: "metadata-upload",
        RunMode.FETCH: "fetch",
        RunMode.CONSISTENCY_METRICS_COMPUTE: "consistency-metrics-compute",
        RunMode.COMPARE: "compare-join-query",
        RunMode.STATS_SUMMARY: "stats-summary",
        RunMode.LOG_SUMMARY: "log-summary",
        RunMode.ANALYZE: "analyze",
        RunMode.LOG_FLATTENER: "log-flattener",
        RunMode.METADATA_EXPORT: "metadata-export",
        RunMode.LABEL_JOIN: "label-join",
        RunMode.SOURCE_JOB: "source-job",
        RunMode.JOIN_PART_JOB: "join-part-job",
        RunMode.MERGE_JOB: "merge-job",
    },
    "staging_queries": {
        RunMode.BACKFILL: "staging-query-backfill",
        RunMode.METADATA_EXPORT: "metadata-export",
    },
}

UNIVERSAL_ROUTES = ["info"]

APP_NAME_TEMPLATE = "chronon_{conf_type}_{mode}_{context}_{name}"
RENDER_INFO_DEFAULT_SCRIPT = "scripts/render_info.py"

ZIPLINE_DIRECTORY = "/tmp/zipline"

CLOUD_PROVIDER_KEYWORD = "CLOUD_PROVIDER"

# cloud provider
AWS = "AWS"
GCP = "GCP"

# arg keywords
ONLINE_CLASS_ARG = "online_class"
ONLINE_JAR_ARG = "online_jar"
ONLINE_ARGS = "online_args"
