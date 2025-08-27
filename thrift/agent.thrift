namespace java ai.chronon.api
include "common.thrift"

// TODO: Need to brainstorm and make necessary changes. just a starting point to unblock other work.
struct YarnAutoScalingSpec {
    1: optional i32 minInstances
    2: optional i32 maxInstances
    3: optional double scaleUpFactor // 1.5x, 2x etc
    4: optional double scaleDownFactor
    5: optional string cooldownPeriod
}

// our clusters are created transiently prior to running the job
struct YarnClusterSpec {
    1: optional string clusterName
    2: optional string hostType
    3: optional i32 hostCount
    4: optional string masterHostType

    // dataproc = x.y.z, emr = x.y.z, etc
    10: optional string yarnOfferingVersion

    // to access the right data and right back to kvstore
    20: optional string networkPolicy
    30: optional YarnAutoScalingSpec autoScalingSpec
}

enum YarnJobType {
    SPARK = 0,
    FLINK = 1,
}

struct YarnJob {
    // create transient cluster with this name and runs an app with the same yarn name
    1: optional string appName
    2: optional YarnJobType jobType

    10: optional list<string> argsList
    11: optional map<string, string> env
    12: optional map<string, string> conf
    // creates local file with this name and contents - relative to cwd
    // contains the groupBy, join, queries etc
    13: optional map<string, string> fileWithContents

    20: optional string chrononVersion
    21: optional YarnClusterSpec clusterSpec
}

struct KvWrite {
    1: optional string key
    2: optional string value
    3: optional string timestamp
}

// currently used for writing join metadata to kvstore needed prior to fetching joins
struct KvWriteJob {
    1: optional string scope // projectId in gcp, account name in aws
    2: optional string dataset
    3: optional string table
    4: optional list<KvWrite> writes
}

struct PartitionListingJob {
    1: optional string scope // projectId in gcp, account name in aws
    2: optional string dataset
    3: optional string table
    4: optional string partitionColumn
    5: optional list<string> extraPartitionFilters
}

// agent accepts jobs and runs them
union JobBase {
    1: YarnJob yarnJob
    2: KvWriteJob kvWriteJob
    3: PartitionListingJob partitionListingJob
}

struct Job {
    1: optional JobInfo jobInfo
    2: optional JobBase jobUnion
    3: optional i32 statusReportInterval
    4: optional i32 maxRetries
}

struct JobListGetRequest {
    // pubsub topic id to pull the jobs from
    1: optional string topicId
}

struct JobListResponse {
    // controller responds with jobs data plane agent is not aware of
    1: optional list<Job> jobsToStart
    2: optional list<string> jobsToStop
}

enum JobStatusType {
    UNKNOWN = 0,
    PENDING = 1,
    RUNNING = 2,
    SUCCEEDED = 3,
    FAILED = 4,
    CANCELLED = 5
}

struct ResourceUsage {
    1: optional i64 vcoreSeconds
    2: optional i64 megaByteSeconds
    3: optional i64 cumulativeDiskWriteBytes
    4: optional i64 cumulativeDiskReadBytes
}

struct YarnIncrementalJobStatus {
    // batch / streaming job
    1: optional map<JobStatusType, i64> statusChangeTimes
    2: optional ResourceUsage resourceUsage
    // driver logs - probably only errors and exceptions
    3: optional list<string> logsSinceLastPush
}

struct JobInfo {
    1: optional string jobId
    2: optional JobStatusType currentStatus

    10: optional YarnIncrementalJobStatus yarnIncrementalStatus
}


struct PartitionListingPutRequest {
    1: optional map<PartitionListingJob, list<common.DateRange>> partitions
    2: optional map<PartitionListingJob, string> errors
}

struct JobInfoPutRequest {
    1: optional list<JobInfo> jobStatuses
}