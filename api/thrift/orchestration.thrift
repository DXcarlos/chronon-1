namespace py ai.chronon.orchestration
namespace java ai.chronon.orchestration

include "common.thrift"
include "api.thrift"
include "planner.thrift"

// This has to be 0-indexed for Java usage
enum ConfType {
    GROUP_BY = 0,
    JOIN = 1,
    STAGING_QUERY = 2,
    MODEL = 3,
}

union LogicalNode {
    1: api.StagingQuery stagingQuery
    2: api.Join join
    3: api.GroupBy groupBy
    4: api.Model model
}

enum NodeRunStatus {
    UNKNOWN = 0,
    WAITING = 1,
    RUNNING = 2,
    SUCCEEDED = 3,
    FAILED = 4,
    CANCELLED = 5,
    ALREADY_EXISTS = 6
}

enum WorkflowStatus {
    UNKNOWN = 0,
    WAITING = 1,
    RUNNING = 2,
    SUCCEEDED = 3,
    FAILED = 4,
    CANCELLED = 5
}

struct Conf {
    1: optional string name
    2: optional string hash
    3: optional string contents
    4: optional ConfType confType
}

struct DiffRequest {
    1: optional map<string, string> namesToHashes
}

struct DiffResponse {
    1: optional list<string> diff
}

struct UploadRequest {
    1: optional list<Conf> diffConfs
    2: optional string branch
}

struct UploadResponse {
    1: optional string message
}

struct SyncRequest {
    1: optional map<string, string> namesToHashes
    2: string branch
}

struct SyncResponse {
    1: optional string message
}

struct WorkflowStartRequest {
    1: optional string confName
    2: optional string mode
    3: optional string branch
    4: optional string user
    5: optional string start
    6: optional string end
    7: optional string confHash
    8: optional bool forceRecompute
}

struct WorkflowStartResponse {
    1: optional string workflowId
}

struct NodeExecutionInfo {
    1: optional string nodeName
    2: optional string nodeHash
    3: optional string startPartition
    4: optional string endPartition
    5: optional list<string> dependencies
    6: optional list<NodeStepRunInfo> stepRuns
}

struct JobTrackingInfo {
    1: optional string jobUrl
    2: optional string sparkUrl
    3: optional string flinkUrl
    4: optional string jobId
}

struct NodeStepRunInfo {
    1: optional string runId
    2: optional string startPartition
    3: optional string endPartition
    4: optional string startTime
    5: optional string endTime
    6: optional NodeRunStatus status
    7: optional list<string> dependentStepRunIds
    8: optional string workflowId
    9: optional JobTrackingInfo jobTrackingInfo
    10: optional string additionalNotes
}

// ====================== Start Orchestration Scheduling ======================
struct ScheduleDeployRequest {
    1: optional string branch
    2: optional string confName
    3: optional string confHash
    4: optional map<planner.Mode, string> modeSchedules
}

struct ScheduleDeployResponse {
    1: optional map<planner.Mode, ScheduleStateResponse> schedules
}

struct ScheduleDeleteRequest {
    1: optional string branch
    2: optional string confName
    3: optional string confHash
}

struct ScheduleDeleteResponse {
    1: optional string branch
    2: optional string confName
    3: optional string confHash
}

/**
  * Various states a schedule can be in
  */
enum ScheduleState {
    NONE = 0,
    ACTIVE = 1,
    PAUSED = 2
}

/**
  * Tracks the status of a schedule from the captured user intent (in the schedule string) to the status of the schedule
  */
struct ScheduleStateResponse {
    1: optional string scheduleInterval
    2: optional ScheduleState state
    3: optional string confName
    4: optional string branch
    5: optional planner.Mode mode
    6: optional string hash
}

struct ScheduleListRequest {
    1: optional i32 limit
    2: optional i32 offset
    3: optional string branch
}

struct ScheduleListResponse {
    1: optional list<ScheduleStateResponse> schedules

    10: optional i32 totalCount // For pagination
}

// ====================== End Orchestration Scheduling ======================

struct WorkflowStatusResponse {
    1: optional string workflowId
    2: optional string confName
    3: optional string mode
    4: optional string branch
    5: optional string user
    6: optional WorkflowStatus status
    7: optional string startPartition
    8: optional string endPartition
    9: optional list<NodeExecutionInfo> nodeExecutions
    10: optional list<string> terminalNodes
}

struct WorkflowStatusRequest {
    1: optional string workflowId
}

struct NodeStatusRequest {
    1: optional string nodeName
    2: optional string start
    3: optional string end
}

struct NodeStatusResponse {
    1: optional list<NodeExecutionInfo> nodeExecutions
}

struct ConfStatusRequest {
    1: optional string confName
    2: optional string start
    3: optional string end
    // TODO: To remove this after adding logic to pull latest hash
    4: optional string confHash
}

struct ConfStatusResponse {
    1: optional list<NodeExecutionInfo> nodeExecutions
    2: optional list<string> terminalNodes
}

struct WorkflowResponse {
    1: optional string workflowId
    2: optional string confName
    3: optional string mode
    4: optional string branch
    5: optional string user
    6: optional WorkflowStatus status
    7: optional string startPartition
    8: optional string endPartition
    10: optional list<string> terminalNodes
    11: optional ConfType confType
    12: optional string submissionTime
}

struct WorkflowListRequest {
    1: optional i32 limit
    // TODO: To remove this after migrating to above limit field
    2: optional i32 numOfWorkflows
}

struct WorkflowListResponse {
    1: optional list<WorkflowResponse> workflows
}

struct WorkflowCancelRequest {
    1: optional string workflowId
}

struct WorkflowCancelResponse {
    1: optional string message
}

/**
  * lists all confs of the specified type
  */
struct ConfListRequest {
    1: optional ConfType confType

    // if not specified we will pull conf list for main branch
    2: optional string branch
}

/**
  * Response for listing configurations of a specific type
  */
struct ConfListItemResponse {
    1: optional string confName
    2: optional ConfType confType
    3: optional string confHash
    4: optional map<planner.Mode, ScheduleStateResponse> modeSchedules
}

struct ConfListResponse {
    1: optional list<ConfListItemResponse> confs
}

struct ConfGetRequest {
    1: optional string confName
    2: optional ConfType confType
}

struct ConfGetResponse {
    1: optional string confName
    2: optional string confHash
    3: optional ConfType confType
    4: optional LogicalNode confContents
    5: optional map<planner.Mode, ScheduleStateResponse> modeSchedules
}

// ====================== End of Orchestration Service API Types ======================

enum CheckResult {
    SUCCESS = 0,
    FAILURE = 1,
    SKIPPED = 2
}

struct BaseEvalResult {
    1: optional CheckResult checkResult
    2: optional string message
}

struct JoinPartEvalResult {
    1: optional string partName
    2: optional GroupByEvalResult gbEvalResult
    3: optional BaseEvalResult keySchemaCheck
}

struct JoinEvalResult {
    1: optional string name
    2: optional BaseEvalResult leftExpressionCheck
    3: optional BaseEvalResult leftTimestampCheck
    4: optional list<JoinPartEvalResult> joinPartChecks
    5: optional BaseEvalResult derivationValidityCheck
    6: optional BaseEvalResult overallCheck
    10: optional map<string, string> leftQuerySchema
    11: optional map<string, string> rightPartsSchema
    12: optional map<string, string> derivationsSchema
    13: optional map<string, string> externalPartsSchema
    100: optional list<EvalUnion> parentEvals
}

struct GroupByEvalResult {
    1: optional string name
    2: optional BaseEvalResult sourceExpressionCheck
    3: optional BaseEvalResult sourceTimestampCheck
    4: optional BaseEvalResult aggExpressionCheck
    5: optional BaseEvalResult derivationsExpressionCheck
    6: optional BaseEvalResult overallCheck
    10: optional map<string, string> keySchema
    11: optional map<string, string> aggSchema
    12: optional map<string, string> derivationsSchema
    100: optional list<EvalUnion> parentEvals
}

struct StagingQueryEvalResult {
    1: optional string name
    2: optional BaseEvalResult overallCheck
    3: optional map<string, string> outputSchema
    100: optional list<EvalUnion> parentEvals
}

struct TableDependencyEvalResult {
    1: optional string tableName
    2: optional map<string, string> schema
    3: optional BaseEvalResult overallCheck
}

struct EvalUnion {
    1: optional StagingQueryEvalResult stagingQueryEval
    2: optional JoinEvalResult joinEval
    3: optional GroupByEvalResult groupByEval
    4: optional TableDependencyEvalResult tableDepEval
}

/**
* -- Phase 0 plan -- (same as chronon oss)
* StagingQuery::query - [deps.table] >> query
*
* GroupBy::upload     - [source.table] >> upload
* GroupBy::backfill   - [source.table] >> snapshot
*
* Join::label_join    - left.table, [bootstrap_part.table]?, [right.table], [label_part.table] >> label_join
*
*
* -- Phase 1 plan -- (broken up join)
* StagingQuery::query - [deps.table] >> query
*
* GroupBy::upload     - [source.table] >> upload
* GroupBy::backfill   - [source.table] >> snapshot
*
* Join::bootstrap     - left.table, [bootstrap_part.table]? >> bootstrap
* Join::right_part    - bootstrap, [right.table] >> right_part
* Join::merge         - bootstrap, [right_part] >> merge
* Join::derived       - merge >> derived
*
* Join::label_part    - bootstrap, [label_part.table] >> label_parts
* Join::label_join    - merge, [label_parts] >> label_join
*
*
* -- Phase 2 Plan -- sharing online w/ backfills (changes only)
* GroupBy::upload     - [source.table] >> sawtooth_ir/snapshot >> upload
* Join::right_part    - bootstrap, sawtooth_ir/snapshot(soft) + [right.table](fallback) >> right_part
*
*
* -- Phase 3 Plan -- incremental compute (changes only)
* GroupBy over events - [source.table] >> partial_ir >> robel_ir? >> sawtooth_ir/snapshot
*
*
* -- Phase 4 Plan -- model training
* Model::training          - [source.table] >> training
* Model::bulk_inference    - [source.table] >> bulk_inference
**/


/**
* physical node -> workflow id
*
*
* ad-hoc -> graph
*    we will trigger the root node with the right start_date and end_date
*
*
* Global Scheduler Workflow:
*   1. wakeup more frequently 15 minutes
*   2. scan database for unscheduled workflows
*   3. trigger unscheduled but required statuses
*
*
* Workflow is always triggered externally:
*
* node = get_node(name, version)
*
* node.trigger(start_date?, end_date, branch, is_scheduled):
*
*    # activity - 1
*    (missing_start, missing_end) = partition_dao.find_missing(start_date?, end_date)
*    missing_steps = compute_steps(missing_start, missing_end, branch_dao.get_step_days(this))
*
*    foreach_par missing_step in missing_steps:
*       foreach_par dependency in dependencies:
*
*           if dependency.is_internal:
*
*              (dep_start, dep_end) = dependency.compute_range(missing_step.start, missing_step.end)
*              # activity - 2
*              dependency.trigger_and_wait(dep_start, dep_end, branch)
*
*           else:
*
*              # activity - 3
*              if is_scheduled:
*                 dependency.wait(dep_start, dep_end)
*              else:
*                 dependency.fail_if_absent(dep_start, dep_end)
*
*       # activity - 4
*       node.submit_work_and_wait(missing_start, missing_end, branch_dao.get_conf(this))
*
*    return
*
*
*
*
* sync(physical_graph):
*
**/
