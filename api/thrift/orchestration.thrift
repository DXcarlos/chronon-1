namespace py ai.chronon.orchestration
namespace java ai.chronon.orchestration

include "common.thrift"
include "api.thrift"

enum TabularDataType {
    EVENT = 1,
    ENTITY = 2,
    CUMULATIVE_EVENTS = 3,
    // SCD2 = 4,
}

enum ConfType {
   STAGING_QUERY = 1
   GROUP_BY = 2
   JOIN = 3
   MODEL = 4
}

/**
* Represents a group of structured data assets that the same data flows through
* just a normalized version of Events + Entity sources.
**/
struct TabularData {
    1: optional string table
    2: optional string topic
    3: optional string mutationTable
    4: optional TabularDataType type
}

union LogicalNode {
    1: api.StagingQuery stagingQuery
    2: api.Join join
    3: api.GroupBy groupBy
    4: api.Model model
    5: TabularData tabularData
}

struct NodeKey {
    1: optional string name

    2: optional ConfType logicalType
    3: optional PhysicalNodeType physicalType
}

struct NodeInfo {
    /**
    * represents the computation that a node does
    * direct changes to conf that change output will affect semantic hash
    * changing spark params etc shouldn't affect this
    **/
    11: optional string semanticHash

    /**
    * simple hash of the entire conf (that is TSimpleJsonProtocol serialized),
    * computed by cli and used to check if new conf_json need to be pushed from user's machine
    **/
    12: optional string confHash

    /**
    * when new/updated conf's are pushed the branch is also set from the cli
    * upon merging the branch will be unset
    **/
    20: optional string branch

    /**
    * will be set to the author of the last semantic change to node
    * (non-semantic changes like code-mods or spark params don't affect this)
    **/
    21: optional string author

    /**
    * contents of the conf itself
    **/
    30: optional LogicalNode conf
}



struct NodeConnections {
    1: optional list<NodeKey> parents
    2: optional list<NodeKey> children
}

struct NodeGraph {
    1: optional map<NodeKey, NodeConnections> connections
    2: optional map<NodeKey, NodeInfo> infoMap
}


// TODO deprecate
// ====================== physical node types ======================
enum GroupByNodeType {
    PARTIAL_IR = 1,  // useful only for events - a day's worth of irs
    SAWTOOTH_IR = 2, // realtime features: useful for join backfills & uploads
    SNAPSHOT = 3,    // batch features: useful for join backfills and uploads

    // online nodes
    PREPARE_UPLOAD = 10,
    UPLOAD = 11,
    STREAMING = 12,
}

enum JoinNodeType{
    LEFT_SOURCE = 1
    BOOTSTRAP = 2,
    RIGHT_PART = 3,
    MERGE = 4,
    DERIVE = 5,
    LABEL_PART = 6,
    LABEL_JOIN = 7,

    // online nodes
    METADATA_UPLOAD = 20,

    // observability nodes
    PREPARE_LOGS = 21,
    SUMMARIZE = 40,
    DRIFT = 41,
    DRIFT_UPLOAD = 42,
}

enum StagingQueryNodeType {
    BACKFILL = 1
}

enum ModelNodeType {
    TRAINING = 300
    BULK_INFERENCE = 301
}

enum TableNodeType {
    MATERIALIZED = 1,
    VIEW = 2
}

union PhysicalNodeType {
    1: GroupByNodeType groupByNodeType
    2: JoinNodeType joinNodeType
    3: StagingQueryNodeType stagingNodeType
    4: ModelNodeType modelNodeType
    5: TableNodeType tableNodeType
}

struct PhysicalNode {
    1: optional string name
    2: optional PhysicalNodeType nodeType
    3: optional LogicalNode logicalNode
    4: optional string confHash
    100: optional list<common.TableDependency> tableDependencies
    101: optional list<string> outputColumns
    102: optional string outputTable
}

struct PhysicalGraph {
    1: optional PhysicalNode node,
    2: optional list<PhysicalGraph> dependencies
    3: optional common.DateRange range
}

// ====================== End of physical node types ======================

/**
* Multiple logical nodes could share the same physical node
* For that reason we don't have a 1-1 mapping between logical and physical nodes
* TODO -- kill this (typescript dependency)
**/
struct PhysicalNodeKey {
    1: optional string name
    2: optional PhysicalNodeType nodeType
}

enum NodeRunStatus {
    UNKNOWN = 0,
    WAITING = 1,
    RUNNING = 2,
    SUCCEEDED = 3,
    FAILED = 4
}

enum WorkflowStatus {
    UNKNOWN = 0,
    SUBMITTED = 1,
    RUNNING = 2,
    SUCCEEDED = 3,
    FAILED = 4
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

struct WorkflowStartRequest {
    1: optional string confName
    2: optional string mode
    3: optional string branch
    4: optional string user
    5: optional string start
    6: optional string end
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

struct NodeStepRunInfo {
    1: optional string runId
    2: optional string startPartition
    3: optional string endPartition
    4: optional string startTime
    5: optional string endTime
    6: optional NodeRunStatus status
    7: optional list<string> dependentStepRunIds
    8: optional string workflowId
}

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
}

struct WorkflowListRequest {
    1: optional i32 numOfWorkflows
}

struct WorkflowListResponse {
    1: optional list<WorkflowResponse> workflows
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
    1: optional BaseEvalResult leftExpressionCheck
    2: optional BaseEvalResult leftTimestampCheck
    3: optional list<JoinPartEvalResult> joinPartChecks
    4: optional BaseEvalResult derivationValidityCheck
    5: optional map<string, string> leftQuerySchema
    6: optional map<string, string> rightPartsSchema
    7: optional map<string, string> derivationsSchema
    8: optional map<string, string> externalPartsSchema
}

struct GroupByEvalResult {
    1: optional BaseEvalResult sourceExpressionCheck
    2: optional BaseEvalResult sourceTimestampCheck
    3: optional BaseEvalResult aggExpressionCheck
    4: optional BaseEvalResult derivationsExpressionCheck
    5: optional map<string, string> keySchema
    6: optional map<string, string> aggSchema
    7: optional map<string, string> derivationsSchema
}

struct StagingQueryEvalResult {
    1: optional BaseEvalResult queryCheck
    2: optional map<string, string> outputSchema
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
