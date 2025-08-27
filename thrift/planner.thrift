namespace py gen_thrift.planner
namespace java ai.chronon.planner

include "common.thrift"
include "api.thrift"

struct SourceWithFilterNode {
    2: optional api.Source source
    3: optional map<string,list<string>> excludeKeys
}

struct JoinBootstrapNode {
    2: optional api.Join join
}

struct JoinMergeNode {
    2: optional api.Join join
    // If set, the merge job will look for the output table of the production version to reuse as many unchanged columns as possible
    3: optional api.Join productionJoin
}

struct JoinDerivationNode {
    2: optional api.Join join
}

struct JoinPartNode {
    2: optional string leftSourceTable
    3: optional api.DataModel leftDataModel
    4: optional api.JoinPart joinPart
    5: optional map<string, list<string>> skewKeys
    // If set, the merge job will look for the output table of the production version to reuse as many unchanged columns as possible
    6: optional api.JoinPart productionJoinPart
}

struct LabelJoinNode {
    2: optional api.Join join
}

struct MonolithJoinNode {
    1: optional api.Join join
}

struct StagingQueryNode {
    2: optional api.StagingQuery stagingQuery
}

struct GroupByBackfillNode {
    2: optional api.GroupBy groupBy
}

struct GroupByUploadNode {
    2: optional api.GroupBy groupBy
}

struct GroupByUploadToKVNode {
    2: optional api.GroupBy groupBy
}

struct GroupByStreamingNode {
    2: optional api.GroupBy groupBy
}

struct JoinMetadataUpload {
    2: optional api.Join join
}

struct ExternalSourceSensorNode {
    1: optional api.MetaData metaData
    2: optional common.TableDependency sourceTableDependency
    3: optional i64 retryCount
    4: optional i64 retryIntervalMin
}
union NodeContent {
    // join nodes
    1: SourceWithFilterNode sourceWithFilter
    2: JoinBootstrapNode joinBootstrap
    3: JoinPartNode joinPart
    4: JoinMergeNode joinMerge
    5: JoinDerivationNode joinDerivation
    6: LabelJoinNode labelJoin
    7: MonolithJoinNode monolithJoin
    8: StagingQueryNode stagingQuery

    10: JoinMetadataUpload joinMetadataUpload
    11: ExternalSourceSensorNode externalSourceSensor


    // groupBy nodes
    100: GroupByBackfillNode groupByBackfill
    101: GroupByUploadNode groupByUpload
    102: GroupByStreamingNode groupByStreaming
    103: GroupByUploadToKVNode groupByUploadToKV

    // stagingQuery nodes
    200: api.StagingQuery stagingQueryBackfill

    // TODO: add metrics nodes
}

enum Mode {
    BACKFILL = 0,
    DEPLOY = 1,
    MONITOR = 2
}

struct Node {
    1: optional api.MetaData metaData
    2: optional NodeContent content
    3: optional string semanticHash
    4: optional bool isLongRunning
}

// some nodes could be depended on by both offline and online nodes
struct ConfPlan {
    1: optional list<Node> nodes
    2: optional map<Mode, string> terminalNodeNames
}

// CLI rough sketch
// zipline backfill conf_name             -- runs all nodes upstream of terminal offline node - prepares training data
// zipline deploy conf_name               -- runs all nodes upstream of terminal online node - makes it ready for serving
// zipline run-only node_name             -- runs one particular node
// zipline run node_name                  -- runs all nodes upstream

// zipline compile                            -- compiles python
// zipline plan conf start end                -- shows the node-step-graph that is produced by planner
// zipline sync                               -- compiles and uploads confs of this branch to remote