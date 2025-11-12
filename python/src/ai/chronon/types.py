"""
importing ai.chronon.types will bring in all the api's needed to create any chronon object
"""

import gen_thrift.api.ttypes as ttypes
import gen_thrift.common.ttypes as common

import ai.chronon.group_by as group_by
import ai.chronon.join as join
import ai.chronon.query as query
import ai.chronon.source as source

# source related concepts
Query = query.Query
selects = query.selects

Source = ttypes.Source
EventSource = source.EventSource
EntitySource = source.EntitySource
JoinSource = source.JoinSource

# Aggregation / GroupBy related concepts
GroupBy = group_by.GroupBy
Aggregation = group_by.Aggregation
Operation = group_by.Operation
Window = group_by.Window
TimeUnit = group_by.TimeUnit
DefaultAggregation = group_by.DefaultAggregation

Accuracy = ttypes.Accuracy
TEMPORAL = ttypes.Accuracy.TEMPORAL
SNAPSHOT = ttypes.Accuracy.SNAPSHOT

Derivation = group_by.Derivation

# join related concepts
Join = join.Join
JoinPart = join.JoinPart
BootstrapPart = join.BootstrapPart
ContextualSource = join.ContextualSource
ExternalPart = join.ExternalPart
ExternalSource = join.ExternalSource
DataType = join.DataType


# Staging Query related concepts
StagingQuery = ttypes.StagingQuery
MetaData = ttypes.MetaData


EnvironmentVariables = common.EnvironmentVariables
ConfigProperties = common.ConfigProperties
ClusterConfigProperties = common.ClusterConfigProperties
ExecutionInfo = common.ExecutionInfo
TableDependency = common.TableDependency

Team = ttypes.Team
