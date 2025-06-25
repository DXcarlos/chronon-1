
import inspect
import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Union

import ai.chronon.airflow_helpers as airflow_helpers
import ai.chronon.api.common.ttypes as common
import ai.chronon.api.ttypes as ttypes
from ai.chronon.constants import AIRFLOW_DEPENDENCIES_KEY


# Wrapper for EngineType
class EngineType:
    SPARK = ttypes.EngineType.SPARK
    BIGQUERY = ttypes.EngineType.BIGQUERY

@dataclass
class TableDependency:
    table: str
    partition_column: Optional[str] = None
    additional_partitions: Optional[List[str]] = None
    offset: Optional[int] = None

    def to_thrift(self):
        offset_window = common.Window(length = self.offset, timeUnit= common.TimeUnit.DAYS) if self.offset else None
        return common.TableDependency(
            tableInfo=common.TableInfo(
                table=self.table, 
                partitionColumn=self.partition_column,
            ),
            startOffset=offset_window,
            endOffset=offset_window,
            startCutOff=None,
            endCutOff=None
        )

def StagingQuery(
    name: str,
    query: str,
    output_namespace: Optional[str] = None,
    start_partition: Optional[str] = None,
    table_properties: Optional[Dict[str, str]] = None,
    setups: Optional[List[str]] = None,
    partition_column: Optional[str] = None,
    engine_type: Optional[EngineType] = None,
    dependencies: Optional[List[Union[TableDependency, Dict]]] = None,
    tags: Optional[Dict[str, str]] = None,
    # execution params
    offline_schedule: str = "@daily",
    conf: Optional[common.ConfigProperties] = None,
    env_vars: Optional[common.EnvironmentVariables] = None,
    cluster_conf: common.ClusterConfigProperties = None,
    step_days: Optional[int] = None,
    recompute_days: Optional[int] = None,
) -> ttypes.StagingQuery:
    """
    Creates a StagingQuery object for executing arbitrary SQL queries with templated date parameters.

    :param query:
        Arbitrary spark query that should be written with template parameters:
        - `{{ start_date }}`: Initial run uses start_partition, future runs use latest partition + 1 day
        - `{{ end_date }}`: The end partition of the computing range
        - `{{ latest_date }}`: End partition independent of the computing range (for cumulative sources)
        - `{{ max_date(table=namespace.my_table) }}`: Max partition available for a given table
        These parameters can be modified with offset and bounds:
        - `{{ start_date(offset=-10, lower_bound='2023-01-01', upper_bound='2024-01-01') }}`
    :type query: str
    :param start_partition:
        On the first run, `{{ start_date }}` will be set to this user provided start date,
        future incremental runs will set it to the latest existing partition + 1 day.
    :type start_partition: str
    :param setups:
        Spark SQL setup statements. Used typically to register UDFs.
    :type setups: List[str]
    :param partition_column:
        Only needed for `max_date` template
    :type partition_column: str
    :param engine_type:
        By default, spark is the compute engine. You can specify an override (eg. bigquery, etc.)
        Use the EngineType class constants: EngineType.SPARK, EngineType.BIGQUERY, etc.
    :type engine_type: int
    :param tags:
        Additional metadata that does not directly affect computation, but is useful for management.
    :type tags: Dict[str, str]
    :param offline_schedule:
        The offline schedule interval for batch jobs. Format examples:
        '@hourly': '0 * * * *',
        '@daily': '0 0 * * *',
        '@weekly': '0 0 * * 0',
        '@monthly': '0 0 1 * *',
        '@yearly': '0 0 1 1 *'
    :type offline_schedule: str
    :param conf:
        Configuration properties for the StagingQuery.
    :type conf: common.ConfigProperties
    :param env_vars:
        Environment variables for the StagingQuery.
    :type env_vars: common.EnvironmentVariables
    :param cluster_conf:
        Cluster configuration properties for the join.
    :param step_days:
        The maximum number of days to process at once
    :type step_days: int
    :param dependencies:
        List of dependencies for the StagingQuery. Each dependency can be either a TableDependency object
        or a dictionary with 'name' and 'spec' keys.
    :type dependencies: List[Union[TableDependency, Dict]]
    :param recompute_days:
        Used by orchestrator to determine how many days are recomputed on each incremental scheduled run. Should be
        set when the source data is changed in-place (i.e. existing partitions overwritten with new data each day up to
        X days later) or when you want partially mature aggregations (i.e. a 7 day window, but start computing it from
        day 1, and refresh it for the next 6 days)
    :type recompute_days: int
    :return:
        A StagingQuery object
    """
    # Get caller's filename to assign team
    team = inspect.stack()[1].filename.split("/")[-2]

    # Create execution info
    exec_info = common.ExecutionInfo(
        scheduleCron=offline_schedule,
        conf=conf,
        env=env_vars,
        stepDays=step_days,
        clusterConf=cluster_conf
    )

    airflow_dependencies = []

    if dependencies:
        for d in dependencies:
            if isinstance(d, TableDependency):
                # Create an Airflow dependency object for the table
                airflow_dependency = airflow_helpers.create_airflow_dependency(
                    d.table,
                    d.partition_column,
                    d.additional_partitions,
                    d.offset,
                )
                airflow_dependencies.append(airflow_dependency)
            elif isinstance(d, dict):
                # If it's already a dictionary, just append it
                airflow_dependencies.append(d)
            else:
                raise ValueError(
                    "Dependencies must be either TableDependency instances or dictionaries."
                )

    custom_json = json.dumps({AIRFLOW_DEPENDENCIES_KEY: airflow_dependencies})

    # Create metadata
    meta_data = ttypes.MetaData(
        name=name,
        outputNamespace=output_namespace,
        team=team,
        executionInfo=exec_info,
        tags=tags,
        customJson=custom_json,
        tableProperties=table_properties,
    )

    thrift_deps = []
    if dependencies and len(dependencies) > 0:
        for d in dependencies:
            if d and isinstance(d, TableDependency):
                thrift_deps.append(d.to_thrift())

    # Create and return the StagingQuery object with camelCase parameter names
    staging_query = ttypes.StagingQuery(
        metaData=meta_data,
        query=query,
        startPartition=start_partition,
        setups=setups,
        partitionColumn=partition_column,
        engineType=engine_type,
        tableDependencies=thrift_deps,
        recomputeDays=recompute_days,
    )

    return staging_query