import hashlib
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple

import ai.chronon.api.ttypes as chronon


def clean_table_name(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)


local_warehouse = Path(os.getenv("CHRONON_ROOT", os.getcwd())) / "local_warehouse"
limit = int(os.getenv("SAMPLE_LIMIT", "100"))
# create local_warehouse if it doesn't exist
local_warehouse.mkdir(parents=True, exist_ok=True)


@dataclass
class TableScan:
    table: str
    partition_col: str
    partition_date: str
    query: chronon.Query
    is_mutations: bool = False

    def output_path(self) -> str:
        return Path(local_warehouse) / f"{self.view_name()}.parquet"

    def view_name(self) -> str:
        return clean_table_name(self.table) + "_" + self.where_id()

    def table_name(self, local_table_view) -> str:
        return self.view_name() if local_table_view else self.table

    def where_id(self) -> str:
        return "_" + hashlib.md5(self.where_block().encode()).hexdigest()[:3]

    def where_block(self) -> str:
        wheres = []
        partition_scan = f"{self.partition_col} = '{self.partition_date}'"
        wheres.append(partition_scan)

        if self.query.wheres:
            wheres.extend(self.query.wheres)

        return " AND\n    ".join([f"({where})" for where in wheres])

    def raw_scan_query(self, local_table_view: bool = True) -> str:
        return f"""
SELECT * FROM {self.table_name(local_table_view)}
WHERE
    {self.where_block()}
LIMIT {limit}
"""

    def scan_query(self, local_table_view=True) -> str:
        selects = []
        base_selects = self.query.selects.copy()

        if self.is_mutations:
            base_selects["is_before"] = coalesce(self.query.reversalColumn, "is_before")
            base_selects["mutation_ts"] = coalesce(
                self.query.mutationTimeColumn, "mutation_ts"
            )

        if self.query.timeColumn:
            base_selects["ts"] = coalesce(self.query.timeColumn, "ts")

        for k, v in base_selects.items():
            selects.append(f"{v} as {k}")
        select_clauses = ",\n    ".join(selects)

        return f"""
SELECT
    {select_clauses}
FROM
    {self.table_name(local_table_view)}
WHERE
    {self.where_block()}
LIMIT
    {limit}
"""


# TODO: use teams.py to get the default date column
DEFAULT_DATE_COLUMN = "_date"
DEFAULT_DATE_FORMAT = "%Y-%m-%d"

two_days_ago = (datetime.now() - timedelta(days=2)).strftime(DEFAULT_DATE_FORMAT)

_sample_date = os.getenv("SAMPLE_DATE", two_days_ago)


def get_date(query: chronon.Query) -> Tuple[str, str]:
    assert query and query.selects, "please specify source.query.selects"

    partition_col = query.selects.get("ds", DEFAULT_DATE_COLUMN)
    partition_date = coalesce(query.endPartition, _sample_date)

    return (partition_col, partition_date)


def coalesce(*args):
    for arg in args:
        if arg:
            return arg


def table_scans_in_source(source: chronon.Source) -> List[TableScan]:
    result = []

    if not source:
        return result

    if source.entities:
        query: chronon.Query = source.entities.query
        col, date = get_date(query)

        snapshot = TableScan(source.entities.snapshotTable, col, date, query)
        result.append(snapshot)

        if source.entities.mutationTable:
            mutations = TableScan(source.entities.mutationTable, col, date, query, True)
            result.append(mutations)

    if source.events:
        query = source.events.query
        col, date = get_date(query)
        table = TableScan(source.events.table, col, date, query)
        result.append(table)

    if source.joinSource:
        result.extend(table_scans_in_source(source.joinSource.join.left))

    return result


def table_scans_in_sources(sources: List[chronon.Source]) -> List[TableScan]:
    result = []

    for source in sources:
        result.extend(table_scans_in_source(source))

    return result


def table_scans_in_group_by(gb: chronon.GroupBy) -> List[TableScan]:
    if not gb:
        return []

    return table_scans_in_sources(gb.sources)


def table_scans_in_join(join: chronon.Join) -> List[TableScan]:

    result = []

    if not join:
        return result

    result.extend(table_scans_in_source(join.left))

    parts: List[chronon.JoinPart] = join.joinParts
    if parts:
        for part in parts:
            result.extend(table_scans_in_group_by(part.groupBy))

    bootstraps: List[chronon.BootstrapPart] = join.bootstrapParts
    if bootstraps:
        for bootstrap in bootstraps:
            query = bootstrap.query
            col, date = get_date(query)
            bootstrap = TableScan(bootstrap.table, col, date, query)

            result.append(bootstrap)

    if join.labelParts:
        labelParts: List[chronon.JoinPart] = join.labelParts.labels
        for part in labelParts:
            result.extend(table_scans_in_sources(part.groupBy))

    return result
