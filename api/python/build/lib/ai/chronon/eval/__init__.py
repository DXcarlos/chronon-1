from typing import Any, List

from pyspark.sql import DataFrame, SparkSession

import ai.chronon.api.ttypes as chronon
from ai.chronon.eval.query_parsing import get_tables_from_query
from ai.chronon.eval.sample_tables import sample_tables, sample_with_query
from ai.chronon.eval.table_scan import (
    TableScan,
    clean_table_name,
    table_scans_in_group_by,
    table_scans_in_join,
    table_scans_in_source,
)


def eval(obj: Any) -> List[DataFrame]:

    if isinstance(obj, chronon.Source):
        return _run_table_scans(table_scans_in_source(obj))

    elif isinstance(obj, chronon.GroupBy):
        return _run_table_scans(table_scans_in_group_by(obj))

    elif isinstance(obj, chronon.Join):
        return _run_table_scans(table_scans_in_join(obj))

    elif isinstance(obj, chronon.StagingQuery):
        return _sample_and_eval_query(_render_staging_query(obj))

    elif isinstance(obj, str):
        has_white_spaces = any(char.isspace() for char in obj)
        if has_white_spaces:
            return _sample_and_eval_query(obj)
        else:
            return _sample_and_eval_query(f"SELECT * FROM {obj} LIMIT 1000")

    elif isinstance(obj, chronon.Model):
        _run_table_scans(table_scans_in_source(obj.source))

    else:
        raise Exception(f"Unsupported object type for: {obj}")


def _sample_and_eval_query(query: str) -> DataFrame:

    table_names = get_tables_from_query(query)
    sample_tables(table_names)

    clean_query = query
    for table_name in table_names:
        clean_name = clean_table_name(table_name)
        clean_query = clean_query.replace(table_name, clean_name)

    return _run_query(clean_query)


def _run_query(query: str) -> DataFrame:
    spark = _get_spark()
    return spark.sql(query)


def _sample_table_scan(table_scan: TableScan) -> str:
    table = table_scan.table
    output_path = table_scan.output_path()
    query = table_scan.raw_scan_query(local_table_view=False)
    return sample_with_query(table, query, output_path)


def _run_table_scans(table_scans: List[TableScan]) -> List[DataFrame]:
    spark = _get_spark()
    df_list = []

    for table_scan in table_scans:
        output_path = table_scan.output_path()

        status = " (exists)" if output_path.exists() else ""
        print(
            f"table: {table_scan.table}\n"
            f"view: {table_scan.view_name()}\n"
            f"local_file: {output_path}{status}\n"
        )

    for table_scan in table_scans:

        view_name = table_scan.view_name()
        output_path = _sample_table_scan(table_scan)

        print(f"Creating view {view_name} from parquet file {output_path}")
        df = spark.read.parquet(str(output_path))
        df.createOrReplaceTempView(view_name)

        scan_query = table_scan.scan_query(local_table_view=True)
        print(f"Scanning {table_scan.table} with query: \n{scan_query}\n")
        df = spark.sql(scan_query)
        df.show(5)
        df_list.append(df)

    return df_list


_spark: SparkSession = None


def _get_spark() -> SparkSession:
    global _spark
    if not _spark:
        _spark = (
            SparkSession.builder.appName("Chronon Evaluator")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.sql.parquet.columnarReaderBatchSize", "16")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.memory", "4g")
            .config("spark.driver.maxResultSize", "2g")
            .getOrCreate()
        )
    return _spark


def _render_staging_query(staging_query: chronon.StagingQuery) -> str:
    raise NotImplementedError("Staging query evals are not yet implemented")
