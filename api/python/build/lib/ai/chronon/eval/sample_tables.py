import os
from typing import List

from ai.chronon.eval.table_scan import local_warehouse


def sample_with_query(table, query, output_path) -> str:
    # if file exists, skip
    if os.path.exists(output_path):
        print(f"File {output_path} already exists. Skipping sampling.")
        return output_path

    raw_scan_query = query
    print(f"Sampling {table} with query: {raw_scan_query}")

    _sample_internal(raw_scan_query, output_path)
    return output_path


def sample_tables(table_names: List[str]) -> None:

    for table in table_names:
        query = f"SELECT * FROM {table} LIMIT 10000"
        sample_with_query(table, query, local_warehouse / f"{table}.parquet")


_sampling_engine = os.getenv("CHRONON_SAMPLING_ENGINE", "bigquery")


def _sample_internal(query, output_path) -> str:
    if _sampling_engine == "bigquery":
        _sample_bigquery(query, output_path)
    elif _sampling_engine == "trino":
        _sample_trino(query, output_path)
    else:
        raise ValueError("Invalid sampling engine")


def _sample_trino(query, output_path):
    raise NotImplementedError("Trino sampling is not yet implemented")


def _sample_bigquery(query, output_path):

    from google.cloud import bigquery

    project_id = os.getenv("GCP_PROJECT_ID")
    assert project_id, "Please set the GCP_PROJECT_ID environment variable"

    client = bigquery.Client(project=project_id)

    results = client.query_and_wait(query)

    df = results.to_dataframe()
    df.to_parquet(output_path)


def _sample_bigquery_fast(query, destination_path):
    import os

    import pyarrow.parquet as pq
    from google.cloud import bigquery
    from google.cloud.bigquery_storage import BigQueryReadClient
    from google.cloud.bigquery_storage_v1.types import DataFormat, ReadSession

    project_id = os.getenv("GCP_PROJECT_ID")
    assert project_id, "Please set the GCP_PROJECT_ID environment variable"

    client = bigquery.Client(project=project_id)
    bqstorage_client = BigQueryReadClient()

    # Create query job
    query_job = client.query(query)
    table_ref = query_job.destination

    # Create read session
    read_session = ReadSession()
    read_session.table = table_ref.to_bqstorage()
    read_session.data_format = DataFormat.ARROW

    print("Fetching from BigQuery... (this might take a while)")

    session = bqstorage_client.create_read_session(
        parent=f"projects/{client.project}",
        read_session=read_session,
        max_stream_count=1,
    )

    print("Writing to local parquet file...")

    # Read using Arrow
    stream = bqstorage_client.read_rows(session.streams[0].name)
    table = stream.to_arrow(read_session=session)

    # Write to Parquet directly
    pq.write_table(table, destination_path)

    print(f"Wrote results to {destination_path}")

    return destination_path
