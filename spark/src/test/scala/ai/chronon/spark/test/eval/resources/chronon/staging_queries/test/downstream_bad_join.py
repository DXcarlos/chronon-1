"""
Staging query that uses the output of the bad_key join
This should fail because the upstream join has key schema failures
"""
from ai.chronon.staging_query import StagingQuery, TableDependency
from ai.chronon.utils import get_join_output_table_name
from joins.test import bad_key

# Define staging query that depends on the failing bad_key join
v0 = StagingQuery(
    # Use the output table from the failing join
    query="""
        SELECT 
            user_id,
            row_id,
            user_id_clicks_sum_1d,
            user_id_purchases_sum_1d,
            ts,
            ds
        FROM {}
        WHERE ds >= {{{{ start_date }}}}
          AND ds <= {{{{ end_date }}}}
    """.format(get_join_output_table_name(bad_key.v1, True)),
    version = 0,
    output_namespace = "local_test",
    dependencies=[
        TableDependency(table=get_join_output_table_name(bad_key.v1, True), partition_column="ds", offset=0)
    ],
)