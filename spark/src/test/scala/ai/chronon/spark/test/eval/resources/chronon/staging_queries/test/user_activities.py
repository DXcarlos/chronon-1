"""
Simple staging query for testing local evaluation
Creates a user events table with basic user activity data
"""
from ai.chronon.staging_query import StagingQuery, TableDependency

# Define the staging query that creates user events
v0 = StagingQuery(
    # Simple transformation to clean and enrich the data
    query="""
        SELECT 
            user_id,
            click_event,
            purchase_event,
            row_id,
            ts,
            ds
        FROM local_test.user_activities
        WHERE ds >= {{ start_date }}
          AND ds <= {{ end_date }}
    """,
    version = 0,
    output_namespace = "local_test",
    dependencies=[
        TableDependency(table="local_test.user_activities", partition_column="ds", offset=0)],
)
