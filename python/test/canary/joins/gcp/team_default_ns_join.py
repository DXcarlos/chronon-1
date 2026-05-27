"""Canary example: a Join whose left source pulls its table name via `.table`
from a StagingQuery that does NOT set `output_namespace=`.

`team_default_ns_example.checkouts_with_team_default_namespace.table` must resolve to `<team_ns>.<clean_name>` at
compile time — via the enclosing `gcp` team's `outputNamespace="data"` default
— without requiring a filesystem lookup at Python authoring time.
"""

from group_bys.gcp import purchases
from staging_queries.gcp import team_default_ns_example

from ai.chronon.types import EventSource, Join, JoinPart, Query, selects

source = EventSource(
    table=team_default_ns_example.checkouts_with_team_default_namespace.table,
    query=Query(
        selects=selects("user_id"),
        time_column="ts",
    ),
)

team_default_training_join = Join(
    left=source,
    row_ids="user_id",
    right_parts=[JoinPart(group_by=purchases.purchase_features_test)],
    version=0,
)
