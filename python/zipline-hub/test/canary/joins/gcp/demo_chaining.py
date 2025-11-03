from group_bys.gcp import user_activities_chained
from joins.gcp import demo_parent
from staging_queries.gcp import exports

from ai.chronon.join import Derivation, Join, JoinPart
from ai.chronon.query import Query, selects
from ai.chronon.source import EventSource

"""
Final Join in a chaining scenario that uses as its left source the
demo_parent Join and enriches it with the chained_user_gb GroupBy.
"""
downstream_join = Join(
    left=demo_parent.source,
    row_ids=["event_id"],
    right_parts=[
        JoinPart(
            group_by=user_activities_chained.chained_user_gb,
        ),
    ],
    version=0,
    online=True,
    output_namespace="data",
)
