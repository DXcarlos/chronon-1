from joins.gcp import user_category_parent

from ai.chronon.group_by import Accuracy, Aggregation, GroupBy, Operation, TimeUnit, Window
from ai.chronon.query import Query, selects
from ai.chronon.source import JoinSource

v1 = GroupBy(
    sources=[
        JoinSource(
            join=user_category_parent.parent_join,
            query=Query(
                selects=selects(
                    user_id="user_id",
                    # Concatenate event_type and primary_category into a single pair string.
                    # listing_id_primary_category is the dim_listings passthrough column prefixed by the join key.
                    event_category_pair="CONCAT(event_type, ':', listing_id_primary_category)",
                ),
                time_column="ts",
            ),
        )
    ],
    keys=["user_id"],
    aggregations=[
        Aggregation(
            input_column="event_category_pair",
            operation=Operation.LAST_K(100),
            windows=[Window(length=7, time_unit=TimeUnit.DAYS)],
        )
    ],
    online=True,
    accuracy=Accuracy.TEMPORAL,
    version=1,
)
