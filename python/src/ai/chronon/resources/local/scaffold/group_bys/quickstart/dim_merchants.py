from staging_queries.quickstart import exports

from ai.chronon.group_by import GroupBy
from ai.chronon.query import Query, selects
from ai.chronon.source import EntitySource

"""
Passthrough GroupBy on the dim_merchants table.
Keyed by merchant_id — joined via merchant_id on the left event source.
"""

source = EntitySource(
    snapshot_table=exports.dim_merchants.table,
    query=Query(
        selects=selects(
            merchant_id="merchant_id",
            primary_category="primary_category",
        ),
        start_partition="2023-11-01",
    ),
)

v1 = GroupBy(
    sources=[source],
    keys=["merchant_id"],
    online=True,
    version=0,
    aggregations=None,  # Simple passthrough — no aggregations
)
