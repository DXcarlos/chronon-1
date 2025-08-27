
from sources import test_sources

from ai.chronon.group_by import Aggregation, Derivation, GroupBy, Operation

# V1 does not include base fields
v1 = GroupBy(
    sources=test_sources.event_source,
    keys=["group_by_subject"],
    aggregations=[
        Aggregation(input_column="event", operation=Operation.SUM, windows=["7d"]),
        Aggregation(input_column="event", operation=Operation.SUM),
    ],
    online=True,
    derivations=[
        Derivation("sum_ratio", "event_sum_7d * 1.0 / event_sum"),
    ],
    output_namespace="sample_namespace",
    version=0,
)


# V2 dooes include base fields
v2 = GroupBy(
    sources=test_sources.event_source,
    keys=["group_by_subject"],
    aggregations=[
        Aggregation(input_column="subject", operation=Operation.SUM, windows=["7d"]),
        Aggregation(input_column="subject", operation=Operation.SUM),
    ],
    online=True,
    derivations=[
        Derivation("subject_x_10", "subject_sum_7d * 10"),
        Derivation("*", "*"),  # Include the base field
    ],
    output_namespace="sample_namespace",
    version=0,
)
