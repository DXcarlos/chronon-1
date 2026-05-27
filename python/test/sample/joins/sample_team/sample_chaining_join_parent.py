from group_bys.sample_team import (
    entity_sample_group_by_from_module,
    event_sample_group_by,
)
from sources import sample_sources

from ai.chronon.types import Join, JoinPart

parent_join = Join(
    left=sample_sources.event_source,
    row_ids="group_by_subject",
    right_parts=[
        JoinPart(
            group_by=event_sample_group_by.event_features,
            key_mapping={"subject": "group_by_subject"},
        ),
        JoinPart(
            group_by=entity_sample_group_by_from_module.entity_module_features,
            key_mapping={"subject": "group_by_subject"},
        ),
    ],
    online=True,
    check_consistency=True,
    historical_backfill=False,
    version=0,
)
