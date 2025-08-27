from gen_thrift.api.ttypes import DataKind, EventSource, Source, TDataType
from ai.chronon.model import Model, ModelType
from ai.chronon.query import Query, selects

"""
This is the "left side" of the join that will comprise our training set. It is responsible for providing the primary keys
and timestamps for which features will be computed.
"""
source = Source(
    events=EventSource(
        table="data.checkouts",
        query=Query(
            selects=selects("user_id"),
            time_column="ts",
        ),
    )
)

v1 = Model(
    source=source, outputSchema=TDataType(DataKind.DOUBLE), modelType=ModelType.XGBoost
)
