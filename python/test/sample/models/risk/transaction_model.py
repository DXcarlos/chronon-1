from joins.risk import user_transactions

from gen_thrift.api.ttypes import DataKind, JoinSource, Source, TDataType
from ai.chronon.model import Model, ModelType
from ai.chronon.query import Query, selects

"""
This is the "left side" of the join that will comprise our training set. It is responsible for providing the primary keys
and timestamps for which features will be computed.
"""
source = Source(
    joinSource=JoinSource(
        join=user_transactions.txn_join,
        query=Query(
            selects=selects("user_id"),
        ),
    )
)

v1 = Model(
    source=source, outputSchema=TDataType(DataKind.DOUBLE), modelType=ModelType.XGBoost
)
