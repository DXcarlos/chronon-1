from gen_thrift.api.ttypes import JoinSource, Source
from joins.risk import user_transactions

from ai.chronon.types import ModelRuntime, Model, ModelBackend, Inference, Query, selects

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

model = Model(
    version="1.0",
    runtime=ModelRuntime(
        backend=ModelBackend.VERTEXAI,
        params={"model_type": "xgboost"}
    )
)

v1 = Inference(
    features=[source],
    models=[model],
    passthrough=["user_id"],
    version=1
)
