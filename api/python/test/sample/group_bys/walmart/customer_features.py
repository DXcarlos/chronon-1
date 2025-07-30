from typing import Dict
from ai.chronon.types import *

# step 1. create source with all necessary entity ids - we will enrich with advisory, product_type and pi_hash use_counts
transaction_source = EventSource(
    table="bigquery://payments.txn_log",  # iceberg, hudi, delta are also supported
    topic="kafka://payments.txn_events",  # pub/sub, kinesis are also supported
    query=Query(
        selects=selects(            
            order_id="order_number", # we are going to enrich this with advisory
            customer_id="cust_id",
            payment_instrument_id="pi_hash", # self join with previous use_counts                         
            items="items"  # we are going to enrich this array with a new "product_type_last" field inside the payload struct
        )
    ),
)

# step 2.a: advisory source to enrich transaction source with
advisory_info = GroupBy(
    sources=[
        EventSource(
            table="kafka://payments_risk.order_advisory",
            topic="bigquery://catalog.product_info_mutation_log",
            query=Query(
                selects=selects(
                    order_id="order_number",
                    advisory_recommendation="recommendation",
                )
            ),
        )
    ],
    keys=["order_number"],
    aggregations=[Aggregation(input_column="recommendation", operation=Operation.LAST)],
)

# step 2.b: pi_hash use counter to enrich transaction source with
# when the count here is zero, it means that this user
# is using this payment instrument for the first time
payment_instrument_use_counts = GroupBy(
    sources=[transaction_source],
    keys=["payment_instrument_id", "customer_id"],
    aggregations=[
        Aggregation(input_column="payment_instrument_id", operation=Operation.COUNT, windows=["7d"])
    ],
)

# step 2.c product_info source to enrich transaction source with
# modeling this as an entity source - given that these typically originate from OLTP tables or dim_tables
product_info = GroupBy(
    sources=[
        EntitySource(
            snapshot_table="bigquery://catalog.product_info_snapshot",
            mutation_topic="kafka://catalog.product_info_mutation_events",
            mutation_table="bigquery://catalog.product_info_mutation_log",
            query=Query(
                selects=selects(
                    item_id="item_id",
                    product_type="product_type",
                    # any other fields if needed
                )
            ),
        )
    ],  # could "union" multiple sources if you want
    keys=["item_id"],  # composite keys are also supported
    aggregations=[Aggregation(input_column="product_type", operation=Operation.LAST)],
)

reward_source = EntitySource(
    snapshot_table="bigquery://user_info.rewards",
    mutation_topic="kafka://user_info.rewards_mutation_events",
    mutation_table="bigquery://user_info.rewards_mutation_log",
    query=Query(
        selects=selects(
            "customer_id",
            "reward_amount",
            # any other fields if needed
        )
    ),
)

# step 2.d reward balance source to enrich transaction source with
reward_balance = GroupBy(
    sources=[reward_source],  # could "union" multiple sources if you want
    keys=["customer_id"],  # composite keys are also supported
    aggregations=[Aggregation(input_column="reward_amount", operation=Operation.LAST)],
)

transactions_with_rewards = Join(
    left = transaction_source,
    right_parts=[reward_balance],    
)

# two level chaining
payment_instrument_rewards = GroupBy(
   source=JoinSource(
       join=transactions_with_rewards,
       query=Query(
           selects=selects(
               "payment_instrument_id", "customer_id", "reward_amount_last"
           )
       ) 
   ),
   keys=["payment_instrument_id"],
   aggregations=[
       Aggregation(input_column="reward_amount_last", operation=Operation.SUM, windows=["7d"])
   ]     
)

enriched_transaction_source = Join(
    left=transaction_source,
    right_parts=[
        JoinPart(group_by=advisory_info),
        JoinPart(group_by=payment_instrument_use_counts),  # effectively a self join
        JoinPart(group_by=product_info, key_mapping={"items.item_id": "item_id"}), # enriches the nested array by joining on inner item_ids
        JoinPart(group_by=payment_instrument_rewards), # contains rewards by pi (2 level chaining)
        JoinPart(group_by=reward_balance) # contains rewards for a customer id (more direct)
    ],
    row_ids=["order_number"] # necessary for enriching nested item data
)

txn_features = GroupBy(
    sources=[
        JoinSource(
            join=enriched_transaction_source,
            query=Query(
                selects=selects(
                    "customer_id",
                    "order_id",
                    "payment_instrument_id",
                    decline="IF(UPPER(recommendation_last) = 'DECLINE', 1, 0)",
                    # filters our electronic items and sums the amounts 
                    # this is a spark sql column expression that operates on nested data
                    # if-s and for-s become FILTER & AGGREGATE or TRANSFORM in spark SQL
                    electronic_amt=""" 
                        AGGREGATE(
                            FILTER(items, 
                                item -> CONTAINS(UPPER(item.product_type_last), 'ELECTRONICS')
                            )
                            0, (acc, item) -> acc + item.amount                        
                        )
                    """,            
                    first_payment_isntrument_use="IF(payment_instrument_id_count = 0, 1, 0)",
                    total_pi_used_cust_reward_7d="reward_amount_last_sum_7d",
                    customer_reward_amout="reward_amount_last"                
                )
            ),
        )
    ],
    keys=["customer_id"],
    aggregations=[
        Aggregation(
            input_column="order_id",
            operation=Operation.COUNT,
            windows=["10m", "1h", "30d"],
        ),
        Aggregation(input_column="decline", operation=Operation.SUM, windows=["90d"]),
        Aggregation(
            input_column="payment_instrument_id", operation=Operation.UNIQUE_COUNT, windows=["7d"]
        ),
        Aggregation(
            input_column="first_payment_isntrument_use", operation=Operation.COUNT, windows=["7d"]
        ),
        Aggregation(
            input_column="electronic_amt", operation=Operation.SUM, windows=["7d"]
        ),
        Aggregation(
            input_column="customer_reward_amout", operation=Operation.LAST
        ),
        Aggregation(
            input_column="total_pi_used_cust_reward_7d", operation=Operation.LAST
        ),
    ],
)

