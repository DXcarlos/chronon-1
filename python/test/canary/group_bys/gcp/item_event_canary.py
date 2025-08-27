from gen_thrift.api.ttypes import EventSource, Source
from ai.chronon.group_by import Aggregation, GroupBy, Operation
from ai.chronon.query import Query, selects
from ai.chronon.types import ConfigProperties, EnvironmentVariables

_action_events = [
    "backend_add_to_cart",
    "view_listing",
    "backend_cart_payment",
    "backend_favorite_item2",
]
_action_events_csv = ", ".join([f"'{event}'" for event in _action_events])
_action_events_filter = f"event_type in ({_action_events_csv})"

def build_source(topic: str) -> Source:
    return Source(
        events=EventSource(
            # This source table contains a custom struct ('attributes') that enables
            # attributes['key'] style access pattern in a BQ native table.
            table="data.item_events_parquet_compat_partitioned",
            topic=topic,
            query=Query(
                selects=selects(
                    listing_id="EXPLODE(TRANSFORM(SPLIT(COALESCE(attributes['sold_listing_ids'], attributes['listing_id']), ','), e -> CAST(e AS LONG)))",
                    add_cart="IF(event_type = 'backend_add_to_cart', 1, 0)",
                    view="IF(event_type = 'view_listing', 1, 0)",
                    purchase="IF(event_type = 'backend_cart_payment', 1, 0)",
                    favorite="IF(event_type = 'backend_favorite_item2', 1, 0)",
                ),
                wheres=[_action_events_filter],
                time_column="timestamp",
            ),
        )
    )

def build_actions_groupby(source: Source) -> GroupBy:
    return GroupBy(
        sources=[source],
        keys=["listing_id"],
        online=True,
        version=0,
        aggregations=[
            Aggregation(input_column="add_cart", operation=Operation.SUM, windows=["1d"]),
            Aggregation(input_column="view", operation=Operation.SUM, windows=["1d"]),
            Aggregation(input_column="purchase", operation=Operation.SUM, windows=["1d"]),
            Aggregation(input_column="favorite", operation=Operation.SUM, windows=["1d"]),
        ],
        conf=ConfigProperties(
            common={
                "spark.chronon.partition.column": "_DATE",
            }
        ),
        env_vars=EnvironmentVariables(
            common={
                "CHRONON_ONLINE_ARGS": "-Ztasks=4 -Zbootstrap=bootstrap.zipline-kafka-cluster.us-central1.managedkafka.canary-443022.cloud.goog:9092",
            }
        ),
    )

# GCP Kafka clusters require TLS
google_kafka_cfgs = "security.protocol=SASL_SSL/sasl.mechanism=OAUTHBEARER/sasl.login.callback.handler.class=com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler/sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"
schema_provider_cfgs = "serde=custom/provider_class=ai.chronon.flink.deser.MockCustomSchemaProvider/schema_name=item_event"
kafka_topic = f"kafka://test-item-event-data/{schema_provider_cfgs}/{google_kafka_cfgs}"
kafka_source = build_source(kafka_topic)

actions_v1 = build_actions_groupby(kafka_source)

# Add a pubsub equivalent source + GroupBy. We use the same item event schema for the events
pubsub_schema_provider_cfgs = "serde=pubsub_schema/project=canary-443022/schemaId=item-event"
pubsub_topic = f"pubsub://test-item-event-data/{pubsub_schema_provider_cfgs}/tasks=4/subscription=test-item-event-data-sub"
pubsub_source = build_source(pubsub_topic)
actions_pubsub_v2 = build_actions_groupby(pubsub_source)
