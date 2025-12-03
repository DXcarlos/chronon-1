"""
Wrappers to directly create Source objects.
"""

import logging
from functools import wraps

import gen_thrift.api.ttypes as ttypes

import ai.chronon.utils as utils
from ai.chronon.repo.entity_register import Entity, EntityRegister


def apply_entities(fn):
    """
    Decorator that applies entity selections using the `entities`
    kwarg passed to the wrapped function.
    I entity_register is passed, it will be used to register the entities based on the query.selects.
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        source = fn(*args, **kwargs)

        entities: Entity = kwargs.get("entities")
        entity_registry: EntityRegister = kwargs.get("entity_registry")
        if not entities and not entity_registry:
            return source
        query = kwargs.get("query")
        table = utils.get_table(source)
        if entities is not None:
            for entity_col, entity in entities.items():
                if entity_col in query.selects:
                    entity.select(entity_col, table, expr=query.selects[entity_col])
        elif entity_registry is not None:
            for entity in entity_registry.entity_registrations.values():
                for column in entity.default:
                    if column in query.selects:
                        logging.debug(f"Auto-registering entity {entity.name} for column {column} in table {table} based on default columns")
                        entity.select(column, table, expr=query.selects[column])
        return source
    return wrapper

@apply_entities
def EventSource(
    table: str,
    query: ttypes.Query,
    topic: str = None,
    is_cumulative: bool = None,
    entities: dict[str, Entity] = None,
    entity_registry: EntityRegister = None,
) -> ttypes.Source:
    """
    Event Sources represent data that gets generated over-time.
    Typically, but not necessarily, logged to message buses like kafka, kinesis or google pub/sub.
    fct tables are also event source worthy.

    Attributes:

     - table: Table currently needs to be a 'ds' (date string - yyyy-MM-dd) partitioned hive table.
              Table names can contain subpartition specs, example db.table/system=mobile/currency=USD
     - topic: Topic is a kafka table. The table contains all the events historically came through this topic.
     - query: The logic used to scan both the table and the topic. Contains row level transformations
              and filtering expressed as Spark SQL statements.
     - isCumulative: If each new hive partition contains not just the current day's events but the entire set
                     of events since the begininng. The key property is that the events are not mutated
                     across partitions.

    """
    return ttypes.Source(
        events=ttypes.EventSource(table=table, topic=topic, query=query, isCumulative=is_cumulative)
    )


@apply_entities
def EntitySource(
    snapshot_table: str,
    query: ttypes.Query,
    mutation_table: str = None,
    mutation_topic: str = None,
    entities: dict[str, Entity] = None,
    entity_registry: EntityRegister = None,
) -> ttypes.Source:
    """
    Entity Sources represent data that gets mutated over-time - at row-level. This is a group of three data elements.
    snapshotTable, mutationTable and mutationTopic. mutationTable and mutationTopic are only necessary if we are trying
    to create realtime or point-in-time aggregations over these sources. Entity sources usually map 1:1 with a database
    tables in your OLTP store that typically serves live application traffic. When mutation data is absent they map 1:1
    to `dim` tables in star schema.

    Attributes:
     - snapshotTable: Snapshot table currently needs to be a 'ds' (date string - yyyy-MM-dd) partitioned hive table.
     - mutationTable: Topic is a kafka table. The table contains
                      all the events that historically came through this topic.
                      We need all the fields present in the snapshot table, PLUS two additional fields,
                      `mutation_time` - milliseconds since epoch of type Long that represents the time of the mutation
                      `is_before` - a boolean flag that represents whether
                                    this row contains values before or after the mutation.
     - mutationTopic: The logic used to scan both the table and the topic. Contains row level transformations
                      and filtering expressed as Spark SQL statements.
     - query: If each new hive partition contains not just the current day's events but the entire set
              of events since the begininng. The key property is that the events are not mutated across partitions.
    """
    return ttypes.Source(
        entities=ttypes.EntitySource(
            snapshotTable=snapshot_table,
            mutationTable=mutation_table,
            mutationTopic=mutation_topic,
            query=query,
        )
    )

@apply_entities
def JoinSource(join: ttypes.Join, query: ttypes.Query = None, entities: dict[str, Entity] = None, entity_registry: EntityRegister = None) -> ttypes.Source:
    """
    The output of a join can be used as a source for `GroupBy`.
    Useful for expressing complex computation in chronon.

    Offline this simply means that we will compute the necessary date ranges of the join
    before we start computing the `GroupBy`.

    Online we will:
    1. enrich the stream/topic of `join.left` with all the columns defined by the join
    2. apply the selects & wheres defined in the `query`
    3. perform aggregations defined in the *downstream* `GroupBy`
    4. write the result to the kv store.
    """
    return ttypes.Source(joinSource=ttypes.JoinSource(join=join, query=query))
