"""
Entity Registry System for Chronon

This module provides a registry system to track how entities (business domain concepts like
user_id, listing_id, merchant_id) are used across Chronon configurations including Sources,
GroupBys, and Joins. This enables better data lineage tracking, feature documentation, and
validation of entity usage consistency.

Entity:
    The class holding all the information about an entity, including which tables have
    selected it, what aggregations use it as keys, and which joins consume features for it.

EntityRegister:
    The class controlling all registered entities. Reset at compile time to track entity
    usage across the entire configuration repository.
"""

import csv
import io
import json
from collections import namedtuple

from gen_thrift.api.ttypes import Operation
from gen_thrift.common.ttypes import TimeUnit

SelectTuple = namedtuple("SelectTuple", ["column", "expr"])


def window_to_str_pretty(length: int, timeUnit: int) -> str:
    """
    Convert a window specification to a human-readable string format.

    Args:
        length: The numeric length of the time window
        timeUnit: The TimeUnit enum value (DAYS, HOURS, MINUTES, etc.)

    Returns:
        A compact string representation like "7d", "24h", "30m"

    Examples:
        >>> window_to_str_pretty(7, TimeUnit.DAYS)
        '7d'
        >>> window_to_str_pretty(24, TimeUnit.HOURS)
        '24h'
    """
    unit = TimeUnit._VALUES_TO_NAMES[timeUnit].lower()
    return f"{length}{unit[0]}"


def op_to_str(operation: int) -> str:
    """
    Convert an Operation enum value to its lowercase string name.

    Args:
        operation: The Operation enum value (SUM, COUNT, LAST, etc.)

    Returns:
        The lowercase string name of the operation

    Examples:
        >>> op_to_str(Operation.SUM)
        'sum'
        >>> op_to_str(Operation.LAST)
        'last'
    """
    return Operation._VALUES_TO_NAMES[operation].lower()

class Entity:
    """
    Represents a business domain entity (e.g., user, listing, merchant) in Chronon.

    An Entity tracks how a business concept is used across different sources, group_bys,
    and joins. It maintains consistency by ensuring that the same entity uses consistent
    column names and expressions across different tables and transformations.

    Entities are registered during source definitions by passing an entity mapping via
    the 'entities' parameter or by using an 'entity_registry' with default column patterns.

    Attributes:
        name: The unique identifier for this entity (e.g., "user", "listing")
        description: Human-readable description of what this entity represents
        default: List of default column names that should auto-register for this entity
        select_registrations: Dict mapping table names to SelectTuple(column, expr)
        aggregation_registrations: Dict mapping tables to list of GroupBy aggregations
        feature_query_registrations: Dict mapping tables to list of Joins using this entity
        _global_register: Class-level reference to the active EntityRegister during compilation
    """
    _global_register = None  # Set by compile.py during compilation

    def __init__(self, name: str, description: str = "", default: list = None):
        """
        Initialize a new Entity.

        Args:
            name: Unique identifier for the entity (e.g., "user", "listing", "merchant")
            description: Human-readable description of what this entity represents
            default: List of column name patterns that should auto-register for this entity
                     when found in source queries. Useful for automatic entity tracking.
        """
        self.name = name
        self.description = description
        self.default = default if default else []
        self.select_registrations = {}
        self.aggregation_registrations = {}
        self.feature_query_registrations = {}

    def pretty_print(self, show_all: bool = False) -> str:
        """
        Generate a human-readable string representation of this entity's registrations.

        Args:
            show_all: If True, show all select registrations even those without aggregations.
                      If False (default), only show tables that have associated GroupBys.

        Returns:
            A formatted multi-line string showing:
            - Entity name and description
            - Feature definitions (tables, columns, GroupBys, aggregations, derivations)
            - Feature queries (Joins that consume this entity's features)
        """
        to_str = f" Entity Name: {self.name} \n"
        to_str += f" Description: {self.description}\n"
        to_str += " Feature Definitions:\n"
        for table in self.select_registrations:
            if not show_all and table not in self.aggregation_registrations:
                continue
            select_tuple = self.select_registrations[table]
            to_str += f"{' ' * 4} Table: {table}\n"
            to_str += f"{' ' * 4} Input Column: {select_tuple.column}"
            if table not in self.aggregation_registrations:
                to_str += f"[No group by on {self.name} registered for this table]\n"
            if table in self.aggregation_registrations:
                for aggregation_info in self.aggregation_registrations[table]:
                    to_str += f"\n{' ' * 6} - GroupBy: {aggregation_info['groupBy'].metaData.name}\n"
                    to_str += f"{' ' * 6} - Keys: {aggregation_info['keys']}\n"
                    if aggregation_info['aggregations']:
                        to_str += f"{' ' * 6} - Aggregations: \n"
                        for aggregation in aggregation_info['aggregations']:
                            aggregation_str = ""
                            if aggregation.inputColumn:
                                aggregation_str += f"inputColumn={aggregation.inputColumn}"
                            if aggregation.operation:
                                aggregation_str += f", operation={op_to_str(aggregation.operation)}"
                            if aggregation.windows:
                                aggregation_str += f", windows=[{','.join([window_to_str_pretty(window.length, window.timeUnit) for window in aggregation.windows])}]"
                            if aggregation.buckets:
                                aggregation_str += f", buckets={aggregation.buckets}"
                            if aggregation.tags:
                                aggregation_str += f", tags={aggregation.tags}"
                            to_str += f"{' ' * 8} - Agg({aggregation_str}) \n"
                    if aggregation_info['derivations']:
                        to_str += f"{' ' * 6} - Derivations: \n"
                        for derivation in aggregation_info['derivations']:
                            to_str += f"{' ' * 8} - {derivation.name}: {derivation.expression}\n"
                    if aggregation_info['selects']:
                        to_str += f"{' ' * 6} - Passthrough fields (NoAgg): \n"
                        for select in aggregation_info['selects']:
                            to_str += f"{' ' * 8} - {select}: {aggregation_info['selects'][select]}\n"
        to_str += " Feature Queries:\n"
        if not self.feature_query_registrations:
            to_str += f"{' ' * 4} No feature queries registered\n"
            return to_str
        for table in self.feature_query_registrations:
            to_str += f"{' ' * 4} Table: {table}\n"
            for query_info in self.feature_query_registrations[table]:
                join = query_info['join']
                join_name = join.metaData.name if join.metaData.name else "<name not set>"
                to_str += f"{' ' * 6} - Join: {join_name}\n"
        return to_str

    def to_str_select_registrations(self, indent: int = 5) -> str:
        """
        Generate a formatted string of all select registrations for this entity.

        Args:
            indent: Number of spaces to indent each line (default: 5)

        Returns:
            A multi-line string showing each table's column and expression registration
        """
        result = "\n"
        for table, select_tuple in self.select_registrations.items():
            result += f"{' ' * indent}Table: {table}\n"
            result += f"{' ' * indent} - Column: {select_tuple.column}\n"
            result += f"{' ' * indent} - Expression: {select_tuple.expr}\n"
        return result

    def select(self, column: str, table: str, expr: str = None) -> "Entity":
        """
        Register a column as a representation of this entity in a specific table.

        This is the primary registration method for entities. It records that a particular
        column (potentially derived from an expression) represents this entity in a given
        table. The method enforces consistency by raising an error if the same table is
        registered with a different expression.

        Args:
            column: The column name after transformation (the final column name in the table)
            table: The fully qualified table name where this entity appears
            expr: The SQL expression used to derive the column. If None, defaults to column name.
                  This captures transformations like "CAST(user_id AS STRING)" or "listing_id"

        Returns:
            Self, to enable method chaining

        Raises:
            ValueError: If this entity was already registered for the table with a different
                       expression, indicating an inconsistency in entity definition

        Note:
            If a global EntityRegister is active (during compilation), this method will
            automatically register the entity with that register on first use.
        """
        # Auto-register entity on first use
        if Entity._global_register is not None:
            Entity._global_register.register_entity(self)

        if table not in self.select_registrations and column is not None:
            self.select_registrations[table] = SelectTuple(column=column, expr=expr or column)
        if table in self.select_registrations and self.select_registrations[table].expr != expr:
            raise ValueError(f"Entity {self.name} has already registered for table {table} with expression {self.select_registrations[table].expr} but received expression {expr}")
        return self
    
    def register_aggregation(self, keys, aggregations, parent, input, derivations=None, selects=None) -> "Entity":
        """
        Register a GroupBy that uses this entity as a key.

        This method is called internally by the GroupBy constructor when an entity is used
        in the 'keys' parameter. It tracks which aggregations are computed for this entity,
        enabling feature lineage and documentation.

        Args:
            keys: List of key columns (may include strings and Entity objects)
            aggregations: List of Aggregation objects defining what's computed
            parent: The GroupBy object that contains these aggregations
            input: The source table name being aggregated
            derivations: Optional list of Derivation objects for computed features
            selects: Optional dict of passthrough fields (for non-aggregated GroupBys)

        Returns:
            Self, to enable method chaining

        Raises:
            ValueError: If the input table was not previously registered via select().
                       This catches cases where an entity is used in a GroupBy but was
                       not properly registered in the source's query selects.

        Note:
            This method is typically called automatically by the GroupBy constructor,
            not by user code directly.
        """
        # Validate that this table was registered via select()
        if input not in self.select_registrations:
            raise ValueError(
                f"Entity '{self.name}' used in GroupBy '{parent.metaData.name}' "
                f"but no select() registration found for table '{input}'. "
                f"Make sure to use entity.select() in the query's selects."
            )

        if input not in self.aggregation_registrations:
            self.aggregation_registrations[input] = []

        self.aggregation_registrations[input].append({
            "groupBy": parent,
            "keys": keys,
            "aggregations": aggregations,
            "derivations": derivations,
            "selects": selects if not aggregations else None,
        })
        return self

    def register_feature_query(self, join, table) -> "Entity":
        """
        Register a Join that consumes features for this entity.

        This method tracks which Joins use this entity in their left source, enabling
        understanding of feature consumption patterns and downstream dependencies.

        Args:
            join: The Join object that uses this entity
            table: The left source table of the join

        Returns:
            Self, to enable method chaining

        Note:
            This method is called automatically by the Join constructor when a join's
            left source matches a table registered for this entity. It does not perform
            validation, unlike register_aggregation().
        """
        if table not in self.feature_query_registrations:
            self.feature_query_registrations[table] = []

        self.feature_query_registrations[table].append({
            "join": join,
            "table": table,
        })
        return self

class EntityRegister:
    """
    Central registry for all entities in a Chronon repository.

    The EntityRegister maintains a collection of all Entity objects and provides methods
    to export entity usage information in various formats (text, JSON, CSV). It is
    typically instantiated once per compilation run and tracks all entity registrations
    across the entire configuration repository.

    Attributes:
        entity_registrations: Dict mapping entity names to Entity objects
    """

    def __init__(self):
        """Initialize an empty EntityRegister."""
        self.entity_registrations = {}

    def register_entity(self, entity: Entity) -> Entity:
        """
        Register an entity with this registry.

        Args:
            entity: The Entity object to register

        Returns:
            The same entity object (for convenience in method chaining)

        Note:
            If an entity with the same name is already registered, it will be replaced.
            Entities are typically auto-registered when first used via Entity.select().
        """
        self.entity_registrations[entity.name] = entity
        return entity

    def get_entity(self, name: str) -> Entity:
        """
        Retrieve an entity by name.

        Args:
            name: The entity name to look up

        Returns:
            The Entity object with the given name

        Raises:
            KeyError: If no entity with the given name is registered
        """
        return self.entity_registrations[name]

    def pretty_print(self) -> None:
        """
        Print a human-readable representation of all registered entities to stdout.

        This iterates through all entities and prints their pretty_print() output,
        which includes feature definitions, aggregations, and feature queries.
        """
        for _entity_name, entity in self.entity_registrations.items():
            print(entity.pretty_print())

    def to_dict(self) -> dict:
        """
        Export all entity registrations as a nested dictionary structure.

        Returns:
            A dictionary with the following structure:
            {
                "entities": {
                    "<entity_name>": {
                        "name": str,
                        "description": str,
                        "feature_definitions": {
                            "<table_name>": {
                                "input_column": str,
                                "expression": str,
                                "group_bys": [
                                    {
                                        "name": str,
                                        "keys": List[str],
                                        "aggregations": [
                                            {
                                                "input_column": str,
                                                "operation": str,
                                                "windows": List[str],
                                                "buckets": Optional[List],
                                                "tags": Optional[Dict]
                                            }
                                        ],
                                        "derivations": [
                                            {
                                                "name": str,
                                                "expression": str
                                            }
                                        ],
                                        "selects": Dict[str, str]
                                    }
                                ]
                            }
                        },
                        "feature_queries": {
                            "<table_name>": [
                                {"name": str}  # Join names
                            ]
                        }
                    }
                }
            }

        Note:
            Only tables that have associated aggregations are included in feature_definitions.
            Tables that are merely selected but not aggregated are excluded.
        """
        result = {"entities": {}}
        for entity_name, entity in self.entity_registrations.items():
            entity_dict = {
                "name": entity.name,
                "description": entity.description,
                "feature_definitions": {},
                "feature_queries": {}
            }

            # Add feature definitions (GroupBys)
            for table, select_tuple in entity.select_registrations.items():
                if table in entity.aggregation_registrations:
                    feature_def = {
                        "input_column": select_tuple.column,
                        "expression": select_tuple.expr,
                        "group_bys": []
                    }

                    for agg_info in entity.aggregation_registrations[table]:
                        group_by_dict = {
                            "name": agg_info['groupBy'].metaData.name,
                            "keys": agg_info['keys'],
                            "aggregations": [],
                            "derivations": [],
                            "selects": {}
                        }

                        # Add aggregations
                        if agg_info['aggregations']:
                            for agg in agg_info['aggregations']:
                                agg_dict = {}
                                if agg.inputColumn:
                                    agg_dict['input_column'] = agg.inputColumn
                                if agg.operation:
                                    agg_dict['operation'] = op_to_str(agg.operation)
                                if agg.windows:
                                    agg_dict['windows'] = [
                                        window_to_str_pretty(w.length, w.timeUnit)
                                        for w in agg.windows
                                    ]
                                if agg.buckets:
                                    agg_dict['buckets'] = agg.buckets
                                if agg.tags:
                                    agg_dict['tags'] = agg.tags
                                group_by_dict['aggregations'].append(agg_dict)

                        # Add derivations
                        if agg_info['derivations']:
                            for deriv in agg_info['derivations']:
                                group_by_dict['derivations'].append({
                                    'name': deriv.name,
                                    'expression': deriv.expression
                                })

                        # Add selects
                        if agg_info['selects']:
                            group_by_dict['selects'] = agg_info['selects']

                        feature_def['group_bys'].append(group_by_dict)

                    entity_dict['feature_definitions'][table] = feature_def

            # Add feature queries (Joins)
            for table, query_list in entity.feature_query_registrations.items():
                joins = []
                for query_info in query_list:
                    join = query_info['join']
                    join_name = join.metaData.name if join.metaData.name else None
                    if join_name:
                        joins.append({"name": join_name})
                if joins:
                    entity_dict['feature_queries'][table] = joins

            result["entities"][entity_name] = entity_dict

        return result

    def to_json(self, indent: int = 2) -> str:
        """
        Export all entity registrations as a JSON string.

        Args:
            indent: Number of spaces for JSON indentation (default: 2)

        Returns:
            A JSON-formatted string containing all entity registration data.
            See to_dict() for the structure.
        """
        return json.dumps(self.to_dict(), indent=indent)

    def to_csv(self) -> str:
        """
        Export entity registrations to CSV format for analysis and reporting.

        This method generates a flattened CSV representation where each row represents
        a single feature (aggregation + window combination) for an entity. This format
        is useful for feature catalogs, dependency analysis, and reporting.

        CSV Columns:
            - entity: Entity name (e.g., "user", "listing")
            - input_column: The source column name from the table
            - operation: Aggregation operation (e.g., "sum", "count", "last", "derivation:name", "passthrough")
            - window: Time window for aggregation (e.g., "7d", "30d") or empty for non-windowed
            - group_by: Name of the GroupBy that defines this feature
            - joins: Comma-separated list of Join names that consume features from this entity
            - online: "true" if the GroupBy is online-enabled, "false" otherwise

        Returns:
            A CSV-formatted string with headers and one row per feature variant.
            Multi-window aggregations create multiple rows (one per window).

        Special Operation Values:
            - "derivation:<name>": For derived features
            - "passthrough": For GroupBys with no aggregations (passthrough fields)
            - Empty string: For tables with select but no aggregations

        Note:
            The joins column is populated based on which Joins have a left source matching
            the entity's registered tables. A single entity may appear in multiple joins.
        """
        output = io.StringIO()
        writer = csv.writer(output)

        # Write header
        writer.writerow(['entity', 'input_column', 'operation', 'window', 'group_by', 'joins', 'online'])

        # Process each entity
        for entity_name, entity in self.entity_registrations.items():
            # Get joins that consume features for this entity
            joins_by_table = {}
            for table, query_list in entity.feature_query_registrations.items():
                join_names = []
                for query_info in query_list:
                    join = query_info['join']
                    if join.metaData and join.metaData.name:
                        join_names.append(join.metaData.name)
                if join_names:
                    joins_by_table[table] = join_names

            # Process each table's aggregations
            for table, select_tuple in entity.select_registrations.items():
                input_column = select_tuple.column
                joins_list = joins_by_table.get(table, [])
                joins_str = ','.join(joins_list) if joins_list else ''

                # Check if this table has aggregations
                if table in entity.aggregation_registrations:
                    for agg_info in entity.aggregation_registrations[table]:
                        group_by = agg_info['groupBy']
                        group_by_name = group_by.metaData.name if (hasattr(group_by, 'metaData') and group_by.metaData and hasattr(group_by.metaData, 'name')) else ''
                        is_online = 'true' if (hasattr(group_by, 'metaData') and
                                                group_by.metaData and
                                                hasattr(group_by.metaData, 'online') and
                                                group_by.metaData.online) else 'false'

                        # Process aggregations
                        if agg_info['aggregations']:
                            for aggregation in agg_info['aggregations']:
                                operation = op_to_str(aggregation.operation) if aggregation.operation else ''

                                # Handle windows - create one row per window
                                if aggregation.windows:
                                    for window in aggregation.windows:
                                        window_str = window_to_str_pretty(window.length, window.timeUnit)
                                        writer.writerow([
                                            entity_name,
                                            input_column,
                                            operation,
                                            window_str,
                                            group_by_name,
                                            joins_str,
                                            is_online
                                        ])
                                else:
                                    # No windows - single row
                                    writer.writerow([
                                        entity_name,
                                        input_column,
                                        operation,
                                        '',
                                        group_by_name,
                                        joins_str,
                                        is_online
                                    ])

                        # Handle derivations
                        if agg_info['derivations']:
                            for derivation in agg_info['derivations']:
                                writer.writerow([
                                    entity_name,
                                    input_column,
                                    f"derivation:{derivation.name}",
                                    '',
                                    group_by_name,
                                    joins_str,
                                    is_online
                                ])

                        # Handle passthrough selects (no aggregations)
                        if agg_info['selects'] and not agg_info['aggregations']:
                            writer.writerow([
                                entity_name,
                                input_column,
                                'passthrough',
                                '',
                                group_by_name,
                                joins_str,
                                is_online
                            ])
                else:
                    # No aggregations for this table, but it's registered
                    writer.writerow([
                        entity_name,
                        input_column,
                        '',
                        '',
                        '',
                        joins_str,
                        'false'
                    ])

        return output.getvalue()