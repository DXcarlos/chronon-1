"""
Entity:
 The class holding all the information about the entity, which tables have been selecting it.
EntityRegister:
  The class controlling all the entities. Reset at compile time.
"""

import csv
import io
import json
from collections import namedtuple

from gen_thrift.api.ttypes import Operation
from gen_thrift.common.ttypes import TimeUnit

SelectTuple = namedtuple("SelectTuple", ["column", "expr"])

def window_to_str_pretty(length, timeUnit):
    unit = TimeUnit._VALUES_TO_NAMES[timeUnit].lower()
    return f"{length}{unit[0]}"


def op_to_str(operation):
    return Operation._VALUES_TO_NAMES[operation].lower()

class Entity:
    _global_register = None  # Set by compile.py during compilation

    def __init__(self, name, description="", default=None):
        self.name = name
        self.description = description
        self.default = default if default else []
        self.select_registrations = {}
        self.aggregation_registrations = {}
        self.feature_query_registrations = {}

    def pretty_print(self, show_all=False):
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

    def to_str_select_registrations(self, indent=5):
        result = "\n"
        for table, select_tuple in self.select_registrations.items():
            result += f"{' ' * indent}Table: {table}\n"
            result += f"{' ' * indent} - Column: {select_tuple.column}\n"
            result += f"{' ' * indent} - Expression: {select_tuple.expr}\n"
        return result

    def select(self, column: str, table: str, expr=None) -> str:
        """
        Main method of registration for an entity.
        - column: The column name to register. Post transformation.
        - table: The table name to register. Table the transformation is being applied to.
        - expr: The expression to register. 
        - Returns the column name.

        The objective is to guarantee consistency of entity definitions.
        """
        # Auto-register entity on first use
        if Entity._global_register is not None:
            Entity._global_register.register_entity(self)

        if table not in self.select_registrations and column is not None:
            self.select_registrations[table] = SelectTuple(column=column, expr=expr or column)
        if table in self.select_registrations and self.select_registrations[table].expr != expr:
            raise ValueError(f"Entity {self.name} has already registered for table {table} with expression {self.select_registrations[table].expr} but received expression {expr}")
        return column
    
    def register_aggregation(self, keys, aggregations, parent, input, derivations=None, selects=None):
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

    def register_feature_query(self, join, table):
        if table not in self.feature_query_registrations:
            self.feature_query_registrations[table] = []

        self.feature_query_registrations[table].append({
            "join": join,
            "table": table,
        })

class EntityRegister:
    def __init__(self):
        self.entity_registrations = {}

    def register_entity(self, entity):
        self.entity_registrations[entity.name] = entity

    def get_entity(self, name):
        return self.entity_registrations[name]

    def pretty_print(self):
        for _entity_name, entity in self.entity_registrations.items():
            print(entity.pretty_print())

    def to_dict(self):
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
                        "alias": select_tuple.alias,
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

    def to_json(self, indent=2):
        return json.dumps(self.to_dict(), indent=indent)

    def to_csv(self):
        """
        Export entity registrations to CSV format with columns:
        - entity: entity name
        - input_column: input column from source table
        - operation: aggregation operation (e.g., sum, count, last)
        - window: time window for aggregation (e.g., 1d, 7d)
        - joins: comma-separated list of joins that consume this feature
        - online: whether the group by is online (true/false)
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