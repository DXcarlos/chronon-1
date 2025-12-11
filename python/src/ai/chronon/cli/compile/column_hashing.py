import hashlib
import re
from collections import defaultdict
from typing import Dict, List

from gen_thrift.api.ttypes import Derivation, ExternalPart, GroupBy, Join, Source

from ai.chronon.group_by import get_output_col_names


# Returns a map of output column to semantic hash, including derivations
def compute_group_by_columns_hashes(
    group_by: GroupBy, exclude_keys: bool = False
) -> Dict[str, str]:
    """
    From the group_by object, get the final output columns after derivations.
    """
    # Get the output columns and their input expressions
    output_to_input = get_pre_derived_group_by_columns(group_by)

    # Get the base semantic fields that apply to all columns
    base_semantics = []
    for source in group_by.sources:
        base_semantics.extend(
            _extract_source_semantic_info(source, group_by.keyColumns)
        )

    # Add the major version to semantic fields
    group_by_minor_version_suffix = f"__{group_by.metaData.version}"
    group_by_major_version = group_by.metaData.name
    if group_by_major_version.endswith(group_by_minor_version_suffix):
        group_by_major_version = group_by_major_version[
            : -len(group_by_minor_version_suffix)
        ]
    base_semantics.append(f"group_by_name:{group_by_major_version}")

    # Compute the semantic hash for each output column
    output_to_hash = {}
    for output_col, input_expr in output_to_input.items():
        semantic_components = base_semantics + [f"input_expr:{input_expr}"]
        semantic_hash = _compute_semantic_hash(semantic_components)
        output_to_hash[output_col] = semantic_hash

    if exclude_keys:
        output = {
            k: v for k, v in output_to_hash.items() if k not in group_by.keyColumns
        }
    else:
        output = output_to_hash
    if group_by.derivations:
        derived = build_derived_columns(output, group_by.derivations, base_semantics)
        if not exclude_keys:
            # We need to add keys back at this point
            for key in group_by.keyColumns:
                if key not in derived:
                    derived[key] = output_to_input.get(key)
        return derived
    else:
        return output


def compute_join_column_hashes(join: Join) -> Dict[str, str]:
    """
    From the join object, get the final output columns -> semantic hash after derivations.
    """
    # Get the base semantics from the left side (table and key expression)
    base_semantic_fields = []

    output_columns = get_pre_derived_join_features(join) | get_pre_derived_source_keys(
        join.left
    )

    if join.derivations:
        return build_derived_columns(
            output_columns, join.derivations, base_semantic_fields
        )
    else:
        return output_columns


def get_pre_derived_join_features(join: Join) -> Dict[str, str]:
    return get_pre_derived_join_internal_features(
        join
    ) | get_pre_derived_external_features(join)


def get_pre_derived_external_features(join: Join) -> Dict[str, str]:
    external_cols = []
    if join.onlineExternalParts:
        for external_part in join.onlineExternalParts:
            original_external_columns = [
                param.name for param in external_part.source.valueSchema.params
            ]
            prefix = get_external_part_full_name(external_part) + "_"
            for col in original_external_columns:
                external_cols.append(prefix + col)
    # No meaningful semantic information on external columns, so we just return the column names as a self map
    return {x: x for x in external_cols}


def get_pre_derived_source_keys(source: Source) -> Dict[str, str]:
    base_semantics = _extract_source_semantic_info(source)
    source_keys_to_hashes = {}
    for key, expression in extract_selects(source).items():
        source_keys_to_hashes[key] = _compute_semantic_hash(
            base_semantics + [f"select:{key}={expression}"]
        )
    return source_keys_to_hashes


def extract_selects(source: Source) -> Dict[str, str]:
    if source.events:
        return source.events.query.selects
    elif source.entities:
        return source.entities.query.selects
    elif source.joinSource:
        return source.joinSource.query.selects


def get_pre_derived_join_internal_features(join: Join) -> Dict[str, str]:
    # Get the base semantic fields from join left side (without key columns)
    join_base_semantic_fields = _extract_source_semantic_info(join.left)

    internal_features = {}
    for jp in join.joinParts:
        # Build key mapping semantics - include left side key expressions
        if jp.keyMapping:
            key_mapping_semantics = [
                "join_keys:"
                + ",".join(f"{k}:{v}" for k, v in sorted(jp.keyMapping.items()))
            ]
        else:
            key_mapping_semantics = []

        # Include left side key expressions that this join part uses
        left_key_expressions = []
        left_selects = extract_selects(join.left)
        for gb_key in jp.groupBy.keyColumns:
            # Get the mapped key name or use the original key name
            left_key_name = gb_key
            if jp.keyMapping:
                # Find the left side key that maps to this groupby key
                for left_key, mapped_key in jp.keyMapping.items():
                    if mapped_key == gb_key:
                        left_key_name = left_key
                        break

            # Add the left side expression for this key
            left_key_expr = left_selects.get(left_key_name, left_key_name)
            left_key_expressions.append(f"left_key:{left_key_name}={left_key_expr}")

        # These semantics apply to all features in the joinPart
        jp_base_semantics = (
            key_mapping_semantics + left_key_expressions + join_base_semantic_fields
        )

        pre_derived_group_by_features = get_pre_derived_group_by_features(
            jp.groupBy, jp_base_semantics
        )

        if jp.groupBy.derivations:
            derived_group_by_features = build_derived_columns(
                pre_derived_group_by_features, jp.groupBy.derivations, jp_base_semantics
            )
        else:
            derived_group_by_features = pre_derived_group_by_features

        for col, semantic_hash in derived_group_by_features.items():
            prefix = jp.prefix + "_" if jp.prefix else ""
            if join.useLongNames:
                gb_prefix = prefix + jp.groupBy.metaData.name.replace(".", "_")
            else:
                key_str = "_".join(jp.groupBy.keyColumns)
                gb_prefix = prefix + key_str
            internal_features[gb_prefix + "_" + col] = semantic_hash
    return internal_features


def get_pre_derived_group_by_columns(group_by: GroupBy) -> Dict[str, str]:
    output_columns_to_hashes = get_pre_derived_group_by_features(group_by)
    for key_column in group_by.keyColumns:
        key_expressions = []
        for source in group_by.sources:
            source_selects = extract_selects(source)  # Map[str, str]
            key_expressions.append(source_selects.get(key_column, key_column))
        output_columns_to_hashes[key_column] = _compute_semantic_hash(
            sorted(key_expressions)
        )
    return output_columns_to_hashes


def get_pre_derived_group_by_features(
    group_by: GroupBy, additional_semantic_fields=None
) -> Dict[str, str]:
    # Get the base semantic fields that apply to all aggs
    if additional_semantic_fields is None:
        additional_semantic_fields = []
    base_semantics = _get_base_group_by_semantic_fields(group_by)

    output_columns = {}
    # For group_bys with aggregations, aggregated columns
    if group_by.aggregations:
        for agg in group_by.aggregations:
            input_expression_str = ",".join(
                get_input_expression_across_sources(group_by, agg.inputColumn)
            )
            for output_col_name in get_output_col_names(agg):
                output_columns[output_col_name] = _compute_semantic_hash(
                    base_semantics + [input_expression_str] + additional_semantic_fields
                )
    # For group_bys without aggregations, selected fields from query
    else:
        combined_selects = defaultdict(set)

        for source in group_by.sources:
            source_selects = extract_selects(source)  # Map[str, str]
            for key, val in source_selects.items():
                combined_selects[key].add(val)

        # Build a unified map of key to select expression from all sources
        unified_selects = {
            key: ",".join(sorted(vals)) for key, vals in combined_selects.items()
        }

        # now compute the hashes on base semantics + expression
        selected_hashes = {
            key: _compute_semantic_hash(
                base_semantics + [val] + additional_semantic_fields
            )
            for key, val in unified_selects.items()
        }
        output_columns.update(selected_hashes)
    return output_columns


def _get_base_group_by_semantic_fields(group_by: GroupBy) -> List[str]:
    """
    Extract base semantic fields from the group_by object.
    This includes source table names, key columns, and any other relevant metadata.
    """
    base_semantics = []
    for source in group_by.sources:
        base_semantics.extend(
            _extract_source_semantic_info(source, group_by.keyColumns)
        )

    return sorted(base_semantics)


def _extract_source_semantic_info(
    source: Source, key_columns: List[str] = None
) -> List[str]:
    """
    Extract source information for semantic hashing.
    Returns list of semantic components.

    Args:
        source: The source to extract info from
        key_columns: List of key columns to include expressions for. If None, includes all selects.
    """
    components = []

    if source.events:
        table = source.events.table
        mutationTable = ""
        query = source.events.query
        cumulative = str(source.events.isCumulative or "")
    elif source.entities:
        table = source.entities.snapshotTable
        mutationTable = source.entities.mutationTable
        query = source.entities.query
        cumulative = ""
    elif source.joinSource:
        table = source.joinSource.join.metaData.name
        mutationTable = ""
        query = source.joinSource.query
        cumulative = ""

    components.append(f"table:{table}")
    components.append(f"mutation_table:{mutationTable}")
    components.append(f"cumulative:{cumulative}")
    components.append(f"filters:{query.wheres or ''}")

    selects = query.selects or {}
    if key_columns:
        # Only include expressions for the specified key columns
        for key_col in sorted(key_columns):
            expr = selects.get(key_col, key_col)
            components.append(f"select:{key_col}={expr}")

    # Add time column expression
    if query.timeColumn:
        time_expr = selects.get(query.timeColumn, query.timeColumn)
        components.append(f"time_column:{query.timeColumn}={time_expr}")

    return sorted(components)


def _compute_semantic_hash(components: List[str]) -> str:
    """
    Compute semantic hash from a list of components.
    Components should be ordered consistently to ensure reproducible hashes.
    """
    # Sort components to ensure consistent ordering
    sorted_components = sorted(components)
    hash_input = "|".join(sorted_components)
    return hashlib.md5(hash_input.encode("utf-8")).hexdigest()


def build_derived_columns(
    base_columns_to_hashes: Dict[str, str],
    derivations: List[Derivation],
    additional_semantic_fields: List[str],
) -> Dict[str, str]:
    """
    Build the derived columns from pre-derived columns and derivations.
    """
    # if derivations contain star, then all columns are included except the columns which are renamed
    output_columns = {}
    if derivations:
        found = any(derivation.expression == "*" for derivation in derivations)
        if found:
            output_columns.update(base_columns_to_hashes)
        for derivation in derivations:
            if base_columns_to_hashes.get(derivation.expression):
                # don't change the semantics if you're just passing a base column through derivations
                output_columns[derivation.name] = base_columns_to_hashes[
                    derivation.expression
                ]
            if derivation.name != "*":
                # Identify base fields present within the derivation to include in the semantic hash
                # We go long to short to avoid taking both a windowed feature and the unwindowed feature
                # i.e. f_7d and f
                derivation_expression = derivation.expression
                base_col_semantic_fields = []
                tokens = re.findall(r"\b\w+\b", derivation_expression)
                for token in tokens:
                    if token in base_columns_to_hashes:
                        base_col_semantic_fields.append(base_columns_to_hashes[token])

                output_columns[derivation.name] = _compute_semantic_hash(
                    additional_semantic_fields
                    + [f"derivation:{derivation.expression}"]
                    + base_col_semantic_fields
                )
    return output_columns


def get_external_part_full_name(external_part: ExternalPart) -> str:
    # The logic should be consistent with the full name logic defined
    # in https://github.com/airbnb/chronon/blob/main/api/src/main/scala/ai/chronon/api/Extensions.scala#L677.
    prefix = external_part.prefix + "_" if external_part.prefix else ""
    name = external_part.source.metadata.name
    sanitized_name = re.sub("[^a-zA-Z0-9_]", "_", name)
    return "ext_" + prefix + sanitized_name


def get_input_expression_across_sources(group_by: GroupBy, input_col: str):
    expressions = []
    for source in group_by.sources:
        selects = extract_selects(source)
        expressions.extend(selects.get(input_col, input_col))
    return sorted(expressions)
