import copy
import glob
import importlib
import os
import sys
from typing import Any, Dict, List

from ai.chronon import airflow_helpers
from ai.chronon.cli.compile import parse_teams, serializer
from ai.chronon.cli.compile.compile_context import CompileContext
from ai.chronon.cli.compile.display.compiled_obj import CompiledObj
from ai.chronon.cli.logger import get_logger
from gen_thrift.api.ttypes import GroupBy, Join

logger = get_logger()


def from_folder(target_classes: List[type], input_dir: str, compile_context: CompileContext) -> Dict[type, List[CompiledObj]]:
    """
    Recursively consumes a folder, and constructs a map of
    object qualifier to StagingQuery, GroupBy, or Join.
    Supports multiple target classes in a single scan.
    """

    python_files = glob.glob(os.path.join(input_dir, "**/*.py"), recursive=True)

    # Results keyed by class type
    results = {cls: [] for cls in target_classes}

    for f in python_files:
        try:
            # Get objects of all target types from this file
            multi_type_results = from_file(f, target_classes, input_dir)

            # Process each type's results
            for target_cls, objects_dict in multi_type_results.items():
                for name, obj in objects_dict.items():
                    parse_teams.update_metadata(obj, compile_context.teams_dict)
                    # Populate columnHashes field with semantic hashes
                    populate_column_hashes(obj)

                    # Airflow deps must be set AFTER updating metadata
                    airflow_helpers.set_airflow_deps(obj)

                    obj.metaData.sourceFile = os.path.relpath(f, compile_context.chronon_root)

                    tjson = serializer.thrift_simple_json(obj)

                    # Perform validation
                    errors = compile_context.validator.validate_obj(obj)

                    # Use actual object type, not target class
                    actual_type = type(obj).__name__

                    result = CompiledObj(
                        name=name,
                        obj=obj,
                        file=f,
                        errors=errors if len(errors) > 0 else None,
                        obj_type=actual_type,
                        tjson=tjson,
                    )
                    results[target_cls].append(result)

                    compile_context.compile_status.add_object_update_display(result, actual_type)

        except Exception as e:
            # Attribute import errors to first target class
            result = CompiledObj(
                name=None,
                obj=None,
                file=f,
                errors=[e],
                obj_type=target_classes[0].__name__,
                tjson=None,
            )

            results[target_classes[0]].append(result)

            compile_context.compile_status.add_object_update_display(result, target_classes[0].__name__)

    return results


def from_file(file_path: str, target_classes: List[type], input_dir: str) -> Dict[type, Dict[str, Any]]:
    """
    Extract config objects from a Python file.
    Supports extracting multiple config types from a single file.

    Args:
        file_path: Path to the Python file to parse
        target_classes: List of config classes to search for (e.g., [GroupBy, Join])
        input_dir: Root directory for the config type

    Returns:
        Nested dict: {GroupBy: {name: obj}, Join: {name: obj}, ...}
    """
    # this is where the python path should have been set to
    chronon_root = os.path.dirname(input_dir)
    rel_path = os.path.relpath(file_path, chronon_root)

    rel_path_without_extension = os.path.splitext(rel_path)[0]

    module_name = rel_path_without_extension.replace("/", ".")

    conf_type, team_name_with_path = module_name.split(".", 1)
    mod_path = team_name_with_path.replace("/", ".")

    modules_before = set(sys.modules.keys())
    try:
        module = importlib.import_module(module_name)
    except Exception as e:
        # Remove any partially-loaded modules from the cache so that downstream
        # files importing this one don't get cascading import errors.
        for mod in set(sys.modules.keys()) - modules_before:
            del sys.modules[mod]
        # Python removes the failed module from sys.modules but leaves it as an
        # attribute on the parent package. Clean that up too to prevent cascade.
        sys.modules.pop(module_name, None)
        parent_name, _, child_name = module_name.rpartition(".")
        parent = sys.modules.get(parent_name)
        if parent is not None and hasattr(parent, child_name):
            delattr(parent, child_name)
        raise ValueError(f"Error parsing {os.path.relpath(file_path)}: {e}") from None

    # Results keyed by class type
    result = {cls: {} for cls in target_classes}

    for var_name, obj in list(module.__dict__.items()):
        # Check if object is an instance of any target class
        for target_cls in target_classes:
            if isinstance(obj, target_cls):
                copied_obj = copy.deepcopy(obj)

                name = f"{mod_path}.{var_name}"

                # Add version suffix if version is set
                if copied_obj.metaData.version is not None:
                    name = name + "__" + str(copied_obj.metaData.version)

                copied_obj.metaData.name = name
                copied_obj.metaData.team = mod_path.split(".")[0]

                result[target_cls][name] = copied_obj
                break  # Each object belongs to only one type

    return result


def populate_column_hashes(obj: Any):
    """
    Populate the columnHashes field in the object's metadata with semantic hashes
    for each output column.
    """
    # Import here to avoid circular imports
    from ai.chronon.cli.compile.column_hashing import (
        compute_group_by_columns_hashes,
        compute_join_column_hashes,
    )

    if isinstance(obj, GroupBy):
        # For GroupBy objects, get column hashes
        column_hashes = compute_group_by_columns_hashes(obj, exclude_keys=False)
        obj.metaData.columnHashes = column_hashes

    elif isinstance(obj, Join):
        # For Join objects, get column hashes
        column_hashes = compute_join_column_hashes(obj)
        obj.metaData.columnHashes = column_hashes

        if obj.joinParts:
            for jp in obj.joinParts or []:
                group_by = jp.groupBy
                group_by_hashes = compute_group_by_columns_hashes(group_by)
                group_by.metaData.columnHashes = group_by_hashes
