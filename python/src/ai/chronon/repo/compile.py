import json
import os
import sys

import click
from gen_thrift.api.ttypes import ConfType

from ai.chronon.cli.compile.compile_context import CompileContext
from ai.chronon.cli.compile.compiler import Compiler
from ai.chronon.cli.compile.display.console import console
from ai.chronon.cli.formatter import Format, jsonify_exceptions_if_json_format
from ai.chronon.repo.entity_register import Entity, EntityRegister


@click.command(name="compile")
@click.option(
    "--chronon-root",
    envvar="CHRONON_ROOT",
    help="Path to the root chronon folder",
    default=os.getcwd(),
)
@click.option(
    "--ignore-python-errors",
    is_flag=True,
    default=False,
    help="Allow compilation to proceed even with Python errors (useful for testing)",
)
@click.option(
    "--format",
    help="Format of the response",
    default=Format.TEXT,
    type=click.Choice(Format, case_sensitive=False)
)
@click.option(
    "--force",
    is_flag=True,
    help="Force compilation to proceed even with errors",
)
@click.option(
    "--entity-summary",
    is_flag=True,
    default=False,
    help="Print entity registry summary to console",
)
@click.option(
    "--entity-csv",
    type=str,
    default=None,
    help="Export entity registry to CSV file (provide file path)",
)
@jsonify_exceptions_if_json_format
def compile(chronon_root, ignore_python_errors, format, force, entity_summary, entity_csv):
    if chronon_root is None or chronon_root == "":
        chronon_root = os.getcwd()

    if chronon_root not in sys.path:
        if format != Format.JSON:
            console.print(
                f"\nAdding [cyan italic]{chronon_root}[/cyan italic] to python path, during compile."
            )
        sys.path.insert(0, chronon_root)
    elif format != Format.JSON:
        console.print(f"\n[cyan italic]{chronon_root}[/cyan italic] already on python path.")

    return __compile(chronon_root, ignore_python_errors, format=format, force=force, entity_summary=entity_summary, entity_csv=entity_csv)


def __compile(chronon_root, ignore_python_errors=False, format=Format.TEXT, force=False, entity_summary=False, entity_csv=None):
    if chronon_root:
        chronon_root_path = os.path.expanduser(chronon_root)
        os.chdir(chronon_root_path)

    # check that a "teams.py" file exists in the current directory
    if not (os.path.exists("teams.py") or os.path.exists("teams.json")):
        raise click.ClickException(
            (
                "teams.py or teams.json file not found in current directory."
                " Please run from the top level of conf directory."
            )
        )
    entity_register = EntityRegister()
    Entity._global_register = entity_register  # Enable auto-registration during compilation
    compile_context = CompileContext(ignore_python_errors=ignore_python_errors, format=format, force=force, entity_register=entity_register)
    compiler = Compiler(compile_context)
    results = compiler.compile()
    # Handle JSON format output
    if format == Format.JSON:
        output = {
            "status": "success",
            "results": {
                ConfType._VALUES_TO_NAMES[conf_type]: list(conf_result.obj_dict.keys())
                for conf_type, conf_result in results.items()
                if conf_result.obj_dict
            }
        }
        if entity_summary:
            output["entity_registry"] = compile_context.entity_register.to_dict()
        print(json.dumps(output, indent=4))
        sys.exit(0)

    # Handle text format output
    if entity_summary:
        compile_context.entity_register.pretty_print()

    # Handle CSV export
    if entity_csv:
        csv_content = compile_context.entity_register.to_csv()
        with open(entity_csv, 'w') as f:
            f.write(csv_content)
        if format != Format.JSON:
            console.print(f"\n[green]Entity registry exported to CSV:[/green] {entity_csv}")

    return results


if __name__ == "__main__":
    compile()
