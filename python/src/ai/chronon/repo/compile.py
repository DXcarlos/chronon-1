import json
import os
import sys

import click
from gen_thrift.api.ttypes import ConfType

from ai.chronon.cli.compile.compile_context import CompileContext
from ai.chronon.cli.compile.compiler import Compiler, CompileResult
from ai.chronon.cli.compile.display.console import console
from ai.chronon.cli.formatter import Format, jsonify_exceptions_if_json_format


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
@jsonify_exceptions_if_json_format
def compile(chronon_root, ignore_python_errors, format, force):
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

    compiled_result, has_errors = __compile(chronon_root, ignore_python_errors, format=format, force=force)

    if has_errors and not ignore_python_errors:
        sys.exit(1)
    return compiled_result


def __compile(chronon_root, ignore_python_errors=False, format=Format.TEXT, force=False) -> tuple[
    dict[ConfType, CompileResult], bool]:
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

    compile_context = CompileContext(ignore_python_errors=ignore_python_errors, format=format, force=force)
    compiler = Compiler(compile_context)
    results = compiler.compile()
    if format == Format.JSON:
        print(json.dumps({
            "status": "success",
            "results": {
                ConfType._VALUES_TO_NAMES[conf_type]: list(conf_result.obj_dict.keys())
                for conf_type, conf_result in results.items()
                if conf_result.obj_dict
            }}, indent=4))
    return results, compiler.has_compilation_errors()


if __name__ == "__main__":
    compile()
