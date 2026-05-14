import json
import os
import sys

import click

from ai.chronon.cli.compile.compile_context import CompileContext
from ai.chronon.cli.compile.compiler import Compiler
from ai.chronon.cli.compile.parse_teams import CompileMode
from ai.chronon.cli.formatter import Format, jsonify_exceptions_if_json_format
from ai.chronon.cli.theme import STYLE_INFO, console
from gen_thrift.api.ttypes import ConfType


@click.command(name="compile")
@click.option(
    "--chronon-root",
    default=None,
    envvar="CHRONON_ROOT",
    help="Path to the root Chronon folder.",
)
@click.option(
    "--ignore-python-errors",
    is_flag=True,
    default=False,
    help="Allow compilation to proceed even with Python errors (useful for testing)",
)
@click.option(
    "-f",
    "--format",
    help="Output format.",
    default=Format.TEXT,
    type=click.Choice(Format, case_sensitive=False),
    show_default=True,
)
@click.option(
    "--force",
    is_flag=True,
    help="Force compilation to proceed even with errors",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Perform a dry run of the compilation without writing any files",
)
@jsonify_exceptions_if_json_format
def compile(chronon_root, ignore_python_errors=False, format=Format.TEXT, force=False, dry_run=False):
    """Compile Chronon configs."""
    if chronon_root is None or chronon_root == "":
        chronon_root = os.getcwd()

    if chronon_root not in sys.path:
        if format != Format.JSON:
            console.print(
                f"\nAdding [{STYLE_INFO} italic]{chronon_root}[/{STYLE_INFO} italic] to python path, during compile."
            )
        sys.path.insert(0, chronon_root)
    elif format != Format.JSON:
        console.print(
            f"\n[{STYLE_INFO} italic]{chronon_root}[/{STYLE_INFO} italic] already on python path."
        )

    compiled_result, has_errors, _ = __compile(chronon_root=chronon_root, ignore_python_errors=ignore_python_errors, format=format, force=force, dry_run=dry_run)

    return compiled_result


def __compile(
    chronon_root, ignore_python_errors=False, format=Format.TEXT, force=False, dry_run=False, validate_all=False
):
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

    text_mode = format != Format.JSON

    # Canary pass first: writes <root>/canary_compiled/ using Team.canaryEnv /
    # canaryConf / canaryClusterConf. Reading happens before prod so a confirm-
    # prompt in pending-changes doesn't get sandwiched between two prod outputs.
    if text_mode:
        console.rule("[bold cyan]🐤 BEGIN CANARY COMPILATION[/]")
    canary_context = CompileContext(
        ignore_python_errors=ignore_python_errors,
        format=format,
        force=force,
        mode=CompileMode.CANARY,
    )
    canary_compiler = Compiler(canary_context)
    canary_compiler.compile(dry_run, validate_all)
    if text_mode:
        console.rule("[bold cyan]🐤 END CANARY COMPILATION[/]")

    # Prod pass: writes <root>/compiled/ as before. Results from this pass are
    # what's returned and (in JSON mode) what's printed — keeping the external
    # contract stable while canary output is recoverable from disk.
    if text_mode:
        console.rule("[bold magenta]🚀 BEGIN PROD COMPILATION[/]")
    compile_context = CompileContext(
        ignore_python_errors=ignore_python_errors,
        format=format,
        force=force,
        mode=CompileMode.PROD,
    )
    compiler = Compiler(compile_context)
    results = compiler.compile(dry_run, validate_all)
    if text_mode:
        console.rule("[bold magenta]🚀 END PROD COMPILATION[/]")
    if format == Format.JSON:
        print(
            json.dumps(
                {
                    "status": "success",
                    "results": {
                        ConfType._VALUES_TO_NAMES[conf_type]: list(conf_result.obj_dict.keys())
                        for conf_type, conf_result in results.items()
                        if conf_result.obj_dict
                    },
                },
                indent=4,
            )
        )
    has_errors = compiler.has_compilation_errors() or canary_compiler.has_compilation_errors()

    if has_errors and not ignore_python_errors:
        sys.exit(1)

    return results, has_errors, compiler.compile_context.validator.pending_changes


if __name__ == "__main__":
    compile()
