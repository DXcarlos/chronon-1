import os
import sys

import click

from ai.chronon.cli.compile.compile_context import CompileContext
from ai.chronon.cli.compile.compiler import Compiler
from ai.chronon.cli.compile.display.console import console


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
def compile(chronon_root, ignore_python_errors):
    print()

    if chronon_root is None or chronon_root == "":
        chronon_root = os.getcwd()

    if chronon_root not in sys.path:
        console.print(
            f"Adding [cyan italic]{chronon_root}[/cyan italic] to python path, during compile."
        )
        sys.path.append(chronon_root)
    else:
        console.print(f"[cyan italic]{chronon_root}[/cyan italic] already on python path.")

    return __compile(chronon_root, ignore_python_errors)


def __compile(chronon_root, ignore_python_errors=False):
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

    compile_context = CompileContext(ignore_python_errors=ignore_python_errors)
    compiler = Compiler(compile_context)
    results = compiler.compile()
    return results


def compile_and_raise_on_errors(chronon_root=None):
    """
    Compile configs and raise an exception if there are compilation errors.
    
    Args:
        chronon_root: Path to chronon root directory, defaults to current directory
        
    Raises:
        click.ClickException: If compilation fails
    """
    if chronon_root is None:
        chronon_root = os.getcwd()
    
    results = __compile(chronon_root, ignore_python_errors=False)
    
    # Check if there were any compilation errors
    for _conf_type, result in results.items():
        if result.error_dict:
            raise click.ClickException("Compilation failed with errors")
    
    return results


if __name__ == "__main__":
    compile()
