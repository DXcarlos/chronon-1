import os
import sys

import click

from ai.chronon.cli.compile.compile_context import CompileContext
from ai.chronon.cli.compile.compiler import Compiler
from ai.chronon.cli.compile.display.console import console
from ai.chronon.repo.utils import change_directory


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
    """- Compile and validate python configs into serializable form"""

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
    chronon_root_path = os.path.expanduser(chronon_root) if chronon_root else os.getcwd()

    with change_directory(chronon_root_path):
        sys.path.append(chronon_root_path)
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


if __name__ == "__main__":
    compile()
