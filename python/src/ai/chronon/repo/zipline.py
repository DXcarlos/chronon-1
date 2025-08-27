from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as ver

import click

from ai.chronon.cli.compile.display.console import console
from ai.chronon.repo.compile import compile
from ai.chronon.repo.hub_runner import hub
from ai.chronon.repo.init import main as init_main
from ai.chronon.repo.run import main as run_main


def _set_package_version():
    try:
        package_version = ver("zipline-ai")
    except PackageNotFoundError:
        console.print("No package found. Continuing with the latest version.")
        package_version = "latest"
    return package_version


@click.group(
    help="The Zipline CLI. A tool for compiling and running Zipline pipelines. For more information, see: https://zipline.ai/docs"
)
@click.version_option(version=_set_package_version())
@click.pass_context
def zipline(ctx):
    ctx.ensure_object(dict)
    ctx.obj["version"] = _set_package_version()


zipline.add_command(compile)
zipline.add_command(run_main)
zipline.add_command(init_main)
zipline.add_command(hub)
