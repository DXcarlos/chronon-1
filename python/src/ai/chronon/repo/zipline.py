from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as ver

import click

from ai.chronon.cli.compile.display.console import console
from ai.chronon.repo.compile import compile
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

# Dynamically load hub commands if zipline-hub is installed
# Hub commands are added at the top level (e.g., zipline backfill, zipline eval)
try:
    from ai.chronon.repo.hub_runner import backfill, eval, schedule, cancel, run_adhoc
    zipline.add_command(backfill)
    zipline.add_command(eval)
    zipline.add_command(schedule)
    zipline.add_command(cancel)
    zipline.add_command(run_adhoc)
except ImportError:
    # zipline-hub not installed, hub commands will not be available
    pass
