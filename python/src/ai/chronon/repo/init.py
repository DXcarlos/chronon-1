#!/usr/bin/env python

import os
import shutil
from configparser import ConfigParser
from pathlib import Path

import click
from importlib_resources import files
from rich.prompt import Prompt
from rich.syntax import Syntax

from ai.chronon.cli.compile.display.console import console


def create_zipline_config(cloud_provider: str) -> None:
    """Create or overwrite ~/.zipline/config with the cloud provider setting."""
    config_dir = Path.home() / ".zipline"
    config_file = config_dir / "config"

    # Create .zipline directory if it doesn't exist
    config_dir.mkdir(parents=True, exist_ok=True)

    # Check if config file already exists and prompt for confirmation
    if config_file.exists():
        choice = Prompt.ask(
            f"[bold yellow] Warning: [/]{config_file} already exists. Overwrite?",
            choices=["y", "n"],
            default="y",
        )
        if choice == "n":
            console.print("[yellow]Config file not updated.[/]")
            return

    # Create config parser and set cloud_provider
    config = ConfigParser()
    config["default"] = {
        "cloud_provider": cloud_provider.lower()
    }

    # Write config file
    with open(config_file, "w") as f:
        config.write(f)

    console.print(f"[bold green]✓[/] Config file created at {config_file}")


@click.command(name="init")
@click.option(
    "--cloud-provider",
    envvar="CLOUD_PROVIDER",
    help="Cloud provider to use.",
    required=True,
    type=click.Choice(["aws", "gcp", "azure"], case_sensitive=False),
)
@click.option(
    "--chronon-root",
    help="Path to the root chronon folder.",
    default=os.path.join(os.getcwd(), "zipline"),
    type=click.Path(file_okay=False, writable=True),
)
@click.pass_context
def main(ctx, chronon_root, cloud_provider):
    template_path = files("ai.chronon").joinpath("resources", cloud_provider.lower())
    target_path = os.path.abspath(chronon_root)

    if os.path.exists(target_path) and os.listdir(target_path):
        choice = Prompt.ask(
            f"[bold yellow] Warning: [/]{target_path} is not empty. Proceed?",
            choices=["y", "n"],
            default="y",
        )
        if choice == "n":
            return

    console.print(f"Generating scaffolding at {target_path} ...")

    try:
        shutil.copytree(template_path, target_path, dirs_exist_ok=True)
        console.print("[bold green] Project scaffolding created successfully! 🎉\n")

        # Create local config file
        create_zipline_config(cloud_provider)
        console.print()

        export_cmd = Syntax(
            f"`export PYTHONPATH={target_path}:$PYTHONPATH`",
            "bash",
            theme="github-dark",
            line_numbers=False,
        )
        console.print("Please copy the following command to your shell config:")
        console.print(export_cmd)
    except Exception:
        console.print_exception()


if __name__ == "__main__":
    main()
