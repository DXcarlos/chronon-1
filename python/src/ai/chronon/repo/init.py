#!/usr/bin/env python

import os
import shutil

import click
from importlib_resources import files
from rich.prompt import Prompt
from rich.syntax import Syntax

from ai.chronon.cli.compile.display.console import console


@click.command(name="init")
@click.option(
    "--cloud-provider",
    envvar="CLOUD_PROVIDER",
    help="Cloud provider to use.",
    required=True,
    type=click.Choice(["aws", "gcp"], case_sensitive=False),
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
