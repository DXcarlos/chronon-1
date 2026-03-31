#!/usr/bin/env python

import os
import platform
import shutil
import subprocess

import click
from importlib_resources import files
from rich.prompt import Prompt
from rich.syntax import Syntax

from ai.chronon.cli.theme import console, print_success
from ai.chronon.repo.constants import VALID_CLOUDS


def _detect_shell_config():
    """Detect the user's shell and return (shell_name, config_file_path).
    Returns (shell_name, None) for unsupported shells.
    """
    shell = os.environ.get("SHELL", "")
    shell_name = os.path.basename(shell)
    home = os.path.expanduser("~")
    if shell_name == "zsh":
        return shell_name, os.path.join(home, ".zshrc")
    elif shell_name == "bash":
        if platform.system() == "Darwin":
            return shell_name, os.path.join(home, ".bash_profile")
        else:
            return shell_name, os.path.join(home, ".bashrc")
    else:
        return shell_name, None


def _add_to_shell_config(config_path, export_line):
    """Append export_line to config_path if not already present.
    Returns True if the line was added, False if already present.
    """
    if os.path.exists(config_path):
        with open(config_path) as f:
            if export_line in f.read():
                return False
    with open(config_path, "a") as f:
        f.write(f"\n{export_line}\n")
    return True


def _apply_pythonpath(target_path):
    """Apply PYTHONPATH to the current process environment."""
    current = os.environ.get("PYTHONPATH", "")
    os.environ["PYTHONPATH"] = f"{target_path}:{current}" if current else target_path


@click.command(name="init")
@click.argument(
    "cloud", type=click.Choice(VALID_CLOUDS, case_sensitive=False), envvar="CLOUD_PROVIDER"
)
@click.option(
    "--chronon-root",
    help="Path to the root chronon folder.",
    default=os.path.join(os.getcwd(), "zipline"),
    type=click.Path(file_okay=False, writable=True),
)
@click.pass_context
def main(ctx, cloud, chronon_root):
    """Initialize a new Zipline project with scaffolding.

    CLOUD is the cloud provider to use (gcp, aws, or azure).
    """
    template_path = files("ai.chronon").joinpath("resources", cloud.lower())
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

        print_success("Project scaffolding created successfully! 🎉")
        export_line = f'export PYTHONPATH="{target_path}:$PYTHONPATH"'
        shell_name, config_path = _detect_shell_config()
        if config_path is not None:
            choice = Prompt.ask(
                f"Add PYTHONPATH to {config_path}?",
                choices=["y", "n"],
                default="y",
            )
            if choice == "y":
                added = _add_to_shell_config(config_path, export_line)
                _apply_pythonpath(target_path)
                if added:
                    console.print(f"Added to {config_path} and applied to current session.")
                else:
                    console.print(f"Already in {config_path}, applied to current session.")
            else:
                _apply_pythonpath(target_path)
                export_cmd = Syntax(
                    export_line,
                    "bash",
                    theme="github-dark",
                    line_numbers=False,
                )
                console.print("Please copy the following command to your shell config:")
                console.print(export_cmd)
                console.print("Applied to current session only.")
        else:
            _apply_pythonpath(target_path)
            export_cmd = Syntax(
                export_line,
                "bash",
                theme="github-dark",
                line_numbers=False,
            )
            console.print(
                f"Unsupported shell ({shell_name}). Please add the following to your shell config:"
            )
            console.print(export_cmd)
            console.print("Applied to current session only.")

        # Initialize git repository if not already initialized (best-effort)
        git_dir = os.path.join(target_path, ".git")
        if not os.path.exists(git_dir):
            result = subprocess.run(
                ["git", "init"],
                cwd=target_path,
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                console.print("Initialized git repository")
            else:
                console.print(
                    f"[yellow]Warning: Could not initialize git repository: {result.stderr.strip()}[/yellow]"
                )
    except Exception:
        console.print_exception()


if __name__ == "__main__":
    main()
