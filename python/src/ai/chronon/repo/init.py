#!/usr/bin/env python

import os
import platform
import shutil

import click
from importlib_resources import files
from rich.prompt import Prompt
from rich.syntax import Syntax

from ai.chronon.cli.theme import console, print_success
from ai.chronon.repo.constants import VALID_CLOUDS

_K8S_STACK_FILENAME = "zipline-stack.yaml"


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


def _copy_resource_tree(src_traversable, dest_dir: str) -> None:
    """Recursively copy a package resource tree to a filesystem directory."""
    os.makedirs(dest_dir, exist_ok=True)
    for item in src_traversable.iterdir():
        dest_path = os.path.join(dest_dir, item.name)
        if item.is_dir():
            _copy_resource_tree(item, dest_path)
        else:
            with item.open("rb") as f_in, open(dest_path, "wb") as f_out:
                f_out.write(f_in.read())


def _init_k8s():
    """Write zipline-stack.yaml and scaffold Chronon configs to the current directory."""
    cwd = os.getcwd()

    # Write zipline-stack.yaml
    dest = os.path.join(cwd, _K8S_STACK_FILENAME)
    if os.path.exists(dest):
        choice = Prompt.ask(
            f"[bold yellow]Warning:[/] {dest} already exists. Overwrite?",
            choices=["y", "n"],
            default="n",
        )
        if choice == "n":
            return

    src = files("ai.chronon").joinpath("resources", "local", _K8S_STACK_FILENAME)
    with src.open("r") as f:
        content = f.read()
    with open(dest, "w") as f:
        f.write(content)
    print_success(f"Created {dest}")

    # Copy quickstart scaffold into ./zipline/
    scaffold_src = files("ai.chronon").joinpath("resources", "local", "scaffold")
    scaffold_dest = os.path.join(cwd, "zipline")
    if os.path.exists(scaffold_dest) and os.listdir(scaffold_dest):
        choice = Prompt.ask(
            f"[bold yellow]Warning:[/] {scaffold_dest} is not empty. Overwrite scaffold?",
            choices=["y", "n"],
            default="n",
        )
        if choice == "n":
            console.print("Skipping scaffold copy.")
            scaffold_dest = None
    if scaffold_dest is not None:
        _copy_resource_tree(scaffold_src, scaffold_dest)
        print_success(f"Scaffold written to {scaffold_dest}/")

    console.print("\nEdit [bold]zipline-stack.yaml[/] then run:")
    console.print("  [bold]zipline admin generate[/]")
    console.print("\nTo compile the Chronon configs:")
    console.print("  [bold]zipline compile[/]")


@click.command(name="init")
@click.argument(
    "cloud",
    type=click.Choice([*VALID_CLOUDS, "k8s"], case_sensitive=False),
    envvar="CLOUD_PROVIDER",
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

    CLOUD is the cloud provider (gcp, aws, azure) or 'k8s' to generate
    a zipline-stack.yaml for local kind development.
    """
    if cloud.lower() == "k8s":
        _init_k8s()
        return

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
    except Exception:
        console.print_exception()


if __name__ == "__main__":
    main()
