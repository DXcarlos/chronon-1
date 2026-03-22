"""zipline init-agent: install AI agent context for Zipline/Chronon development."""

import importlib.resources
import os

import click

AGENTS = ["claude", "codex", "cursor", "gemini", "windsurf", "copilot"]

AGENT_LABELS = {
    "claude": "Claude Code",
    "codex": "Codex",
    "cursor": "Cursor",
    "gemini": "Gemini CLI",
    "windsurf": "Windsurf",
    "copilot": "GitHub Copilot",
}

# Destination paths relative to the target directory
AGENT_DEST = {
    "claude": os.path.join(".claude", "skills", "zipline", "skill.md"),
    "codex": "AGENTS.md",
    "cursor": os.path.join(".cursor", "rules", "zipline.mdc"),
    "gemini": "GEMINI.md",
    "windsurf": ".windsurfrules",
    "copilot": os.path.join(".github", "copilot-instructions.md"),
}

CURSOR_FRONTMATTER = """\
---
description: Zipline/Chronon feature engineering expert
alwaysApply: true
---

"""


def _load_context() -> str:
    with importlib.resources.open_text("ai.chronon.resources", "agent_context.md") as f:
        return f.read()


def _write_file(path: str, content: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(content)


@click.command("init-agent")
@click.option(
    "--agent",
    type=click.Choice(AGENTS, case_sensitive=False),
    default=None,
    help="AI agent to install context for. Prompted interactively if not provided.",
)
@click.option(
    "--dir",
    "target_dir",
    default=".",
    show_default=True,
    help="Target directory (project root) to install context into.",
)
def init_agent(agent, target_dir):
    """Install Zipline/Chronon agent context for your AI coding assistant.

    Supports Claude Code, Codex, Cursor, Gemini CLI, Windsurf, and GitHub
    Copilot. Installs the context file into the appropriate location for
    the selected agent.
    """
    if agent is None:
        agent = click.prompt(
            "Which AI agent would you like to set up?",
            type=click.Choice(AGENTS, case_sensitive=False),
            show_choices=True,
        )

    agent = agent.lower()
    rel_dest = AGENT_DEST[agent]
    abs_dest = os.path.join(os.path.abspath(target_dir), rel_dest)

    if os.path.exists(abs_dest):
        overwrite = click.confirm(
            f"{abs_dest} already exists. Overwrite?", default=False
        )
        if not overwrite:
            click.echo("Aborted.")
            return

    content = _load_context()
    if agent in ("cursor",):
        content = CURSOR_FRONTMATTER + content

    _write_file(abs_dest, content)
    click.echo(f"✓ {AGENT_LABELS[agent]} context installed at: {abs_dest}")
