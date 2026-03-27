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

AGENT_DEST = {
    "claude": os.path.join("~", ".claude", "skills", "zipline", "skill.md"),
    "codex": os.path.join("~", ".codex", "skills", "zipline", "SKILL.md"),
    "cursor": os.path.join("~", ".cursor", "skills", "zipline", "SKILL.md"),
    "gemini": os.path.join("~", ".gemini", "skills", "zipline", "SKILL.md"),
    "windsurf": os.path.join("~", ".windsurf", "skills", "zipline", "SKILL.md"),
    "copilot": os.path.join("~", ".github", "skills", "zipline", "SKILL.md"),
}


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
def init_agent(agent):
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
    abs_dest = os.path.expanduser(AGENT_DEST[agent])

    if os.path.exists(abs_dest):
        overwrite = click.confirm(
            f"{abs_dest} already exists. Overwrite?", default=False
        )
        if not overwrite:
            click.echo("Aborted.")
            return

    _write_file(abs_dest, _load_context())
    click.echo(f"✓ {AGENT_LABELS[agent]} context installed at: {abs_dest}")
