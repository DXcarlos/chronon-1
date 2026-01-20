import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

from ai.chronon.cli.logger import get_logger

logger = get_logger()


def get_git_user_email() -> str:
    """Get the email of the current git user, falling back to USER env var."""
    try:
        result = subprocess.run(
            ["git", "config", "user.email"],
            capture_output=True,
            text=True,
            check=True,
        )
        email = result.stdout.strip()
        if email:
            return email
    except Exception:
        pass
    return os.environ.get("USER", "")


def get_current_branch() -> str:
    try:
        subprocess.check_output(["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL)

        return (
            subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"])
            .decode("utf-8")
            .strip()
        )

    except subprocess.CalledProcessError as e:
        try:
            head_file = Path(".git/HEAD").resolve()

            if head_file.exists():
                content = head_file.read_text().strip()

                if content.startswith("ref: refs/heads/"):
                    return content.split("/")[-1]

        except Exception:
            pass

        print(
            f"⛔ Error: {e.stderr.decode('utf-8') if e.stderr else 'Not a git repository or no commits'}",
            file=sys.stderr,
        )

        raise


def get_fork_point(base_branch: str = "main") -> str:
    try:
        return (
            subprocess.check_output(["git", "merge-base", base_branch, "HEAD"])
            .decode("utf-8")
            .strip()
        )

    except subprocess.CalledProcessError as e:
        print(
            f"⛔ Error: {e.stderr.decode('utf-8') if e.stderr else f'Could not determine fork point from {base_branch}'}",
            file=sys.stderr,
        )
        raise


def get_file_content_at_commit(file_path: str, commit: str) -> Optional[str]:
    try:
        return subprocess.check_output(["git", "show", f"{commit}:{file_path}"]).decode("utf-8")
    except subprocess.CalledProcessError:
        return None


def get_current_file_content(file_path: str) -> Optional[str]:
    try:
        return Path(file_path).read_text()
    except Exception:
        return None


def get_changes_since_commit(path: str, commit: Optional[str] = None) -> List[str]:
    path = Path(path).resolve()
    if not path.exists():
        print(f"⛔ Error: Path does not exist: {path}", file=sys.stderr)
        raise ValueError(f"Path does not exist: {path}")

    try:
        subprocess.check_output(["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL)
        commit_range = f"{commit}..HEAD" if commit else "HEAD"

        changes = (
            subprocess.check_output(["git", "diff", "--name-only", commit_range, "--", str(path)])
            .decode("utf-8")
            .splitlines()
        )

    except subprocess.CalledProcessError:
        changes = (
            subprocess.check_output(["git", "diff", "--name-only", "--", str(path)])
            .decode("utf-8")
            .splitlines()
        )

    try:
        untracked = (
            subprocess.check_output(
                ["git", "ls-files", "--others", "--exclude-standard", str(path)]
            )
            .decode("utf-8")
            .splitlines()
        )

        changes.extend(untracked)

    except subprocess.CalledProcessError as e:
        print(
            f"⛔ Error: {e.stderr.decode('utf-8') if e.stderr else 'Failed to get untracked files'}",
            file=sys.stderr,
        )

        raise

    logger.info(f"Changes since commit: {changes}")

    return [change for change in changes if change.strip()]


def get_changes_since_fork(path: str, base_branch: str = "main") -> List[str]:
    try:
        fork_point = get_fork_point(base_branch)
        path = Path(path).resolve()

        # Get all potential changes
        changed_files = set(get_changes_since_commit(str(path), fork_point))

        # Filter out files that are identical to fork point
        real_changes = []
        for file in changed_files:
            fork_content = get_file_content_at_commit(file, fork_point)
            current_content = get_current_file_content(file)

            if fork_content != current_content:
                real_changes.append(file)

        logger.info(f"Changes since fork: {real_changes}")

        return real_changes

    except subprocess.CalledProcessError as e:
        print(
            f"⛔ Error: {e.stderr.decode('utf-8') if e.stderr else f'Failed to get changes since fork from {base_branch}'}",
            file=sys.stderr,
        )
        raise
