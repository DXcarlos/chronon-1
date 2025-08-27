import os
import shutil
import subprocess

import pytest

from ai.chronon.cli.git_utils import (
    get_changes_since_commit,
    get_changes_since_fork,
    get_current_branch,
)


@pytest.fixture
def git_repo(tmp_path):
    repo_dir = tmp_path / "test_repo"
    repo_dir.mkdir()
    os.chdir(repo_dir)

    def cleanup():
        os.chdir(os.path.dirname(repo_dir))
        shutil.rmtree(repo_dir)

    return repo_dir, cleanup


def test_subfolder_changes(git_repo):
    repo_dir, cleanup = git_repo
    try:
        # configure git settings for test
        subprocess.run(
            ["git", "config", "--global", "init.defaultBranch", "main"], check=True
        )
        
        # 1. Init git repo
        subprocess.run(["git", "init"], check=True)
        
        # Set local git configs
        subprocess.run(
            ["git", "config", "--local", "user.email", "test@example.com"], check=True
        )
        subprocess.run(
            ["git", "config", "--local", "user.name", "Test User"], check=True
        )
        subprocess.run(["git", "checkout", "-b", "main"], check=True)
        assert get_current_branch() == "main"

        # 2. Create and commit initial files
        subfolder = repo_dir / "subfolder"
        subfolder.mkdir()
        (subfolder / "sub_file.txt").write_text("initial")
        (repo_dir / "root_file.txt").write_text("initial")
        subprocess.run(["git", "add", "."], check=True)
        subprocess.run(["git", "commit", "-m", "Initial commit"], check=True)

        # 3. Create test branch
        subprocess.run(["git", "checkout", "-b", "test"], check=True)
        assert get_current_branch() == "test"

        # 4. Modify files
        (subfolder / "sub_file.txt").write_text("modified")
        (subfolder / "new_sub_file.txt").write_text("new")
        (repo_dir / "root_file.txt").write_text("modified")
        (repo_dir / "new_root_file.txt").write_text("new")

        # 5. Test uncommitted changes
        changes = get_changes_since_commit(str(subfolder))
        assert len(changes) == 2
        assert "subfolder/sub_file.txt" in changes
        assert "subfolder/new_sub_file.txt" in changes
        assert "root_file.txt" not in changes
        assert "new_root_file.txt" not in changes

        # 6. Commit changes
        subprocess.run(["git", "add", "."], check=True)
        subprocess.run(["git", "commit", "-m", "Test branch changes"], check=True)

        # 7. Revert one file
        (subfolder / "sub_file.txt").write_text("initial")

        # 8. Test changes since commit
        changes = get_changes_since_commit(str(subfolder))
        assert len(changes) == 1
        assert "subfolder/new_sub_file.txt" not in changes
        assert "subfolder/sub_file.txt" in changes

        # 9. Test changes since fork point
        changes = get_changes_since_fork(str(subfolder))
        assert len(changes) == 1
        assert "subfolder/new_sub_file.txt" in changes
        assert "subfolder/sub_file.txt" not in changes

    finally:
        cleanup()
