from typing import Optional

from git import Repo


def get_default_origin_branch(path, repo: Optional[Repo] = None):
    if not repo:
        repo = Repo(path, search_parent_directories=True)
    return repo.remotes.origin.refs.HEAD.reference.name.split("/")[-1]


def get_current_branch(path, repo: Optional[Repo] = None):
    if not repo:
        repo = Repo(path, search_parent_directories=True)
    return repo.active_branch.name
