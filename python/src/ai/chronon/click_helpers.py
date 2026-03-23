import functools
import sys

import click

from ai.chronon.cli.theme import print_error
from ai.chronon.repo.compile import __compile


def handle_compile(func):
    """
    Handler for compiling the confs before running commands
    Requires repo arg
    """

    @click.option("--skip-compile", help="Skip compile before running the command", is_flag=True)
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not kwargs.get("skip_compile"):
            sys.path.append(kwargs.get("repo"))
            __compile(kwargs.get("repo"), force=kwargs.get("force"))
        return func(*args, **kwargs)

    return wrapper


def handle_dry_run_compile(func):
    """
    Handler for compiling the confs before running commands
    Requires repo arg
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        sys.path.append(kwargs.get("repo"))
        results, has_errors, pending_changes = __compile(
            kwargs.get("repo"), force=kwargs.get("force"), dry_run=True, validate_all=True, format=kwargs.get("format")
        )
        kwargs["compile_pending_changes"] = pending_changes
        return func(*args, **kwargs)

    return wrapper


def handle_conf_not_found(log_error=True, callback=None):
    """
    Handler for when a conf is not found
    """

    def wrapper(func):
        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except FileNotFoundError as e:
                if log_error:
                    print_error(f"File not found in {func.__name__}: {e}")
                if callback:
                    callback(*args, **kwargs)
                return

        return wrapped

    return wrapper
