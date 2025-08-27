"""
Utilities for handling config versioning in Chronon.
"""

from typing import Optional, Tuple


def parse_name_and_version(name: str) -> Tuple[str, Optional[int]]:
    """Parse config name to extract base name and version.

    Args:
        name: Config name (e.g., 'config_name__1' or 'config_name')

    Returns:
        Tuple of (base_name, version) where version is None if no version suffix
    """
    if "__" in name:
        parts = name.rsplit("__", 1)
        if len(parts) == 2 and parts[1].isdigit():
            return parts[0], int(parts[1])
    return name, None


def is_version_change(deleted_name: str, added_name: str) -> bool:
    """Check if a deleted/added pair represents a version change.

    Args:
        deleted_name: Name of deleted config
        added_name: Name of added config

    Returns:
        True if this represents a version bump of the same config
    """
    deleted_base, deleted_version = parse_name_and_version(deleted_name)
    added_base, added_version = parse_name_and_version(added_name)

    return (
        deleted_base == added_base
        and deleted_version is not None
        and added_version is not None
        and deleted_version != added_version
    )
