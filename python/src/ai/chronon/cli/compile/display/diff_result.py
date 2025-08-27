from typing import List, Tuple

from rich.text import Text

from ai.chronon.cli.compile.version_utils import parse_name_and_version


class DiffResult:
    def __init__(self):
        self.added: List[str] = []
        self.updated: List[str] = []
        self.version_bumped: List[
            Tuple[str, int, int]
        ] = []  # (base_name, old_version, new_version)

    def detect_version_changes(self, deleted_names: List[str]) -> Tuple[List[str], List[str]]:
        """Detect version changes between deleted and added names.

        Returns:
            Tuple of (remaining_deleted, remaining_added) after removing version changes
        """
        # Create mappings of base names to versions
        deleted_base_to_name = {}
        added_base_to_name = {}

        for name in deleted_names:
            base_name, version = parse_name_and_version(name)
            if version is not None:
                deleted_base_to_name[base_name] = (name, version)

        for name in self.added:
            base_name, version = parse_name_and_version(name)
            if version is not None:
                added_base_to_name[base_name] = (name, version)

        # Find version changes
        remaining_deleted = []
        remaining_added = []

        for name in deleted_names:
            base_name, old_version = parse_name_and_version(name)
            if (
                base_name in added_base_to_name
                and old_version is not None
                and base_name in deleted_base_to_name
            ):
                # This is a version change
                _, new_version = added_base_to_name[base_name]
                self.version_bumped.append((base_name, old_version, new_version))
            else:
                remaining_deleted.append(name)

        for name in self.added:
            base_name, version = parse_name_and_version(name)
            if not (base_name in deleted_base_to_name and version is not None):
                # This is not part of a version change
                remaining_added.append(name)

        return remaining_deleted, remaining_added

    def render(self, deleted_names: List[str], indent="  ") -> Text:
        # Detect version changes first
        remaining_deleted, remaining_added = self.detect_version_changes(deleted_names)

        def added_signage():
            return Text("➕ Added", style="dim green")

        def updated_signage():
            return Text("❗ Changed in place (no version change)", style="dim yellow")

        def deleted_signage():
            return Text("🗑️ Deleted", style="red")

        def version_bumped_signage():
            return Text("⬆️ Version changed", style="dim blue")

        added = [(added_signage(), name) for name in remaining_added]
        updated = [(updated_signage(), name) for name in self.updated]
        version_bumped = [
            (version_bumped_signage(), f"{base_name} (v{old_ver} -> v{new_ver})")
            for base_name, old_ver, new_ver in self.version_bumped
        ]

        # Put version changes and additions first, changed items at the bottom
        # Sort each group separately to maintain grouping
        version_bumped_sorted = sorted(version_bumped, key=lambda t: t[1])
        added_sorted = sorted(added, key=lambda t: t[1])
        updated_sorted = sorted(updated, key=lambda t: t[1])

        result_order = version_bumped_sorted + added_sorted

        if remaining_deleted:
            deleted = [(deleted_signage(), name) for name in remaining_deleted]
            deleted_sorted = sorted(deleted, key=lambda t: t[1])
            result_order += deleted_sorted

        # Add updated (changed in place) at the very end
        result_order += updated_sorted

        text = Text(overflow="fold", no_wrap=False)
        for signage, name in result_order:
            text.append(indent)
            text.append(signage)
            text.append(" ")
            text.append(name)
            text.append("\n")

        if not text:
            return Text(indent + "No new changes detected\n", style="dim")

        return text
