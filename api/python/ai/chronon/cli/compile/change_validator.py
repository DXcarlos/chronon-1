import os
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import ai.chronon.cli.logger as logger
from ai.chronon.cli.compile.compile_context import CompileContext
from ai.chronon.cli.compile.display.console import console

logger = logger.get_logger()


@dataclass
class ConfigChange:
    """Represents a change to a compiled config file."""
    name: str
    file_path: str
    online: bool = False
    production: bool = False


class ChangeValidator:
    """
    Validates compiled files before overwriting, categorizing changes by online/production flags.
    """

    def __init__(self, compile_context: CompileContext):
        self.compile_context = compile_context

    def validate_changed_files(self, staging_dir: str):
        """
        Detect and categorize compiled files that have changed, organized by:
        1) changed: modified configs with online/production flags in dataclass
        2) deleted: deleted configs with online/production flags in dataclass
        3) added: new compiled files with online/production flags in dataclass
        
        Prompts user for confirmation if there are changes to existing configs.
        """
        output_dir = self.compile_context.output_dir()
        if not os.path.exists(output_dir):
            # No existing compiled files to compare against
            return

        changed_files: Dict[str, List[ConfigChange]] = {
            'changed': [],
            'deleted': [],
            'added': [],
        }

        # Walk through all files in staging directory
        for staging_root, _staging_dirs, staging_files in os.walk(staging_dir):
            for staging_file in staging_files:
                if staging_file.startswith("."):  # ignore hidden files
                    continue

                staging_path = os.path.join(staging_root, staging_file)
                # Get corresponding path in existing output directory
                relative_path = os.path.relpath(staging_path, staging_dir)
                existing_path = os.path.join(output_dir, relative_path)

                # Check if file is different or new
                if not os.path.exists(existing_path):
                    # New file
                    change = self._categorize_file(staging_path, relative_path)
                    changed_files['added'].append(change)
                else:
                    # Compare existing vs new file content
                    with open(staging_path, 'r') as f:
                        staging_content = f.read()
                    with open(existing_path, 'r') as f:
                        existing_content = f.read()
                    
                    if staging_content != existing_content:
                        # Modified file
                        change = self._categorize_file(staging_path, relative_path)
                        changed_files['changed'].append(change)

        # Check for deleted files (exist in output but not in staging)
        for output_root, _output_dirs, output_files in os.walk(output_dir):
            for output_file in output_files:
                if output_file.startswith("."):  # ignore hidden files
                    continue

                output_path = os.path.join(output_root, output_file)
                # Get corresponding path in staging directory
                relative_path = os.path.relpath(output_path, output_dir)
                staging_path = os.path.join(staging_dir, relative_path)

                if not os.path.exists(staging_path):
                    # File is being deleted
                    change = self._categorize_file(output_path, relative_path)
                    changed_files['deleted'].append(change)

        # Always report changes (including new files)
        self._report_changed_files(changed_files)
        
        # Check if we need user confirmation (only for existing changes, not new files)
        existing_changes = (
            changed_files['changed'] + 
            changed_files['deleted']
        )
        
        if existing_changes:
            if not self._prompt_user_confirmation():
                console.print("❌ Compilation cancelled by user.")
                sys.exit(1)

    def _parse_file_metadata(self, file_path: str) -> Optional[Dict[str, Any]]:
        """
        Parse a compiled file to extract metadata including online/production flags.
        Returns dict with metadata info, or None if file cannot be parsed.
        """
        try:
            import json
            
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Extract metadata from top-level dict
            metadata = data.get('metaData')
            if metadata:
                name = metadata.get('name', file_path)
                return {
                    'name': name,
                    'online': metadata.get('online', False),
                    'production': metadata.get('production', False)
                }
            
            return None
            
        except Exception as e:
            logger.warning(f"Failed to parse file metadata {file_path}: {str(e)}")
            return None

    def _categorize_file(self, file_path: str, relative_path: str) -> ConfigChange:
        """
        Categorize a file and return ConfigChange with online/production flags.
        """
        metadata = self._parse_file_metadata(file_path)
        if metadata:
            return ConfigChange(
                name=metadata['name'],
                file_path=relative_path,
                online=metadata['online'],
                production=metadata['production']
            )
        else:
            return ConfigChange(
                name=relative_path,
                file_path=relative_path,
                online=False,
                production=False
            )

    def _report_changed_files(self, changed_files: Dict[str, List[ConfigChange]]):
        """
        Report categorized changed files to the user.
        """
        total_existing_changes = len(changed_files['changed']) + len(changed_files['deleted'])
        total_new_files = len(changed_files['added'])
        
        if total_existing_changes == 0 and total_new_files == 0:
            console.print("\n✅ No changes detected in compiled files.")
            return
        
        # Helper function to format config name with flags
        def format_config_name(change: ConfigChange) -> str:
            name = change.name
            flags = []
            if change.online:
                flags.append("[bold red][ONLINE][/bold red]")
            if change.production:
                flags.append("[bold magenta][PRODUCTION][/bold magenta]")
            if flags:
                return f"{name} {' '.join(flags)}"
            return name
        
        # Report changed files
        if changed_files['changed']:
            console.print(f"\n🔄 [bold yellow]CHANGED configs ({len(changed_files['changed'])} files):[/bold yellow]")
            for change in changed_files['changed']:
                console.print(f"  • {format_config_name(change)}")
        
        # Report deleted files
        if changed_files['deleted']:
            console.print(f"\n🗑️  [bold red]DELETED configs ({len(changed_files['deleted'])} files):[/bold red]")
            for change in changed_files['deleted']:
                console.print(f"  • {format_config_name(change)}")
        
        # Report added files
        if changed_files['added']:
            console.print(f"\n➕ [bold green]ADDED configs ({len(changed_files['added'])} files):[/bold green]")
            for change in changed_files['added']:
                console.print(f"  • {format_config_name(change)}")

    def _prompt_user_confirmation(self) -> bool:
        """
        Prompt user for Y/N confirmation to proceed with overwriting existing configs.
        Returns True if user confirms, False otherwise.
        """
        console.print("\n❓ [bold]Do you want to proceed with overwriting these existing compiled configs? (y/N):[/bold]", end=" ")
        
        try:
            response = input().strip().lower()
            return response in ['y', 'yes']
        except (EOFError, KeyboardInterrupt):
            console.print("\n❌ Compilation cancelled.")
            return False
