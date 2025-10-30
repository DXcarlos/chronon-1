#!/usr/bin/env python3
"""
Template generator for Chronon test files using Jinja2.

This script demonstrates how to use the templated versions of:
- python/test/canary/group_bys/gcp/purchases.py.j2
- python/test/canary/joins/gcp/training_set.py.j2

Usage:
    python generate_templates.py --test-id my_test_123
    python generate_templates.py --test-id experiment_456
"""

import argparse
import os
from pathlib import Path
from jinja2 import Environment, FileSystemLoader


def generate_from_template(template_path: str, output_path: str, test_id: str):
    """Generate a Python file from a Jinja2 template with the given test_id."""
    
    # Get the directory containing the template
    template_dir = os.path.dirname(template_path)
    template_name = os.path.basename(template_path)
    
    # Create Jinja2 environment
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template(template_name)
    
    # Render the template with the test_id
    rendered_content = template.render(test_id=test_id)
    
    # Write the rendered content to the output file
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(rendered_content)
    
    print(f"Generated: {output_path}")


def main():
    parser = argparse.ArgumentParser(description='Generate templated Chronon test files')
    parser.add_argument('--test-id', required=True, help='Test ID to use in variable names')
    parser.add_argument('--output-dir', default='./generated', help='Output directory for generated files')
    
    args = parser.parse_args()
    
    # Template mappings: (template_path, relative_output_path)
    templates = [
        (
            'python/test/canary/group_bys/gcp/purchases.py.j2',
            f'python/test/canary/group_bys/gcp/purchases_{args.test_id}.py'
        ),
        (
            'python/test/canary/joins/gcp/training_set.py.j2',
            f'python/test/canary/joins/gcp/training_set_{args.test_id}.py'
        ),
        (
            'python/test/canary/joins/gcp/demo.py.j2',
            f'python/test/canary/joins/gcp/demo_{args.test_id}.py'
        ),
        (
            'python/test/canary/staging_queries/gcp/exports.py.j2',
            f'python/test/canary/staging_queries/gcp/exports_{args.test_id}.py'
        )

    ]
    
    for template_path, relative_output_path in templates:
        output_path = os.path.join(args.output_dir, relative_output_path)
        generate_from_template(template_path, output_path, args.test_id)
    
    print(f"\nAll templates generated successfully with test_id='{args.test_id}'")


if __name__ == '__main__':
    main()