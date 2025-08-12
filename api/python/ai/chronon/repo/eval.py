#!/usr/bin/env python3
"""
Evaluation command for Chronon configurations.

This module provides the 'zipline eval' command that evaluates GroupBy, Join, and StagingQuery
configurations and renders the results with lineage analysis, upstream failure detection,
and schema validation.
"""

#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import os
from pathlib import Path

import click

from ai.chronon.eval.eval import ChrononEvaluator


@click.command(
    name="eval",
    help="Evaluate Chronon configurations (GroupBy, Join, StagingQuery) with schema validation and lineage analysis"
)
@click.option(
    "--conf", 
    required=True, 
    help="Path to configuration file (relative or absolute). Must contain '/joins/', '/group_bys/', or '/staging_queries/'"
)
@click.option(
    "--resample", 
    is_flag=True, 
    default=False,
    help="Resample source tables (useful for testing with fresh data)"
)
@click.pass_context
def main(ctx, conf, resample):
    """
    Evaluate Chronon configuration files and display results.
    
    This command evaluates GroupBy, Join, and StagingQuery configurations,
    performing schema validation, dependency checking, and lineage analysis.
    
    Examples:
        zipline eval --conf compiled/joins/my_team/user_features.json
        zipline eval --conf group_bys/features/click_features.json
        zipline eval --conf staging_queries/events/user_events.json --resample
    """
    try:
        # Convert relative path to absolute if needed
        if not os.path.isabs(conf):
            conf_path = os.path.abspath(conf)
        else:
            conf_path = conf
        
        # Verify the file exists
        if not os.path.exists(conf_path):
            click.echo(f"❌ Error: Configuration file not found: {conf_path}", err=True)
            ctx.exit(1)
        
        # Determine configuration type from path
        config_type = None
        if "/joins/" in conf_path:
            config_type = "join"
        elif "/group_bys/" in conf_path:
            config_type = "group_by"
        elif "/staging_queries/" in conf_path:
            config_type = "staging_query"
        else:
            click.echo(
                f"❌ Error: Cannot determine configuration type from path: {conf_path}\n"
                "Path must contain '/joins/', '/group_bys/', or '/staging_queries/'", 
                err=True
            )
            ctx.exit(1)
        
        # Initialize evaluator
        click.echo(f"🔍 Evaluating {config_type}: {conf_path}")
        evaluator = ChrononEvaluator(resample=resample)
        
        # Evaluate based on type
        if config_type == "join":
            result = evaluator.evaluate_join(conf_path)
        elif config_type == "group_by":
            result = evaluator.evaluate_group_by(conf_path)
        elif config_type == "staging_query":
            result = evaluator.evaluate_staging_query(conf_path)
        
        # Output the result
        click.echo(result)
        
    except KeyboardInterrupt:
        click.echo("\n⚠️  Evaluation interrupted by user", err=True)
        ctx.exit(1)
    except Exception as e:
        click.echo(f"❌ Error: {str(e)}", err=True)
        ctx.exit(1)


if __name__ == "__main__":
    main()