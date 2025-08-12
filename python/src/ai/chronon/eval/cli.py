#!/usr/bin/env python3
"""
CLI command for the evaluation functionality.
"""

import click

from ai.chronon.eval.eval import ChrononEvaluator
from ai.chronon.repo.compile import compile_and_raise_on_errors


@click.command(
    name="eval",
    help="Evaluate Chronon configuration files (GroupBy, Join, StagingQuery)"
)
@click.argument("conf_path", type=str)
@click.option("--resample", is_flag=True, default=False, 
              help="Force resampling of source tables from BigQuery")
@click.option("--skip-compile", is_flag=True, default=False,
              help="Skip compilation step if compiled file already exists")
def main(conf_path: str, resample: bool, skip_compile: bool):
    """Evaluate a Chronon configuration file and display the results."""
    
    # Run compile first unless explicitly skipped
    if not skip_compile:
        click.echo("🔄 Running compile first to ensure configs are up to date...")
        try:
            compile_and_raise_on_errors()
            click.echo("✅ Compile completed successfully")
        except Exception as e:
            click.echo(f"❌ Compile failed: {str(e)}", err=True)
            click.echo("💡 Use --skip-compile if you're sure your compiled file already exists", err=True)
            raise click.Abort() from e
    
    evaluator = ChrononEvaluator(resample=resample)
    
    try:
        # Determine the type of configuration based on path
        if "/joins/" in conf_path:
            result = evaluator.evaluate_join(conf_path)
        elif "/group_bys/" in conf_path:
            result = evaluator.evaluate_group_by(conf_path)
        elif "/staging_queries/" in conf_path:
            result = evaluator.evaluate_staging_query(conf_path)
        else:
            click.echo(f"Error: Cannot determine configuration type from path: {conf_path}")
            click.echo("Path must contain '/joins/', '/group_bys/', or '/staging_queries/'")
            raise click.Abort()
        
        # Output the rendered result
        click.echo(result)
        
    except Exception as e:
        click.echo(f"❌ Evaluation failed: {str(e)}", err=True)
        raise click.Abort() from e


if __name__ == "__main__":
    main()