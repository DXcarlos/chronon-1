#!/usr/bin/env python3
"""
Evaluation script that integrates BigQuery sampling with Scala evaluation engine.

This script:
1) Finds all source tables for GroupBy/Join using ConfIndex
2) Samples tables from BigQuery and builds local warehouse  
3) Calls eval.scala via py4j using the local warehouse
4) Returns JSON response from evaluation
"""

import json
import os
import subprocess
import time
import urllib.request
import shutil
from pathlib import Path
from typing import Any, Dict

from py4j.java_gateway import JavaGateway

from ai.chronon.eval.render import (
    render_group_by_result,
    render_join_result,
    render_staging_query_result,
)

# Local warehouse location constant
LOCAL_WAREHOUSE_PATH = Path(os.getenv("CHRONON_ROOT", os.getcwd())) / "local_warehouse"

# Remote JAR download URL - TODO: Set actual URL
CHRONON_EVAL_JAR_URL = "TODO"


class ChrononEvaluator:
    """Evaluator that integrates BigQuery sampling with Scala evaluation engine."""

    def __init__(self, resample: bool = False):
        self.resample = resample
        self.gateway = None
        self.eval_conf = None
        self._ensure_local_warehouse()

    def _ensure_local_warehouse(self):
        """Ensure local warehouse directory exists."""
        LOCAL_WAREHOUSE_PATH.mkdir(parents=True, exist_ok=True)
        print(f"Local warehouse directory: {LOCAL_WAREHOUSE_PATH}")

    def _resolve_config_path(self, conf_path: str) -> str:
        """Resolve relative path to absolute path."""
        if os.path.isabs(conf_path):
            return conf_path
        
        # If relative path, resolve it relative to current working directory
        absolute_path = os.path.abspath(conf_path)
        
        # Verify the file exists
        if not os.path.exists(absolute_path):
            raise FileNotFoundError(f"Configuration file not found: {absolute_path}")
        
        return absolute_path

    def _get_or_download_eval_jar(self) -> str:
        """Get evaluation JAR path, either from environment or by downloading."""
        # Check if CHRONON_EVAL_JAR environment variable is set
        eval_jar_path = os.getenv("CHRONON_EVAL_JAR")
        if eval_jar_path:
            if os.path.exists(eval_jar_path):
                print(f"Using evaluation JAR from CHRONON_EVAL_JAR: {eval_jar_path}")
                return eval_jar_path
            else:
                print(f"Warning: CHRONON_EVAL_JAR points to non-existent file: {eval_jar_path}")
        
        # Download JAR to cache directory
        cache_dir = Path.home() / ".chronon" / "cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        
        jar_filename = "chronon-eval.jar"
        cached_jar_path = cache_dir / jar_filename
        
        # Check if JAR already exists in cache
        if cached_jar_path.exists():
            print(f"Using cached evaluation JAR: {cached_jar_path}")
            return str(cached_jar_path)
        
        # Check if URL is set
        if CHRONON_EVAL_JAR_URL == "TODO":
            raise RuntimeError("JAR download URL not configured. Please set CHRONON_EVAL_JAR environment variable or configure CHRONON_EVAL_JAR_URL.")
        
        print(f"Downloading evaluation JAR from: {CHRONON_EVAL_JAR_URL}")
        print(f"Saving to: {cached_jar_path}")
        
        try:
            # Download the JAR file
            with urllib.request.urlopen(CHRONON_EVAL_JAR_URL) as response:
                with open(cached_jar_path, 'wb') as f:
                    shutil.copyfileobj(response, f)
            print("JAR download completed successfully")
            return str(cached_jar_path)
        except Exception as e:
            raise RuntimeError(f"Failed to download evaluation JAR from {CHRONON_EVAL_JAR_URL}: {str(e)}")

    def _start_eval_conf_gateway_if_needed(self):
        """Start the EvalConf gateway server if not already running."""
        if self.gateway is not None:
            return

        try:
            # Try to connect to existing gateway
            test_gateway = JavaGateway()
            test_entry_point = test_gateway.entry_point
            # Try to call a method to verify the connection works
            test_entry_point.reset()
            # If we get here, connection is good
            self.gateway = test_gateway
            self.eval_conf = test_entry_point
            print("Connected to existing EvalConf gateway")
            return
        except Exception as e:
            print(f"No existing gateway found ({str(e)}), starting new EvalConf gateway...")
            
        # Start new gateway
        self._start_eval_conf_gateway()
        
        # Wait a bit for gateway to start
        time.sleep(5)
        
        # Connect to the new gateway
        max_retries = 10
        for attempt in range(max_retries):
            try:
                self.gateway = JavaGateway()
                self.eval_conf = self.gateway.entry_point
                # Test the connection
                self.eval_conf.reset()
                print("Connected to new EvalConf gateway")
                return
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Attempt {attempt + 1} failed, retrying in 2 seconds...")
                    time.sleep(2)
                else:
                    raise RuntimeError(f"Failed to connect to EvalConf gateway after {max_retries} attempts: {str(e)}")

    def _start_eval_conf_gateway(self):
        """Start the EvalConf gateway server."""
        # Get the JAR path (either from environment or download)
        try:
            eval_jar_path = self._get_or_download_eval_jar()
        except Exception as e:
            raise RuntimeError(f"Failed to get evaluation JAR: {str(e)}")
        
        # Set environment variables if not already set
        env = os.environ.copy()
        if "CHRONON_ROOT" not in env:
            env["CHRONON_ROOT"] = os.getcwd()
        
        # Check if java is available
        try:
            result = subprocess.run(["java", "-version"], capture_output=True, text=True)
            if result.returncode != 0:
                raise FileNotFoundError("Java not found")
        except FileNotFoundError:
            raise RuntimeError(
                "Java (JDK) is not installed or not in PATH. Please install Java Development Kit (JDK) 11 or later.\n"
                "You can download it from: https://adoptium.net/ or https://www.oracle.com/java/technologies/downloads/"
            )
        
        # Use java command to start the EvalConf server
        java_cmd = [
            "java",
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
            "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED", 
            "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
            "--add-opens=java.base/java.io=ALL-UNNAMED",
            "--add-opens=java.base/java.net=ALL-UNNAMED",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--add-opens=java.base/java.util=ALL-UNNAMED",
            "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
            "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
            "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
            "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
            "-cp",
            eval_jar_path,
            "ai.chronon.integrations.cloud_gcp.EvalConfServer"
        ]
        
        print(f"Starting EvalConf gateway with JAR: {eval_jar_path}")
        
        # Start gateway in background
        try:
            # Create log files to capture output
            log_dir = Path.home() / ".chronon" / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
            
            stdout_log = log_dir / "eval_gateway_stdout.log"
            stderr_log = log_dir / "eval_gateway_stderr.log"
            
            print(f"Gateway logs will be written to:")
            print(f"  STDOUT: {stdout_log}")
            print(f"  STDERR: {stderr_log}")
            
            # Start the process in the background with logging
            with open(stdout_log, 'w') as stdout_file, open(stderr_log, 'w') as stderr_file:
                subprocess.Popen(
                    java_cmd, 
                    env=env, 
                    stdout=stdout_file, 
                    stderr=stderr_file,
                    start_new_session=True  # This detaches the process
                )
        except Exception as e:
            raise RuntimeError(f"Failed to start EvalConf gateway server: {str(e)}")

    def evaluate_join(self, join_conf_path: str) -> str:
        """Evaluate a join configuration and return the rendered result."""
        self._start_eval_conf_gateway_if_needed()
        
        try:
            # Reset the EvalConf state to ensure clean evaluation
            self.eval_conf.reset()
            
            # Resolve relative path to absolute path
            absolute_path = self._resolve_config_path(join_conf_path)
            
            # Call the evalJoinConf method on the EvalConf gateway
            result = self.eval_conf.evalJoinConf(absolute_path)
            
            # Render the result directly
            return render_join_result(result)
        except Exception as e:
            return f"❌ Error: Failed to evaluate join: {str(e)}"

    def evaluate_group_by(self, groupby_conf_path: str) -> str:
        """Evaluate a group by configuration and return the rendered result."""
        self._start_eval_conf_gateway_if_needed()
        
        try:
            # Reset the EvalConf state to ensure clean evaluation
            self.eval_conf.reset()
            
            # Resolve relative path to absolute path
            absolute_path = self._resolve_config_path(groupby_conf_path)
            
            # Call the evalGroupByConf method on the EvalConf gateway
            result = self.eval_conf.evalGroupByConf(absolute_path)
            
            # Render the result directly
            return render_group_by_result(result)
        except Exception as e:
            return f"❌ Error: Failed to evaluate group by: {str(e)}"

    def evaluate_staging_query(self, staging_query_conf_path: str) -> str:
        """Evaluate a staging query configuration and return the rendered result."""
        self._start_eval_conf_gateway_if_needed()
        
        try:
            # Reset the EvalConf state to ensure clean evaluation
            self.eval_conf.reset()
            
            # Resolve relative path to absolute path
            absolute_path = self._resolve_config_path(staging_query_conf_path)
            
            # Call the evalStagingQueryConf method on the EvalConf gateway
            result = self.eval_conf.evalStagingQueryConf(absolute_path)
            
            # Render the result directly
            return render_staging_query_result(result)
        except Exception as e:
            return f"❌ Error: Failed to evaluate staging query: {str(e)}"

    def _thrift_to_dict(self, thrift_obj) -> Dict[str, Any]:
        """Convert a Thrift object to a Python dictionary."""
        if thrift_obj is None:
            return {}
        
        # Use the Thrift object's built-in serialization
        # This is a simplified approach - in practice you might want more sophisticated conversion
        try:
            # Try to access common fields that exist on eval result objects
            result_dict = {}
            
            # Check for common methods/fields on Thrift objects
            if hasattr(thrift_obj, '__dict__'):
                for key, value in thrift_obj.__dict__.items():
                    if not key.startswith('_'):
                        result_dict[key] = str(value) if value is not None else None
            
            return result_dict
        except Exception as e:
            return {"serialization_error": str(e), "type": str(type(thrift_obj))}


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python eval.py <conf_path>")
        print("Example: python eval.py /path/to/joins/my_join.json")
        sys.exit(1)
    
    conf_path = sys.argv[1]
    evaluator = ChrononEvaluator()
    
    try:
        # Determine the type of configuration based on path
        if "/joins/" in conf_path:
            result = evaluator.evaluate_join(conf_path)
        elif "/group_bys/" in conf_path:
            result = evaluator.evaluate_group_by(conf_path)
        elif "/staging_queries/" in conf_path:
            result = evaluator.evaluate_staging_query(conf_path)
        else:
            print(f"Error: Cannot determine configuration type from path: {conf_path}")
            print("Path must contain '/joins/', '/group_bys/', or '/staging_queries/'")
            sys.exit(1)
        
        # Output the rendered result
        print(result)
        
    except Exception as e:
        error_result = {"error": f"Evaluation failed: {str(e)}"}
        print(json.dumps(error_result, indent=2))
        sys.exit(1)
    