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
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict

from py4j.java_gateway import JavaGateway

try:
    from google.auth import default
    from google.cloud import bigquery
    GOOGLE_AUTH_AVAILABLE = True
    BIGQUERY_AVAILABLE = True
except ImportError:
    GOOGLE_AUTH_AVAILABLE = False
    BIGQUERY_AVAILABLE = False

from ai.chronon.eval.gateway_helpers import (
    connect_to_gateway,
    get_eval_jar,
    kill_existing_gateway,
    start_eval_conf_gateway,
)
from ai.chronon.eval.render import (
    render_group_by_result,
    render_join_result,
    render_staging_query_result,
)

# Local warehouse location constant
LOCAL_WAREHOUSE_PATH = Path(os.getenv("CHRONON_ROOT", os.getcwd())) / "local_warehouse"


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

    def _validate_gcp_auth(self) -> None:
        """Validate GCP authentication and provide helpful error messages."""
        if not GOOGLE_AUTH_AVAILABLE:
            print("⚠️  Google Cloud authentication library not available.")
            print("   This may cause issues if your config uses BigQuery.")
            print("   Install with: pip install google-auth google-cloud-bigquery")
            return
        
        try:
            credentials, project = default()
            if project:
                print(f"✅ GCP authentication verified (project: {project})")
                
                # Test BigQuery connectivity specifically
                if BIGQUERY_AVAILABLE:
                    print("🔍 Testing BigQuery connectivity...")
                    client = bigquery.Client(project=project, credentials=credentials)
                    
                    # Try a simple query that should work if auth is properly set up
                    query = "SELECT 1 as test_column"
                    client.query(query, job_config=bigquery.QueryJobConfig(dry_run=True))
                    print("✅ BigQuery connectivity verified")
                    
                else:
                    print("⚠️  BigQuery client not available - skipping connectivity test")
            else:
                print("✅ GCP authentication verified")
        except Exception as bq_error:
                    print("❌ BigQuery Authentication Error")
                    print("   Common fixes:")
                    print("   • Run: gcloud auth application-default login --scopes=https://www.googleapis.com/auth/cloud-platform")
                    print("   • Or try: gcloud auth login --update-adc")
                    print("   • Check if your credentials need refreshing")
                    print("")
                    print(f"   BigQuery error: {str(bq_error)}")
                    raise RuntimeError("BigQuery authentication required") from bq_error

    def _setup_for_evaluation(self) -> None:
        """Setup required for evaluation: validate auth and start gateway."""
        try:
            self._validate_gcp_auth()
        except RuntimeError as e:
            # If auth validation fails, kill any existing gateway server
            # since it likely has stale credentials
            kill_existing_gateway()
            raise e
        
        self._start_eval_conf_gateway_if_needed()

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


    def _start_eval_conf_gateway_if_needed(self):
        """Start the EvalConf gateway server if not already running."""
        if self.gateway is not None:
            return

        # Suppress py4j connection error logging temporarily
        py4j_logger = logging.getLogger('py4j.java_gateway')
        original_level = py4j_logger.level
        py4j_logger.setLevel(logging.CRITICAL)
        
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
        except Exception:
            print("No existing gateway found, starting new EvalConf gateway...")
        finally:
            # Restore original logging level
            py4j_logger.setLevel(original_level)
            
        # Start new gateway using helper
        eval_jar_path = get_eval_jar()
        start_eval_conf_gateway(eval_jar_path)
        
        # Wait a bit for gateway to start
        time.sleep(5)
        
        # Connect to the new gateway using helper
        self.gateway, self.eval_conf = connect_to_gateway()



    def evaluate_join(self, join_conf_path: str) -> str:
        """Evaluate a join configuration and return the rendered result."""
        self._setup_for_evaluation()
        
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
        self._setup_for_evaluation()
        
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
        self._setup_for_evaluation()
        
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
    