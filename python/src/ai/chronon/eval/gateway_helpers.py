#!/usr/bin/env python3
"""
Gateway management helpers for EvalConf server.

This module handles starting, stopping, and connecting to the EvalConf gateway server.
"""

import os
import subprocess
import time
from pathlib import Path
from typing import Tuple

from py4j.java_gateway import JavaGateway


def get_gateway_pid_file() -> Path:
    """Get the path to the gateway PID file."""
    pid_dir = Path.home() / ".chronon"
    pid_dir.mkdir(parents=True, exist_ok=True)
    return pid_dir / "eval_gateway.pid"


def kill_existing_gateway() -> None:
    """Kill any existing EvalConf gateway server processes using PID file."""
    pid_file = get_gateway_pid_file()
    
    if not pid_file.exists():
        print("🔍 No gateway PID file found - no existing gateway to kill")
        return
    
    try:
        # Read PID from file
        pid = int(pid_file.read_text().strip())
        print(f"🔄 Killing existing EvalConf gateway process (PID: {pid})...")
        
        try:
            # Try graceful kill first
            subprocess.run(["kill", str(pid)], timeout=5, check=True)
            print(f"   Killed process {pid}")
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            try:
                # Force kill if graceful kill fails
                print(f"   Force killing process {pid}")
                subprocess.run(["kill", "-9", str(pid)], timeout=5, check=True)
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
                print(f"   Failed to kill process {pid}: {e}")
        
        # Clean up PID file after killing
        pid_file.unlink(missing_ok=True)
        
        # Give process time to shut down
        time.sleep(2)
        
    except (ValueError, OSError) as e:
        print(f"⚠️  Error reading PID file: {e}")
        # Clean up corrupted PID file
        pid_file.unlink(missing_ok=True)
    except Exception as e:
        print(f"⚠️  Error killing gateway process: {e}")


def get_eval_jar() -> str:
    """Get the path to the evaluation JAR file."""
    try:
        # First, try to use packaged JAR from wheel
        import importlib.resources
        try:
            jar_path = importlib.resources.files('ai.chronon.jars') / 'chronon-eval.jar'
            if jar_path.exists():
                print(f"Using packaged evaluation JAR: {jar_path}")
                return str(jar_path)
        except (ImportError, AttributeError, FileNotFoundError):
            pass
        
        # Fallback to environment variable (for development)
        if "CHRONON_EVAL_JAR" in os.environ:
            jar_path = os.environ["CHRONON_EVAL_JAR"]
            if os.path.exists(jar_path):
                print(f"Using JAR from CHRONON_EVAL_JAR: {jar_path}")
                return jar_path
            else:
                raise FileNotFoundError(f"CHRONON_EVAL_JAR path does not exist: {jar_path}")
        
        raise FileNotFoundError("No evaluation JAR found. Either install the wheel or set CHRONON_EVAL_JAR")
        
    except Exception as e:
        raise RuntimeError(f"Failed to locate evaluation JAR: {str(e)}") from e


def start_eval_conf_gateway(eval_jar_path: str) -> None:
    """Start the EvalConf gateway server."""
    # Set environment variables if not already set
    env = os.environ.copy()
    if "CHRONON_ROOT" not in env:
        env["CHRONON_ROOT"] = os.getcwd()
    
    # Check if java is available
    try:
        result = subprocess.run(["java", "-version"], capture_output=True, text=True)
        if result.returncode != 0:
            raise FileNotFoundError("Java not found")
    except FileNotFoundError as e:
        raise RuntimeError(
            "Java (JDK) is not installed or not in PATH. Please install Java Development Kit (JDK) 11 or later.\n"
            "You can download it from: https://adoptium.net/ or https://www.oracle.com/java/technologies/downloads/"
        ) from e
    
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
        "-cp", eval_jar_path,
        "ai.chronon.integrations.cloud_gcp.EvalConfServer"
    ]
    
    print(f"Starting EvalConf gateway with JAR: {eval_jar_path}")
    
    try:
        # Create log files to capture output
        log_dir = Path.home() / ".chronon" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        stdout_log = log_dir / "eval_gateway_stdout.log"
        stderr_log = log_dir / "eval_gateway_stderr.log"
        
        print("Gateway logs will be written to:")
        print(f"  STDOUT: {stdout_log}")
        print(f"  STDERR: {stderr_log}")
        
        # Start the process in the background with logging
        with open(stdout_log, 'w') as stdout_file, open(stderr_log, 'w') as stderr_file:
            process = subprocess.Popen(
                java_cmd, 
                env=env, 
                stdout=stdout_file, 
                stderr=stderr_file,
                start_new_session=True  # This detaches the process
            )
            
        # Save PID to file for later cleanup
        pid_file = get_gateway_pid_file()
        pid_file.write_text(str(process.pid))
        print(f"Gateway process started with PID: {process.pid}")
        print(f"PID saved to: {pid_file}")
    except Exception as e:
        raise RuntimeError(f"Failed to start EvalConf gateway server: {str(e)}") from e


def connect_to_gateway(max_retries: int = 10) -> Tuple[JavaGateway, object]:
    """Connect to the EvalConf gateway server with retries."""
    for attempt in range(max_retries):
        try:
            gateway = JavaGateway()
            eval_conf = gateway.entry_point
            # Test the connection
            eval_conf.reset()
            print("Connected to new EvalConf gateway")
            return gateway, eval_conf
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Attempt {attempt + 1} failed, retrying in 2 seconds...")
                time.sleep(2)
            else:
                raise RuntimeError(f"Failed to connect to EvalConf gateway after {max_retries} attempts: {str(e)}") from e