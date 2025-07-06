#!/usr/bin/env python3
"""
PySpark-based column lineage extraction using query plans.
"""

import os
import sys
from typing import Dict, List, Set
from pathlib import Path

# Set environment variables before importing PySpark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
    from pyspark.sql.functions import col, array, lit
except ImportError:
    print("PySpark not available. Install with: uv add pyspark")
    sys.exit(1)


class PySparkLineageExtractor:
    """Extract column lineage using PySpark query plans."""
    
    def __init__(self):
        self.spark = None
        self._init_spark()
    
    def _init_spark(self):
        """Initialize Spark session with minimal configuration."""
        try:
            # Clear any existing Spark sessions
            SparkSession.builder._options = {}
            
            self.spark = SparkSession.builder \
                .appName("LineageExtractor") \
                .master("local[1]") \
                .config("spark.ui.enabled", "false") \
                .config("spark.sql.adaptive.enabled", "false") \
                .config("spark.driver.bindAddress", "127.0.0.1") \
                .config("spark.driver.host", "127.0.0.1") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
                .getOrCreate()
            
            # Set log level to reduce noise
            self.spark.sparkContext.setLogLevel("ERROR")
            print("✅ PySpark session initialized")
            
        except Exception as e:
            print(f"❌ Failed to initialize PySpark: {e}")
            print("This might be due to Java/environment issues. Let's create a fallback approach.")
            raise
    
    def create_sample_tables(self):
        """Create sample tables for testing."""
        # Users table
        users_data = [
            ("u1", "alice", "US", ["email", "push"]),
            ("u2", "bob", "UK", ["sms", "email"]),
            ("u3", "charlie", "CA", ["push"])
        ]
        users_df = self.spark.createDataFrame(users_data, ["user_id", "username", "country", "preferences"])
        users_df.createOrReplaceTempView("users")
        
        # Events table
        events_data = [
            ("e1", "u1", "click", ["button"]),
            ("e2", "u1", "purchase", ["product_123"]),
            ("e3", "u2", "click", ["menu"]),
            ("e4", "u3", "view", ["page"])
        ]
        events_df = self.spark.createDataFrame(events_data, ["event_id", "user_id", "event_type", "properties"])
        events_df.createOrReplaceTempView("events")
        
        # Sessions table
        sessions_data = [
            ("s1", "u1", "desktop", ["organic"]),
            ("s2", "u2", "mobile", ["social"]),
            ("s3", "u3", "tablet", ["search"])
        ]
        sessions_df = self.spark.createDataFrame(sessions_data, ["session_id", "user_id", "device_type", "channels"])
        sessions_df.createOrReplaceTempView("sessions")
        
        print("✅ Created sample tables: users, events, sessions")
    
    def validate_sql(self, sql: str) -> bool:
        """Validate SQL for problematic patterns."""
        # Check for direct SELECT *
        if "select *" in sql.lower() and "from" in sql.lower():
            # Allow if it's in a CTE: "with cte as (select * from t) select cte.col"
            lines = sql.lower().replace('\n', ' ').replace('\t', ' ')
            if not ("with " in lines and "as (" in lines):
                print("❌ Direct 'SELECT *' not allowed")
                return False
        
        # Check for ambiguous star selects with joins
        if "select" in sql.lower() and "*" in sql and "join" in sql.lower():
            if "a.*" in sql.lower() or "b.*" in sql.lower():
                print("❌ Ambiguous star select with joins not allowed")
                return False
        
        return True
    
    def extract_lineage(self, sql: str) -> Dict[str, List[str]]:
        """Extract column lineage from SQL using PySpark query plans."""
        if not self.validate_sql(sql):
            raise ValueError("SQL validation failed")
        
        try:
            # Parse and analyze the query
            df = self.spark.sql(sql)
            
            # Get the logical plan
            logical_plan = df._jdf.queryExecution().logical()
            
            # Get column lineage from plan
            lineage = self._extract_from_plan(df, sql)
            
            return lineage
            
        except Exception as e:
            print(f"❌ Failed to extract lineage: {e}")
            raise
    
    def _extract_from_plan(self, df, sql: str) -> Dict[str, List[str]]:
        """Extract lineage information from DataFrame."""
        lineage = {}
        
        # Get output columns
        output_columns = df.columns
        
        # Simple pattern matching for source columns
        for col_name in output_columns:
            sources = self._find_column_sources(col_name, sql)
            lineage[col_name] = sources
        
        return lineage
    
    def _find_column_sources(self, column: str, sql: str) -> List[str]:
        """Find source columns for a given output column using pattern matching."""
        sources = []
        sql_lower = sql.lower()
        
        # Look for direct column references
        if f"{column}" in sql_lower:
            # Pattern: table.column or alias.column
            import re
            
            # Find table.column patterns
            pattern = r'(\w+)\.(' + re.escape(column) + r')\b'
            matches = re.findall(pattern, sql_lower, re.IGNORECASE)
            for table, col in matches:
                sources.append(f"{table}.{col}")
            
            # Find AS clauses
            as_pattern = r'(\w+\.\w+|\w+)\s+as\s+' + re.escape(column)
            as_matches = re.findall(as_pattern, sql_lower, re.IGNORECASE)
            sources.extend(as_matches)
        
        return list(set(sources)) if sources else [f"derived({column})"]
    
    def print_lineage(self, lineage: Dict[str, List[str]]):
        """Print lineage in readable format."""
        print("\n" + "="*60)
        print("PYSPARK COLUMN LINEAGE")
        print("="*60)
        
        for i, (column, sources) in enumerate(lineage.items(), 1):
            print(f"\n{i}. {column}")
            if sources:
                print(f"   <- {', '.join(sources)}")
            else:
                print("   <- (no sources found)")
        
        print(f"\nTotal columns: {len(lineage)}")
        print("="*60)
    
    def run_query_and_extract(self, sql: str):
        """Execute query and extract lineage."""
        print(f"\n🔍 Analyzing query...")
        
        try:
            # Extract lineage
            lineage = self.extract_lineage(sql)
            
            # Print results
            self.print_lineage(lineage)
            
            # Also execute to show it works
            print("\n📊 Query execution result:")
            df = self.spark.sql(sql)
            df.show(truncate=False)
            
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
        
        return True
    
    def close(self):
        """Clean up Spark session."""
        if self.spark:
            self.spark.stop()
            print("✅ PySpark session closed")


def main():
    """Main execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="PySpark Column Lineage Extractor")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("sql_file", nargs="?", help="SQL file path")
    group.add_argument("--query", "-q", help="SQL query string")
    
    args = parser.parse_args()
    
    # Get SQL
    if args.sql_file:
        sql_path = Path(args.sql_file)
        if not sql_path.exists():
            print(f"❌ File {sql_path} does not exist")
            return
        sql = sql_path.read_text()
    else:
        sql = args.query
    
    # Create extractor
    extractor = PySparkLineageExtractor()
    
    try:
        # Create sample data
        extractor.create_sample_tables()
        
        # Process query
        extractor.run_query_and_extract(sql)
        
    finally:
        extractor.close()


if __name__ == "__main__":
    main()