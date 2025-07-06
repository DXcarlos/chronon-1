#!/usr/bin/env python3
"""
Simple column lineage extraction using sqlglot.
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Set

import sqlglot
from sqlglot import expressions as exp
from sqlglot.lineage import lineage


class LineageExtractor:
    def __init__(self, dialect: str = "spark"):
        self.dialect = dialect
        
    def extract_lineage(self, sql: str) -> Dict[str, Dict]:
        """Extract column lineage from SQL query."""
        try:
            # Parse the SQL
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
            
            # Validate star selects
            self._validate_star_selects(parsed)
            
            # Find the final SELECT to get output columns
            final_select = self._find_main_select(parsed)
            if not final_select:
                return {}
            
            # Extract lineage for each output column
            result = {}
            for expr in final_select.expressions:
                if isinstance(expr, exp.Alias):
                    column_name = expr.alias
                elif isinstance(expr, exp.Column):
                    column_name = expr.name
                else:
                    column_name = str(expr)
                
                try:
                    # Get lineage for this column
                    lineage_node = lineage(column_name, sql, dialect=self.dialect)
                    
                    # Extract upstream dependencies
                    upstream = []
                    def collect_upstream(node):
                        if hasattr(node, 'upstream') and node.upstream:
                            for upstream_node in node.upstream:
                                if hasattr(upstream_node, 'column') and upstream_node.column:
                                    table_col = f"{upstream_node.table}.{upstream_node.column}" if upstream_node.table else upstream_node.column
                                    upstream.append(table_col)
                                collect_upstream(upstream_node)
                        elif hasattr(node, 'source') and node.source:
                            # Alternative attribute name
                            upstream.append(str(node.source))
                    
                    collect_upstream(lineage_node)
                    
                    result[column_name] = {
                        'upstream': list(set(upstream)),
                        'expression': str(expr)
                    }
                    
                except Exception as e:
                    # If lineage fails for this column, still include it
                    result[column_name] = {
                        'upstream': [],
                        'expression': str(expr),
                        'error': str(e)
                    }
            
            return result
            
        except Exception as e:
            error_msg = str(e).lower()
            if "star" in error_msg or "*" in str(e):
                raise ValueError(f"Star select error: {e}")
            elif "ambiguous" in error_msg:
                raise ValueError(f"Ambiguous column: {e}")
            else:
                raise ValueError(f"Parsing error: {e}")
    
    def _validate_star_selects(self, parsed: exp.Expression) -> None:
        """Validate star selects according to requirements."""
        main_select = self._find_main_select(parsed)
        if not main_select:
            return
            
        # Check for bare SELECT * in main query
        for expr in main_select.expressions:
            if isinstance(expr, exp.Star):
                raise ValueError("Direct 'SELECT *' not allowed")
        
        # Check for ambiguous star selects with joins
        for node in parsed.walk():
            if isinstance(node, exp.Star):
                select_node = node.parent
                while select_node and not isinstance(select_node, exp.Select):
                    select_node = select_node.parent
                
                if select_node:
                    from_clause = select_node.find(exp.From)
                    joins = list(select_node.find_all(exp.Join))
                    
                    if from_clause and joins and not getattr(node, 'table', None):
                        raise ValueError("Ambiguous star select with joins")
    
    def _find_main_select(self, parsed: exp.Expression) -> exp.Select:
        """Find the main SELECT (not in CTEs)."""
        for node in parsed.walk():
            if isinstance(node, exp.Select):
                current = node
                while current.parent:
                    if isinstance(current.parent, exp.With):
                        break
                    current = current.parent
                else:
                    return node
        return None


def print_lineage(lineage: Dict[str, Dict]) -> None:
    """Print lineage in readable format."""
    print("=" * 60)
    print("COLUMN LINEAGE")
    print("=" * 60)
    
    for i, (column, info) in enumerate(lineage.items(), 1):
        print(f"\n{i}. {column}")
        if info['upstream']:
            print(f"   <- {', '.join(info['upstream'])}")
        else:
            print("   <- (no upstream dependencies)")
    
    print(f"\nTotal columns: {len(lineage)}")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Extract column lineage")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("sql_file", nargs="?", help="SQL file path")
    group.add_argument("--query", "-q", help="SQL query string")
    
    args = parser.parse_args()
    
    if args.sql_file:
        sql_path = Path(args.sql_file)
        if not sql_path.exists():
            print(f"Error: File {sql_path} does not exist", file=sys.stderr)
            sys.exit(1)
        sql = sql_path.read_text()
    else:
        sql = args.query
    
    try:
        extractor = LineageExtractor()
        lineage = extractor.extract_lineage(sql)
        print_lineage(lineage)
        
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"UNEXPECTED ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()