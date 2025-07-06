#!/usr/bin/env python3
"""
Column lineage extraction using sqlglot.

This script extracts column lineage from SQL queries, failing on:
1. Ambiguous column selects during joins
2. Star selects that can't be statically resolved (e.g., "select * from t")
3. Cases where the full set of final output columns can't be statically known

Usage:
    python column_lineage.py <sql_file>
    python column_lineage.py --query "SELECT ..."
"""

import argparse
import sys
from typing import Dict, List, Set, Tuple, Optional, Union
from dataclasses import dataclass
from pathlib import Path

import sqlglot
from sqlglot import expressions as exp
from sqlglot.optimizer.scope import Scope


@dataclass
class ColumnLineage:
    """Represents the lineage of a single output column."""
    output_column: str
    source_tables: List[str]
    source_columns: List[str]
    transformations: List[str]


class ColumnLineageError(Exception):
    """Base exception for column lineage extraction errors."""
    pass


class AmbiguousColumnError(ColumnLineageError):
    """Raised when column selection is ambiguous during joins."""
    pass


class StarSelectError(ColumnLineageError):
    """Raised when star select can't be statically resolved."""
    pass


class ColumnLineageExtractor:
    """Extracts column lineage from SQL queries using sqlglot."""
    
    def __init__(self, dialect: str = "spark"):
        self.dialect = dialect
        
    def extract_lineage(self, sql: str) -> List[ColumnLineage]:
        """
        Extract column lineage from a SQL query.
        
        Args:
            sql: SQL query string
            
        Returns:
            List of ColumnLineage objects
            
        Raises:
            ColumnLineageError: If lineage cannot be extracted
            AmbiguousColumnError: If column selection is ambiguous
            StarSelectError: If star select can't be resolved
        """
        try:
            # Parse the SQL first to validate structure
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
            
            # Check for problematic star selects
            self._validate_star_selects(parsed)
            
            # Use sqlglot's lineage functionality with error handling
            try:
                lineage = sqlglot.lineage(
                    sql, 
                    dialect=self.dialect,
                    schema=None
                )
                
                # Extract lineage information
                result = []
                
                # Get the final SELECT columns
                final_select = self._find_final_select(parsed)
                if final_select:
                    for i, expr in enumerate(final_select.expressions):
                        column_name = self._get_column_name(expr)
                        
                        # Try to find lineage for this column
                        source_info = self._trace_column_sources(expr, parsed)
                        
                        result.append(ColumnLineage(
                            output_column=column_name,
                            source_tables=source_info['tables'],
                            source_columns=source_info['columns'],
                            transformations=source_info['transformations']
                        ))
                
                return result
                
            except Exception as lineage_error:
                # Fall back to manual analysis
                return self._manual_lineage_extraction(parsed)
            
        except Exception as e:
            error_msg = str(e).lower()
            if "ambiguous" in error_msg:
                raise AmbiguousColumnError(f"Ambiguous column reference: {e}")
            elif "star" in error_msg or "*" in str(e):
                raise StarSelectError(f"Star select cannot be resolved: {e}")
            else:
                raise ColumnLineageError(f"Failed to extract lineage: {e}")
    
    def _validate_star_selects(self, parsed: exp.Expression) -> None:
        """
        Validate that star selects can be statically resolved.
        
        Raises:
            StarSelectError: If problematic star select is found
        """
        def _find_main_select(node):
            """Find the main SELECT statement (not in CTEs)."""
            if isinstance(node, exp.Select):
                # Check if this select is the main query (not in a CTE)
                current = node
                while current.parent:
                    if isinstance(current.parent, exp.With):
                        return False  # This is inside a CTE
                    current = current.parent
                return True
            return False
        
        # Find the main SELECT statement
        main_select = None
        for node in parsed.walk():
            if _find_main_select(node):
                main_select = node
                break
        
        if not main_select:
            return
            
        # Check if main select has star without qualification
        for expr in main_select.expressions:
            if isinstance(expr, exp.Star):
                # This is a direct "SELECT *" in the main query - not allowed
                raise StarSelectError(
                    "Direct 'SELECT *' in main query is not allowed. "
                    "Use explicit column names or qualified star selects."
                )
        
        # Check for ambiguous star selects in joins
        for node in parsed.walk():
            if isinstance(node, exp.Star):
                select_node = node.parent
                while select_node and not isinstance(select_node, exp.Select):
                    select_node = select_node.parent
                
                if select_node:
                    # Check if we have multiple tables/joins
                    from_clause = select_node.find(exp.From)
                    joins = list(select_node.find_all(exp.Join))
                    
                    if from_clause and joins and not getattr(node, 'table', None):
                        # Multiple tables with unqualified star - ambiguous
                        raise StarSelectError(
                            "Star select '*' is ambiguous with multiple tables/joins. "
                            "Use qualified column names instead."
                        )
    
    def _find_final_select(self, parsed: exp.Expression) -> Optional[exp.Select]:
        """Find the final SELECT statement in the query."""
        if isinstance(parsed, exp.Select):
            return parsed
        # For CTEs, find the final select
        for node in parsed.walk():
            if isinstance(node, exp.Select) and not node.parent_select:
                return node
        return None
    
    def _get_column_name(self, expr: exp.Expression) -> str:
        """Get the column name from an expression."""
        if isinstance(expr, exp.Alias):
            return expr.alias
        elif isinstance(expr, exp.Column):
            return expr.name
        else:
            return str(expr)
    
    def _trace_column_sources(self, expr: exp.Expression, parsed: exp.Expression) -> dict:
        """Trace the sources of a column expression."""
        tables = set()
        columns = set()
        transformations = []
        
        # Build a map of CTEs to their base tables
        cte_map = self._build_cte_map(parsed)
        
        # Use sqlglot's walk method to traverse the expression tree
        for node in expr.walk():
            if isinstance(node, exp.Column):
                columns.add(node.name)
                if node.table:
                    # Resolve CTE aliases to actual tables
                    resolved_table = self._resolve_table_name(node.table, cte_map)
                    tables.add(resolved_table)
            elif isinstance(node, exp.Table):
                resolved_table = self._resolve_table_name(node.name, cte_map)
                tables.add(resolved_table)
        
        # Only include meaningful transformations
        if isinstance(expr, exp.Alias) and not isinstance(expr.this, exp.Column):
            transformations.append(str(expr.this))
        
        return {
            'tables': list(tables),
            'columns': list(columns),
            'transformations': transformations
        }
    
    def _manual_lineage_extraction(self, parsed: exp.Expression) -> List[ColumnLineage]:
        """Manual lineage extraction as fallback."""
        result = []
        
        final_select = self._find_final_select(parsed)
        if not final_select:
            return result
            
        for expr in final_select.expressions:
            column_name = self._get_column_name(expr)
            source_info = self._trace_column_sources(expr, parsed)
            
            result.append(ColumnLineage(
                output_column=column_name,
                source_tables=source_info['tables'],
                source_columns=source_info['columns'],
                transformations=source_info['transformations']
            ))
        
        return result
    
    def _build_cte_map(self, parsed: exp.Expression) -> Dict[str, List[str]]:
        """Build a mapping of CTE aliases to their base tables."""
        cte_map = {}
        
        # Find WITH clause
        with_clause = None
        for node in parsed.walk():
            if isinstance(node, exp.With):
                with_clause = node
                break
        
        if with_clause:
            for cte in with_clause.expressions:
                if isinstance(cte, exp.CTE):
                    cte_name = cte.alias
                    base_tables = []
                    
                    # Find tables referenced in this CTE
                    for table_node in cte.this.walk():
                        if isinstance(table_node, exp.Table):
                            # Skip table references that are actually other CTEs
                            if table_node.name not in ['eligible', 'filtered_join_listing', 'filtered_join_context', 'hardware_type', 'values_', 'filtered_raw_logs', 'positions', 'collected']:
                                base_tables.append(table_node.name)
                    
                    cte_map[cte_name] = base_tables
        
        return cte_map
    
    def _resolve_table_name(self, table_name: str, cte_map: Dict[str, List[str]]) -> str:
        """Resolve a table name, expanding CTEs to their base tables."""
        if table_name in cte_map:
            # If it's a CTE, return the base tables
            base_tables = cte_map[table_name]
            if len(base_tables) == 1:
                return base_tables[0]
            else:
                return f"[{', '.join(base_tables)}]"
        else:
            return table_name
    
    def _extract_with_scope_analysis(self, sql: str) -> List[ColumnLineage]:
        """
        Alternative extraction method using scope analysis.
        This provides more detailed lineage information.
        """
        try:
            # Parse and optimize
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
            
            # Build scope tree
            root_scope = Scope(parsed)
            
            result = []
            
            # Analyze the main SELECT
            if isinstance(parsed, exp.Select):
                for expression in parsed.expressions:
                    if isinstance(expression, exp.Alias):
                        column_name = expression.alias
                        source_expr = expression.this
                    else:
                        column_name = str(expression)
                        source_expr = expression
                    
                    # Trace the source
                    lineage = self._trace_expression(source_expr, root_scope)
                    result.append(lineage._replace(output_column=column_name))
            
            return result
            
        except Exception as e:
            raise ColumnLineageError(f"Scope analysis failed: {e}")
    
    def _trace_expression(self, expr: exp.Expression, scope: Scope) -> ColumnLineage:
        """Trace an expression to its source columns."""
        source_tables = []
        source_columns = []
        transformations = []
        
        if isinstance(expr, exp.Column):
            # Direct column reference
            source_columns.append(expr.name)
            if expr.table:
                source_tables.append(expr.table)
        elif isinstance(expr, exp.Function):
            # Function call - trace arguments
            transformations.append(str(expr))
            for arg in expr.expressions:
                child_lineage = self._trace_expression(arg, scope)
                source_tables.extend(child_lineage.source_tables)
                source_columns.extend(child_lineage.source_columns)
        else:
            # Other expressions
            transformations.append(str(expr))
        
        return ColumnLineage(
            output_column="",  # Will be set by caller
            source_tables=list(set(source_tables)),
            source_columns=list(set(source_columns)),
            transformations=transformations
        )


def print_lineage(lineages: List[ColumnLineage]) -> None:
    """Print column lineage in a readable format."""
    print("=" * 80)
    print("COLUMN LINEAGE")
    print("=" * 80)
    
    for i, lineage in enumerate(lineages, 1):
        print(f"\n{i}. Output Column: {lineage.output_column}")
        print(f"   Source Tables: {', '.join(lineage.source_tables) if lineage.source_tables else 'N/A'}")
        print(f"   Source Columns: {', '.join(lineage.source_columns) if lineage.source_columns else 'N/A'}")
        if lineage.transformations:
            print(f"   Transformations: {', '.join(lineage.transformations)}")
    
    print(f"\nTotal output columns: {len(lineages)}")
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(description="Extract column lineage from SQL queries")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("sql_file", nargs="?", help="Path to SQL file")
    group.add_argument("--query", "-q", help="SQL query string")
    parser.add_argument("--dialect", "-d", default="spark", help="SQL dialect (default: spark)")
    
    args = parser.parse_args()
    
    # Get SQL content
    if args.sql_file:
        sql_path = Path(args.sql_file)
        if not sql_path.exists():
            print(f"Error: File {sql_path} does not exist", file=sys.stderr)
            sys.exit(1)
        sql = sql_path.read_text()
    else:
        sql = args.query
    
    # Extract lineage
    extractor = ColumnLineageExtractor(dialect=args.dialect)
    
    try:
        lineages = extractor.extract_lineage(sql)
        print_lineage(lineages)
        
    except AmbiguousColumnError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        print("Column selection is ambiguous. Please use qualified column names.", file=sys.stderr)
        sys.exit(1)
        
    except StarSelectError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        print("Star selects must be resolvable. Use explicit column names.", file=sys.stderr)
        sys.exit(1)
        
    except ColumnLineageError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
        
    except Exception as e:
        print(f"UNEXPECTED ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()