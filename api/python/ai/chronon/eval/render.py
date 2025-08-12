#!/usr/bin/env python3
"""
Rendering utilities for Chronon evaluation results.

This module provides functions to render evaluation results in a human-readable format.
"""

from typing import Dict, Optional


def get_overall_check_status(eval_obj) -> str:
    """Extract the overall check status from any evaluation object."""
    try:
        # All evaluation results now have overallCheck - use that consistently
        overall_check = eval_obj.getOverallCheck()
        if overall_check:
            return overall_check.getCheckResult().name()
        else:
            return 'UNKNOWN'
    except Exception:
        return 'SUCCESS'


def get_eval_name(eval_obj) -> str:
    """Extract the name from any evaluation object."""
    try:
        if hasattr(eval_obj, 'getName'):
            return eval_obj.getName() or 'Unknown'
        if hasattr(eval_obj, 'getTableName'):
            return eval_obj.getTableName() or 'Unknown'
        return 'Unknown'
    except Exception:
        return 'Unknown'


def get_parent_evals(eval_obj) -> list:
    """Extract parent evaluations from any evaluation object."""
    try:
        parent_evals = []
        
        # Get standard parent evaluations (left side dependencies, etc.)
        if hasattr(eval_obj, 'getParentEvals'):
            standard_parents = eval_obj.getParentEvals()
            if standard_parents:
                parent_evals.extend(list(standard_parents))
        
        # For JoinEvalResult, also include GroupBy evaluations from join parts as additional parents
        # This shows the right-side GroupBy dependencies in the lineage tree
        try:
            join_parts = eval_obj.getJoinPartChecks()
            if join_parts:
                for part in join_parts:
                    try:
                        gb_result = part.getGbEvalResult()
                        if gb_result:
                            # Create an EvalUnion wrapper for the GroupBy result
                            gb_eval_union = create_eval_union_for_groupby(gb_result)
                            parent_evals.append(gb_eval_union)
                    except Exception:
                        pass  # Skip if we can't get the GroupBy result
        except Exception:
            # Object doesn't have getJoinPartChecks (e.g., GroupBy, StagingQuery objects)
            pass
        
        return parent_evals
    except Exception:
        return []


def create_eval_union_for_groupby(gb_result):
    """Create a mock EvalUnion object that wraps a GroupBy result for lineage traversal."""
    class MockEvalUnion:
        def __init__(self, gb_eval):
            self.gb_eval = gb_eval
        
        def getGroupByEval(self):
            return self.gb_eval
        
        def getJoinEval(self):
            return None
        
        def getStagingQueryEval(self):
            return None
        
        def getTableDepEval(self):
            return None
    
    return MockEvalUnion(gb_result)


def extract_eval_from_union(eval_union) -> tuple:
    """Extract the actual evaluation object and its type from EvalUnion."""
    try:
        if hasattr(eval_union, 'getJoinEval') and eval_union.getJoinEval():
            return eval_union.getJoinEval(), 'Join'
        elif hasattr(eval_union, 'getGroupByEval') and eval_union.getGroupByEval():
            return eval_union.getGroupByEval(), 'GroupBy'
        elif hasattr(eval_union, 'getStagingQueryEval') and eval_union.getStagingQueryEval():
            return eval_union.getStagingQueryEval(), 'StagingQuery'
        elif hasattr(eval_union, 'getTableDepEval') and eval_union.getTableDepEval():
            return eval_union.getTableDepEval(), 'Table'
        return None, 'Unknown'
    except Exception:
        return None, 'Unknown'


def render_lineage_tree(root_eval, prefix="", is_last=True) -> list:
    """
    Render the lineage tree recursively.
    Returns a list of formatted lines.
    """
    lines = []
    
    # Get name and status for current node
    name = get_eval_name(root_eval)
    status = get_overall_check_status(root_eval)
    status_symbol = render_check_result(status)
    
    # Create the current line
    node_line = f"{prefix}{status_symbol} {name}"
    lines.append(node_line)
    
    # Get parent evaluations
    parent_evals = get_parent_evals(root_eval)
    
    if parent_evals:
        for i, parent_union in enumerate(parent_evals):
            is_last_parent = (i == len(parent_evals) - 1)
            
            # Extract actual evaluation object from union
            parent_eval, eval_type = extract_eval_from_union(parent_union)
            
            if parent_eval:
                # Add tree branch character
                if is_last_parent:
                    branch_prefix = prefix + "└── "
                    child_prefix = prefix + "    "  # Empty space for last child
                else:
                    branch_prefix = prefix + "├── "
                    child_prefix = prefix + "│   "  # Vertical line for continuing children
                
                # Recursively render child - will now show full tree
                child_lines = render_lineage_tree(parent_eval, child_prefix, is_last_parent)
                # Override the first line with the correct branch prefix
                if child_lines:
                    child_lines[0] = child_lines[0].replace(child_prefix, branch_prefix, 1)
                lines.extend(child_lines)
    
    return lines


def collect_all_parent_unions(eval_obj, all_unions=None) -> list:
    """Recursively collect all parent evaluation unions in the lineage."""
    if all_unions is None:
        all_unions = []
    
    try:
        parent_evals = get_parent_evals(eval_obj)
        for parent_union in parent_evals:
            all_unions.append(parent_union)
            parent_eval, eval_type = extract_eval_from_union(parent_union)
            if parent_eval:
                collect_all_parent_unions(parent_eval, all_unions)
    except Exception:
        pass  # Skip any errors during collection
    
    return all_unions


def render_upstream_failures(result) -> str:
    """Render concise upstream failures summary."""
    try:
        # Get all parent unions
        all_unions = collect_all_parent_unions(result)
        
        # Filter to failed ones and dedupe by name
        failed_evals = []
        seen_names = set()
        
        for union in all_unions:
            parent_eval, eval_type = extract_eval_from_union(union)
            if parent_eval:
                status = get_overall_check_status(parent_eval)
                name = get_eval_name(parent_eval)
                if status == "FAILURE" and name not in seen_names:
                    failed_evals.append((parent_eval, eval_type))
                    seen_names.add(name)
        
        if not failed_evals:
            return ""  # No failures, don't show the section
        
        lines = ["⚠️ ======== UPSTREAM FAILURES ======== ⚠️", ""]
        
        for failed_eval, eval_type in failed_evals:
            try:
                name = get_eval_name(failed_eval)
                overall_check = failed_eval.getOverallCheck()
                message = overall_check.getMessage() if overall_check else ''
                
                # Show the full error message for upstream failures
                if message:
                    lines.append(f"❌ {name} ({eval_type}): {message}")
                else:
                    lines.append(f"❌ {name} ({eval_type})")
                
            except Exception as e:
                lines.append(f"❌ Error reading failure: {str(e)}")
        
        lines.append("")  # Empty line after failures
        return '\n'.join(lines)
    except Exception as e:
        return f"❌ Error collecting upstream failures: {str(e)}\n\n"


def render_lineage_analysis(result) -> str:
    """Render the lineage analysis section with ASCII tree."""
    lines = ["📊 ======== LINEAGE ANALYSIS ======== 📊", ""]
    
    try:
        tree_lines = render_lineage_tree(result)
        lines.extend(tree_lines)
        lines.append("")  # Empty line after tree
    except Exception as e:
        lines.append(f"❌ Error rendering lineage: {str(e)}")
        lines.append("")
    
    return '\n'.join(lines)


def render_common_sections(result) -> str:
    """Render the common sections (upstream failures + lineage) that appear in all evaluation types."""
    sections = []
    
    # Upstream Failures
    upstream_failures = render_upstream_failures(result)
    if upstream_failures:
        sections.append(upstream_failures)
    
    # Lineage Analysis
    lineage_section = render_lineage_analysis(result)
    sections.append(lineage_section)
    
    return '\n'.join(sections)


def render_final_lineage(result) -> str:
    """Render lineage analysis at the end for final reference."""
    return render_lineage_analysis(result)


def render_check_result(check_result: str) -> str:
    """Convert check result to emoji format."""
    if check_result == "SUCCESS":
        return "✅"
    elif check_result == "FAILURE":
        return "❌"
    elif check_result == "SKIPPED":
        return "⏭️"
    else:
        return "❓"


def render_schema_dict(schema_dict: Optional[Dict[str, str]], indent: int = 0) -> str:
    """Render a schema dictionary with proper indentation."""
    if not schema_dict:
        return f"{'  ' * indent}(none)"
    
    lines = []
    for field_name, field_type in schema_dict.items():
        lines.append(f"{'  ' * indent}{field_name}: {field_type}")
    return '\n'.join(lines)


def render_staging_query_result(result) -> str:
    """Render a StagingQueryEvalResult in human-readable format."""
    lines = []
    
    # Handle error case
    if isinstance(result, dict) and 'error' in result:
        return f"❌ Error: {result['error']}"
    
    # Name
    try:
        name = result.getName()
    except Exception:
        name = 'Unknown'
    
    # Section header
    lines.append(f"📋 ======== EVAL SUMMARY FOR {name} ======== 📋")
    lines.append("")
    
    # Query check
    try:
        overall_check = result.getOverallCheck()
        if overall_check:
            check_result = overall_check.getCheckResult().name()
            message = overall_check.getMessage() or ''
            
            status_line = f"{render_check_result(check_result)} Query"
            if message and check_result != "SUCCESS":
                status_line += f": {message}"
            lines.append(status_line)
        else:
            lines.append("❓ Query status unknown")
    except Exception as e:
        lines.append(f"❌ Error reading query check: {str(e)}")
    
    lines.append("")
    
    # Output Schema (only show if present)
    try:
        output_schema = result.getOutputSchema()
        if output_schema:
            output_schema_dict = dict(output_schema)
            lines.append("📊 Output Schema:")
            lines.append(render_schema_dict(output_schema_dict, indent=1))
            lines.append("")
    except Exception as e:
        lines.append(f"❌ Error reading output schema: {str(e)}")
        lines.append("")
    
    # Common sections (upstream failures + lineage) after summary
    common_sections = render_common_sections(result)
    lines.append(common_sections)
    
    return '\n'.join(lines)


def render_group_by_result(result) -> str:
    """Render a GroupByEvalResult in human-readable format."""
    lines = []
    
    # Handle error case
    if isinstance(result, dict) and 'error' in result:
        return f"❌ Error: {result['error']}"
    
    # Name
    try:
        name = result.getName()
    except Exception:
        name = 'Unknown'
    
    # Section header
    lines.append(f"📋 ======== EVAL SUMMARY FOR {name} ======== 📋")
    lines.append("")
    
    # Source check
    try:
        source_check = result.getSourceExpressionCheck()
        source_status = source_check.getCheckResult().name()
        source_message = source_check.getMessage() or ''
        
        source_line = f"{render_check_result(source_status)} Source"
        if source_message and source_status != "SUCCESS":
            source_line += f": {source_message}"
        lines.append(source_line)
    except Exception as e:
        lines.append(f"❌ Error reading source check: {str(e)}")
    
    # Aggregation check
    try:
        agg_check = result.getAggExpressionCheck()
        agg_status = agg_check.getCheckResult().name()
        agg_message = agg_check.getMessage() or ''
        
        agg_line = f"{render_check_result(agg_status)} Aggregations"
        if agg_message and agg_status != "SUCCESS":
            agg_line += f": {agg_message}"
        lines.append(agg_line)
    except Exception as e:
        lines.append(f"❌ Error reading aggregation check: {str(e)}")
    
    # Derivations check (only if present)
    try:
        deriv_check = result.getDerivationsExpressionCheck()
        if deriv_check:
            deriv_status = deriv_check.getCheckResult().name()
            deriv_message = deriv_check.getMessage() or ''
            
            deriv_line = f"{render_check_result(deriv_status)} Derivations"
            if deriv_message and deriv_status != "SUCCESS":
                deriv_line += f": {deriv_message}"
            lines.append(deriv_line)
    except Exception:
        pass  # Derivations are optional
    
    lines.append("")
    
    # Schemas - convert Java Map to Python dict for rendering
    try:
        key_schema = result.getKeySchema()
        if key_schema:
            key_schema_dict = dict(key_schema)
            lines.append("🔑 Key Schema:")
            lines.append(render_schema_dict(key_schema_dict, indent=1))
            lines.append("")
    except Exception as e:
        lines.append(f"❌ Error reading key schema: {str(e)}")
    
    try:
        agg_schema = result.getAggSchema()
        if agg_schema:
            agg_schema_dict = dict(agg_schema)
            lines.append("📊 Aggregation Schema:")
            lines.append(render_schema_dict(agg_schema_dict, indent=1))
            lines.append("")
    except Exception as e:
        lines.append(f"❌ Error reading aggregation schema: {str(e)}")
    
    try:
        deriv_schema = result.getDerivationsSchema()
        if deriv_schema:
            deriv_schema_dict = dict(deriv_schema)
            lines.append("🔄 Post-Derivations Schema:")
            lines.append(render_schema_dict(deriv_schema_dict, indent=1))
            lines.append("")
    except Exception:
        pass  # Derivations schema is optional
    
    # Common sections (upstream failures + lineage) after summary
    common_sections = render_common_sections(result)
    lines.append(common_sections)
    
    return '\n'.join(lines)


def render_join_result(result) -> str:
    """Render a JoinEvalResult in human-readable format."""
    lines = []
    
    # Handle error case
    if isinstance(result, dict) and 'error' in result:
        return f"❌ Error: {result['error']}"
    
    # Name
    try:
        name = result.getName()
    except Exception:
        name = 'Unknown'
    
    # Section header
    lines.append(f"📋 ======== EVAL SUMMARY FOR {name} ======== 📋")
    lines.append("")
    
    # Left expression check
    try:
        left_check = result.getLeftExpressionCheck()
        left_status = left_check.getCheckResult().name()
        left_message = left_check.getMessage() or ''
        
        status_symbol = render_check_result(left_status)
        left_line = f"{status_symbol} Left side query"
        if left_message and left_status != "SUCCESS":
            left_line += f": {left_message}"
        lines.append(left_line)
    except Exception as e:
        lines.append(f"❌ Error reading left check: {str(e)}")
    
    lines.append("")
    
    # Join parts
    try:
        join_parts = result.getJoinPartChecks()
        if join_parts:
            lines.append("🔗 Join Parts:")
            for part in join_parts:
                try:
                    part_name = part.getPartName()
                    lines.append(f"  📦 {part_name}")
                    
                    # GroupBy eval result
                    gb_result = part.getGbEvalResult()
                    gb_overall = gb_result.getOverallCheck()
                    gb_status = gb_overall.getCheckResult().name()
                    gb_message = gb_overall.getMessage() or ''
                    
                    gb_line = f"    {render_check_result(gb_status)} GroupBy Eval"
                    if gb_message and gb_status != "SUCCESS":
                        gb_line += f": {gb_message}"
                    lines.append(gb_line)
                    
                    # Key schema check (only show if not skipped)
                    key_check = part.getKeySchemaCheck()
                    key_status = key_check.getCheckResult().name()
                    
                    if key_status != "SKIPPED":
                        key_message = key_check.getMessage() or ''
                        key_line = f"    {render_check_result(key_status)} Key schema check"
                        if key_message and key_status != "SUCCESS":
                            key_line += f": {key_message}"
                        lines.append(key_line)
                    lines.append("")
                except Exception as e:
                    lines.append(f"    ❌ Error reading join part: {str(e)}")
                    lines.append("")
        else:
            lines.append("🔗 Join Parts: None")
            lines.append("")
    except Exception as e:
        lines.append(f"❌ Error reading join parts: {str(e)}")
        lines.append("")
    
    # Derivations (only if present and not null)
    try:
        deriv_check = result.getDerivationValidityCheck()
        if deriv_check:
            deriv_status = deriv_check.getCheckResult().name()
            deriv_message = deriv_check.getMessage() or ''
            
            deriv_line = f"{render_check_result(deriv_status)} Derivations"
            if deriv_message and deriv_status != "SUCCESS":
                deriv_line += f": {deriv_message}"
            lines.append(deriv_line)
            lines.append("")
    except Exception:
        pass  # Derivations are optional
    
    # Output schemas
    lines.append("📊 Output Schema:")
    
    # Left schema
    try:
        left_schema = result.getLeftQuerySchema()
        if left_schema:
            left_schema_dict = dict(left_schema)
            lines.append("  🔵 Left Schema:")
            lines.append(render_schema_dict(left_schema_dict, indent=2))
            lines.append("")
    except Exception as e:
        lines.append(f"  ❌ Error reading left schema: {str(e)}")
        lines.append("")
    
    # Right schema
    try:
        right_schema = result.getRightPartsSchema()
        if right_schema:
            right_schema_dict = dict(right_schema)
            lines.append("  🟢 Right Schema:")
            lines.append(render_schema_dict(right_schema_dict, indent=2))
            lines.append("")
    except Exception as e:
        lines.append(f"  ❌ Error reading right schema: {str(e)}")
        lines.append("")
    
    # Post-derivations schema (only if present)
    try:
        post_deriv_schema = result.getDerivationsSchema()
        if post_deriv_schema:
            post_deriv_schema_dict = dict(post_deriv_schema)
            lines.append("  🔄 Post-Derivations Schema:")
            lines.append(render_schema_dict(post_deriv_schema_dict, indent=2))
            lines.append("")
    except Exception:
        pass  # Post-derivations schema is optional
    
    # External parts schema (only if present)
    try:
        external_schema = result.getExternalPartsSchema()
        if external_schema:
            external_schema_dict = dict(external_schema)
            lines.append("  🔗 External Parts Schema:")
            lines.append(render_schema_dict(external_schema_dict, indent=2))
            lines.append("")
    except Exception:
        pass  # External schema is optional
    
    # Common sections (upstream failures + lineage) after summary
    common_sections = render_common_sections(result)
    lines.append(common_sections)
    
    return '\n'.join(lines)




if __name__ == "__main__":
    # Example usage for testing
    sample_result = {
        "name": "test.demo.v1",
        "leftExpressionCheck": {"checkResult": "SUCCESS"},
        "joinPartChecks": [
            {
                "partName": "user_id_test_groupby",
                "gbEvalResult": {
                    "overallCheck": {"checkResult": "SUCCESS"}
                },
                "keySchemaCheck": {"checkResult": "SUCCESS"}
            }
        ],
        "leftQuerySchema": {"user_id": "StringType", "ts": "LongType"},
        "rightPartsSchema": {"feature1": "DoubleType", "feature2": "LongType"}
    }
    
    # Example usage - would need to import appropriate render function
    # print(render_join_result(sample_result))