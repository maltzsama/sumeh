"""
SQLCore Compiler.
Compiles rules into a single SELECT statement using SQLGlot AST.
Supports Global Filters and Per-Rule Filters (SQL FILTER / CASE WHEN).
"""
import sqlglot
import sqlglot.expressions as exp
from typing import List, Tuple, Optional
from sumeh.engines.sql_core.registry import get_analyzer

def compile_rules_to_sql(
    rules: List, 
    table_name: str, 
    dialect: str = None,
    global_filter: Optional[str] = None
) -> Tuple[str, List[str]]:
    """
    Compiles rules into a single optimized SQL query.
    
    Args:
        rules: List of RuleDef objects
        table_name: Target table name
        dialect: Target SQL dialect (duckdb, spark, postgres, bigquery)
        global_filter: Optional SQL WHERE clause (e.g., "date = '2024-01-01'")
        
    Returns:
        (sql_string, list_of_rule_ids_in_order)
    """
    projections = []
    rule_ids = []
    
    for rule in rules:
        analyzer = get_analyzer(rule.check_type)
        if not analyzer:
            # Log warning in production
            continue
            
        expression_ast = analyzer.analyze(rule)
        
        if hasattr(rule, 'filter') and rule.filter:
            filter_condition = sqlglot.parse_one(rule.filter)
            expression_ast = exp.Filter(
                this=expression_ast,
                expression=exp.Where(this=filter_condition)
            )
        
        r_id = getattr(rule, 'id', None) or f"{rule.check_type}:{rule.field}"
        rule_ids.append(r_id)
        
        safe_alias = f"rule_{len(rule_ids)-1}" 
        projections.append(expression_ast.as_(safe_alias))
    
    if not projections:
        return "", []

    query_ast = exp.select(*projections).from_(table_name)
    
    if global_filter:
        where_ast = sqlglot.parse_one(global_filter)
        query_ast = query_ast.where(where_ast)
        
    sql = query_ast.sql(dialect=dialect, pretty=True)
    
    return sql, rule_ids