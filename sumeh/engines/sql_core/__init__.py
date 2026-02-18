"""
SQL Core Engine - SQL-based validation for databases.

Translates validation rules into optimized SQL queries using SQLGlot.
Supports multiple dialects: duckdb, postgres, spark, bigquery, etc.

Example:
    >>> from sumeh import sql_core
    >>> 
    >>> rules = [
    ...     {"check_type": "is_complete", "field": "email"},
    ...     {"check_type": "has_mean", "field": "salary", "value": 50000}
    ... ]
    >>> 
    >>> sql, rule_ids = sql_core.compile_rules_to_sql(
    ...     rules=rules,
    ...     table_name="users",
    ...     dialect="duckdb",
    ...     global_filter="date = '2024-01-15'"
    ... )
    >>> print(sql)
    >>> 
    >>> # Execute with your own DB client
    >>> result = conn.execute(sql).fetchone()
    >>> report = sql_core.validate_results(result, rule_ids, rules)
"""
from sumeh.engines.sql_core.compiler import compile_rules_to_sql
from sumeh.engines.sql_core.validator import validate_results

__all__ = [
    "compile_rules_to_sql",
    "validate_results",
]