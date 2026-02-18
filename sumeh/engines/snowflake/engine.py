"""
Snowflake Engine - SQL-based validation with Snowflake.

Snowflake is a cloud-native data warehouse with separation of storage and compute.
Uses pure SQL validation (no Snowpark Client dependency).

Two modes:
1. validate() - Execute validation and return ValidationReport
2. get_validation_sql() - Generate SQL without executing (Open SQL)
"""
import time
from typing import List, Optional, Any

from sumeh.core.models import ValidationReport
from sumeh.core.rules.rule_model import RuleDef
from sumeh.engines.sql_core.compiler import compile_rules_to_sql
from sumeh.engines.sql_core.validator import validate_results


def get_validation_sql(
    rules: List[RuleDef],
    table_name: str,
    global_filter: Optional[str] = None
) -> str:
    """
    Generate Snowflake validation SQL without executing (Open SQL mode).
    
    Returns the SQL query as a string for manual execution or inspection.
    
    Args:
        rules: Validation rules
        table_name: Target table name (database.schema.table)
        global_filter: Optional WHERE clause (e.g., "date = '2024-01-01'")
    
    Returns:
        SQL query string (Snowflake dialect)
    
    Example:
        >>> from sumeh import snowflake as sumeh_snowflake
        >>> 
        >>> sql = sumeh_snowflake.get_validation_sql(
        ...     rules=rules,
        ...     table_name="analytics.public.users",
        ...     global_filter="created_at >= '2024-01-01'"
        ... )
        >>> print(sql)
        >>> # Copy-paste into Snowflake Worksheet or execute via snowflake-connector-python
    """
    sql_query, _ = compile_rules_to_sql(
        rules=rules,
        table_name=table_name,
        dialect="snowflake",
        global_filter=global_filter
    )
    return sql_query


def validate(
    table_name: str,
    rules: List[RuleDef],
    connection: Any,
    baseline_provider=None
) -> ValidationReport:
    """
    Validates data using Snowflake (pure SQL, no Snowpark dependency).
    
    Args:
        table_name: Fully qualified table name (database.schema.table)
        rules: Validation rules
        connection: snowflake.connector.connection or cursor
        baseline_provider: Optional baseline provider
    
    Returns:
        ValidationReport with .generated_sql for inspection
    
    Example:
        >>> import snowflake.connector
        >>> from sumeh import snowflake as sumeh_snowflake
        >>> 
        >>> conn = snowflake.connector.connect(
        ...     user='admin',
        ...     password='password',
        ...     account='xy12345.us-east-1',
        ...     warehouse='COMPUTE_WH',
        ...     database='analytics',
        ...     schema='public'
        ... )
        >>> 
        >>> report = sumeh_snowflake.validate("analytics.public.users", rules, conn)
        >>> print(report.generated_sql)
        >>> report.explain()
        >>> 
        >>> # Works with Snowpark Connect for Spark too!
        >>> # Just use PySpark engine with snowpark_connect session
    """
    start_time = time.time()
    
    try:
        # Get cursor from connection
        cursor = connection.cursor() if hasattr(connection, 'cursor') else connection
        
        # Get total rows
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_rows = cursor.fetchone()[0]
        except Exception:
            return ValidationReport(
                results=[],
                total_rows=0,
                execution_time_ms=(time.time() - start_time) * 1000,
                engine="snowflake",
            )
        
        # Compile and execute validation
        sql_query, rule_ids = compile_rules_to_sql(rules, table_name, dialect="snowflake")
        if not sql_query:
            return ValidationReport(
                results=[],
                total_rows=total_rows,
                execution_time_ms=(time.time() - start_time) * 1000,
                engine="snowflake",
                generated_sql=""
            )
        
        cursor.execute(sql_query)
        metrics_row = cursor.fetchone()
        
        if metrics_row is None:
            raise ValueError("Validation query returned no results.")
        
        results = validate_results(metrics_row, rule_ids, rules, total_rows)
        
        return ValidationReport(
            results=results,
            total_rows=total_rows,
            execution_time_ms=(time.time() - start_time) * 1000,
            engine="snowflake",
            generated_sql=sql_query,
        )
    
    except Exception as e:
        return ValidationReport(
            results=[],
            total_rows=0,
            execution_time_ms=(time.time() - start_time) * 1000,
            engine="snowflake",
        )
