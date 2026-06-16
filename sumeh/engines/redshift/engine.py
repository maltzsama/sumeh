"""
Redshift Engine - SQL-based validation with AWS Redshift.

AWS Redshift is a fully managed, petabyte-scale data warehouse.
Postgres-based with MPP architecture.

Two modes:
1. validate() - Execute validation and return ValidationReport
2. get_validation_sql() - Generate SQL without executing (Open SQL)
"""

import time
from typing import List, Optional, Any

from sumeh.core.models.validation import ValidationReport
from sumeh.core.rules.rule_definition import RuleDefinition
from sumeh.engines.sql_core.compiler import compile_rules_to_sql
from sumeh.engines.sql_core.validator import validate_results


def get_validation_sql(
    rules: List[RuleDefinition], table_name: str, global_filter: Optional[str] = None
) -> str:
    """
    Generate Redshift validation SQL without executing (Open SQL mode).

    Returns the SQL query as a string for manual execution or inspection.

    Args:
        rules: Validation rules
        table_name: Target table name (schema.table)
        global_filter: Optional WHERE clause (e.g., "date = '2024-01-01'")

    Returns:
        SQL query string (Redshift dialect)

    Example:
        >>> from sumeh import redshift as sumeh_redshift
        >>>
        >>> sql = sumeh_redshift.get_validation_sql(
        ...     rules=rules,
        ...     table_name="public.users",
        ...     global_filter="created_at >= '2024-01-01'"
        ... )
        >>> print(sql)
        >>> # Copy-paste into Redshift Query Editor or execute via psycopg2
    """
    sql_query, _ = compile_rules_to_sql(
        rules=rules,
        table_name=table_name,
        dialect="redshift",
        global_filter=global_filter,
    )
    return sql_query


def validate(
    table_name: str,
    rules: List[RuleDefinition],
    connection: Any,
    baseline_provider=None,
) -> ValidationReport:
    """
    Validates data using AWS Redshift.

    Args:
        table_name: Fully qualified table name (schema.table)
        rules: Validation rules
        connection: psycopg2 or redshift_connector connection
        baseline_provider: Optional baseline provider

    Returns:
        ValidationReport with .generated_sql for inspection

    Example:
        >>> import psycopg2
        >>> from sumeh import redshift as sumeh_redshift
        >>>
        >>> conn = psycopg2.connect(
        ...     host='my-cluster.region.redshift.amazonaws.com',
        ...     port=5439,
        ...     user='admin',
        ...     password='password',
        ...     database='analytics'
        ... )
        >>>
        >>> report = sumeh_redshift.validate("public.users", rules, conn)
        >>> print(report.generated_sql)
        >>> report.explain()
    """
    start_time = time.time()

    try:
        # Get total rows
        try:
            cursor = connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_rows = cursor.fetchone()[0]
        except Exception:
            return ValidationReport(
                results=[],
                total_rows=0,
                execution_time_ms=(time.time() - start_time) * 1000,
                engine="redshift",
            )

        # Compile and execute validation
        sql_query, rule_ids = compile_rules_to_sql(
            rules, table_name, dialect="redshift"
        )
        if not sql_query:
            return ValidationReport(
                results=[],
                total_rows=total_rows,
                execution_time_ms=(time.time() - start_time) * 1000,
                engine="redshift",
                generated_sql="",
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
            engine="redshift",
            generated_sql=sql_query,
        )

    except Exception as e:
        return ValidationReport(
            results=[],
            total_rows=0,
            execution_time_ms=(time.time() - start_time) * 1000,
            engine="redshift",
        )
