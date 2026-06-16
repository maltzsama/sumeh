"""
Trino Engine - SQL-based validation with Trino.

Trino (formerly PrestoSQL) is a distributed SQL query engine.
Supports federated queries across multiple data sources.

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
    Generate Trino validation SQL without executing (Open SQL mode).

    Returns the SQL query as a string for manual execution or inspection.

    Args:
        rules: Validation rules
        table_name: Target table name (catalog.schema.table)
        global_filter: Optional WHERE clause (e.g., "date = DATE '2024-01-01'")

    Returns:
        SQL query string (Trino dialect)

    Example:
        >>> from sumeh import trino as sumeh_trino
        >>>
        >>> sql = sumeh_trino.get_validation_sql(
        ...     rules=rules,
        ...     table_name="hive.default.users",
        ...     global_filter="created_at >= DATE '2024-01-01'"
        ... )
        >>> print(sql)
        >>> # Copy-paste into Trino CLI or execute via trino-python-client
    """
    sql_query, _ = compile_rules_to_sql(
        rules=rules, table_name=table_name, dialect="trino", global_filter=global_filter
    )
    return sql_query


def validate(
    table_name: str,
    rules: List[RuleDefinition],
    connection: Any,
    baseline_provider=None,
) -> ValidationReport:
    """
    Validates data using Trino.

    Args:
        table_name: Fully qualified table name (catalog.schema.table)
        rules: Validation rules
        connection: trino.dbapi.Connection or trino.auth connection
        baseline_provider: Optional baseline provider

    Returns:
        ValidationReport with .generated_sql for inspection

    Example:
        >>> from trino.dbapi import connect
        >>> from sumeh import trino as sumeh_trino
        >>>
        >>> conn = connect(
        ...     host='trino.example.com',
        ...     port=443,
        ...     user='admin',
        ...     catalog='hive',
        ...     schema='default'
        ... )
        >>>
        >>> report = sumeh_trino.validate(
        ...     "hive.default.users",
        ...     rules,
        ...     conn
        ... )
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
        except Exception as e:
            return ValidationReport(
                results=[],
                total_rows=0,
                execution_time_ms=(time.time() - start_time) * 1000,
                engine="trino",
            )

        # Compile and execute validation
        sql_query, rule_ids = compile_rules_to_sql(rules, table_name, dialect="trino")
        if not sql_query:
            return ValidationReport(
                results=[],
                total_rows=total_rows,
                execution_time_ms=(time.time() - start_time) * 1000,
                engine="trino",
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
            engine="trino",
            generated_sql=sql_query,
        )

    except Exception as e:
        return ValidationReport(
            results=[],
            total_rows=0,
            execution_time_ms=(time.time() - start_time) * 1000,
            engine="trino",
        )
