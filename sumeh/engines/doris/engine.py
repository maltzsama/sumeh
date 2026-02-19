"""
Doris Engine - SQL-based validation with Apache Doris.

Apache Doris is a high-performance, real-time analytical database
based on MPP architecture (formerly Palo).

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
    rules: List[RuleDef], table_name: str, global_filter: Optional[str] = None
) -> str:
    """
    Generate Doris validation SQL without executing (Open SQL mode).

    Returns the SQL query as a string for manual execution or inspection.

    Args:
        rules: Validation rules
        table_name: Target table name (database.table)
        global_filter: Optional WHERE clause (e.g., "date = '2024-01-01'")

    Returns:
        SQL query string (Doris dialect)

    Example:
        >>> from sumeh import doris as sumeh_doris
        >>>
        >>> sql = sumeh_doris.get_validation_sql(
        ...     rules=rules,
        ...     table_name="analytics.users",
        ...     global_filter="created_at >= '2024-01-01'"
        ... )
        >>> print(sql)
        >>> # Copy-paste into Doris MySQL client
    """
    sql_query, _ = compile_rules_to_sql(
        rules=rules, table_name=table_name, dialect="doris", global_filter=global_filter
    )
    return sql_query


def validate(
    table_name: str, rules: List[RuleDef], connection: Any, baseline_provider=None
) -> ValidationReport:
    """
    Validates data using Apache Doris.

    Args:
        table_name: Fully qualified table name (database.table)
        rules: Validation rules
        connection: MySQL-compatible connection (pymysql or mysql.connector)
        baseline_provider: Optional baseline provider

    Returns:
        ValidationReport with .generated_sql for inspection

    Example:
        >>> import pymysql
        >>> from sumeh import doris as sumeh_doris
        >>>
        >>> conn = pymysql.connect(
        ...     host='doris-fe.example.com',
        ...     port=9030,
        ...     user='root',
        ...     password='',
        ...     database='analytics'
        ... )
        >>>
        >>> report = sumeh_doris.validate("analytics.users", rules, conn)
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
                engine="doris",
            )

        # Compile and execute validation
        sql_query, rule_ids = compile_rules_to_sql(rules, table_name, dialect="doris")
        if not sql_query:
            return ValidationReport(
                results=[],
                total_rows=total_rows,
                execution_time_ms=(time.time() - start_time) * 1000,
                engine="doris",
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
            engine="doris",
            generated_sql=sql_query,
        )

    except Exception as e:
        return ValidationReport(
            results=[],
            total_rows=0,
            execution_time_ms=(time.time() - start_time) * 1000,
            engine="doris",
        )
