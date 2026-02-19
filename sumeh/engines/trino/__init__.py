"""
Trino Engine - SQL-based validation with Trino.

Trino (formerly PrestoSQL) is a distributed SQL query engine
for federated queries across multiple data sources.

Two modes:
1. validate() - Execute and return ValidationReport
2. get_validation_sql() - Generate SQL without executing (Open SQL)

Example - Execute:
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
    >>> report = sumeh_trino.validate("hive.default.users", rules, conn)
    >>> print(f"Pass rate: {report.pass_rate:.2%}")
    >>> report.explain()

Example - Generate SQL:
    >>> sql = sumeh_trino.get_validation_sql(
    ...     rules=rules,
    ...     table_name="hive.default.users",
    ...     global_filter="created_at >= DATE '2024-01-01'"
    ... )
    >>> print(sql)
    >>> # Copy-paste into Trino CLI
"""

from sumeh.engines.trino.engine import validate, get_validation_sql

__all__ = ["validate", "get_validation_sql"]
