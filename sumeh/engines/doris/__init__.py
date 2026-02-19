"""
Doris Engine - SQL-based validation with Apache Doris.

Apache Doris is a high-performance, real-time analytical database
based on MPP architecture. MySQL-compatible protocol.

Two modes:
1. validate() - Execute and return ValidationReport
2. get_validation_sql() - Generate SQL without executing (Open SQL)

Example - Execute:
    >>> import pymysql
    >>> from sumeh import doris as sumeh_doris
    >>>
    >>> conn = pymysql.connect(
    ...     host='doris-fe.example.com',
    ...     port=9030,
    ...     user='root',
    ...     database='analytics'
    ... )
    >>>
    >>> report = sumeh_doris.validate("analytics.users", rules, conn)
    >>> print(f"Pass rate: {report.pass_rate:.2%}")

Example - Generate SQL:
    >>> sql = sumeh_doris.get_validation_sql(
    ...     rules=rules,
    ...     table_name="analytics.users"
    ... )
    >>> print(sql)
    >>> # Execute manually in Doris MySQL client
"""

from sumeh.engines.doris.engine import validate, get_validation_sql


CAPABILITIES = {
    'schema_validation': True,
    'profiling': True,
    'aggregation_analyzers': True,
    'bifurcation': True,
    'streaming': False,
}

__all__ = ['validate', 'get_validation_sql', 'CAPABILITIES']