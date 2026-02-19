"""
Redshift Engine - SQL-based validation with AWS Redshift.

AWS Redshift is a fully managed, petabyte-scale data warehouse
based on Postgres with MPP architecture.

Two modes:
1. validate() - Execute and return ValidationReport
2. get_validation_sql() - Generate SQL without executing (Open SQL)

Example - Execute:
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
    >>> print(f"Pass rate: {report.pass_rate:.2%}")

Example - Generate SQL:
    >>> sql = sumeh_redshift.get_validation_sql(
    ...     rules=rules,
    ...     table_name="public.users"
    ... )
    >>> print(sql)
    >>> # Execute in Redshift Query Editor
"""

from sumeh.engines.redshift.engine import validate, get_validation_sql

__all__ = ["validate", "get_validation_sql"]
