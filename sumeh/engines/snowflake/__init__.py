"""
Snowflake Engine - SQL-based validation with Snowflake.

Pure SQL validation (no Snowpark Client dependency).
Works with snowflake-connector-python.

Two modes:
1. validate() - Execute and return ValidationReport
2. get_validation_sql() - Generate SQL without executing (Open SQL)

Example - Execute (Pure SQL):
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
    >>> print(f"Pass rate: {report.pass_rate:.2%}")
    >>> report.explain()

Example - Generate SQL (Open SQL):
    >>> sql = sumeh_snowflake.get_validation_sql(
    ...     rules=rules,
    ...     table_name="analytics.public.users"
    ... )
    >>> print(sql)
    >>> # Execute in Snowflake Worksheet

Note - Snowpark Connect for Spark:
    If you want to use Snowpark Connect for Apache Spark,
    just use the PySpark engine with snowpark_connect session:

    >>> from sumeh import pyspark as sumeh_pyspark
    >>> spark = snowpark_connect.get_session()
    >>> report = sumeh_pyspark.validate(spark.table("users"), rules)
"""

from sumeh.engines.snowflake.engine import validate, get_validation_sql

CAPABILITIES = {
    'schema_validation': True,
    'profiling': True,
    'aggregation_analyzers': True,
    'bifurcation': True,
    'streaming': False,
}

__all__ = ['validate', 'get_validation_sql', 'CAPABILITIES']
