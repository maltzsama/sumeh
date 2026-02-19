"""
Validation engines.

Each engine is a namespace with a validate() function.

Usage:
    from sumeh import pandas

    report = pandas.validate(df, rules)
"""

from sumeh.engines import pandas, polars, duckdb, dask, bigquery, pyspark, ray_data, pyflink, sql_core, athena, redshift, snowflake, trino, doris

__all__ = ["pandas", "polars", "duckdb", "dask", "bigquery", "pyspark", "ray_data", "pyflink", "sql_core", "athena", "redshift", "snowflake", "trino", "doris"]
