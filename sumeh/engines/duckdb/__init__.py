"""
DuckDB Engine - SQL-based validation with DuckDB.

Executes validation rules directly in DuckDB, supporting:
- In-memory DataFrames (Pandas, Polars, Arrow)
- File paths (Parquet, CSV, JSON)
- External tables (if connection is shared)

Example:
    >>> from sumeh import duckdb as sumeh_duckdb
    >>> 
    >>> # Validate a Parquet file
    >>> report = sumeh_duckdb.validate("data/users.parquet", rules)
    >>> 
    >>> # Validate a Pandas DataFrame
    >>> import pandas as pd
    >>> df = pd.read_csv("data.csv")
    >>> report = sumeh_duckdb.validate(df, rules)
    >>> 
    >>> print(f"Pass rate: {report.pass_rate:.2%}")
    >>> for result in report.failed:
    >>>     print(f"  - {result.check_type} on {result.field}: {result.message}")
"""
from sumeh.engines.duckdb.engine import validate

__all__ = ["validate"]