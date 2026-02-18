"""
Polars validation engine.

Fast, memory-efficient DataFrame library with native Rust backend.

Example:
    >>> import polars as pl
    >>> from sumeh import polars as sumeh_polars
    >>>
    >>> df = pl.read_csv("data.csv")
    >>> rules = [...]
    >>> report = sumeh_polars.validate(df, rules)
    >>>
    >>> good_df, bad_df = report.split()
    >>> good_df.write_parquet("clean/")
"""

from sumeh.engines.polars.engine import validate

__all__ = ["validate"]
