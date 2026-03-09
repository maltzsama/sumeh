"""
Polars DataFrame wrapper with DQ capabilities.

Wraps pl.DataFrame to add split_by_errors() and other DQ methods.
"""

from typing import Tuple

import polars as pl


class ValidatedPolarsDataFrame:
    """
    Wrapper around Polars DataFrame with data quality methods.

    Implements IValidatedDataFrame protocol for Polars engine.
    """

    def __init__(self, df: pl.DataFrame):
        self._df = df

    def split_by_errors(
        self, error_column: str = "_dq_errors"
    ) -> Tuple[pl.DataFrame, pl.DataFrame]:
        """
        Split DataFrame into good and bad rows.
        """
        if error_column not in self._df.columns:
            raise ValueError(f"Column '{error_column}' not found in DataFrame")

        # FIX: Use .list.len() instead of .arr.lengths()
        has_errors = pl.col(error_column).list.len() > 0

        # Split
        good_df = self._df.filter(~has_errors)
        bad_df = self._df.filter(has_errors)

        # Remove error column from good data
        good_df = good_df.drop(error_column)

        return good_df, bad_df

    def to_native(self) -> pl.DataFrame:
        """Return the underlying Polars DataFrame."""
        return self._df

    # ========================================================================
    # Proxy common Polars inspection methods (read-only)
    # ========================================================================

    @property
    def columns(self):
        return self._df.columns

    @property
    def shape(self):
        return self._df.shape

    @property
    def dtypes(self):
        return self._df.dtypes

    @property
    def schema(self):
        return self._df.schema

    def head(self, n=5):
        return self._df.head(n)

    def tail(self, n=5):
        return self._df.tail(n)

    def describe(self):
        return self._df.describe()

    def __len__(self):
        return len(self._df)

    def __getitem__(self, key):
        return self._df[key]

    def __repr__(self):
        """Show underlying DataFrame repr"""
        # FIX: Use .list.len() here too for the display count
        errors = (
            self._df.select(pl.col("_dq_errors").list.len().sum()).item()
            if "_dq_errors" in self._df.columns
            else 0
        )
        return f"ValidatedPolarsDataFrame({len(self._df)} rows, {errors} errors)"

    def __str__(self):
        return str(self._df)
