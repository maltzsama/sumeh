"""
PySpark DataFrame wrapper with DQ capabilities.

Wraps pyspark.sql.DataFrame to add split_by_errors() and other DQ methods.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Tuple


class ValidatedSparkDataFrame:
    """
    Wrapper around PySpark DataFrame with data quality methods.

    Implements IValidatedDataFrame protocol for PySpark engine.

    Example:
        >>> validated = ValidatedSparkDataFrame(df)
        >>> good_df, bad_df = validated.split_by_errors()
        >>> validated.write.parquet("output.parquet")
    """

    def __init__(self, df: DataFrame):
        """
        Wrap a PySpark DataFrame.

        Args:
            df: PySpark DataFrame (should have _dq_errors column)
        """
        self._df = df

    def split_by_errors(
        self, error_column: str = "_dq_errors"
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Split DataFrame into good and bad rows.

        Args:
            error_column: Name of error column (default: _dq_errors)

        Returns:
            (good_df, bad_df) tuple where:
              - good_df: PySpark DataFrame without errors (no error column)
              - bad_df: PySpark DataFrame with errors (WITH error column)
        """
        if error_column not in self._df.columns:
            raise ValueError(f"Column '{error_column}' not found in DataFrame")

        # Detect rows with errors (PySpark-specific)
        # size() returns length of array
        has_errors = F.size(F.col(error_column)) > 0

        # Split (lazy - no shuffle until write)
        good_df = self._df.filter(~has_errors).drop(error_column)
        bad_df = self._df.filter(has_errors)

        return good_df, bad_df

    def to_native(self) -> DataFrame:
        """Return the underlying PySpark DataFrame."""
        return self._df

    # ========================================================================
    # Proxy common PySpark inspection methods (read-only)
    # ========================================================================

    @property
    def columns(self):
        """Proxy to PySpark columns"""
        return self._df.columns

    @property
    def dtypes(self):
        """Proxy to PySpark dtypes"""
        return self._df.dtypes

    @property
    def schema(self):
        """Proxy to PySpark schema"""
        return self._df.schema

    def count(self):
        """Proxy to PySpark count() - triggers action"""
        return self._df.count()

    def show(self, n=20, truncate=True):
        """Proxy to PySpark show()"""
        return self._df.show(n, truncate)

    def printSchema(self):
        """Proxy to PySpark printSchema()"""
        return self._df.printSchema()

    def describe(self, *cols):
        """Proxy to PySpark describe()"""
        return self._df.describe(*cols)

    @property
    def write(self):
        """Proxy to PySpark write (DataFrameWriter)"""
        return self._df.write

    def __repr__(self):
        """Show underlying DataFrame repr"""
        # Count errors (expensive - triggers action)
        # Only show schema for repr
        return f"ValidatedSparkDataFrame(columns={len(self._df.columns)}, schema={self._df.schema.simpleString()})"

    def __str__(self):
        """Show underlying DataFrame str"""
        return str(self._df)
