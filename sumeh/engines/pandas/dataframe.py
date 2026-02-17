"""
Pandas DataFrame wrapper with DQ capabilities.

Wraps pd.DataFrame to add split_by_errors() and other DQ methods.
"""
import pandas as pd
from typing import Tuple


class ValidatedPandasDataFrame:
    """
    Wrapper around pandas DataFrame with data quality methods.
    
    This wrapper implements IValidatedDataFrame protocol and adds
    DQ-specific functionality while proxying standard pandas operations.
    
    Example:
        >>> validated = ValidatedPandasDataFrame(df)
        >>> good_df, bad_df = validated.split_by_errors()
        >>> validated.to_parquet("output.parquet")  # Proxied to pandas
    """
    
    def __init__(self, df: pd.DataFrame):
        """
        Wrap a pandas DataFrame.
        
        Args:
            df: pandas DataFrame (should have _dq_errors column)
        """
        self._df = df
    
    def split_by_errors(self, error_column: str = "_dq_errors") -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Split DataFrame into good and bad rows.
        
        Args:
            error_column: Name of error column (default: _dq_errors)
        
        Returns:
            (good_df, bad_df) tuple where:
              - good_df: pandas DataFrame without errors (no error column)
              - bad_df: pandas DataFrame with errors (WITH error column)
        """
        if error_column not in self._df.columns:
            raise ValueError(f"Column '{error_column}' not found in DataFrame")
        
        # Detect rows with errors
        has_errors = self._df[error_column].apply(
            lambda x: len(x) > 0 if isinstance(x, list) else False
        )
        
        # Split
        good_df = self._df[~has_errors].copy()
        bad_df = self._df[has_errors].copy()
        
        # Remove error column from good data (clean storage)
        good_df = good_df.drop(columns=[error_column])
        
        return good_df, bad_df
    
    def to_native(self) -> pd.DataFrame:
        """Return the underlying pandas DataFrame."""
        return self._df
    
    
    def head(self, *args, **kwargs):
        """Proxy to pandas.head()"""
        return self._df.head(*args, **kwargs)
    
    def tail(self, *args, **kwargs):
        """Proxy to pandas.tail()"""
        return self._df.tail(*args, **kwargs)
    
    def describe(self, *args, **kwargs):
        """Proxy to pandas.describe()"""
        return self._df.describe(*args, **kwargs)
    
    def info(self, *args, **kwargs):
        """Proxy to pandas.info()"""
        return self._df.info(*args, **kwargs)
    
    @property
    def columns(self):
        """Proxy to pandas.columns"""
        return self._df.columns
    
    @property
    def shape(self):
        """Proxy to pandas.shape"""
        return self._df.shape
    
    @property
    def dtypes(self):
        """Proxy to pandas.dtypes"""
        return self._df.dtypes
    
    def __len__(self):
        """Proxy to len(df)"""
        return len(self._df)
    
    def __getitem__(self, key):
        """Proxy to df[key]"""
        return self._df[key]
    
    def __repr__(self):
        """Show underlying DataFrame repr"""
        return f"ValidatedPandasDataFrame(\n{repr(self._df)}\n)"
    
    def __str__(self):
        """Show underlying DataFrame str"""
        return str(self._df)