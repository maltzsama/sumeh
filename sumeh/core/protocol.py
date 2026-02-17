"""
Protocols for engine-agnostic interfaces.

These protocols define what validated DataFrames must implement,
regardless of the underlying engine (pandas, pyspark, polars, etc).
"""
from typing import Protocol, Tuple, Any, runtime_checkable


@runtime_checkable
class IValidatedDataFrame(Protocol):
    """
    Protocol for validated DataFrames across all engines.
    
    Any engine that implements this protocol can be used with
    ValidationReport.split() and other generic operations.
    
    Each engine (pandas, pyspark, polars) creates a wrapper class
    that implements this protocol.
    """
    
    def split_by_errors(self, error_column: str = "_dq_errors") -> Tuple[Any, Any]:
        """
        Split DataFrame into good and bad rows based on error column.
        
        Args:
            error_column: Name of the column containing error lists
        
        Returns:
            (good_df, bad_df) where:
              - good_df: Rows with no errors (without error column)
              - bad_df: Rows with errors (WITH error column for DLQ)
        """
        ...
    
    def to_native(self) -> Any:
        """
        Return the underlying native DataFrame.
        
        Returns:
            pd.DataFrame, pyspark.DataFrame, pl.DataFrame, etc
        """
        ...