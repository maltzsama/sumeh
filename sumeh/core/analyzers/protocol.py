"""
Analyzer Protocol - computes metrics without opinions.

An Analyzer:
  - Takes data + field
  - Computes a metric
  - Returns MetricResult
  - NEVER knows about thresholds or rules
  - Is PURE (same input = same output)

Philosophy:
  Analyzers are the "measurement" layer.
  They answer "what is" not "is this good".
  
  Example:
    - Analyzer: "This column has 50 null values out of 1000 rows"
    - Constraint: "50 nulls is BAD because threshold is 0.99"
"""
from typing import Protocol, Any, runtime_checkable
from sumeh.core.models import MetricResult


@runtime_checkable
class IAnalyzer(Protocol):
    """
    Protocol for all analyzers.
    
    Analyzers compute metrics on data.
    They don't validate - they just measure.
    
    Implementation Requirements:
      1. Must have an analyze() static method
      2. Must return MetricResult
      3. Must be deterministic (pure function)
      4. Must NOT access rule thresholds
    
    Example Implementation:
        >>> class MyAnalyzer:
        >>>     @staticmethod
        >>>     def analyze(df, field: str, **kwargs) -> MetricResult:
        >>>         # Compute metric
        >>>         value = df[field].some_computation()
        >>>         return MetricResult(
        >>>             metric_type="my_metric",
        >>>             field=field,
        >>>             value=value,
        >>>             total_rows=len(df),
        >>>         )
    """
    
    @staticmethod
    def analyze(data: Any, field: str, **kwargs) -> MetricResult:
        """
        Compute metric on data.
        
        Args:
            data: DataFrame/data structure (engine-specific)
            field: Column/field to analyze
            **kwargs: Analyzer-specific parameters
        
        Returns:
            MetricResult with computed metric
        
        Raises:
            ValueError: If field doesn't exist or data is invalid
        """
        ...