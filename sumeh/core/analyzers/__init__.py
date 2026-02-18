"""
Analyzers - compute metrics without opinions.

Available Analyzers:
  - CompletenessAnalyzer: Count null values
  - MultiFieldCompletenessAnalyzer: Count nulls across multiple fields

Usage:
    >>> from sumeh.core.analyzers import CompletenessAnalyzer
    >>> metric = CompletenessAnalyzer.analyze(df, "email")
    >>> print(f"Completeness: {metric.value:.2%}")
"""

from sumeh.core.analyzers.protocol import IAnalyzer

__all__ = [
    "IAnalyzer",
    "CompletenessAnalyzer",
    "MultiFieldCompletenessAnalyzer",
]
