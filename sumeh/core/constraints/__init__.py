"""
Constraints - validate metrics against rules.

Available Constraints:
  - CompletenessConstraint: Validates completeness threshold
  - MultiFieldCompletenessConstraint: Validates multi-field completeness

Usage:
    >>> from sumeh.core.constraints import CompletenessConstraint
    >>> from sumeh.core.analyzers import CompletenessAnalyzer
    >>>
    >>> metric = CompletenessAnalyzer.analyze(df, "email")
    >>> result = CompletenessConstraint.check(metric, rule)
    >>>
    >>> print(result.status)  # PASS or FAIL
"""

from sumeh.core.logic.comparators import (
    CompletenessConstraint,
    MultiFieldCompletenessConstraint,
)
from sumeh.core.constraints.protocol import IConstraint

__all__ = [
    "IConstraint",
    "CompletenessConstraint",
    "MultiFieldCompletenessConstraint",
]
