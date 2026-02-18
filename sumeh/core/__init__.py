"""
Core components of Sumeh validation framework.

Models:
  - ValidationResult: Single validation result
  - ValidationReport: Collection of validation results
  - MetricResult: Output of analyzers
  - ValidationStatus: PASS/FAIL/ERROR enum
  - ValidationLevel: ROW/TABLE enum

Analyzers:
  - CompletenessAnalyzer: Count nulls

Constraints:
  - CompletenessConstraint: Validate completeness threshold
"""

from sumeh.core.models import (
    ValidationResult,
    ValidationReport,
    MetricResult,
    ValidationStatus,
    ValidationLevel,
)

__all__ = [
    "ValidationResult",
    "ValidationReport",
    "MetricResult",
    "ValidationStatus",
    "ValidationLevel",
]
