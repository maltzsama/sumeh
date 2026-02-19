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

from sumeh.core.utils import (
    detect_engine,
    has_capability,
    get_capabilities,
    require_capability,
    list_engines_with_capability,
)

__all__ = [
    "ValidationResult",
    "ValidationReport",
    "MetricResult",
    "ValidationStatus",
    "ValidationLevel",
    "detect_engine",
    "has_capability",
    "get_capabilities",
    "require_capability",
    "list_engines_with_capability",
]
