"""
Constraint Protocol - validates metrics against rules.

A Constraint:
  - Takes MetricResult + RuleDef
  - Compares metric to rule expectation
  - Returns ValidationResult
  - Is where PASS/FAIL decisions happen

Philosophy:
  Constraints are the "judgment" layer.
  They answer "is this good" based on what analyzers measured.

  Example:
    - Analyzer says: "Completeness is 0.95"
    - Constraint says: "0.95 >= 0.99? NO → FAIL"
"""

from typing import Protocol, runtime_checkable

from sumeh.core.models import MetricResult, ValidationResult
from sumeh.core.rules.rule_model import RuleDef


@runtime_checkable
class IConstraint(Protocol):
    """
    Protocol for all constraints.

    Constraints validate metrics against rules.
    They make PASS/FAIL decisions.

    Implementation Requirements:
      1. Must have a check() static method
      2. Must accept MetricResult + RuleDef
      3. Must return ValidationResult
      4. Must set status to PASS, FAIL, or ERROR

    Example Implementation:
        >>> class MyConstraint:
        >>>     @staticmethod
        >>>     def check(metric: MetricResult, rule: RuleDef) -> ValidationResult:
        >>>         threshold = rule.threshold or 1.0
        >>>         passed = metric.value >= threshold
        >>>
        >>>         return ValidationResult(
        >>>             check_type=rule.check_type,
        >>>             field=rule.field,
        >>>             status=ValidationStatus.PASS if passed else ValidationStatus.FAIL,
        >>>             expected_value=threshold,
        >>>             actual_value=metric.value,
        >>>         )
    """

    @staticmethod
    def check(metric: MetricResult, rule: RuleDef) -> ValidationResult:
        """
        Validate metric against rule.

        Args:
            metric: Computed metric from an Analyzer
            rule: Validation rule with expectations (threshold, value, etc)

        Returns:
            ValidationResult with PASS/FAIL decision

        Note:
            - Set status to ERROR if metric is invalid or check fails to execute
            - Always populate expected_value and actual_value for traceability
            - Include helpful message for failures
        """
        ...
