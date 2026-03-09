"""
Constraint comparators - COMPLETE IMPLEMENTATION.

All constraints for validating metrics against rules.
"""

from sumeh.core.models.validation import (
    ValidationResult,
    ValidationLevel,
    ValidationStatus,
)

from sumeh.core.models.metrics import MetricResult

from sumeh.core.rules.rule_model import RuleDefinition


class CompletenessConstraint:
    @staticmethod
    def check(metric: MetricResult, rule: RuleDefinition) -> ValidationResult:
        threshold = rule.threshold if rule.threshold is not None else 1.0
        passed = metric.value >= threshold
        fail_count = metric.metadata.get("null_count", 0)

        return ValidationResult(
            rule_id=getattr(rule, "id", ""),
            level=ValidationLevel.ROW,
            category=rule.category or "completeness",
            check_type=rule.check_type,
            field=str(rule.field),
            status=ValidationStatus.PASS if passed else ValidationStatus.FAIL,
            pass_rate=metric.value,
            expected_value=threshold,
            actual_value=metric.value,
            violating_row_ids=[] if passed else metric.affected_row_ids,
            message=None if passed else f"{fail_count} null value(s) found",
            metadata=metric.metadata,
        )


class MultiFieldCompletenessConstraint:
    @staticmethod
    def check(metric: MetricResult, rule: RuleDefinition) -> ValidationResult:
        threshold = rule.threshold if rule.threshold is not None else 1.0
        passed = metric.value >= threshold
        fail_count = metric.metadata.get("incomplete_count", 0)

        return ValidationResult(
            rule_id=getattr(rule, "id", ""),
            level=ValidationLevel.ROW,
            category=rule.category or "completeness",
            check_type=rule.check_type,
            field=metric.field,
            status=ValidationStatus.PASS if passed else ValidationStatus.FAIL,
            pass_rate=metric.value,
            expected_value=threshold,
            actual_value=metric.value,
            violating_row_ids=[] if passed else metric.affected_row_ids,
            message=None if passed else f"{fail_count} row(s) incomplete",
            metadata=metric.metadata,
        )


class UniquenessConstraint:
    @staticmethod
    def check(metric: MetricResult, rule: RuleDefinition) -> ValidationResult:
        threshold = rule.threshold if rule.threshold is not None else 1.0
        passed = metric.value >= threshold
        fail_count = metric.metadata.get("duplicate_count", 0)

        return ValidationResult(
            rule_id=getattr(rule, "id", ""),
            level=ValidationLevel.ROW,
            category=rule.category or "uniqueness",
            check_type=rule.check_type,
            field=str(rule.field),
            status=ValidationStatus.PASS if passed else ValidationStatus.FAIL,
            pass_rate=metric.value,
            expected_value=threshold,
            actual_value=metric.value,
            violating_row_ids=[] if passed else metric.affected_row_ids,
            message=None if passed else f"{fail_count} duplicate value(s) found",
            metadata=metric.metadata,
        )


class GenericConstraint:
    @staticmethod
    def check(metric: MetricResult, rule: RuleDefinition) -> ValidationResult:
        threshold = rule.threshold if rule.threshold is not None else 1.0
        passed = metric.value >= threshold
        fail_count = metric.metadata.get("fail_count", 0)

        return ValidationResult(
            rule_id=getattr(rule, "id", ""),
            level=ValidationLevel.ROW,
            category=rule.category or "validation",
            check_type=rule.check_type,
            field=str(rule.field),
            status=ValidationStatus.PASS if passed else ValidationStatus.FAIL,
            pass_rate=metric.value,
            expected_value=threshold,
            actual_value=metric.value,
            violating_row_ids=[] if passed else metric.affected_row_ids,
            message=None if passed else f"{fail_count} row(s) failed validation",
            metadata=metric.metadata,
        )


class AggregationConstraint:
    @staticmethod
    def check(metric: MetricResult, rule: RuleDefinition) -> ValidationResult:
        expected = rule.value
        actual = metric.value
        tolerance = rule.threshold or 0.0

        if tolerance > 0:
            passed = abs(actual - expected) / expected <= tolerance
        else:
            passed = actual == expected

        return ValidationResult(
            rule_id=getattr(rule, "id", ""),
            level=ValidationLevel.TABLE,
            category=rule.category or "aggregation",
            check_type=rule.check_type,
            field=str(rule.field),
            status=ValidationStatus.PASS if passed else ValidationStatus.FAIL,
            pass_rate=1.0 if passed else 0.0,
            expected_value=expected,
            actual_value=actual,
            violating_row_ids=[],
            message=None if passed else f"Expected {expected}, got {actual}",
            metadata=metric.metadata,
        )
