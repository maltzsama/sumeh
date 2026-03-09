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
    """
    A constraint validator for checking data completeness.

    This class validates that a dataset meets a specified completeness threshold,
    ensuring that the proportion of non-null values meets or exceeds the expected level.

    Methods:
        check(metric: MetricResult, rule: RuleDefinition) -> ValidationResult:
            Validates the completeness of data against a defined rule.
            
            Args:
                metric: A MetricResult object containing the completeness metric value
                       and metadata including null counts and affected row IDs.
                rule: A RuleDefinition object specifying the completeness threshold
                     and other validation parameters.
            
            Returns:
                ValidationResult: An object containing the validation outcome including:
                    - pass/fail status based on whether the metric value meets the threshold
                    - pass rate (non-null proportion)
                    - count of null values found
                    - IDs of rows with null values (if validation failed)
                    - detailed metadata about the validation
            
            Note:
                If no threshold is specified in the rule, defaults to 1.0 (100% completeness).
    """
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
    """
    A constraint validator for multi-field data completeness checks.

    This class validates whether metric results meet specified completeness thresholds
    across multiple fields in a dataset. It compares the actual completeness rate against
    an expected threshold and generates validation results with detailed failure information.

    Attributes:
        None (static utility class)

    Methods:
        check(metric: MetricResult, rule: RuleDefinition) -> ValidationResult:
            Validates a metric against a completeness rule.
            
            Args:
                metric (MetricResult): The computed completeness metric containing the 
                    completeness value (0.0-1.0), affected row IDs, and metadata about 
                    incomplete records.
                rule (RuleDefinition): The validation rule defining the threshold and 
                    other constraint properties.
            
            Returns:
                ValidationResult: A validation result object containing:
                    - PASS status if metric.value >= threshold
                    - FAIL status otherwise
                    - Row-level violation details (IDs of incomplete rows)
                    - Pass rate and expected/actual values for reporting
                    - Count of incomplete rows in the failure message
            
            Note:
                - Uses a default threshold of 1.0 (100% complete) if not specified in the rule
                - Extracts incomplete row count from metric metadata
    """
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
    """
    A constraint validator for checking the uniqueness of values in a dataset.

    This class provides validation logic to ensure that a metric meets a minimum
    uniqueness threshold. It compares the actual uniqueness metric against an
    expected threshold and generates a detailed validation result including
    information about duplicate values found.

    Methods:
        check(metric: MetricResult, rule: RuleDefinition) -> ValidationResult:
            Validates the uniqueness constraint by comparing the metric value
            against the rule's threshold.
            
            Args:
                metric: A MetricResult containing the uniqueness score (0.0-1.0)
                       and metadata with duplicate count and affected row IDs.
                rule: A RuleDefinition containing the uniqueness threshold and
                     other validation parameters.
            
            Returns:
                ValidationResult: A validation result object containing:
                    - status: PASS if metric value meets or exceeds threshold,
                             FAIL otherwise
                    - pass_rate: The actual uniqueness metric value
                    - expected_value: The threshold value
                    - message: Human-readable description of duplicates found
                    - violating_row_ids: List of row IDs with duplicate values
                    - metadata: Additional metadata from the metric
    """
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
    """
    A class that validates metrics against generic constraint rules.

    This class provides a static method to check if a metric result meets a defined
    threshold constraint and generates a validation report.

    Methods:
        check(metric: MetricResult, rule: RuleDefinition) -> ValidationResult:
            Validates a metric against a rule definition by comparing the metric value
            against a threshold. Returns a detailed validation result including pass/fail
            status, pass rate, expected vs actual values, and affected row information.
            
            Args:
                metric (MetricResult): The metric result to validate, containing a value,
                    metadata, and list of affected row IDs.
                rule (RuleDefinition): The rule definition containing the threshold,
                    category, check type, field, and ID to validate against.
            
            Returns:
                ValidationResult: A validation result object containing:
                    - rule_id: The identifier of the rule
                    - level: The validation level (ROW)
                    - category: The validation category
                    - check_type: The type of check performed
                    - field: The field being validated
                    - status: PASS if metric.value >= threshold, otherwise FAIL
                    - pass_rate: The metric value
                    - expected_value: The threshold value
                    - actual_value: The metric value
                    - violating_row_ids: List of affected row IDs if validation fails
                    - message: Descriptive message with fail count if validation fails
                    - metadata: Additional metadata from the metric
    """
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
    """
    A constraint validator for aggregation-based metrics.

    This class provides static methods to validate aggregated metric results against
    defined rules with optional tolerance thresholds.

    Methods:
        check(metric: MetricResult, rule: RuleDefinition) -> ValidationResult:
            Validates a metric result against a rule definition.
            
            Compares the actual metric value against an expected value, optionally
            allowing a percentage-based tolerance. When tolerance is specified (> 0),
            the validation uses relative error calculation. Otherwise, it performs
            exact equality comparison.
            
            Args:
                metric (MetricResult): The metric result containing the actual value
                    and associated metadata to validate.
                rule (RuleDefinition): The rule definition containing expected value,
                    threshold tolerance, and validation metadata.
            
            Returns:
                ValidationResult: A validation result object containing:
                    - Validation status (PASS/FAIL)
                    - Pass rate (1.0 if passed, 0.0 if failed)
                    - Expected and actual values
                    - Human-readable message on failure
                    - Rule metadata and categorization
    """
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
