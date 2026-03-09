"""
Tests for sumeh/core/logic/comparators.py

Covers:
- CompletenessConstraint
- MultiFieldCompletenessConstraint
- UniquenessConstraint
- GenericConstraint
- AggregationConstraint
"""

import pytest
from sumeh.core.logic.comparators import (
    CompletenessConstraint,
    MultiFieldCompletenessConstraint,
    UniquenessConstraint,
    GenericConstraint,
    AggregationConstraint,
)
from sumeh.core.models.metrics import MetricResult
from sumeh.core.models.validation import ValidationStatus, ValidationLevel
from sumeh.core.rules.rule_model import RuleDefinition

# Helpers


def make_metric(
    value: float,
    field: str = "email",
    metric_type: str = "completeness",
    total_rows: int = 100,
    affected_row_ids=None,
    metadata=None,
) -> MetricResult:
    return MetricResult(
        metric_type=metric_type,
        field=field,
        value=value,
        total_rows=total_rows,
        affected_row_ids=affected_row_ids or [],
        metadata=metadata or {},
    )


# CompletenessConstraint


class TestCompletenessConstraint:

    def test_pass_when_metric_meets_threshold(self):
        metric = make_metric(value=1.0, metadata={"null_count": 0})
        rule = RuleDefinition(field="email", check_type="is_complete", threshold=1.0)
        result = CompletenessConstraint.check(metric, rule)
        assert result.status == ValidationStatus.PASS

    def test_fail_when_metric_below_threshold(self):
        metric = make_metric(
            value=0.8,
            affected_row_ids=[2, 5],
            metadata={"null_count": 20},
        )
        rule = RuleDefinition(field="email", check_type="is_complete", threshold=1.0)
        result = CompletenessConstraint.check(metric, rule)
        assert result.status == ValidationStatus.FAIL

    def test_pass_with_partial_threshold(self):
        metric = make_metric(value=0.95, metadata={"null_count": 5})
        rule = RuleDefinition(field="email", check_type="is_complete", threshold=0.90)
        result = CompletenessConstraint.check(metric, rule)
        assert result.status == ValidationStatus.PASS

    def test_fail_message_includes_null_count(self):
        metric = make_metric(value=0.8, metadata={"null_count": 20})
        rule = RuleDefinition(field="email", check_type="is_complete", threshold=1.0)
        result = CompletenessConstraint.check(metric, rule)
        assert "20" in result.message

    def test_pass_message_is_none(self):
        metric = make_metric(value=1.0, metadata={"null_count": 0})
        rule = RuleDefinition(field="email", check_type="is_complete", threshold=1.0)
        result = CompletenessConstraint.check(metric, rule)
        assert result.message is None

    def test_violating_ids_empty_on_pass(self):
        metric = make_metric(value=1.0, metadata={"null_count": 0})
        rule = RuleDefinition(field="email", check_type="is_complete", threshold=1.0)
        result = CompletenessConstraint.check(metric, rule)
        assert result.violating_row_ids == []

    def test_violating_ids_populated_on_fail(self):
        metric = make_metric(
            value=0.8,
            affected_row_ids=[1, 3, 5],
            metadata={"null_count": 3},
        )
        rule = RuleDefinition(field="email", check_type="is_complete", threshold=1.0)
        result = CompletenessConstraint.check(metric, rule)
        assert result.violating_row_ids == [1, 3, 5]

    def test_level_is_row(self):
        metric = make_metric(value=1.0)
        rule = RuleDefinition(field="email", check_type="is_complete", threshold=1.0)
        result = CompletenessConstraint.check(metric, rule)
        assert result.level == ValidationLevel.ROW

    def test_pass_rate_matches_metric_value(self):
        metric = make_metric(value=0.75, metadata={"null_count": 25})
        rule = RuleDefinition(field="email", check_type="is_complete", threshold=1.0)
        result = CompletenessConstraint.check(metric, rule)
        assert result.pass_rate == pytest.approx(0.75)

    def test_expected_value_matches_threshold(self):
        metric = make_metric(value=1.0)
        rule = RuleDefinition(field="email", check_type="is_complete", threshold=0.95)
        result = CompletenessConstraint.check(metric, rule)
        assert result.expected_value == pytest.approx(0.95)


# MultiFieldCompletenessConstraint


class TestMultiFieldCompletenessConstraint:

    def test_pass_when_metric_meets_threshold(self):
        metric = make_metric(value=1.0, metadata={"incomplete_count": 0})
        rule = RuleDefinition(
            field=["name", "email"], check_type="are_complete", threshold=1.0
        )
        result = MultiFieldCompletenessConstraint.check(metric, rule)
        assert result.status == ValidationStatus.PASS

    def test_fail_when_metric_below_threshold(self):
        metric = make_metric(
            value=0.7,
            affected_row_ids=[0, 3],
            metadata={"incomplete_count": 30},
        )
        rule = RuleDefinition(
            field=["name", "email"], check_type="are_complete", threshold=1.0
        )
        result = MultiFieldCompletenessConstraint.check(metric, rule)
        assert result.status == ValidationStatus.FAIL

    def test_fail_message_includes_incomplete_count(self):
        metric = make_metric(value=0.7, metadata={"incomplete_count": 30})
        rule = RuleDefinition(
            field=["name", "email"], check_type="are_complete", threshold=1.0
        )
        result = MultiFieldCompletenessConstraint.check(metric, rule)
        assert "30" in result.message

    def test_level_is_row(self):
        metric = make_metric(value=1.0, metadata={"incomplete_count": 0})
        rule = RuleDefinition(
            field=["name", "email"], check_type="are_complete", threshold=1.0
        )
        result = MultiFieldCompletenessConstraint.check(metric, rule)
        assert result.level == ValidationLevel.ROW


# UniquenessConstraint


class TestUniquenessConstraint:

    def test_pass_when_all_unique(self):
        metric = make_metric(value=1.0, metadata={"duplicate_count": 0})
        rule = RuleDefinition(field="user_id", check_type="is_unique", threshold=1.0)
        result = UniquenessConstraint.check(metric, rule)
        assert result.status == ValidationStatus.PASS

    def test_fail_when_duplicates_exist(self):
        metric = make_metric(
            value=0.8,
            affected_row_ids=[2, 4],
            metadata={"duplicate_count": 2},
        )
        rule = RuleDefinition(field="user_id", check_type="is_unique", threshold=1.0)
        result = UniquenessConstraint.check(metric, rule)
        assert result.status == ValidationStatus.FAIL

    def test_fail_message_includes_duplicate_count(self):
        metric = make_metric(value=0.8, metadata={"duplicate_count": 5})
        rule = RuleDefinition(field="user_id", check_type="is_unique", threshold=1.0)
        result = UniquenessConstraint.check(metric, rule)
        assert "5" in result.message
        assert "duplicate" in result.message.lower()

    def test_violating_ids_on_fail(self):
        metric = make_metric(
            value=0.8,
            affected_row_ids=[1, 2],
            metadata={"duplicate_count": 2},
        )
        rule = RuleDefinition(field="user_id", check_type="is_unique", threshold=1.0)
        result = UniquenessConstraint.check(metric, rule)
        assert result.violating_row_ids == [1, 2]

    def test_level_is_row(self):
        metric = make_metric(value=1.0, metadata={"duplicate_count": 0})
        rule = RuleDefinition(field="user_id", check_type="is_unique", threshold=1.0)
        result = UniquenessConstraint.check(metric, rule)
        assert result.level == ValidationLevel.ROW


# GenericConstraint


class TestGenericConstraint:

    def test_pass_when_metric_meets_threshold(self):
        metric = make_metric(value=1.0, metadata={"fail_count": 0})
        rule = RuleDefinition(field="age", check_type="is_positive", threshold=1.0)
        result = GenericConstraint.check(metric, rule)
        assert result.status == ValidationStatus.PASS

    def test_fail_when_metric_below_threshold(self):
        metric = make_metric(
            value=0.8,
            affected_row_ids=[3],
            metadata={"fail_count": 20},
        )
        rule = RuleDefinition(field="age", check_type="is_positive", threshold=1.0)
        result = GenericConstraint.check(metric, rule)
        assert result.status == ValidationStatus.FAIL

    def test_fail_message_includes_fail_count(self):
        metric = make_metric(value=0.8, metadata={"fail_count": 20})
        rule = RuleDefinition(field="age", check_type="is_positive", threshold=1.0)
        result = GenericConstraint.check(metric, rule)
        assert "20" in result.message

    def test_pass_message_is_none(self):
        metric = make_metric(value=1.0, metadata={"fail_count": 0})
        rule = RuleDefinition(field="age", check_type="is_positive", threshold=1.0)
        result = GenericConstraint.check(metric, rule)
        assert result.message is None

    def test_level_is_row(self):
        metric = make_metric(value=1.0)
        rule = RuleDefinition(field="age", check_type="is_positive", threshold=1.0)
        result = GenericConstraint.check(metric, rule)
        assert result.level == ValidationLevel.ROW

    def test_partial_threshold_pass(self):
        metric = make_metric(value=0.95, metadata={"fail_count": 5})
        rule = RuleDefinition(field="age", check_type="is_positive", threshold=0.90)
        result = GenericConstraint.check(metric, rule)
        assert result.status == ValidationStatus.PASS


# AggregationConstraint


class TestAggregationConstraint:

    def test_pass_exact_match(self):
        metric = make_metric(value=40.0, metric_type="mean", field="age")
        rule = RuleDefinition(
            field="age", check_type="has_mean", value=40.0, threshold=0.0
        )
        result = AggregationConstraint.check(metric, rule)
        assert result.status == ValidationStatus.PASS

    def test_fail_exact_mismatch(self):
        metric = make_metric(value=35.0, metric_type="mean", field="age")
        rule = RuleDefinition(
            field="age", check_type="has_mean", value=40.0, threshold=0.0
        )
        result = AggregationConstraint.check(metric, rule)
        assert result.status == ValidationStatus.FAIL

    def test_pass_within_tolerance(self):
        metric = make_metric(value=41.0, metric_type="mean", field="age")
        rule = RuleDefinition(
            field="age", check_type="has_mean", value=40.0, threshold=0.05
        )
        result = AggregationConstraint.check(metric, rule)
        assert result.status == ValidationStatus.PASS

    def test_fail_outside_tolerance(self):
        metric = make_metric(value=50.0, metric_type="mean", field="age")
        rule = RuleDefinition(
            field="age", check_type="has_mean", value=40.0, threshold=0.05
        )
        result = AggregationConstraint.check(metric, rule)
        assert result.status == ValidationStatus.FAIL

    def test_fail_message_includes_expected_and_actual(self):
        metric = make_metric(value=50.0, metric_type="mean", field="age")
        rule = RuleDefinition(
            field="age", check_type="has_mean", value=40.0, threshold=0.0
        )
        result = AggregationConstraint.check(metric, rule)
        assert "40" in result.message
        assert "50" in result.message

    def test_level_is_table(self):
        metric = make_metric(value=40.0, metric_type="mean", field="age")
        rule = RuleDefinition(
            field="age", check_type="has_mean", value=40.0, threshold=0.0
        )
        result = AggregationConstraint.check(metric, rule)
        assert result.level == ValidationLevel.TABLE

    def test_pass_rate_is_one_on_pass(self):
        metric = make_metric(value=40.0, metric_type="mean", field="age")
        rule = RuleDefinition(
            field="age", check_type="has_mean", value=40.0, threshold=0.0
        )
        result = AggregationConstraint.check(metric, rule)
        assert result.pass_rate == pytest.approx(1.0)

    def test_pass_rate_is_zero_on_fail(self):
        metric = make_metric(value=10.0, metric_type="mean", field="age")
        rule = RuleDefinition(
            field="age", check_type="has_mean", value=40.0, threshold=0.0
        )
        result = AggregationConstraint.check(metric, rule)
        assert result.pass_rate == pytest.approx(0.0)

    def test_violating_row_ids_always_empty(self):
        metric = make_metric(value=10.0, metric_type="mean", field="age")
        rule = RuleDefinition(
            field="age", check_type="has_mean", value=40.0, threshold=0.0
        )
        result = AggregationConstraint.check(metric, rule)
        assert result.violating_row_ids == []

    def test_expected_and_actual_values_preserved(self):
        metric = make_metric(value=35.0, metric_type="mean", field="age")
        rule = RuleDefinition(
            field="age", check_type="has_mean", value=40.0, threshold=0.05
        )
        result = AggregationConstraint.check(metric, rule)
        assert result.expected_value == pytest.approx(40.0)
        assert result.actual_value == pytest.approx(35.0)
