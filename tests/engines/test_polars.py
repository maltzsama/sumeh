"""
Tests for sumeh/engines/polars/

Covers:
- analyzers: all analyzer classes
- engine: validate() integration, bifurcation, row/table rules, error handling
"""

from datetime import date, timedelta

import polars as pl
import pytest

from sumeh.core.models.validation import ValidationStatus
from sumeh.core.rules.rule_model import RuleDefinition
from sumeh.engines.polars import validate
from sumeh.engines.polars.analyzers import (
    CompletenessAnalyzer,
    MultiFieldCompletenessAnalyzer,
    UniquenessAnalyzer,
    MultiFieldUniquenessAnalyzer,
    ComparisonAnalyzer,
    BetweenAnalyzer,
    MembershipAnalyzer,
    PatternAnalyzer,
    LegitAnalyzer,
    DateAnalyzer,
    DateBetweenAnalyzer,
    DateComparisonAnalyzer,
    AggregationAnalyzer,
)


# Fixtures


@pytest.fixture
def df_base():
    today = date.today()
    yesterday = today - timedelta(days=1)
    return pl.DataFrame(
        {
            "user_id": [1, 2, 3, 4, 5],
            "email": ["a@a.com", "b@b.com", None, "d@d.com", "e@e.com"],
            "age": [25, 30, 17, 45, 60],
            "score": [80.0, 90.0, 70.0, 85.0, 95.0],
            "revenue": [1000.0, 2000.0, -500.0, 4000.0, 5000.0],
            "status": ["active", "inactive", "active", "banned", "active"],
            "notes": ["ok", "  ", None, "valid", "also ok"],
            "big_value": [1_000_000, 2_000_000, 500, 1_500_000, 2_500_000],
            "col_a": [1, 2, 3, 4, 5],
            "col_b": [1, 2, 3, 4, 5],
            "created_at": [today, today, today, today, today],
            "updated_at": [yesterday, yesterday, yesterday, yesterday, yesterday],
        },
        schema_overrides={
            "created_at": pl.Date,
            "updated_at": pl.Date,
        },
    )


# CompletenessAnalyzer


class TestCompletenessAnalyzer:

    def test_full_completeness(self, df_base):
        rule = RuleDefinition(field="user_id", check_type="is_complete")
        metric = CompletenessAnalyzer.analyze(df_base, rule)
        assert metric.value == pytest.approx(1.0)
        assert metric.metadata["null_count"] == 0

    def test_partial_completeness(self, df_base):
        rule = RuleDefinition(field="email", check_type="is_complete")
        metric = CompletenessAnalyzer.analyze(df_base, rule)
        assert metric.value == pytest.approx(0.8)
        assert metric.metadata["null_count"] == 1

    def test_affected_row_ids_correct(self, df_base):
        rule = RuleDefinition(field="email", check_type="is_complete")
        metric = CompletenessAnalyzer.analyze(df_base, rule)
        assert 2 in metric.affected_row_ids

    def test_missing_field_raises(self, df_base):
        rule = RuleDefinition(field="nonexistent", check_type="is_complete")
        with pytest.raises(KeyError):
            CompletenessAnalyzer.analyze(df_base, rule)


class TestMultiFieldCompletenessAnalyzer:

    def test_all_complete(self, df_base):
        rule = RuleDefinition(field=["user_id", "col_a"], check_type="are_complete")
        metric = MultiFieldCompletenessAnalyzer.analyze(df_base, rule)
        assert metric.value == pytest.approx(1.0)

    def test_partial_completeness(self, df_base):
        rule = RuleDefinition(field=["email", "notes"], check_type="are_complete")
        metric = MultiFieldCompletenessAnalyzer.analyze(df_base, rule)
        assert metric.value < 1.0

    def test_missing_field_raises(self, df_base):
        rule = RuleDefinition(field=["email", "nonexistent"], check_type="are_complete")
        with pytest.raises(KeyError):
            MultiFieldCompletenessAnalyzer.analyze(df_base, rule)


# UniquenessAnalyzer


class TestUniquenessAnalyzer:

    def test_all_unique(self, df_base):
        rule = RuleDefinition(field="user_id", check_type="is_unique")
        metric = UniquenessAnalyzer.analyze(df_base, rule)
        assert metric.value == pytest.approx(1.0)
        assert metric.metadata["duplicate_count"] == 0

    def test_duplicates_detected(self):
        df = pl.DataFrame({"id": [1, 2, 2, 3, 3]})
        rule = RuleDefinition(field="id", check_type="is_unique")
        metric = UniquenessAnalyzer.analyze(df, rule)
        assert metric.value < 1.0
        assert metric.metadata["duplicate_count"] == 4

    def test_missing_field_raises(self, df_base):
        rule = RuleDefinition(field="nonexistent", check_type="is_unique")
        with pytest.raises(KeyError):
            UniquenessAnalyzer.analyze(df_base, rule)


class TestMultiFieldUniquenessAnalyzer:

    def test_all_unique(self, df_base):
        rule = RuleDefinition(field=["user_id", "email"], check_type="are_unique")
        metric = MultiFieldUniquenessAnalyzer.analyze(df_base, rule)
        assert metric.value == pytest.approx(1.0)

    def test_duplicates_detected(self):
        df = pl.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
        rule = RuleDefinition(field=["a", "b"], check_type="are_unique")
        metric = MultiFieldUniquenessAnalyzer.analyze(df, rule)
        assert metric.value < 1.0


# ComparisonAnalyzer


class TestComparisonAnalyzer:

    def test_is_positive_pass(self):
        df = pl.DataFrame({"revenue": [1.0, 2.0, 3.0]})
        rule = RuleDefinition(field="revenue", check_type="is_positive")
        metric = ComparisonAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(1.0)

    def test_is_positive_fail(self):
        df = pl.DataFrame({"revenue": [1.0, -1.0, 3.0]})
        rule = RuleDefinition(field="revenue", check_type="is_positive")
        metric = ComparisonAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(2 / 3)

    def test_is_greater_than(self):
        df = pl.DataFrame({"score": [80, 90, 40, 70]})
        rule = RuleDefinition(field="score", check_type="is_greater_than", value=50)
        metric = ComparisonAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(0.75)

    def test_is_less_than(self):
        df = pl.DataFrame({"age": [20, 30, 150, 45]})
        rule = RuleDefinition(field="age", check_type="is_less_than", value=100)
        metric = ComparisonAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(0.75)

    def test_is_equal(self):
        df = pl.DataFrame({"score": [100, 100, 90, 100]})
        rule = RuleDefinition(field="score", check_type="is_equal", value=100)
        metric = ComparisonAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(0.75)

    def test_is_negative(self):
        df = pl.DataFrame({"val": [-1.0, -2.0, -3.0]})
        rule = RuleDefinition(field="val", check_type="is_negative")
        metric = ComparisonAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(1.0)

    def test_is_in_millions(self):
        df = pl.DataFrame({"val": [1_000_000, 2_000_000, 500, 1_500_000]})
        rule = RuleDefinition(field="val", check_type="is_in_millions")
        metric = ComparisonAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(0.75)

    def test_missing_field_raises(self, df_base):
        rule = RuleDefinition(field="nonexistent", check_type="is_positive")
        with pytest.raises(KeyError):
            ComparisonAnalyzer.analyze(df_base, rule)


class TestBetweenAnalyzer:

    def test_all_within_range(self):
        df = pl.DataFrame({"age": [20, 30, 40, 50]})
        rule = RuleDefinition(field="age", check_type="is_between", value=[18, 120])
        metric = BetweenAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(1.0)

    def test_some_out_of_range(self):
        df = pl.DataFrame({"age": [20, 30, 5, 200]})
        rule = RuleDefinition(field="age", check_type="is_between", value=[18, 120])
        metric = BetweenAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(0.5)

    def test_invalid_bounds_raises(self):
        df = pl.DataFrame({"age": [20, 30]})
        rule = RuleDefinition(field="age", check_type="is_between", value=50)
        with pytest.raises(ValueError):
            BetweenAnalyzer.analyze(df, rule)


# MembershipAnalyzer


class TestMembershipAnalyzer:

    def test_is_contained_in_pass(self):
        df = pl.DataFrame({"status": ["active", "inactive", "pending"]})
        rule = RuleDefinition(
            field="status",
            check_type="is_contained_in",
            value=["active", "inactive", "pending"],
        )
        metric = MembershipAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(1.0)

    def test_is_contained_in_fail(self):
        df = pl.DataFrame({"status": ["active", "banned", "pending"]})
        rule = RuleDefinition(
            field="status",
            check_type="is_contained_in",
            value=["active", "inactive", "pending"],
        )
        metric = MembershipAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(2 / 3)

    def test_not_contained_in_pass(self):
        df = pl.DataFrame({"status": ["active", "inactive", "pending"]})
        rule = RuleDefinition(
            field="status", check_type="not_contained_in", value=["banned", "deleted"]
        )
        metric = MembershipAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(1.0)

    def test_non_list_value_raises(self):
        df = pl.DataFrame({"status": ["active"]})
        rule = RuleDefinition(
            field="status", check_type="is_contained_in", value="active"
        )
        with pytest.raises(ValueError):
            MembershipAnalyzer.analyze(df, rule)


# PatternAnalyzer / LegitAnalyzer


class TestPatternAnalyzer:

    def test_all_match(self):
        df = pl.DataFrame({"email": ["a@a.com", "b@b.com", "c@c.com"]})
        rule = RuleDefinition(
            field="email",
            check_type="has_pattern",
            value=r"[\w.-]+@[\w.-]+\.[a-zA-Z]{2,}",
        )
        metric = PatternAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(1.0)

    def test_some_fail(self):
        df = pl.DataFrame({"email": ["a@a.com", "not-an-email", "c@c.com"]})
        rule = RuleDefinition(
            field="email",
            check_type="has_pattern",
            value=r"[\w.-]+@[\w.-]+\.[a-zA-Z]{2,}",
        )
        metric = PatternAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(2 / 3)

    def test_missing_pattern_raises(self):
        df = pl.DataFrame({"email": ["a@a.com"]})
        rule = RuleDefinition(field="email", check_type="has_pattern", value=None)
        with pytest.raises(ValueError):
            PatternAnalyzer.analyze(df, rule)


class TestLegitAnalyzer:

    def test_all_legit(self):
        df = pl.DataFrame({"notes": ["ok", "valid", "also ok"]})
        rule = RuleDefinition(field="notes", check_type="is_legit")
        metric = LegitAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(1.0)

    def test_whitespace_fails(self):
        df = pl.DataFrame({"notes": ["ok", "  ", "valid"]})
        rule = RuleDefinition(field="notes", check_type="is_legit")
        metric = LegitAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(2 / 3)

    def test_none_fails(self):
        df = pl.DataFrame({"notes": ["ok", None, "valid"]})
        rule = RuleDefinition(field="notes", check_type="is_legit")
        metric = LegitAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(2 / 3)


# DateAnalyzer


class TestDateAnalyzer:

    def test_is_today_pass(self):
        today = date.today()
        df = pl.DataFrame({"dt": [today, today, today]}, schema={"dt": pl.Date})
        rule = RuleDefinition(field="dt", check_type="is_today")
        metric = DateAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(1.0)

    def test_is_today_fail(self):
        yesterday = date.today() - timedelta(days=1)
        df = pl.DataFrame({"dt": [yesterday, yesterday]}, schema={"dt": pl.Date})
        rule = RuleDefinition(field="dt", check_type="is_today")
        metric = DateAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(0.0)

    def test_is_yesterday_pass(self):
        yesterday = date.today() - timedelta(days=1)
        df = pl.DataFrame({"dt": [yesterday, yesterday]}, schema={"dt": pl.Date})
        rule = RuleDefinition(field="dt", check_type="is_yesterday")
        metric = DateAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(1.0)

    def test_is_past_date_pass(self):
        df = pl.DataFrame(
            {"dt": [date(2020, 1, 1), date(2019, 6, 15)]}, schema={"dt": pl.Date}
        )
        rule = RuleDefinition(field="dt", check_type="is_past_date")
        metric = DateAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(1.0)

    def test_is_future_date_pass(self):
        future = date.today() + timedelta(days=10)
        df = pl.DataFrame({"dt": [future, future]}, schema={"dt": pl.Date})
        rule = RuleDefinition(field="dt", check_type="is_future_date")
        metric = DateAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(1.0)

    def test_is_on_weekday(self):
        monday = date(2024, 1, 1)  # known Monday
        saturday = date(2024, 1, 6)  # known Saturday
        df = pl.DataFrame({"dt": [monday, saturday]}, schema={"dt": pl.Date})
        rule = RuleDefinition(field="dt", check_type="is_on_weekday")
        metric = DateAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(0.5)


class TestDateBetweenAnalyzer:

    def test_all_within_range(self):
        df = pl.DataFrame(
            {"dt": [date(2022, 1, 1), date(2023, 6, 15)]}, schema={"dt": pl.Date}
        )
        rule = RuleDefinition(
            field="dt", check_type="is_date_between", value=["2020-01-01", "2024-12-31"]
        )
        metric = DateBetweenAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(1.0)

    def test_some_outside_range(self):
        df = pl.DataFrame(
            {"dt": [date(2018, 1, 1), date(2023, 6, 15)]}, schema={"dt": pl.Date}
        )
        rule = RuleDefinition(
            field="dt", check_type="is_date_between", value=["2020-01-01", "2024-12-31"]
        )
        metric = DateBetweenAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(0.5)

    def test_invalid_bounds_raises(self):
        df = pl.DataFrame({"dt": [date(2022, 1, 1)]}, schema={"dt": pl.Date})
        rule = RuleDefinition(
            field="dt", check_type="is_date_between", value="2020-01-01"
        )
        with pytest.raises(ValueError):
            DateBetweenAnalyzer.analyze(df, rule)


class TestDateComparisonAnalyzer:

    def test_is_date_after_pass(self):
        df = pl.DataFrame(
            {"dt": [date(2022, 1, 1), date(2023, 6, 15)]}, schema={"dt": pl.Date}
        )
        rule = RuleDefinition(
            field="dt", check_type="is_date_after", value="2020-01-01"
        )
        metric = DateComparisonAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(1.0)

    def test_is_date_before_pass(self):
        df = pl.DataFrame(
            {"dt": [date(2022, 1, 1), date(2023, 6, 15)]}, schema={"dt": pl.Date}
        )
        rule = RuleDefinition(
            field="dt", check_type="is_date_before", value="2025-01-01"
        )
        metric = DateComparisonAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(1.0)

    def test_is_date_after_partial_fail(self):
        df = pl.DataFrame(
            {"dt": [date(2018, 1, 1), date(2023, 6, 15)]}, schema={"dt": pl.Date}
        )
        rule = RuleDefinition(
            field="dt", check_type="is_date_after", value="2020-01-01"
        )
        metric = DateComparisonAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(0.5)


# AggregationAnalyzer


class TestAggregationAnalyzer:

    def test_has_mean(self):
        df = pl.DataFrame({"age": [20, 30, 40, 50, 60]})
        rule = RuleDefinition(field="age", check_type="has_mean", value=40.0)
        metric = AggregationAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(40.0)

    def test_has_min(self):
        df = pl.DataFrame({"age": [20, 30, 40]})
        rule = RuleDefinition(field="age", check_type="has_min", value=20.0)
        metric = AggregationAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(20.0)

    def test_has_max(self):
        df = pl.DataFrame({"age": [20, 30, 40]})
        rule = RuleDefinition(field="age", check_type="has_max", value=40.0)
        metric = AggregationAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(40.0)

    def test_has_sum(self):
        df = pl.DataFrame({"age": [20, 30, 40, 50, 60]})
        rule = RuleDefinition(field="age", check_type="has_sum", value=200.0)
        metric = AggregationAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(200.0)

    def test_has_cardinality(self):
        df = pl.DataFrame({"cat": ["A", "B", "A", "C", "B"]})
        rule = RuleDefinition(field="cat", check_type="has_cardinality", value=3)
        metric = AggregationAnalyzer.analyze(df, rule)
        assert metric.value == pytest.approx(3.0)

    def test_missing_field_raises(self):
        df = pl.DataFrame({"age": [20, 30]})
        rule = RuleDefinition(field="nonexistent", check_type="has_mean", value=25.0)
        with pytest.raises(KeyError):
            AggregationAnalyzer.analyze(df, rule)


# validate() — integration


class TestPolarsValidate:

    def test_returns_validation_report(self, df_base):
        rules = [RuleDefinition(field="user_id", check_type="is_unique")]
        report = validate(df_base, rules)
        from sumeh.core.models.validation import ValidationReport

        assert isinstance(report, ValidationReport)

    def test_engine_is_polars(self, df_base):
        rules = [RuleDefinition(field="user_id", check_type="is_unique")]
        report = validate(df_base, rules)
        assert report.engine == "polars"

    def test_total_rows_correct(self, df_base):
        rules = [RuleDefinition(field="user_id", check_type="is_unique")]
        report = validate(df_base, rules)
        assert report.total_rows == len(df_base)

    def test_all_rules_pass(self):
        df = pl.DataFrame(
            {
                "user_id": [1, 2, 3],
                "email": ["a@a.com", "b@b.com", "c@c.com"],
            }
        )
        rules = [
            RuleDefinition(field="user_id", check_type="is_unique"),
            RuleDefinition(field="email", check_type="is_complete"),
        ]
        report = validate(df, rules)
        assert report.pass_rate == pytest.approx(1.0)
        assert len(report.failed) == 0

    def test_rule_fails_correctly(self):
        df = pl.DataFrame({"email": ["a@a.com", None, "c@c.com"]})
        rules = [RuleDefinition(field="email", check_type="is_complete", threshold=1.0)]
        report = validate(df, rules)
        assert len(report.failed) == 1

    def test_dq_errors_column_added(self, df_base):
        rules = [RuleDefinition(field="email", check_type="is_complete", threshold=1.0)]
        report = validate(df_base, rules)
        validated_df = report.df
        assert "_dq_errors" in validated_df.columns

    def test_split_returns_good_and_bad(self):
        df = pl.DataFrame(
            {
                "user_id": [1, 2, 3],
                "email": ["a@a.com", None, "c@c.com"],
            }
        )
        rules = [RuleDefinition(field="email", check_type="is_complete", threshold=1.0)]
        report = validate(df, rules)
        good, bad = report.split()
        assert len(good) == 2
        assert len(bad) == 1

    def test_good_df_has_no_dq_errors_column(self):
        df = pl.DataFrame(
            {
                "user_id": [1, 2, 3],
                "email": ["a@a.com", None, "c@c.com"],
            }
        )
        rules = [RuleDefinition(field="email", check_type="is_complete", threshold=1.0)]
        report = validate(df, rules)
        good, _ = report.split()
        assert "_dq_errors" not in good.columns

    def test_table_level_rule_passes(self):
        df = pl.DataFrame({"age": [20, 30, 40, 50, 60]})
        rules = [
            RuleDefinition(
                field="age", check_type="has_mean", value=40.0, threshold=0.0
            )
        ]
        report = validate(df, rules)
        assert report.results[0].status == ValidationStatus.PASS

    def test_empty_rules_returns_empty_report(self, df_base):
        report = validate(df_base, [])
        assert len(report.results) == 0
        assert report.pass_rate == pytest.approx(1.0)

    def test_mixed_row_and_table_rules(self):
        df = pl.DataFrame(
            {
                "user_id": [1, 2, 3, 4, 5],
                "age": [20, 30, 40, 50, 60],
            }
        )
        rules = [
            RuleDefinition(field="user_id", check_type="is_unique"),
            RuleDefinition(
                field="age", check_type="has_mean", value=40.0, threshold=0.0
            ),
        ]
        report = validate(df, rules)
        assert len(report.results) == 2
        assert all(r.status == ValidationStatus.PASS for r in report.results)

    def test_row_id_column_removed_after_validate(self, df_base):
        rules = [RuleDefinition(field="user_id", check_type="is_unique")]
        report = validate(df_base, rules)
        assert "_row_id" not in report.df.columns
