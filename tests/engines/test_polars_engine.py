# tests/engines/test_polars_engine.py
"""
Quick tests for Polars Engine
"""
from datetime import date, timedelta

import pytest

pytest.importorskip("polars")

import polars as pl

from sumeh.engines.polars_engine import (
    validate,
    validate_row_level,
    validate_table_level,
    summarize,
    is_positive,
    is_negative,
    is_complete,
    is_unique,
    are_unique,
    is_between,
    has_pattern,
    has_max,
    has_min,
    has_mean,
    has_cardinality,
)


@pytest.fixture
def sample_data(sample_pandas_df):
    """Convert pandas sample to Polars DataFrame"""
    return pl.from_pandas(sample_pandas_df)


@pytest.fixture
def empty_data():
    """Empty Polars DataFrame"""
    return pl.DataFrame({
        "id": pl.Series([], dtype=pl.Int64),
        "name": pl.Series([], dtype=pl.Utf8),
        "value": pl.Series([], dtype=pl.Float64),
    })


class TestRowLevelFunctions:

    def test_is_positive_finds_violations(self, sample_data, rule_factory):
        rule = rule_factory("age", "is_positive")
        result = is_positive(sample_data, rule)

        assert result.shape[0] == 2  # -30 and -45
        assert "dq_status" in result.columns

    def test_is_negative_finds_violations(self, sample_data, rule_factory):
        rule = rule_factory("age", "is_negative")
        result = is_negative(sample_data, rule)

        assert result.shape[0] == 3  # 25, 35, 40
        assert all(result["age"] >= 0)

    def test_is_complete_finds_nulls(self, sample_data, rule_factory):
        rule = rule_factory("name", "is_complete")
        result = is_complete(sample_data, rule)

        assert result.shape[0] == 1
        assert "dq_status" in result.columns

    def test_is_unique_finds_duplicates(self, sample_data, rule_factory):
        rule = rule_factory("department", "is_unique")
        result = is_unique(sample_data, rule)

        assert result.shape[0] > 0

    def test_are_unique_multiple_fields(self, sample_data, rule_factory):
        rule = rule_factory(["name", "age"], "are_unique")
        result = are_unique(sample_data, rule)

        assert result.shape[0] == 0  # All combinations unique

    def test_is_between(self, sample_data, rule_factory):
        rule = rule_factory("salary", "is_between", value="[3000,10000]")
        result = is_between(sample_data, rule)

        assert result.shape[0] == 0  # All within range

    def test_has_pattern(self, sample_data, rule_factory):
        rule = rule_factory("email", "has_pattern", value=r"^[\w\.-]+@[\w\.-]+\.\w+$")
        result = has_pattern(sample_data, rule)

        assert result.shape[0] >= 1


class TestTableLevelFunctions:

    def test_has_max(self, sample_data, rule_factory):
        rule = rule_factory("salary", "has_max", value=10000, level="TABLE")
        result = has_max(sample_data, rule)

        assert result["status"] == "PASS"
        assert result["actual"] == 9000.0

    def test_has_min(self, sample_data, rule_factory):
        rule = rule_factory("salary", "has_min", value=4000, level="TABLE")
        result = has_min(sample_data, rule)

        assert result["status"] == "PASS"
        assert result["actual"] == 5000.0

    def test_has_mean(self, sample_data, rule_factory):
        rule = rule_factory("salary", "has_mean", value=6000, level="TABLE")
        result = has_mean(sample_data, rule)

        assert result["status"] == "PASS"
        assert result["actual"] == 7000.0

    def test_has_cardinality(self, sample_data, rule_factory):
        rule = rule_factory("department", "has_cardinality", value=3, level="TABLE")
        result = has_cardinality(sample_data, rule)

        assert result["status"] == "PASS"
        assert result["actual"] == 3.0


class TestValidateRowLevel:

    def test_returns_two_dataframes(self, sample_data, rule_factory):
        rules = [rule_factory("age", "is_positive", level="ROW")]

        df_with_status, raw_violations = validate_row_level(sample_data, rules)

        assert df_with_status is not None
        assert raw_violations is not None
        assert "dq_status" in df_with_status.columns

    def test_finds_violations(self, sample_data, rule_factory):
        rules = [rule_factory("age", "is_positive", level="ROW")]

        df_with_status, raw_violations = validate_row_level(sample_data, rules)

        assert df_with_status.shape[0] > 0
        assert raw_violations.shape[0] > 0


class TestValidateTableLevel:

    def test_returns_dataframe(self, sample_data, rule_factory):
        rules = [rule_factory("salary", "has_max", value=10000, level="TABLE")]

        summary = validate_table_level(sample_data, rules)

        assert summary.shape[0] == 1
        assert "status" in summary.columns
        assert "expected" in summary.columns
        assert "actual" in summary.columns


class TestValidate:

    def test_returns_three_values(self, sample_data, rule_factory):
        rules = [rule_factory("age", "is_positive", level="ROW")]

        result = validate(sample_data, rules)

        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_with_row_rules(self, sample_data, rule_factory):
        rules = [
            rule_factory("age", "is_positive", level="ROW"),
            rule_factory("name", "is_complete", level="ROW"),
        ]

        df_with_status, violations, table_summary = validate(sample_data, rules)

        assert violations.shape[0] > 0
        assert "dq_status" in violations.columns

    def test_with_table_rules(self, sample_data, rule_factory):
        rules = [
            rule_factory("salary", "has_max", value=10000, level="TABLE"),
            rule_factory("salary", "has_min", value=4000, level="TABLE"),
        ]

        _, _, table_summary = validate(sample_data, rules)

        assert table_summary.shape[0] == 2

    def test_with_mixed_rules(self, sample_data, rule_factory):
        rules = [
            rule_factory("age", "is_positive", level="ROW"),
            rule_factory("salary", "has_max", value=10000, level="TABLE"),
        ]

        df_with_status, violations, table_summary = validate(sample_data, rules)

        assert violations.shape[0] > 0
        assert table_summary.shape[0] == 1


class TestSummarize:

    def test_with_row_rules(self, sample_data, rule_factory):
        rules = [rule_factory("age", "is_positive", level="ROW")]

        _, violations, _ = validate(sample_data, rules)
        total_rows = sample_data.shape[0]

        summary = summarize(rules, total_rows, df_with_errors=violations)

        assert summary.shape[0] == 1
        assert "pass_rate" in summary.columns
        assert "status" in summary.columns

    def test_with_table_rules(self, sample_data, rule_factory):
        rules = [rule_factory("salary", "has_max", value=10000, level="TABLE")]

        _, _, table_summary = validate(sample_data, rules)
        total_rows = sample_data.shape[0]

        summary = summarize(rules, total_rows, table_error=table_summary)

        assert summary.shape[0] == 1
        assert summary["level"][0] == "TABLE"

    def test_with_mixed_rules(self, sample_data, rule_factory):
        rules = [
            rule_factory("age", "is_positive", level="ROW"),
            rule_factory("salary", "has_max", value=10000, level="TABLE"),
        ]

        _, violations, table_summary = validate(sample_data, rules)
        total_rows = sample_data.shape[0]

        summary = summarize(
            rules,
            total_rows,
            df_with_errors=violations,
            table_error=table_summary
        )

        assert summary.shape[0] == 2


class TestEdgeCases:

    def test_empty_dataframe(self, empty_data, rule_factory):
        rules = [rule_factory("name", "is_complete", level="ROW")]

        df_with_status, violations, _ = validate(empty_data, rules)

        assert violations.shape[0] == 0

    def test_no_rules(self, sample_data):
        df_with_status, violations, table_summary = validate(sample_data, [])

        assert violations.shape[0] == 0
        assert table_summary.shape[0] == 0



# tests/engines/test_polars_engine.py
class TestDateFunctions:

    def test_is_future_date(self, rule_factory):
        today = date.today()
        past = today - timedelta(days=1)
        future = today + timedelta(days=1)

        # Convert to ISO strings
        df = pl.DataFrame({
            "d": [past.isoformat(), today.isoformat(), future.isoformat()]
        })

        rule = rule_factory("d", "is_future_date")

        from sumeh.engines.polars_engine import is_future_date
        result = is_future_date(df, rule)

        assert result.shape[0] == 1
        assert result["d"][0] == future.isoformat()

    def test_is_past_date(self, rule_factory):
        today = date.today()
        past = today - timedelta(days=1)
        future = today + timedelta(days=1)

        # Convert to ISO strings
        df = pl.DataFrame({
            "d": [past.isoformat(), today.isoformat(), future.isoformat()]
        })

        rule = rule_factory("d", "is_past_date")

        from sumeh.engines.polars_engine import is_past_date
        result = is_past_date(df, rule)

        assert result.shape[0] == 1
        assert result["d"][0] == past.isoformat()
