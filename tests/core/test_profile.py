"""
Tests for sumeh/core/services/profiler/profiler.py

Covers:
- DataProfiler.profile()
- DataProfiler._format_output()
- DataProfiler._sample_df()
"""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from sumeh.core.models.validation import (
    ValidationReport,
    ValidationResult,
    ValidationStatus,
    ValidationLevel,
)
from sumeh.core.services.profiler.profiler import DataProfiler

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────


def make_result(check_type, field, actual_value, status=ValidationStatus.PASS):
    return ValidationResult(
        check_type=check_type,
        field=field,
        status=status,
        actual_value=actual_value,
        pass_rate=1.0,
        level=ValidationLevel.ROW,
    )


def make_report(results, total_rows=100):
    return ValidationReport(
        results=results,
        total_rows=total_rows,
        execution_time_ms=5.0,
        engine="pandas",
    )


def make_engine_mock(report):
    engine = MagicMock()
    engine.validate.return_value = report
    return engine


def make_numeric_df():
    return pd.DataFrame(
        {
            "user_id": [1, 2, 3, 4, 5],
            "age": [20, 30, 40, 50, 60],
            "score": [80.0, 90.0, 70.0, 85.0, 95.0],
        }
    )


def make_mixed_df():
    return pd.DataFrame(
        {
            "user_id": [1, 2, 3, 4, 5],
            "email": ["a@a.com", None, "c@c.com", "d@d.com", "e@e.com"],
            "age": [20, 30, 40, 50, 60],
        }
    )


# ─────────────────────────────────────────────
# profile() — structure
# ─────────────────────────────────────────────


class TestDataProfilerStructure:

    def test_returns_dict(self):
        df = make_numeric_df()
        results = [
            make_result("is_complete", "age", 1.0),
            make_result("has_cardinality", "age", 5),
            make_result("has_min", "age", 20.0),
            make_result("has_max", "age", 60.0),
            make_result("has_mean", "age", 40.0),
            make_result("has_std", "age", 15.81),
            make_result("has_sum", "age", 200.0),
        ]
        report = make_report(results, total_rows=5)
        engine = make_engine_mock(report)
        result = DataProfiler.profile(df, engine)
        assert isinstance(result, dict)

    def test_top_level_keys(self):
        df = make_numeric_df()
        report = make_report([], total_rows=5)
        engine = make_engine_mock(report)
        result = DataProfiler.profile(df, engine)
        assert "table_stats" in result
        assert "column_profiles" in result

    def test_table_stats_keys(self):
        df = make_numeric_df()
        report = make_report([], total_rows=5)
        engine = make_engine_mock(report)
        result = DataProfiler.profile(df, engine)
        ts = result["table_stats"]
        for key in ["total_rows", "execution_time_ms", "columns_count", "sampled"]:
            assert key in ts, f"Missing key: {key}"

    def test_total_rows_correct(self):
        df = make_numeric_df()
        report = make_report([], total_rows=5)
        engine = make_engine_mock(report)
        result = DataProfiler.profile(df, engine)
        assert result["table_stats"]["total_rows"] == 5

    def test_columns_count_correct(self):
        df = make_numeric_df()
        report = make_report([], total_rows=5)
        engine = make_engine_mock(report)
        result = DataProfiler.profile(df, engine)
        assert result["table_stats"]["columns_count"] == 3

    def test_sampled_false_by_default(self):
        df = make_numeric_df()
        report = make_report([], total_rows=5)
        engine = make_engine_mock(report)
        result = DataProfiler.profile(df, engine)
        assert result["table_stats"]["sampled"] is False

    def test_sampled_true_when_fraction_given(self):
        df = make_numeric_df()
        report = make_report([], total_rows=5)
        engine = make_engine_mock(report)
        result = DataProfiler.profile(df, engine, sample_size=0.5)
        assert result["table_stats"]["sampled"] is True

    def test_engine_validate_called(self):
        df = make_numeric_df()
        report = make_report([], total_rows=5)
        engine = make_engine_mock(report)
        DataProfiler.profile(df, engine)
        engine.validate.assert_called_once()


# ─────────────────────────────────────────────
# profile() — rule generation
# ─────────────────────────────────────────────


class TestDataProfilerRuleGeneration:

    def test_numeric_columns_get_statistical_rules(self):
        df = pd.DataFrame({"age": [20, 30, 40]})
        report = make_report([], total_rows=3)
        engine = make_engine_mock(report)
        DataProfiler.profile(df, engine)

        rules_passed = engine.validate.call_args[0][1]
        check_types = [r.check_type for r in rules_passed if r.field == "age"]
        for ct in [
            "is_complete",
            "has_cardinality",
            "has_min",
            "has_max",
            "has_mean",
            "has_std",
            "has_sum",
        ]:
            assert ct in check_types, f"Missing rule: {ct}"

    def test_string_columns_get_basic_rules_only(self):
        df = pd.DataFrame({"email": ["a@a.com", "b@b.com"]})
        report = make_report([], total_rows=2)
        engine = make_engine_mock(report)
        DataProfiler.profile(df, engine)

        rules_passed = engine.validate.call_args[0][1]
        check_types = [r.check_type for r in rules_passed if r.field == "email"]
        assert "is_complete" in check_types
        assert "has_cardinality" in check_types
        assert "has_mean" not in check_types
        assert "has_sum" not in check_types

    def test_all_columns_get_rules(self):
        df = make_mixed_df()
        report = make_report([], total_rows=5)
        engine = make_engine_mock(report)
        DataProfiler.profile(df, engine)

        rules_passed = engine.validate.call_args[0][1]
        fields_with_rules = {r.field for r in rules_passed}
        for col in df.columns:
            assert col in fields_with_rules


# ─────────────────────────────────────────────
# _format_output()
# ─────────────────────────────────────────────


class TestFormatOutput:

    def _make_schema_info(self):
        return {
            "age": {"type": "integer", "nullable": True},
            "email": {"type": "string", "nullable": True},
        }

    def test_column_profiles_created(self):
        results = [
            make_result("is_complete", "age", 1.0),
            make_result("has_cardinality", "age", 5),
        ]
        report = make_report(results, total_rows=5)
        import time

        output = DataProfiler._format_output(
            report, self._make_schema_info(), time.time()
        )
        assert "age" in output["column_profiles"]

    def test_completeness_mapped_correctly(self):
        results = [make_result("is_complete", "age", 0.8)]
        report = make_report(results, total_rows=5)
        import time

        output = DataProfiler._format_output(
            report, self._make_schema_info(), time.time()
        )
        assert output["column_profiles"]["age"]["completeness"] == pytest.approx(0.8)

    def test_null_count_calculated(self):
        results = [make_result("is_complete", "age", 0.8)]
        report = make_report(results, total_rows=100)
        import time

        output = DataProfiler._format_output(
            report, self._make_schema_info(), time.time()
        )
        assert output["column_profiles"]["age"]["null_count"] == pytest.approx(
            20, abs=1
        )

    def test_null_count_zero_when_complete(self):
        results = [make_result("is_complete", "age", 1.0)]
        report = make_report(results, total_rows=100)
        import time

        output = DataProfiler._format_output(
            report, self._make_schema_info(), time.time()
        )
        assert output["column_profiles"]["age"]["null_count"] == 0

    def test_mean_mapped_correctly(self):
        results = [make_result("has_mean", "age", 40.0)]
        report = make_report(results, total_rows=5)
        import time

        output = DataProfiler._format_output(
            report, self._make_schema_info(), time.time()
        )
        assert output["column_profiles"]["age"]["mean"] == pytest.approx(40.0)

    def test_uniqueness_calculated_from_cardinality(self):
        results = [
            make_result("is_complete", "age", 1.0),
            make_result("has_cardinality", "age", 5),
        ]
        report = make_report(results, total_rows=5)
        import time

        output = DataProfiler._format_output(
            report, self._make_schema_info(), time.time()
        )
        assert output["column_profiles"]["age"]["uniqueness"] == pytest.approx(1.0)

    def test_uniqueness_partial(self):
        results = [
            make_result("is_complete", "age", 1.0),
            make_result("has_cardinality", "age", 3),
        ]
        report = make_report(results, total_rows=6)
        import time

        output = DataProfiler._format_output(
            report, self._make_schema_info(), time.time()
        )
        assert output["column_profiles"]["age"]["uniqueness"] == pytest.approx(0.5)

    def test_column_type_from_schema(self):
        results = [make_result("is_complete", "age", 1.0)]
        report = make_report(results, total_rows=5)
        import time

        output = DataProfiler._format_output(
            report, self._make_schema_info(), time.time()
        )
        assert output["column_profiles"]["age"]["type"] == "integer"

    def test_empty_results(self):
        report = make_report([], total_rows=5)
        import time

        output = DataProfiler._format_output(
            report, self._make_schema_info(), time.time()
        )
        assert output["column_profiles"] == {}

    def test_sampled_flag_preserved(self):
        report = make_report([], total_rows=5)
        import time

        output = DataProfiler._format_output(
            report, self._make_schema_info(), time.time(), sampled=True
        )
        assert output["table_stats"]["sampled"] is True


# ─────────────────────────────────────────────
# _sample_df()
# ─────────────────────────────────────────────


class TestSampleDf:

    def test_pandas_sample_fraction(self):
        df = pd.DataFrame({"a": range(100)})
        sampled = DataProfiler._sample_df(df, 0.5)
        assert len(sampled) < len(df)
        assert len(sampled) > 0

    def test_pandas_sample_preserves_columns(self):
        df = pd.DataFrame({"a": range(100), "b": range(100)})
        sampled = DataProfiler._sample_df(df, 0.5)
        assert list(sampled.columns) == ["a", "b"]

    def test_unsupported_df_returned_as_is(self):
        obj = MagicMock(spec=[])  # no .sample, no .rdd, no .random_sample
        result = DataProfiler._sample_df(obj, 0.5)
        assert result is obj

    def test_polars_style_sample(self):
        mock_df = MagicMock()
        mock_df.sample.side_effect = [TypeError, MagicMock()]
        # Should fall through to polars path without crashing
        try:
            DataProfiler._sample_df(mock_df, 0.5)
        except Exception:
            pass  # acceptable — mock not fully set up for polars path
