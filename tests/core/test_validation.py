"""
Tests for sumeh/core/models/validation.py

Covers:
- ValidationStatus, ValidationLevel enums
- ValidationResult: instantiation, defaults, repr
- ValidationReport: properties, summary, split, df, dunder methods
"""

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from sumeh.core.models.validation import (
    ValidationLevel,
    ValidationStatus,
    ValidationResult,
    ValidationReport,
)

# Helpers


def make_result(
    check_type="is_complete",
    field="email",
    status=ValidationStatus.PASS,
    pass_rate=1.0,
    violating_row_ids=None,
    message=None,
    level=ValidationLevel.ROW,
    category="completeness",
) -> ValidationResult:
    return ValidationResult(
        check_type=check_type,
        field=field,
        status=status,
        pass_rate=pass_rate,
        violating_row_ids=violating_row_ids or [],
        message=message,
        level=level,
        category=category,
    )


def make_report(results=None, total_rows=100, engine="pandas") -> ValidationReport:
    return ValidationReport(
        results=results or [],
        total_rows=total_rows,
        execution_time_ms=12.5,
        engine=engine,
    )


# Enums


class TestEnums:

    def test_validation_status_values(self):
        assert ValidationStatus.PASS.value == "PASS"
        assert ValidationStatus.FAIL.value == "FAIL"
        assert ValidationStatus.ERROR.value == "ERROR"

    def test_validation_level_values(self):
        assert ValidationLevel.ROW.value == "ROW"
        assert ValidationLevel.TABLE.value == "TABLE"


# ValidationResult


class TestValidationResult:

    def test_instantiation_defaults(self):
        result = ValidationResult()
        assert result.status == ValidationStatus.ERROR
        assert result.violating_row_ids == []
        assert result.metadata == {}
        assert result.pass_rate is None
        assert result.message is None

    def test_id_auto_generated(self):
        r1 = ValidationResult()
        r2 = ValidationResult()
        assert r1.id != r2.id

    def test_timestamp_auto_generated(self):
        result = ValidationResult()
        assert isinstance(result.timestamp, datetime)

    def test_pass_result(self):
        result = make_result(status=ValidationStatus.PASS, pass_rate=1.0)
        assert result.status == ValidationStatus.PASS
        assert result.pass_rate == pytest.approx(1.0)

    def test_fail_result(self):
        result = make_result(
            status=ValidationStatus.FAIL,
            pass_rate=0.8,
            violating_row_ids=[2, 5, 7],
            message="3 null values found",
        )
        assert result.status == ValidationStatus.FAIL
        assert len(result.violating_row_ids) == 3
        assert result.message == "3 null values found"

    def test_repr_contains_check_type_and_field(self):
        result = make_result(check_type="is_complete", field="email")
        r = repr(result)
        assert "is_complete" in r
        assert "email" in r

    def test_repr_contains_status(self):
        result = make_result(status=ValidationStatus.FAIL)
        assert "FAIL" in repr(result)

    def test_level_default_is_row(self):
        result = ValidationResult()
        assert result.level == ValidationLevel.ROW

    def test_metadata_independent_per_instance(self):
        r1 = ValidationResult()
        r2 = ValidationResult()
        r1.metadata["key"] = "value"
        assert "key" not in r2.metadata


# ValidationReport — properties


class TestValidationReportProperties:

    def test_passed_filters_correctly(self):
        results = [
            make_result(status=ValidationStatus.PASS),
            make_result(status=ValidationStatus.FAIL),
            make_result(status=ValidationStatus.PASS),
        ]
        report = make_report(results)
        assert len(report.passed) == 2

    def test_failed_filters_correctly(self):
        results = [
            make_result(status=ValidationStatus.PASS),
            make_result(status=ValidationStatus.FAIL),
            make_result(status=ValidationStatus.FAIL),
        ]
        report = make_report(results)
        assert len(report.failed) == 2

    def test_errors_filters_correctly(self):
        results = [
            make_result(status=ValidationStatus.PASS),
            make_result(status=ValidationStatus.ERROR),
        ]
        report = make_report(results)
        assert len(report.errors) == 1

    def test_pass_rate_all_pass(self):
        results = [make_result(status=ValidationStatus.PASS) for _ in range(5)]
        report = make_report(results)
        assert report.pass_rate == pytest.approx(1.0)

    def test_pass_rate_all_fail(self):
        results = [make_result(status=ValidationStatus.FAIL) for _ in range(4)]
        report = make_report(results)
        assert report.pass_rate == pytest.approx(0.0)

    def test_pass_rate_mixed(self):
        results = [
            make_result(status=ValidationStatus.PASS),
            make_result(status=ValidationStatus.PASS),
            make_result(status=ValidationStatus.FAIL),
            make_result(status=ValidationStatus.FAIL),
        ]
        report = make_report(results)
        assert report.pass_rate == pytest.approx(0.5)

    def test_pass_rate_empty_results_is_one(self):
        report = make_report(results=[])
        assert report.pass_rate == pytest.approx(1.0)


# ValidationReport — summary


class TestValidationReportSummary:

    def test_summary_returns_dict(self):
        report = make_report([make_result()])
        s = report.summary()
        assert isinstance(s, dict)

    def test_summary_required_keys(self):
        report = make_report([make_result()])
        s = report.summary()
        for key in [
            "timestamp",
            "engine",
            "total_rows",
            "pass_rate",
            "passed",
            "failed",
            "errors",
            "validations",
        ]:
            assert key in s, f"Missing key: {key}"

    def test_summary_counts_correct(self):
        results = [
            make_result(status=ValidationStatus.PASS),
            make_result(status=ValidationStatus.FAIL),
            make_result(status=ValidationStatus.ERROR),
        ]
        report = make_report(results)
        s = report.summary()
        assert s["passed"] == 1
        assert s["failed"] == 1
        assert s["errors"] == 1
        assert s["total_validations"] == 3

    def test_summary_validations_list(self):
        results = [make_result(check_type="is_complete", field="email")]
        report = make_report(results)
        s = report.summary()
        assert len(s["validations"]) == 1
        v = s["validations"][0]
        assert v["check_type"] == "is_complete"
        assert v["field"] == "email"

    def test_summary_sample_ids_capped(self):
        ids = list(range(200))
        result = make_result(status=ValidationStatus.FAIL, violating_row_ids=ids)
        report = make_report([result])
        s = report.summary(max_sample_ids=50)
        assert len(s["validations"][0]["sample_violating_ids"]) == 50

    def test_summary_engine_preserved(self):
        report = make_report(engine="duckdb")
        assert report.summary()["engine"] == "duckdb"

    def test_summary_total_rows_preserved(self):
        report = make_report(total_rows=9999)
        assert report.summary()["total_rows"] == 9999


# ValidationReport — df / split


class TestValidationReportDataFrame:

    def test_df_none_when_no_wrapper(self):
        report = make_report()
        assert report.df is None

    def test_get_validated_df_none_when_no_wrapper(self):
        report = make_report()
        assert report.get_validated_df() is None

    def test_df_calls_to_native(self):
        mock_wrapper = MagicMock()
        mock_wrapper.to_native.return_value = "native_df"
        report = make_report()
        report.df_validated = mock_wrapper
        assert report.df == "native_df"
        mock_wrapper.to_native.assert_called_once()

    def test_split_raises_when_no_wrapper(self):
        report = make_report()
        with pytest.raises(ValueError, match="No validated DataFrame"):
            report.split()

    def test_split_raises_when_wrapper_has_no_split(self):
        report = make_report()
        report.df_validated = object()  # no split_by_errors
        with pytest.raises(AttributeError, match="split_by_errors"):
            report.split()

    def test_split_delegates_to_wrapper(self):
        mock_wrapper = MagicMock()
        mock_wrapper.split_by_errors.return_value = ("good", "bad")
        report = make_report()
        report.df_validated = mock_wrapper
        good, bad = report.split()
        assert good == "good"
        assert bad == "bad"

    def test_get_good_df_shortcut(self):
        mock_wrapper = MagicMock()
        mock_wrapper.split_by_errors.return_value = ("good_df", "bad_df")
        report = make_report()
        report.df_validated = mock_wrapper
        assert report.get_good_df() == "good_df"

    def test_get_bad_df_shortcut(self):
        mock_wrapper = MagicMock()
        mock_wrapper.split_by_errors.return_value = ("good_df", "bad_df")
        report = make_report()
        report.df_validated = mock_wrapper
        assert report.get_bad_df() == "bad_df"


# ValidationReport — dunder methods


class TestValidationReportDunders:

    def test_len(self):
        results = [make_result() for _ in range(4)]
        report = make_report(results)
        assert len(report) == 4

    def test_iter(self):
        results = [make_result() for _ in range(3)]
        report = make_report(results)
        assert list(report) == results

    def test_getitem(self):
        results = [make_result(field=f"col_{i}") for i in range(3)]
        report = make_report(results)
        assert report[0].field == "col_0"
        assert report[2].field == "col_2"

    def test_repr_contains_key_info(self):
        results = [
            make_result(status=ValidationStatus.PASS),
            make_result(status=ValidationStatus.FAIL),
        ]
        report = make_report(results)
        r = repr(report)
        assert "2" in r
        assert "fail" in r.lower() or "FAIL" in r
