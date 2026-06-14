"""
Tests for sumeh/exporters/openmetadata.py

Covers:
- OpenMetadataExport.__init__
- profile()
- validation() — definitions + results
- test_definitions()
- test_results()
- _test_name()
- _entity_link()
- _map_test_definition()
"""

import pytest

from sumeh.core.models.validation import (
    ValidationResult,
    ValidationReport,
    ValidationStatus,
    ValidationLevel,
)
from sumeh.exporters.openmetadata import OpenMetadataExport

# Helpers

TABLE_FQN = "iceberg.bronze.stg_transactions"


def make_result(
    check_type="is_complete",
    field="email",
    status=ValidationStatus.PASS,
    pass_rate=1.0,
    level=ValidationLevel.ROW,
) -> ValidationResult:
    return ValidationResult(
        check_type=check_type,
        field=field,
        status=status,
        pass_rate=pass_rate,
        level=level,
    )


def make_report(results=None) -> ValidationReport:
    return ValidationReport(
        results=results or [],
        total_rows=100,
        execution_time_ms=10.0,
        engine="pandas",
    )


def make_profile_data(rows=1000, columns=5):
    return {
        "table_stats": {
            "total_rows": rows,
            "columns_count": columns,
        },
        "column_profiles": {
            "email": {
                "null_count": 10,
                "distinct_count": 990,
                "min": None,
                "max": None,
                "mean": None,
                "sum": None,
            },
            "age": {
                "null_count": 0,
                "distinct_count": 50,
                "min": 18,
                "max": 90,
                "mean": 35.5,
                "sum": 35500,
            },
        },
    }


# __init__


class TestOpenMetadataExportInit:

    def test_table_fqn_stored(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        assert exporter.table_fqn == TABLE_FQN

    def test_implements_iexporter(self):
        from sumeh.core.base.protocols import IExporter

        exporter = OpenMetadataExport(TABLE_FQN)
        assert isinstance(exporter, IExporter)


# profile()


class TestOpenMetadataProfile:

    def test_returns_dict(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        result = exporter.profile(make_profile_data())
        assert isinstance(result, dict)

    def test_required_keys_present(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        result = exporter.profile(make_profile_data())
        assert "timestamp" in result
        assert "rowCount" in result
        assert "columnCount" in result
        assert "columnProfile" in result

    def test_row_count_correct(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        result = exporter.profile(make_profile_data(rows=500))
        assert result["rowCount"] == 500

    def test_column_count_correct(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        result = exporter.profile(make_profile_data(columns=12))
        assert result["columnCount"] == 12

    def test_column_profile_is_list(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        result = exporter.profile(make_profile_data())
        assert isinstance(result["columnProfile"], list)

    def test_column_profile_length(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        result = exporter.profile(make_profile_data())
        assert len(result["columnProfile"]) == 2

    def test_column_profile_has_required_keys(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        result = exporter.profile(make_profile_data())
        for col in result["columnProfile"]:
            for key in ["name", "timestamp", "nullCount", "distinctCount"]:
                assert key in col, f"Missing key: {key}"

    def test_column_profile_name_correct(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        result = exporter.profile(make_profile_data())
        names = {c["name"] for c in result["columnProfile"]}
        assert "email" in names
        assert "age" in names

    def test_numeric_column_has_mean(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        result = exporter.profile(make_profile_data())
        age_col = next(c for c in result["columnProfile"] if c["name"] == "age")
        assert age_col["mean"] == pytest.approx(35.5)

    def test_none_min_stays_none(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        result = exporter.profile(make_profile_data())
        email_col = next(c for c in result["columnProfile"] if c["name"] == "email")
        assert email_col["min"] is None

    def test_numeric_min_converted_to_string(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        result = exporter.profile(make_profile_data())
        age_col = next(c for c in result["columnProfile"] if c["name"] == "age")
        assert age_col["min"] == "18"

    def test_timestamp_is_int(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        result = exporter.profile(make_profile_data())
        assert isinstance(result["timestamp"], int)

    def test_empty_profile_data(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        result = exporter.profile({"table_stats": {}, "column_profiles": {}})
        assert result["rowCount"] is None
        assert result["columnProfile"] == []


# validation()


class TestOpenMetadataValidation:

    def test_returns_dict_with_definitions_and_results(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        report = make_report([make_result()])
        result = exporter.validation(report)
        assert "definitions" in result
        assert "results" in result

    def test_definitions_is_list(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        report = make_report([make_result()])
        result = exporter.validation(report)
        assert isinstance(result["definitions"], list)

    def test_results_is_list(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        report = make_report([make_result()])
        result = exporter.validation(report)
        assert isinstance(result["results"], list)

    def test_counts_match_report_results(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        results = [make_result(), make_result(check_type="is_unique", field="user_id")]
        report = make_report(results)
        payload = exporter.validation(report)
        assert len(payload["definitions"]) == 2
        assert len(payload["results"]) == 2

    def test_empty_report(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        report = make_report([])
        payload = exporter.validation(report)
        assert payload["definitions"] == []
        assert payload["results"] == []


# test_definitions()


class TestOpenMetadataTestDefinitions:

    def test_definition_has_required_keys(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        report = make_report([make_result()])
        defs = exporter.test_definitions(report)
        for key in ["name", "description", "testDefinition", "entityLink", "testSuite"]:
            assert key in defs[0], f"Missing key: {key}"

    def test_test_suite_contains_table_fqn(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        report = make_report([make_result()])
        defs = exporter.test_definitions(report)
        assert TABLE_FQN in defs[0]["testSuite"]

    def test_description_contains_check_type(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        report = make_report([make_result(check_type="is_complete")])
        defs = exporter.test_definitions(report)
        assert "is_complete" in defs[0]["description"]

    def test_name_contains_check_type_and_field(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        report = make_report([make_result(check_type="is_complete", field="email")])
        defs = exporter.test_definitions(report)
        assert "is_complete" in defs[0]["name"]
        assert "email" in defs[0]["name"]


# test_results()


class TestOpenMetadataTestResults:

    def test_result_has_required_keys(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        report = make_report([make_result()])
        results = exporter.test_results(report)
        assert "test_case_fqn" in results[0]
        assert "payload" in results[0]

    def test_payload_has_required_keys(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        report = make_report([make_result()])
        results = exporter.test_results(report)
        payload = results[0]["payload"]
        for key in ["timestamp", "testCaseStatus", "result"]:
            assert key in payload, f"Missing key: {key}"

    def test_pass_maps_to_success(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        report = make_report([make_result(status=ValidationStatus.PASS)])
        results = exporter.test_results(report)
        assert results[0]["payload"]["testCaseStatus"] == "Success"

    def test_fail_maps_to_failed(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        report = make_report([make_result(status=ValidationStatus.FAIL)])
        results = exporter.test_results(report)
        assert results[0]["payload"]["testCaseStatus"] == "Failed"

    def test_error_maps_to_failed(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        report = make_report([make_result(status=ValidationStatus.ERROR)])
        results = exporter.test_results(report)
        assert results[0]["payload"]["testCaseStatus"] == "Failed"

    def test_fqn_contains_table_and_test_name(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        report = make_report([make_result(check_type="is_complete", field="email")])
        results = exporter.test_results(report)
        assert TABLE_FQN in results[0]["test_case_fqn"]
        assert "is_complete" in results[0]["test_case_fqn"]

    def test_timestamp_is_int(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        report = make_report([make_result()])
        results = exporter.test_results(report)
        assert isinstance(results[0]["payload"]["timestamp"], int)


# _test_name()


class TestTestName:

    def test_row_level_includes_field(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        result = make_result(check_type="is_complete", field="email")
        name = exporter._test_name(result)
        assert "is_complete" in name
        assert "email" in name

    def test_table_level_uses_table(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        result = make_result(check_type="has_mean", field=None)
        result.field = None
        name = exporter._test_name(result)
        assert "has_mean" in name

    def test_name_starts_with_sumeh(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        result = make_result()
        name = exporter._test_name(result)
        assert name.startswith("sumeh_")


# _entity_link()


class TestEntityLink:

    def test_row_level_includes_column(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        result = make_result(field="email", level=ValidationLevel.ROW)
        link = exporter._entity_link(result)
        assert "email" in link
        assert TABLE_FQN in link

    def test_table_level_no_column(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        result = make_result(level=ValidationLevel.TABLE)
        link = exporter._entity_link(result)
        assert TABLE_FQN in link
        assert "columns" not in link

    def test_link_format_row(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        result = make_result(field="email", level=ValidationLevel.ROW)
        link = exporter._entity_link(result)
        assert link.startswith("<#E::table::")
        assert link.endswith(">")


# _map_test_definition()


class TestMapTestDefinition:

    def test_known_mapping_is_complete(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        assert exporter._map_test_definition("is_complete") == "columnValuesToBeNotNull"

    def test_known_mapping_is_positive(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        assert exporter._map_test_definition("is_positive") == "columnValuesToBeAtLeast"

    def test_known_mapping_is_contained_in(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        assert (
            exporter._map_test_definition("is_contained_in") == "columnValuesToBeInSet"
        )

    def test_unknown_check_type_returns_default(self):
        exporter = OpenMetadataExport(TABLE_FQN)
        result = exporter._map_test_definition("some_unknown_check")
        assert result == "columnValuesToBeNotNull"
