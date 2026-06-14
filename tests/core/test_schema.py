"""
Tests for sumeh/core/services/schema/

Covers:
- models: ColumnDef, SchemaDef, SchemaReport
- validator: _to_canonical_type, _extract_actual_schema, validate, extract_schema
"""

import json

import pandas as pd
import pytest

from sumeh.core.services.schema.models import ColumnDef, SchemaDef, SchemaReport
from sumeh.core.services.schema.validator import (
    _to_canonical_type,
    _extract_actual_schema,
    validate,
    extract_schema,
)


# ColumnDef


class TestColumnDef:

    def test_basic_instantiation(self):
        col = ColumnDef(name="email", expected_type="string")
        assert col.name == "email"
        assert col.expected_type == "string"

    def test_defaults(self):
        col = ColumnDef(name="email", expected_type="string")
        assert col.is_optional is False
        assert col.nullable is True
        assert col.element_type is None
        assert col.require_comment is False
        assert col.expected_comment is None
        assert col.fields is None

    def test_from_dict_string_shorthand(self):
        col = ColumnDef.from_dict("email", "string")
        assert col.name == "email"
        assert col.expected_type == "string"

    def test_from_dict_full_props(self):
        props = {
            "type": "integer",
            "is_optional": True,
            "nullable": False,
            "require_comment": True,
            "expected_comment": "User age",
        }
        col = ColumnDef.from_dict("age", props)
        assert col.expected_type == "integer"
        assert col.is_optional is True
        assert col.nullable is False
        assert col.require_comment is True
        assert col.expected_comment == "User age"

    def test_from_dict_default_type_is_string(self):
        col = ColumnDef.from_dict("notes", {})
        assert col.expected_type == "string"

    def test_from_dict_nested_fields(self):
        props = {
            "type": "complex",
            "fields": {
                "street": "string",
                "city": "string",
            },
        }
        col = ColumnDef.from_dict("address", props)
        assert col.fields is not None
        assert len(col.fields) == 2
        names = {f.name for f in col.fields}
        assert "street" in names
        assert "city" in names

    def test_from_dict_element_type(self):
        col = ColumnDef.from_dict("tags", {"type": "array", "element_type": "string"})
        assert col.element_type == "string"


# SchemaDef


class TestSchemaDef:

    def test_from_dict_creates_columns(self):
        schema = SchemaDef.from_dict({"email": "string", "age": "integer"})
        assert len(schema.columns) == 2

    def test_from_dict_column_names(self):
        schema = SchemaDef.from_dict({"email": "string", "age": "integer"})
        names = {c.name for c in schema.columns}
        assert "email" in names
        assert "age" in names

    def test_strict_false_by_default(self):
        schema = SchemaDef.from_dict({"email": "string"})
        assert schema.strict_columns is False

    def test_strict_true_when_passed(self):
        schema = SchemaDef.from_dict({"email": "string"}, strict=True)
        assert schema.strict_columns is True


# SchemaReport


class TestSchemaReport:

    def test_passed_report(self):
        report = SchemaReport(passed=True)
        assert report.passed is True
        assert bool(report) is True

    def test_failed_report(self):
        report = SchemaReport(passed=False, missing_cols=["email"])
        assert report.passed is False
        assert bool(report) is False

    def test_to_dict_keys(self):
        report = SchemaReport(passed=True)
        d = report.to_dict()
        for key in [
            "passed",
            "missing_columns",
            "type_errors",
            "metadata_errors",
            "extra_columns",
            "total_issues",
        ]:
            assert key in d

    def test_to_dict_total_issues_correct(self):
        report = SchemaReport(
            passed=False,
            missing_cols=["col_a"],
            type_errors={"col_b": "Expected integer, got string"},
            metadata_errors={"col_c": "Missing comment"},
            extra_cols=["col_d", "col_e"],
        )
        assert report.to_dict()["total_issues"] == 5

    def test_to_dict_total_issues_zero_on_pass(self):
        report = SchemaReport(passed=True)
        assert report.to_dict()["total_issues"] == 0

    def test_repr_passed(self):
        report = SchemaReport(passed=True)
        assert "PASSED" in repr(report)

    def test_repr_failed(self):
        report = SchemaReport(passed=False, missing_cols=["col_a"])
        assert "FAILED" in repr(report)

    def test_defaults_are_empty(self):
        report = SchemaReport(passed=True)
        assert report.missing_cols == []
        assert report.type_errors == {}
        assert report.metadata_errors == {}
        assert report.extra_cols == []


# _to_canonical_type


class TestToCanonicalType:

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("int64", "integer"),
            ("int32", "integer"),
            ("int8", "integer"),
            ("Int64", "integer"),
            ("uint8", "integer"),
            ("uint64", "integer"),
            ("float64", "float"),
            ("float32", "float"),
            ("double", "float"),
            ("decimal", "float"),
            ("object", "string"),
            ("str", "string"),
            ("string", "string"),
            ("utf8", "string"),
            ("bool", "boolean"),
            ("boolean", "boolean"),
            ("datetime64", "datetime"),
            ("timestamp", "datetime"),
            ("date", "datetime"),
            ("array", "array"),
            ("struct", "complex"),
            ("totally_unknown_type", "unknown"),
        ],
    )
    def test_canonical_mapping(self, raw, expected):
        assert _to_canonical_type(raw) == expected


# _extract_actual_schema


class TestExtractActualSchema:

    def test_dict_input(self):
        d = {"email": "string", "age": "int64"}
        result = _extract_actual_schema(d)
        assert "email" in result
        assert "age" in result
        assert result["email"]["raw_type"] == "string"

    def test_dict_nullable_true(self):
        result = _extract_actual_schema({"email": "string"})
        assert result["email"]["nullable"] is True

    def test_pandas_df(self):
        df = pd.DataFrame({"email": ["a@a.com"], "age": [25]})
        result = _extract_actual_schema(df)
        assert "email" in result
        assert "age" in result

    def test_pandas_raw_type_present(self):
        df = pd.DataFrame({"age": [25]})
        result = _extract_actual_schema(df)
        assert "raw_type" in result["age"]

    def test_unsupported_type_raises(self):
        with pytest.raises(TypeError, match="Cannot extract schema"):
            _extract_actual_schema(42)

    def test_unsupported_type_raises_on_string(self):
        with pytest.raises(TypeError, match="Cannot extract schema"):
            _extract_actual_schema("not_a_df")


# validate()


class TestValidate:

    def test_all_columns_correct_type(self):
        df = pd.DataFrame(
            {
                "user_id": [1, 2, 3],
                "email": ["a@a.com", "b@b.com", "c@c.com"],
            }
        )
        schema = SchemaDef.from_dict({"user_id": "integer", "email": "string"})
        report = validate(df, schema)
        assert report.passed is True

    def test_missing_required_column(self):
        df = pd.DataFrame({"user_id": [1, 2, 3]})
        schema = SchemaDef.from_dict({"user_id": "integer", "email": "string"})
        report = validate(df, schema)
        assert report.passed is False
        assert "email" in report.missing_cols

    def test_optional_missing_column_passes(self):
        df = pd.DataFrame({"user_id": [1, 2, 3]})
        schema = SchemaDef.from_dict(
            {
                "user_id": "integer",
                "email": {"type": "string", "is_optional": True},
            }
        )
        report = validate(df, schema)
        assert report.passed is True
        assert "email" not in report.missing_cols

    def test_type_mismatch_detected(self):
        df = pd.DataFrame({"age": ["twenty", "thirty"]})  # string, not integer
        schema = SchemaDef.from_dict({"age": "integer"})
        report = validate(df, schema)
        assert report.passed is False
        assert "age" in report.type_errors

    def test_strict_mode_detects_extra_columns(self):
        df = pd.DataFrame({"user_id": [1], "email": ["a@a.com"], "extra_col": ["x"]})
        schema = SchemaDef.from_dict(
            {"user_id": "integer", "email": "string"}, strict=True
        )
        report = validate(df, schema)
        assert report.passed is False
        assert "extra_col" in report.extra_cols

    def test_non_strict_mode_ignores_extra_columns(self):
        df = pd.DataFrame({"user_id": [1], "email": ["a@a.com"], "extra_col": ["x"]})
        schema = SchemaDef.from_dict(
            {"user_id": "integer", "email": "string"}, strict=False
        )
        report = validate(df, schema)
        assert report.passed is True

    def test_returns_schema_report(self):
        df = pd.DataFrame({"user_id": [1]})
        schema = SchemaDef.from_dict({"user_id": "integer"})
        report = validate(df, schema)
        assert isinstance(report, SchemaReport)

    def test_multiple_missing_columns(self):
        df = pd.DataFrame({"user_id": [1]})
        schema = SchemaDef.from_dict(
            {
                "user_id": "integer",
                "email": "string",
                "age": "integer",
            }
        )
        report = validate(df, schema)
        assert "email" in report.missing_cols
        assert "age" in report.missing_cols


# extract_schema()


class TestExtractSchema:

    def test_pandas_returns_dict(self):
        df = pd.DataFrame({"age": [25], "email": ["a@a.com"]})
        result = extract_schema(df)
        assert isinstance(result, dict)

    def test_pandas_column_names_present(self):
        df = pd.DataFrame({"age": [25], "email": ["a@a.com"]})
        result = extract_schema(df)
        assert "age" in result
        assert "email" in result

    def test_pandas_type_key_present(self):
        df = pd.DataFrame({"age": [25]})
        result = extract_schema(df)
        assert "type" in result["age"]

    def test_pandas_integer_type(self):
        df = pd.DataFrame({"age": pd.array([25, 30], dtype="int64")})
        result = extract_schema(df)
        assert result["age"]["type"] == "integer"

    def test_pandas_string_type(self):
        df = pd.DataFrame({"email": ["a@a.com"]})
        result = extract_schema(df)
        assert result["email"]["type"] == "string"

    def test_as_json_returns_string(self):
        df = pd.DataFrame({"age": [25]})
        result = extract_schema(df, as_json=True)
        assert isinstance(result, str)

    def test_as_json_is_valid_json(self):
        df = pd.DataFrame({"age": [25], "email": ["a@a.com"]})
        result = extract_schema(df, as_json=True)
        parsed = json.loads(result)
        assert "age" in parsed
        assert "email" in parsed

    def test_dict_input(self):
        result = extract_schema({"email": "string", "age": "int64"})
        assert "email" in result
        assert "age" in result

    def test_unsupported_type_raises(self):
        with pytest.raises(TypeError, match="Cannot extract schema"):
            extract_schema(42)
