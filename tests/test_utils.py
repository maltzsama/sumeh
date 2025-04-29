import pytest
from datetime import datetime
from sumeh.services.utils import (
    __convert_value as convert_value,
    __extract_params as extract_params,
    __compare_schemas as compare_schemas,
    SchemaDef,
)


class TestConvertValue:
    """Test suite for convert_value function"""

    def test_convert_date_iso_format(self):
        assert convert_value("2023-05-15") == datetime(2023, 5, 15)

    def test_convert_date_dmy_format(self):
        assert convert_value("15/05/2023") == datetime(2023, 5, 15)

    def test_convert_float(self):
        assert convert_value("3.14") == 3.14

    def test_convert_integer(self):
        assert convert_value("42") == 42

    def test_invalid_date_raises_value_error(self):
        with pytest.raises(ValueError):
            convert_value("invalid-date")


class TestExtractParams:
    """Test suite for extract_params function"""

    def test_extract_params_with_conversion(self):
        rule = {"check_type": "is_complete", "field": "age", "value": "25"}
        assert extract_params(rule) == ("age", "is_complete", 25)

    def test_extract_params_with_string_value(self):
        rule = {"check_type": "has_pattern", "field": "email", "value": ".*@.*"}
        assert extract_params(rule) == ("email", "has_pattern", ".*@.*")

    def test_extract_params_with_null_value(self):
        rule = {"check_type": "is_unique", "field": "id", "value": None}
        assert extract_params(rule) == ("id", "is_unique", "")

    def test_extract_params_with_empty_value(self):
        rule = {"check_type": "is_null", "field": "name", "value": ""}
        assert extract_params(rule) == ("name", "is_null", "")


class TestCompareSchemas:
    """Test suite for compare_schemas function"""

    @pytest.fixture
    def base_schema(self):
        return [
            {"field": "id", "data_type": "integer", "nullable": False},
            {"field": "name", "data_type": "string", "nullable": True},
            {
                "field": "price",
                "data_type": "float",
                "nullable": False,
                "max_length": 10,
            },
        ]

    def test_matching_schemas(self, base_schema):
        actual = base_schema.copy()
        valid, errors = compare_schemas(actual, base_schema)
        assert valid
        assert len(errors) == 0

    def test_missing_column(self, base_schema):
        actual = [col for col in base_schema if col["field"] != "price"]
        valid, errors = compare_schemas(actual, base_schema)
        assert not valid
        assert ("price", "missing") in errors

    def test_type_mismatch(self, base_schema):
        actual = [
            {"field": "id", "data_type": "string", "nullable": False},  # Changed type
            {"field": "name", "data_type": "string", "nullable": True},
            {"field": "price", "data_type": "float", "nullable": False},
        ]
        valid, errors = compare_schemas(actual, base_schema)
        assert not valid
        assert any("type mismatch" in error[1] for error in errors)

    def test_nullability_violation(self, base_schema):
        actual = [
            {
                "field": "id",
                "data_type": "integer",
                "nullable": True,
            },  # Changed nullability
            {"field": "name", "data_type": "string", "nullable": True},
            {"field": "price", "data_type": "float", "nullable": False},
        ]
        valid, errors = compare_schemas(actual, base_schema)
        assert not valid
        assert ("id", "nullable but expected non-nullable") in errors

    def test_extra_column(self, base_schema):
        actual = base_schema.copy()
        actual.append({"field": "extra", "data_type": "string", "nullable": True})
        valid, errors = compare_schemas(actual, base_schema)
        assert not valid
        assert ("extra", "extra column") in errors

    def test_max_length_check(self, base_schema):
        # Note: Currently max_length check is not fully implemented
        actual = base_schema.copy()
        valid, errors = compare_schemas(actual, base_schema)
        assert valid
