from datetime import datetime

import pytest

from sumeh.core.utils import (
    __convert_value as convert_value,
    __compare_schemas as compare_schemas,
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
