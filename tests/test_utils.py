# tests/test_utils.py
"""
Clean and modern tests for utility functions
"""
import pytest
from datetime import datetime

from sumeh.core.utils import (
    __convert_value as convert_value,
    __compare_schemas as compare_schemas,
    __parse_field_list as parse_field_list,
    __detect_engine as detect_engine,
    __transform_date_format_in_pattern as transform_date_format,
)


class TestConvertValue:

    def test_convert_date_iso_format(self):
        result = convert_value("2023-05-15")
        assert result == datetime(2023, 5, 15)

    def test_convert_date_dmy_format(self):
        result = convert_value("15/05/2023")
        assert result == datetime(2023, 5, 15)

    def test_convert_float(self):
        result = convert_value("3.14")
        assert result == 3.14
        assert isinstance(result, float)

    def test_convert_integer(self):
        result = convert_value("42")
        assert result == 42
        assert isinstance(result, int)

    def test_convert_negative_integer(self):
        result = convert_value("-100")
        assert result == -100

    def test_convert_negative_float(self):
        result = convert_value("-3.14")
        assert result == -3.14

    def test_invalid_format_raises_error(self):
        with pytest.raises(ValueError):
            convert_value("not-a-number")


class TestParseFieldList:

    def test_parse_single_field(self):
        result = parse_field_list("column_name")
        assert result == "column_name"

    def test_parse_list_notation(self):
        result = parse_field_list("[col1, col2, col3]")
        assert result == ["col1", "col2", "col3"]

    def test_parse_list_with_quotes(self):
        result = parse_field_list("['col1', 'col2']")
        assert result == ["col1", "col2"]

    def test_parse_comma_separated(self):
        result = parse_field_list("col1, col2, col3")
        assert result == ["col1", "col2", "col3"]

    def test_parse_single_item_list_returns_string(self):
        result = parse_field_list("[single_col]")
        assert result == "single_col"

    def test_parse_empty_string(self):
        result = parse_field_list("")
        assert result == ""

    def test_parse_quoted_field(self):
        result = parse_field_list('"column_name"')
        assert result == "column_name"

    def test_parse_already_list(self):
        result = parse_field_list(["col1", "col2"])
        assert result == ["col1", "col2"]

    def test_parse_empty_brackets(self):
        result = parse_field_list("[]")
        assert result == ""


class TestCompareSchemas:

    @pytest.fixture
    def base_schema(self):
        return [
            {"field": "id", "data_type": "integer", "nullable": False},
            {"field": "name", "data_type": "string", "nullable": True},
            {"field": "price", "data_type": "float", "nullable": False, "max_length": 10},
        ]

    def test_matching_schemas(self, base_schema):
        actual = base_schema.copy()
        valid, errors = compare_schemas(actual, base_schema)

        assert valid is True
        assert len(errors) == 0

    def test_missing_column(self, base_schema):
        actual = [col for col in base_schema if col["field"] != "price"]
        valid, errors = compare_schemas(actual, base_schema)

        assert valid is False
        assert len(errors) == 1
        assert errors[0]["field"] == "price"
        assert errors[0]["error_type"] == "missing_field"
        assert errors[0]["actual"] is None

    def test_type_mismatch(self, base_schema):
        actual = [
            {"field": "id", "data_type": "string", "nullable": False},
            {"field": "name", "data_type": "string", "nullable": True},
            {"field": "price", "data_type": "float", "nullable": False},
        ]
        valid, errors = compare_schemas(actual, base_schema)

        assert valid is False
        assert len(errors) == 1
        assert errors[0]["field"] == "id"
        assert errors[0]["error_type"] == "type_mismatch"
        assert errors[0]["expected_type"] == "integer"
        assert errors[0]["actual_type"] == "string"

    def test_nullability_violation(self, base_schema):
        actual = [
            {"field": "id", "data_type": "integer", "nullable": True},
            {"field": "name", "data_type": "string", "nullable": True},
            {"field": "price", "data_type": "float", "nullable": False},
        ]
        valid, errors = compare_schemas(actual, base_schema)

        assert valid is False
        assert len(errors) == 1
        assert errors[0]["field"] == "id"
        assert errors[0]["error_type"] == "nullable_mismatch"
        assert errors[0]["expected_nullable"] is False
        assert errors[0]["actual_nullable"] is True

    def test_extra_column(self, base_schema):
        actual = base_schema.copy()
        actual.append({"field": "extra", "data_type": "string", "nullable": True})
        valid, errors = compare_schemas(actual, base_schema)

        assert valid is False
        assert len(errors) == 1
        assert errors[0]["field"] == "extra"
        assert errors[0]["error_type"] == "extra_field"
        assert errors[0]["expected"] is None

    def test_multiple_errors(self, base_schema):
        actual = [
            {"field": "id", "data_type": "string", "nullable": True},
            {"field": "name", "data_type": "integer", "nullable": True},
            {"field": "extra", "data_type": "boolean", "nullable": True},
        ]
        valid, errors = compare_schemas(actual, base_schema)

        assert valid is False
        assert len(errors) >= 3

        error_types = {err["error_type"] for err in errors}
        assert "type_mismatch" in error_types
        assert "missing_field" in error_types
        assert "extra_field" in error_types

    def test_empty_schemas(self):
        valid, errors = compare_schemas([], [])
        assert valid is True
        assert len(errors) == 0


class TestTransformDateFormat:

    def test_transform_dd_mm_yyyy(self):
        result = transform_date_format("DD/MM/YYYY")
        assert "(0[1-9]|[12][0-9]|3[01])" in result
        assert "(0[1-9]|1[012])" in result
        assert "(19|20)\\d\\d" in result

    def test_transform_yyyy_mm_dd(self):
        result = transform_date_format("YYYY-MM-DD")
        assert "(19|20)\\d\\d" in result
        assert "(0[1-9]|1[012])" in result
        assert "(0[1-9]|[12][0-9]|3[01])" in result

    def test_transform_with_dots(self):
        result = transform_date_format("DD.MM.YYYY")
        assert "\\." in result

    def test_transform_with_spaces(self):
        result = transform_date_format("DD MM YYYY")
        assert "\\s" in result

    def test_transform_yy_format(self):
        result = transform_date_format("DD/MM/YY")
        assert "\\d\\d" in result


class TestDetectEngine:

    def test_detect_pandas(self):
        import pandas as pd
        df = pd.DataFrame({"a": [1, 2, 3]})

        engine = detect_engine(df)
        assert engine == "pandas_engine"

    def test_detect_dask(self):
        import dask.dataframe as dd
        import pandas as pd

        pdf = pd.DataFrame({"a": [1, 2, 3]})
        df = dd.from_pandas(pdf, npartitions=1)

        engine = detect_engine(df)
        assert engine == "dask_engine"

    def test_detect_duckdb(self):
        import duckdb

        conn = duckdb.connect(":memory:")
        df = conn.execute("SELECT 1 as a").df()
        rel = conn.from_df(df)

        engine = detect_engine(rel)
        assert engine == "duckdb_engine"

    def test_detect_polars(self):
        try:
            import polars as pl
            df = pl.DataFrame({"a": [1, 2, 3]})

            engine = detect_engine(df)
            assert engine == "polars_engine"
        except ImportError:
            pytest.skip("Polars not installed")

    def test_detect_pyspark(self):
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.master("local[1]").getOrCreate()
            df = spark.createDataFrame([(1,)], ["a"])

            engine = detect_engine(df)
            assert engine == "pyspark_engine"

            spark.stop()
        except ImportError:
            pytest.skip("PySpark not installed")

    def test_detect_bigquery_with_context(self):
        import pandas as pd
        df = pd.DataFrame({"a": [1, 2, 3]})

        engine = detect_engine(df, client="mock_client", table_ref="mock_ref")
        assert engine == "bigquery_engine"

    def test_unsupported_type_raises_error(self):
        with pytest.raises(TypeError, match="Unsupported DataFrame type"):
            detect_engine("not a dataframe")


class TestEdgeCases:

    def test_convert_value_with_whitespace(self):
        result = convert_value("  42  ")
        assert result == 42

    def test_parse_field_list_with_extra_whitespace(self):
        result = parse_field_list("  col1 ,  col2  ,  col3  ")
        assert result == ["col1", "col2", "col3"]

    def test_compare_schemas_with_none_values(self):
        actual = [{"field": "id", "data_type": "integer", "nullable": False}]
        expected = [{"field": "id", "data_type": "integer", "nullable": False}]

        valid, errors = compare_schemas(actual, expected)
        assert valid is True