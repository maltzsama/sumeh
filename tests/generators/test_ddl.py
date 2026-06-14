"""
Tests for sumeh/generators/ddl.py

Covers:
- SQLGenerator.generate()
- SQLGenerator._generate_table_ddl()
- SQLGenerator._apply_dialect_customizations()
- SQLGenerator.list_dialects()
- SQLGenerator.list_tables()
"""

import pytest

from sumeh.generators.ddl import SQLGenerator


# list_dialects / list_tables


class TestListMethods:

    def test_list_dialects_returns_list(self):
        result = SQLGenerator.list_dialects()
        assert isinstance(result, list)

    def test_list_dialects_sorted(self):
        result = SQLGenerator.list_dialects()
        assert result == sorted(result)

    def test_list_dialects_contains_common(self):
        dialects = SQLGenerator.list_dialects()
        for d in ["postgres", "bigquery", "snowflake", "duckdb", "mysql"]:
            assert d in dialects

    def test_list_tables_returns_list(self):
        result = SQLGenerator.list_tables()
        assert isinstance(result, list)

    def test_list_tables_contains_rules(self):
        assert "rules" in SQLGenerator.list_tables()

    def test_list_tables_contains_schema_registry(self):
        assert "schema_registry" in SQLGenerator.list_tables()

    def test_list_tables_sorted(self):
        result = SQLGenerator.list_tables()
        assert result == sorted(result)


# generate() — basic


class TestGenerate:

    def test_returns_string(self):
        result = SQLGenerator.generate("rules", "postgres")
        assert isinstance(result, str)

    def test_result_not_empty(self):
        result = SQLGenerator.generate("rules", "postgres")
        assert len(result) > 0

    def test_contains_create_table(self):
        result = SQLGenerator.generate("rules", "postgres")
        assert "CREATE TABLE" in result.upper()

    def test_contains_table_name(self):
        result = SQLGenerator.generate("rules", "postgres")
        assert "rules" in result.lower()

    def test_schema_registry_table(self):
        result = SQLGenerator.generate("schema_registry", "postgres")
        assert "schema_registry" in result.lower()

    def test_all_generates_both_tables(self):
        result = SQLGenerator.generate("all", "postgres")
        assert "rules" in result.lower()
        assert "schema_registry" in result.lower()

    def test_all_separates_with_blank_line(self):
        result = SQLGenerator.generate("all", "postgres")
        assert "\n\n" in result

    def test_with_schema_prefix(self):
        result = SQLGenerator.generate("rules", "postgres", schema="dq")
        assert "dq" in result

    def test_unknown_table_raises(self):
        with pytest.raises(ValueError, match="Unknown table"):
            SQLGenerator.generate("nonexistent_table", "postgres")

    def test_unknown_dialect_raises(self):
        with pytest.raises(ValueError, match="Unknown dialect"):
            SQLGenerator.generate("rules", "nonexistent_dialect_xyz")

    def test_error_message_includes_available_dialects(self):
        with pytest.raises(ValueError) as exc_info:
            SQLGenerator.generate("rules", "nonexistent_dialect_xyz")
        assert "postgres" in str(exc_info.value)

    def test_error_message_includes_available_tables(self):
        with pytest.raises(ValueError) as exc_info:
            SQLGenerator.generate("nonexistent_table", "postgres")
        assert "rules" in str(exc_info.value)


# generate() — dialects


class TestGenerateDialects:

    @pytest.mark.parametrize(
        "dialect",
        [
            "postgres",
            "mysql",
            "duckdb",
            "sqlite",
            "snowflake",
            "bigquery",
            "redshift",
            "athena",
            "trino",
            "spark",
        ],
    )
    def test_dialect_produces_output(self, dialect):
        result = SQLGenerator.generate("rules", dialect)
        assert isinstance(result, str)
        assert len(result) > 0

    def test_postgresql_alias_works(self):
        result = SQLGenerator.generate("rules", "postgresql")
        assert isinstance(result, str)
        assert len(result) > 0

    def test_mssql_alias_works(self):
        result = SQLGenerator.generate("rules", "mssql")
        assert isinstance(result, str)

    def test_sqlserver_alias_works(self):
        result = SQLGenerator.generate("rules", "sqlserver")
        assert isinstance(result, str)

    def test_dialects_produce_different_output(self):
        pg = SQLGenerator.generate("rules", "postgres")
        bq = SQLGenerator.generate("rules", "bigquery")
        # Different dialects should produce at least somewhat different SQL
        assert pg != bq


# generate() — columns present


class TestGenerateColumns:

    def test_rules_table_has_check_type_column(self):
        result = SQLGenerator.generate("rules", "postgres")
        assert "check_type" in result

    def test_rules_table_has_field_column(self):
        result = SQLGenerator.generate("rules", "postgres")
        assert "field" in result

    def test_rules_table_has_threshold_column(self):
        result = SQLGenerator.generate("rules", "postgres")
        assert "threshold" in result

    def test_schema_registry_has_data_type_column(self):
        result = SQLGenerator.generate("schema_registry", "postgres")
        assert "data_type" in result

    def test_schema_registry_has_nullable_column(self):
        result = SQLGenerator.generate("schema_registry", "postgres")
        assert "nullable" in result


# _apply_dialect_customizations


class TestDialectCustomizations:

    def test_bigquery_partition_by(self):
        result = SQLGenerator.generate("rules", "bigquery", partition_by="created_at")
        assert "PARTITION BY" in result
        assert "created_at" in result

    def test_bigquery_cluster_by(self):
        result = SQLGenerator.generate(
            "rules", "bigquery", cluster_by=["environment", "table_name"]
        )
        assert "CLUSTER BY" in result
        assert "environment" in result

    def test_snowflake_cluster_by(self):
        result = SQLGenerator.generate("rules", "snowflake", cluster_by=["environment"])
        assert "CLUSTER BY" in result

    def test_redshift_distkey(self):
        result = SQLGenerator.generate("rules", "redshift", distkey="id")
        assert "DISTKEY" in result
        assert "id" in result

    def test_redshift_sortkey(self):
        result = SQLGenerator.generate("rules", "redshift", sortkey=["created_at"])
        assert "SORTKEY" in result

    def test_athena_location(self):
        result = SQLGenerator.generate(
            "rules", "athena", location="s3://my-bucket/sumeh/"
        )
        assert "LOCATION" in result
        assert "s3://" in result

    def test_athena_format(self):
        result = SQLGenerator.generate("rules", "athena", format="PARQUET")
        assert "PARQUET" in result

    def test_mysql_engine(self):
        result = SQLGenerator.generate("rules", "mysql", engine="InnoDB")
        assert "InnoDB" in result

    def test_no_options_no_extra_lines(self):
        result = SQLGenerator.generate("rules", "postgres")
        # Should not have dangling option lines
        assert "PARTITION BY" not in result
        assert "DISTKEY" not in result
