"""Tests for SQL DDL generation."""

import pytest
from sumeh.services.sql import SQLGenerator


def test_list_dialects():
    """Test listing available dialects."""
    dialects = SQLGenerator.list_dialects()
    assert "postgres" in dialects
    assert "mysql" in dialects
    assert "bigquery" in dialects
    assert "duckdb" in dialects
    assert "athena" in dialects
    assert "sqlite" in dialects
    assert "snowflake" in dialects
    assert "redshift" in dialects


def test_list_tables():
    """Test listing available tables."""
    tables = SQLGenerator.list_tables()
    assert "rules" in tables
    assert "schema_registry" in tables


def test_generate_postgres_rules():
    """Test generating PostgreSQL DDL for rules table."""
    ddl = SQLGenerator.generate("rules", "postgres")
    assert "CREATE TABLE rules" in ddl
    assert "SERIAL" in ddl  # Auto-increment
    assert "field VARCHAR(255)" in ddl
    assert "check_type VARCHAR(100)" in ddl
    assert "threshold DOUBLE PRECISION" in ddl
    assert "PRIMARY KEY" in ddl  # Pode estar inline ou como constraint


def test_generate_mysql_schema_registry():
    """Test generating MySQL DDL for schema_registry."""
    ddl = SQLGenerator.generate("schema_registry", "mysql", schema="mydb")
    assert "CREATE TABLE mydb.schema_registry" in ddl
    assert "INT" in ddl
    assert "AUTO_INCREMENT" in ddl
    assert "PRIMARY KEY" in ddl
    assert "ENGINE=InnoDB" in ddl


def test_generate_bigquery_with_partitioning():
    """Test generating BigQuery DDL with partitioning."""
    ddl = SQLGenerator.generate(
        "rules",
        "bigquery",
        schema="mydataset",
        partition_by="DATE(created_at)",
        cluster_by=["table_name", "environment"]
    )
    assert "`mydataset.rules`" in ddl
    assert "PARTITION BY DATE(created_at)" in ddl
    assert "CLUSTER BY table_name, environment" in ddl


def test_generate_athena_external_table():
    """Test generating Athena external table DDL."""
    ddl = SQLGenerator.generate(
        "rules",
        "athena",
        schema="my_database",
        format="PARQUET",
        location="s3://my-bucket/rules/"
    )
    assert "CREATE EXTERNAL TABLE my_database.rules" in ddl
    assert "STORED AS PARQUET" in ddl
    assert "LOCATION 's3://my-bucket/rules/'" in ddl


def test_generate_all_tables():
    """Test generating DDL for all tables."""
    ddl = SQLGenerator.generate("all", "postgres")
    assert "CREATE TABLE rules" in ddl
    assert "CREATE TABLE schema_registry" in ddl


def test_invalid_dialect():
    """Test error handling for invalid dialect."""
    with pytest.raises(ValueError, match="Unknown dialect"):
        SQLGenerator.generate("rules", "invalid_dialect")


def test_invalid_table():
    """Test error handling for invalid table."""
    with pytest.raises(ValueError, match="Unknown table"):
        SQLGenerator.generate("invalid_table", "postgres")


def test_sqlite_types():
    """Test SQLite type mapping."""
    ddl = SQLGenerator.generate("rules", "sqlite")
    assert "INTEGER" in ddl
    assert "AUTOINCREMENT" in ddl
    assert "PRIMARY KEY" in ddl
    assert "REAL" in ddl  # Float type
    # SQLite usa INTEGER para boolean e TEXT para strings


def test_snowflake_ddl():
    """Test Snowflake DDL generation."""
    ddl = SQLGenerator.generate("rules", "snowflake", schema="my_schema")
    assert "CREATE TABLE my_schema.rules" in ddl
    assert "NUMBER(38,0)" in ddl  # Integer type
    assert "AUTOINCREMENT" in ddl
    assert "TIMESTAMP_NTZ" in ddl


def test_redshift_with_distribution():
    """Test Redshift DDL with distribution and sort keys."""
    ddl = SQLGenerator.generate(
        "rules",
        "redshift",
        distkey="table_name",
        sortkey=["created_at", "table_name"]
    )
    assert "DISTKEY(table_name)" in ddl
    assert "SORTKEY(created_at, table_name)" in ddl


def test_duckdb_ddl():
    """Test DuckDB DDL generation."""
    ddl = SQLGenerator.generate("rules", "duckdb")
    assert "CREATE TABLE rules" in ddl
    assert "INTEGER" in ddl
    assert "PRIMARY KEY" in ddl
    assert "DOUBLE" in ddl  # Float type


def test_postgres_with_schema():
    """Test PostgreSQL with schema name."""
    ddl = SQLGenerator.generate("rules", "postgres", schema="public")
    assert "CREATE TABLE public.rules" in ddl


def test_mysql_with_options():
    """Test MySQL with custom engine."""
    ddl = SQLGenerator.generate("rules", "mysql", engine="MyISAM")
    assert "ENGINE=MyISAM" in ddl


def test_bigquery_types():
    """Test BigQuery type mapping."""
    ddl = SQLGenerator.generate("rules", "bigquery")
    assert "INT64" in ddl
    assert "STRING" in ddl
    assert "FLOAT64" in ddl
    assert "BOOL" in ddl
    assert "TIMESTAMP" in ddl


def test_athena_no_constraints():
    """Test Athena doesn't include constraints."""
    ddl = SQLGenerator.generate("rules", "athena")
    # Athena doesn't support these
    assert "PRIMARY KEY" not in ddl
    assert "NOT NULL" not in ddl
    assert "AUTO_INCREMENT" not in ddl
    assert "DEFAULT" not in ddl


def test_schema_registry_columns():
    """Test schema_registry has correct columns."""
    ddl = SQLGenerator.generate("schema_registry", "postgres")
    assert "data_type" in ddl
    assert "nullable" in ddl
    assert "max_length" in ddl
    assert "comment" in ddl


def test_rules_columns():
    """Test rules table has correct columns."""
    ddl = SQLGenerator.generate("rules", "postgres")
    assert "environment" in ddl
    assert "table_name" in ddl
    assert "field" in ddl
    assert "check_type" in ddl
    assert "threshold" in ddl
    assert "execute" in ddl