"""
Tests for sumeh/generators/transpiler.py

Covers:
- transpile(): basic functionality, dialect pairs, error handling
"""

import pytest

from sumeh.generators.transpiler import transpile


# Basic behavior


class TestTranspileBasic:

    def test_returns_string(self):
        result = transpile("SELECT 1", "postgres", "mysql")
        assert isinstance(result, str)

    def test_result_not_empty(self):
        result = transpile("SELECT * FROM users", "postgres", "mysql")
        assert len(result) > 0

    def test_select_star_preserved(self):
        result = transpile("SELECT * FROM users", "postgres", "bigquery")
        assert "SELECT" in result.upper()
        assert "users" in result

    def test_same_dialect_roundtrip(self):
        sql = "SELECT id, name FROM users WHERE id = 1"
        result = transpile(sql, "postgres", "postgres")
        assert "SELECT" in result.upper()
        assert "users" in result

    def test_invalid_sql_raises(self):
        with pytest.raises(Exception):
            transpile("THIS IS NOT SQL @@##", "postgres", "mysql")


# Dialect pairs


class TestTranspileDialects:

    def test_mysql_to_postgres(self):
        sql = "SELECT * FROM users LIMIT 10"
        result = transpile(sql, "mysql", "postgres")
        assert "LIMIT" in result.upper()

    def test_postgres_to_bigquery(self):
        sql = "SELECT id, name FROM users WHERE active = TRUE"
        result = transpile(sql, "postgres", "bigquery")
        assert "SELECT" in result.upper()

    def test_postgres_to_duckdb(self):
        sql = "SELECT COUNT(*) FROM orders"
        result = transpile(sql, "postgres", "duckdb")
        assert "COUNT" in result.upper()

    def test_mysql_to_mssql_limit(self):
        sql = "SELECT * FROM users LIMIT 10"
        result = transpile(sql, "mysql", "tsql")
        # MSSQL uses TOP or FETCH NEXT instead of LIMIT
        assert "10" in result

    def test_postgres_to_snowflake(self):
        sql = "SELECT id FROM users WHERE id > 5"
        result = transpile(sql, "postgres", "snowflake")
        assert "SELECT" in result.upper()
        assert "5" in result

    def test_postgres_to_spark(self):
        sql = "SELECT id, name FROM users"
        result = transpile(sql, "postgres", "spark")
        assert "SELECT" in result.upper()

    def test_postgres_to_trino(self):
        sql = "SELECT * FROM orders WHERE total > 100"
        result = transpile(sql, "postgres", "trino")
        assert "SELECT" in result.upper()

    def test_hive_to_spark(self):
        sql = "SELECT id, name FROM users WHERE dt = '2024-01-01'"
        result = transpile(sql, "hive", "spark")
        assert "SELECT" in result.upper()


# SQL constructs


class TestTranspileConstructs:

    def test_where_clause_preserved(self):
        sql = "SELECT id FROM users WHERE active = 1"
        result = transpile(sql, "mysql", "postgres")
        assert "WHERE" in result.upper()

    def test_join_preserved(self):
        sql = "SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id"
        result = transpile(sql, "postgres", "duckdb")
        assert "JOIN" in result.upper()

    def test_aggregate_preserved(self):
        sql = "SELECT COUNT(*), SUM(total) FROM orders GROUP BY status"
        result = transpile(sql, "postgres", "bigquery")
        assert "COUNT" in result.upper()
        assert "SUM" in result.upper()
        assert "GROUP BY" in result.upper()

    def test_cte_preserved(self):
        sql = "WITH ranked AS (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM users) SELECT * FROM ranked"
        result = transpile(sql, "postgres", "duckdb")
        assert "WITH" in result.upper()

    def test_subquery_preserved(self):
        sql = "SELECT * FROM (SELECT id FROM users WHERE active = 1) AS sub"
        result = transpile(sql, "postgres", "mysql")
        assert "SELECT" in result.upper()
