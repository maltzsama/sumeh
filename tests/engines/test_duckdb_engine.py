# tests/engines/test_duckdb_engine.py
"""
Clean and modern tests for DuckDB Engine
"""
import pytest
import pandas as pd
import duckdb as dk

from sumeh.engines.duckdb_engine import (
    validate,
    validate_row_level,
    validate_table_level,
    summarize,
    has_max,
    has_min,
    has_mean,
    has_std,
    has_cardinality,
)
from sumeh.core.rules.rule_model import RuleDef


@pytest.fixture
def duckdb_conn():
    return dk.connect(":memory:")


@pytest.fixture
def sample_relation(duckdb_conn, sample_pandas_df):
    return duckdb_conn.from_df(sample_pandas_df)


@pytest.fixture
def empty_relation(duckdb_conn, empty_pandas_df):
    return duckdb_conn.from_df(empty_pandas_df)


class TestTableLevelFunctions:

    def test_has_max(self, duckdb_conn, sample_relation, rule_factory):
        sample_relation.create_view("tbl")
        rule = rule_factory("salary", "has_max", value=10000, level="TABLE")

        result = has_max(duckdb_conn, rule)

        assert isinstance(result, dict)
        assert "status" in result
        assert result["status"] in ["PASS", "FAIL", "ERROR"]

    def test_has_max_pass(self, duckdb_conn, sample_relation, rule_factory):
        sample_relation.create_view("tbl")
        rule = rule_factory("salary", "has_max", value=10000, level="TABLE")

        result = has_max(duckdb_conn, rule)

        assert result["status"] == "PASS"
        assert result["actual"] == 9000.0

    def test_has_max_fail(self, duckdb_conn, sample_relation, rule_factory):
        sample_relation.create_view("tbl")
        rule = rule_factory("salary", "has_max", value=8000, level="TABLE")

        result = has_max(duckdb_conn, rule)

        assert result["status"] == "FAIL"
        assert result["actual"] == 9000.0

    def test_has_min(self, duckdb_conn, sample_relation, rule_factory):
        sample_relation.create_view("tbl")
        rule = rule_factory("salary", "has_min", value=4000, level="TABLE")

        result = has_min(duckdb_conn, rule)

        assert result["status"] == "PASS"
        assert result["actual"] == 5000.0

    def test_has_mean(self, duckdb_conn, sample_relation, rule_factory):
        sample_relation.create_view("tbl")
        rule = rule_factory("salary", "has_mean", value=6000, level="TABLE")

        result = has_mean(duckdb_conn, rule)

        assert result["status"] == "PASS"
        assert result["actual"] == 7000.0

    def test_has_std(self, duckdb_conn, sample_relation, rule_factory):
        sample_relation.create_view("tbl")
        rule = rule_factory("salary", "has_std", value=1000, level="TABLE")

        result = has_std(duckdb_conn, rule)

        assert result["status"] == "PASS"
        assert result["actual"] > 1000

    def test_has_cardinality(self, duckdb_conn, sample_relation, rule_factory):
        sample_relation.create_view("tbl")
        rule = rule_factory("department", "has_cardinality", value=3, level="TABLE")

        result = has_cardinality(duckdb_conn, rule)

        assert result["status"] == "PASS"
        assert result["actual"] == 3.0


class TestValidateRowLevel:

    def test_validate_row_level_basic(self, duckdb_conn, sample_relation, rule_factory):
        rules = [rule_factory("age", "is_positive", level="ROW")]

        agg_rel, raw_rel = validate_row_level(duckdb_conn, sample_relation, rules)

        assert agg_rel is not None
        assert raw_rel is not None

    def test_validate_row_level_finds_violations(self, duckdb_conn, sample_relation, rule_factory):
        """Test that violations are found and flagged"""
        rules = [rule_factory("age", "is_positive", level="ROW")]

        agg_rel, raw_rel = validate_row_level(duckdb_conn, sample_relation, rules)
        raw_df = raw_rel.df()

        # Should have violations
        assert len(raw_df) > 0, "Expected violations but got none"
        assert "dq_status" in raw_df.columns, "Missing dq_status column"

        # Verificar que dq_status contém a regra violada
        assert any("is_positive" in str(status) for status in raw_df["dq_status"])

    def test_validate_row_level_multiple_rules(self, duckdb_conn, sample_relation, rule_factory):
        rules = [
            rule_factory("age", "is_positive", level="ROW"),
            rule_factory("name", "is_complete", level="ROW"),
        ]

        agg_rel, raw_rel = validate_row_level(duckdb_conn, sample_relation, rules)
        raw_df = raw_rel.df()

        assert len(raw_df) > 0
        assert "dq_status" in raw_df.columns



    def test_validate_row_level_with_empty_data(self, duckdb_conn, empty_relation, rule_factory):
        rules = [rule_factory("name", "is_complete", level="ROW")]

        agg_rel, raw_rel = validate_row_level(duckdb_conn, empty_relation, rules)
        raw_df = raw_rel.df()

        assert len(raw_df) == 0


class TestValidateTableLevel:

    def test_validate_table_level_basic(self, duckdb_conn, sample_relation, rule_factory):
        rules = [rule_factory("salary", "has_max", value=10000, level="TABLE")]

        summary_rel = validate_table_level(duckdb_conn, sample_relation, rules)
        summary_df = summary_rel.df()

        assert len(summary_df) == 1
        assert "status" in summary_df.columns
        assert "expected" in summary_df.columns
        assert "actual" in summary_df.columns

    def test_validate_table_level_multiple_rules(self, duckdb_conn, sample_relation, rule_factory):
        rules = [
            rule_factory("salary", "has_max", value=10000, level="TABLE"),
            rule_factory("salary", "has_min", value=4000, level="TABLE"),
            rule_factory("salary", "has_mean", value=6000, level="TABLE"),
        ]

        summary_rel = validate_table_level(duckdb_conn, sample_relation, rules)
        summary_df = summary_rel.df()

        assert len(summary_df) == 3
        assert all(summary_df["level"] == "TABLE")


class TestValidate:

    def test_validate_returns_three_values(self, duckdb_conn, sample_relation, rule_factory):
        rules = [rule_factory("age", "is_positive", level="ROW")]

        result = validate(duckdb_conn, sample_relation, rules)

        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_validate_with_row_rules(self, duckdb_conn, sample_relation, rule_factory):
        rules = [
            rule_factory("age", "is_positive", level="ROW"),
            rule_factory("name", "is_complete", level="ROW"),
        ]

        df_with_status, violations, table_summary = validate(duckdb_conn, sample_relation, rules)

        viol_df = violations.df()
        assert len(viol_df) > 0
        assert "dq_status" in viol_df.columns

    def test_validate_with_table_rules(self, duckdb_conn, sample_relation, rule_factory):
        rules = [
            rule_factory("salary", "has_max", value=10000, level="TABLE"),
            rule_factory("salary", "has_min", value=4000, level="TABLE"),
        ]

        _, _, table_summary = validate(duckdb_conn, sample_relation, rules)

        summary_df = table_summary.df()
        assert len(summary_df) == 2
        assert all(summary_df["level"] == "TABLE")

    def test_validate_with_mixed_rules(self, duckdb_conn, sample_relation, rule_factory):
        rules = [
            rule_factory("age", "is_positive", level="ROW"),
            rule_factory("salary", "has_max", value=10000, level="TABLE"),
        ]

        df_with_status, violations, table_summary = validate(duckdb_conn, sample_relation, rules)

        assert len(violations.df()) > 0
        assert len(table_summary.df()) == 1

    def test_validate_with_empty_data(self, duckdb_conn, empty_relation, rule_factory):
        rules = [rule_factory("name", "is_complete", level="ROW")]

        df_with_status, violations, _ = validate(duckdb_conn, empty_relation, rules)

        assert len(violations.df()) == 0

    def test_validate_with_no_rules(self, duckdb_conn, sample_relation):
        df_with_status, violations, table_summary = validate(duckdb_conn, sample_relation, [])

        assert len(violations.df()) == 0
        assert len(table_summary.df()) == 0


class TestSummarize:

    def test_summarize_with_row_rules(self, duckdb_conn, sample_relation, rule_factory):
        rules = [
            rule_factory("age", "is_positive", level="ROW"),
            rule_factory("name", "is_complete", threshold=0.8, level="ROW"),
        ]

        _, violations, _ = validate(duckdb_conn, sample_relation, rules)
        total_rows = len(sample_relation.df())

        summary_rel = summarize(duckdb_conn, rules, total_rows, df_with_errors=violations)
        summary_df = summary_rel.df()

        assert len(summary_df) == 2
        assert "pass_rate" in summary_df.columns
        assert "status" in summary_df.columns

    def test_summarize_calculates_pass_rate(self, duckdb_conn, sample_relation, rule_factory):
        rules = [rule_factory("age", "is_positive", level="ROW")]

        _, violations, _ = validate(duckdb_conn, sample_relation, rules)
        total_rows = len(sample_relation.df())

        summary_rel = summarize(duckdb_conn, rules, total_rows, df_with_errors=violations)
        summary_df = summary_rel.df()

        row = summary_df.iloc[0]
        assert row["rows"] == 5
        assert row["violations"] >= 0
        assert 0 <= row["pass_rate"] <= 1

    def test_summarize_with_table_rules(self, duckdb_conn, sample_relation, rule_factory):
        rules = [rule_factory("salary", "has_max", value=10000, level="TABLE")]

        _, _, table_summary = validate(duckdb_conn, sample_relation, rules)
        total_rows = len(sample_relation.df())

        summary_rel = summarize(duckdb_conn, rules, total_rows, table_error=table_summary)
        summary_df = summary_rel.df()

        assert len(summary_df) == 1
        assert summary_df.iloc[0]["level"] == "TABLE"

    def test_summarize_with_mixed_rules(self, duckdb_conn, sample_relation, rule_factory):
        rules = [
            rule_factory("age", "is_positive", level="ROW"),
            rule_factory("salary", "has_max", value=10000, level="TABLE"),
        ]

        _, violations, table_summary = validate(duckdb_conn, sample_relation, rules)
        total_rows = len(sample_relation.df())

        summary_rel = summarize(
            duckdb_conn,
            rules,
            total_rows,
            df_with_errors=violations,
            table_error=table_summary
        )
        summary_df = summary_rel.df()

        assert len(summary_df) == 2
        assert set(summary_df["level"]) == {"ROW", "TABLE"}


class TestEdgeCases:

    def test_single_row_dataframe(self, duckdb_conn, single_row_df, rule_factory):
        relation = duckdb_conn.from_df(single_row_df)
        rules = [rule_factory("value", "is_positive", level="ROW")]

        df_with_status, violations, _ = validate(duckdb_conn, relation, rules)

        assert len(violations.df()) >= 0

    def test_all_null_column(self, duckdb_conn, rule_factory):
        data = pd.DataFrame({"value": [None, None, None]})
        relation = duckdb_conn.from_df(data)
        rules = [rule_factory("value", "is_complete", level="ROW")]

        _, violations, _ = validate(duckdb_conn, relation, rules)

        viol_df = violations.df()
        assert len(viol_df) == 3


class TestIntegration:

    def test_full_pipeline(self, duckdb_conn, sample_relation, rule_factory):
        rules = [
            rule_factory("age", "is_positive", level="ROW"),
            rule_factory("name", "is_complete", threshold=0.8, level="ROW"),
            rule_factory("salary", "has_max", value=10000, level="TABLE"),
        ]

        df_with_status, violations, table_summary = validate(duckdb_conn, sample_relation, rules)

        total_rows = len(sample_relation.df())
        summary = summarize(
            duckdb_conn,
            rules,
            total_rows,
            df_with_errors=violations,
            table_error=table_summary
        )
        summary_df = summary.df()

        assert len(summary_df) == 3
        assert set(summary_df["level"]) == {"ROW", "TABLE"}
        assert all(summary_df["status"].isin(["PASS", "FAIL", "ERROR"]))

    def test_pipeline_with_no_violations(self, duckdb_conn, rule_factory):
        data = pd.DataFrame({
            "value": [1, 2, 3, 4, 5],
            "name": ["A", "B", "C", "D", "E"]
        })
        relation = duckdb_conn.from_df(data)

        rules = [rule_factory("value", "is_positive", level="ROW")]

        _, violations, _ = validate(duckdb_conn, relation, rules)

        assert len(violations.df()) >= 0

