"""
Tests for sumeh/engines/duckdb/ and sumeh/engines/sql_core/

Covers:
- sql_core/analyzers: SQL AST generation
- sql_core/compiler: compile_rules_to_sql
- sql_core/validator: validate_results
- engines/duckdb/engine: validate() integration with real DuckDB
"""

import pytest
import duckdb
import pandas as pd

import sqlglot.expressions as exp

from sumeh.core.rules.rule_model import RuleDefinition
from sumeh.core.models.validation import ValidationStatus, ValidationLevel
from sumeh.engines.sql_core.analyzers import (
    CompletenessAnalyzer,
    MultiFieldCompletenessAnalyzer,
    UniquenessAnalyzer,
    ComparisonAnalyzer,
    BetweenAnalyzer,
    MembershipAnalyzer,
    PatternAnalyzer,
    LegitAnalyzer,
    DateAnalyzer,
    DateBetweenAnalyzer,
    DateComparisonAnalyzer,
    AggregationAnalyzer,
)
from sumeh.engines.sql_core.compiler import compile_rules_to_sql
from sumeh.engines.sql_core.validator import validate_results
from sumeh.engines.duckdb.engine import validate

# Helpers


def is_expression(obj) -> bool:
    return isinstance(obj, exp.Expression)


def sql_str(expr, dialect="duckdb") -> str:
    return expr.sql(dialect=dialect)


# SQLCore Analyzers — AST generation


class TestCompletenessAnalyzerSQL:

    def test_returns_expression(self):
        rule = RuleDefinition(field="email", check_type="is_complete")
        result = CompletenessAnalyzer.analyze(rule)
        assert is_expression(result)

    def test_sql_contains_count(self):
        rule = RuleDefinition(field="email", check_type="is_complete")
        sql = sql_str(CompletenessAnalyzer.analyze(rule))
        assert "COUNT" in sql.upper()

    def test_sql_contains_field(self):
        rule = RuleDefinition(field="email", check_type="is_complete")
        sql = sql_str(CompletenessAnalyzer.analyze(rule))
        assert "email" in sql


class TestMultiFieldCompletenessAnalyzerSQL:

    def test_returns_expression(self):
        rule = RuleDefinition(field=["name", "email"], check_type="are_complete")
        result = MultiFieldCompletenessAnalyzer.analyze(rule)
        assert is_expression(result)

    def test_sql_contains_avg(self):
        rule = RuleDefinition(field=["name", "email"], check_type="are_complete")
        sql = sql_str(MultiFieldCompletenessAnalyzer.analyze(rule))
        assert "AVG" in sql.upper()

    def test_sql_contains_all_fields(self):
        rule = RuleDefinition(field=["name", "email"], check_type="are_complete")
        sql = sql_str(MultiFieldCompletenessAnalyzer.analyze(rule))
        assert "name" in sql
        assert "email" in sql


class TestUniquenessAnalyzerSQL:

    def test_returns_expression(self):
        rule = RuleDefinition(field="user_id", check_type="is_unique")
        result = UniquenessAnalyzer.analyze(rule)
        assert is_expression(result)

    def test_sql_contains_distinct(self):
        rule = RuleDefinition(field="user_id", check_type="is_unique")
        sql = sql_str(UniquenessAnalyzer.analyze(rule))
        assert "DISTINCT" in sql.upper()


class TestComparisonAnalyzerSQL:

    @pytest.mark.parametrize(
        "check_type,value",
        [
            ("is_positive", None),
            ("is_negative", None),
            ("is_greater_than", 18),
            ("is_less_than", 100),
            ("is_equal", 42),
            ("is_greater_or_equal_than", 0),
            ("is_less_or_equal_than", 999),
            ("is_in_millions", None),
            ("is_in_billions", None),
        ],
    )
    def test_returns_expression(self, check_type, value):
        rule = RuleDefinition(field="age", check_type=check_type, value=value)
        result = ComparisonAnalyzer.analyze(rule)
        assert is_expression(result)

    def test_is_positive_sql_contains_gt_zero(self):
        rule = RuleDefinition(field="revenue", check_type="is_positive")
        sql = sql_str(ComparisonAnalyzer.analyze(rule))
        assert "revenue" in sql
        assert "0" in sql


class TestBetweenAnalyzerSQL:

    def test_returns_expression(self):
        rule = RuleDefinition(field="age", check_type="is_between", value=[18, 65])
        result = BetweenAnalyzer.analyze(rule)
        assert is_expression(result)

    def test_sql_contains_between(self):
        rule = RuleDefinition(field="age", check_type="is_between", value=[18, 65])
        sql = sql_str(BetweenAnalyzer.analyze(rule))
        assert "BETWEEN" in sql.upper()

    def test_sql_contains_bounds(self):
        rule = RuleDefinition(field="age", check_type="is_between", value=[18, 65])
        sql = sql_str(BetweenAnalyzer.analyze(rule))
        assert "18" in sql
        assert "65" in sql


class TestMembershipAnalyzerSQL:

    def test_is_contained_in_sql(self):
        rule = RuleDefinition(
            field="status", check_type="is_contained_in", value=["active", "inactive"]
        )
        sql = sql_str(MembershipAnalyzer.analyze(rule))
        assert "IN" in sql.upper()
        assert "active" in sql

    def test_not_contained_in_sql(self):
        rule = RuleDefinition(
            field="status", check_type="not_contained_in", value=["banned"]
        )
        sql = sql_str(MembershipAnalyzer.analyze(rule))
        assert "NOT" in sql.upper()
        assert "banned" in sql


class TestPatternAnalyzerSQL:

    def test_returns_expression(self):
        rule = RuleDefinition(field="email", check_type="has_pattern", value=r"@")
        result = PatternAnalyzer.analyze(rule)
        assert is_expression(result)

    def test_sql_contains_pattern(self):
        rule = RuleDefinition(field="email", check_type="has_pattern", value=r"@")
        sql = sql_str(PatternAnalyzer.analyze(rule))
        assert "@" in sql


class TestLegitAnalyzerSQL:

    def test_returns_expression(self):
        rule = RuleDefinition(field="notes", check_type="is_legit")
        result = LegitAnalyzer.analyze(rule)
        assert is_expression(result)

    def test_sql_contains_null_check(self):
        rule = RuleDefinition(field="notes", check_type="is_legit")
        sql = sql_str(LegitAnalyzer.analyze(rule))
        assert "IS" in sql.upper()  # IS NULL ou IS <expr>
        assert "notes" in sql.lower()


class TestDateAnalyzerSQL:

    @pytest.mark.parametrize(
        "check_type",
        [
            "is_today",
            "is_yesterday",
            "is_past_date",
            "is_future_date",
            "is_on_weekday",
            "is_on_weekend",
        ],
    )
    def test_returns_expression(self, check_type):
        rule = RuleDefinition(field="created_at", check_type=check_type)
        result = DateAnalyzer.analyze(rule)
        assert is_expression(result)


class TestDateBetweenAnalyzerSQL:

    def test_returns_expression(self):
        rule = RuleDefinition(
            field="dt", check_type="is_date_between", value=["2020-01-01", "2024-12-31"]
        )
        result = DateBetweenAnalyzer.analyze(rule)
        assert is_expression(result)

    def test_sql_contains_between(self):
        rule = RuleDefinition(
            field="dt", check_type="is_date_between", value=["2020-01-01", "2024-12-31"]
        )
        sql = sql_str(DateBetweenAnalyzer.analyze(rule))
        assert "BETWEEN" in sql.upper()


class TestDateComparisonAnalyzerSQL:

    def test_is_date_after_expression(self):
        rule = RuleDefinition(
            field="dt", check_type="is_date_after", value="2020-01-01"
        )
        result = DateComparisonAnalyzer.analyze(rule)
        assert is_expression(result)

    def test_is_date_before_expression(self):
        rule = RuleDefinition(
            field="dt", check_type="is_date_before", value="2025-01-01"
        )
        result = DateComparisonAnalyzer.analyze(rule)
        assert is_expression(result)


class TestAggregationAnalyzerSQL:

    @pytest.mark.parametrize(
        "check_type,expected_func",
        [
            ("has_min", "MIN"),
            ("has_max", "MAX"),
            ("has_sum", "SUM"),
            ("has_mean", "AVG"),
            ("has_cardinality", "COUNT"),
        ],
    )
    def test_returns_correct_aggregation(self, check_type, expected_func):
        rule = RuleDefinition(field="age", check_type=check_type, value=40.0)
        sql = sql_str(AggregationAnalyzer.analyze(rule))
        assert expected_func in sql.upper()


# compile_rules_to_sql


class TestCompileRulesToSQL:

    def test_returns_tuple(self):
        rules = [RuleDefinition(field="email", check_type="is_complete")]
        result = compile_rules_to_sql(rules, "users")
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_sql_is_string(self):
        rules = [RuleDefinition(field="email", check_type="is_complete")]
        sql, _ = compile_rules_to_sql(rules, "users")
        assert isinstance(sql, str)

    def test_rule_ids_length_matches_rules(self):
        rules = [
            RuleDefinition(field="email", check_type="is_complete"),
            RuleDefinition(field="user_id", check_type="is_unique"),
        ]
        _, rule_ids = compile_rules_to_sql(rules, "users")
        assert len(rule_ids) == 2

    def test_sql_contains_table_name(self):
        rules = [RuleDefinition(field="email", check_type="is_complete")]
        sql, _ = compile_rules_to_sql(rules, "stg_users")
        assert "stg_users" in sql

    def test_empty_rules_returns_empty(self):
        sql, rule_ids = compile_rules_to_sql([], "users")
        assert sql == ""
        assert rule_ids == []

    def test_global_filter_included(self):
        rules = [RuleDefinition(field="email", check_type="is_complete")]
        sql, _ = compile_rules_to_sql(rules, "users", global_filter="status = 'active'")
        assert "active" in sql
        assert "WHERE" in sql.upper()

    def test_duckdb_dialect(self):
        rules = [RuleDefinition(field="email", check_type="is_complete")]
        sql, _ = compile_rules_to_sql(rules, "users", dialect="duckdb")
        assert isinstance(sql, str)
        assert len(sql) > 0

    def test_multiple_rules_single_query(self):
        rules = [
            RuleDefinition(field="email", check_type="is_complete"),
            RuleDefinition(field="user_id", check_type="is_unique"),
            RuleDefinition(field="age", check_type="is_positive"),
        ]
        sql, rule_ids = compile_rules_to_sql(rules, "users", dialect="duckdb")
        assert sql.upper().count("SELECT") == 1
        assert len(rule_ids) == 3


# validate_results


class TestValidateResults:

    def _make_rule(self, check_type="is_complete", field="email", threshold=1.0):
        return RuleDefinition(field=field, check_type=check_type, threshold=threshold)

    def test_returns_list(self):
        rule = self._make_rule()
        rule_id = f"{rule.check_type}:{rule.field}"
        results = validate_results((1.0,), [rule_id], [rule], total_rows=100)
        assert isinstance(results, list)

    def test_pass_when_metric_meets_threshold(self):
        rule = self._make_rule(threshold=1.0)
        rule_id = f"{rule.check_type}:{rule.field}"
        results = validate_results((1.0,), [rule_id], [rule], total_rows=100)
        assert results[0].status == ValidationStatus.PASS

    def test_fail_when_metric_below_threshold(self):
        rule = self._make_rule(threshold=1.0)
        rule_id = f"{rule.check_type}:{rule.field}"
        results = validate_results((0.8,), [rule_id], [rule], total_rows=100)
        assert results[0].status == ValidationStatus.FAIL

    def test_aggregation_pass(self):
        rule = RuleDefinition(
            field="age", check_type="has_mean", value=40.0, threshold=0.0
        )
        rule_id = f"{rule.check_type}:{rule.field}"
        results = validate_results((40.0,), [rule_id], [rule], total_rows=100)
        assert results[0].status == ValidationStatus.PASS

    def test_aggregation_fail(self):
        rule = RuleDefinition(
            field="age", check_type="has_mean", value=40.0, threshold=0.0
        )
        rule_id = f"{rule.check_type}:{rule.field}"
        results = validate_results((35.0,), [rule_id], [rule], total_rows=100)
        assert results[0].status == ValidationStatus.FAIL

    def test_unknown_rule_id_skipped(self):
        rule = self._make_rule()
        results = validate_results((1.0,), ["nonexistent_id"], [rule], total_rows=100)
        assert len(results) == 0

    def test_none_value_handled(self):
        rule = self._make_rule(threshold=1.0)
        rule_id = f"{rule.check_type}:{rule.field}"
        results = validate_results((None,), [rule_id], [rule], total_rows=100)
        assert results[0].status in (
            ValidationStatus.PASS,
            ValidationStatus.FAIL,
            ValidationStatus.ERROR,
        )

    def test_multiple_rules(self):
        rules = [
            RuleDefinition(field="email", check_type="is_complete", threshold=1.0),
            RuleDefinition(field="user_id", check_type="is_unique", threshold=1.0),
        ]
        rule_ids = [f"{r.check_type}:{r.field}" for r in rules]
        results = validate_results((1.0, 1.0), rule_ids, rules, total_rows=100)
        assert len(results) == 2
        assert all(r.status == ValidationStatus.PASS for r in results)


# DuckDB engine — integration


class TestDuckDBValidate:

    @pytest.fixture
    def df(self):
        return pd.DataFrame(
            {
                "user_id": [1, 2, 3, 4, 5],
                "email": ["a@a.com", "b@b.com", None, "d@d.com", "e@e.com"],
                "age": [25, 30, 17, 45, 60],
                "status": ["active", "inactive", "active", "banned", "active"],
            }
        )

    def test_returns_validation_report(self, df):
        from sumeh.core.models.validation import ValidationReport

        rules = [RuleDefinition(field="user_id", check_type="is_unique")]
        report = validate(df, rules)
        assert isinstance(report, ValidationReport)

    def test_engine_is_duckdb(self, df):
        rules = [RuleDefinition(field="user_id", check_type="is_unique")]
        report = validate(df, rules)
        assert report.engine == "duckdb"

    def test_total_rows_correct(self, df):
        rules = [RuleDefinition(field="user_id", check_type="is_unique")]
        report = validate(df, rules)
        assert report.total_rows == 5

    def test_completeness_pass(self, df):
        rules = [
            RuleDefinition(field="user_id", check_type="is_complete", threshold=1.0)
        ]
        report = validate(df, rules)
        assert report.results[0].status == ValidationStatus.PASS

    def test_completeness_fail(self, df):
        rules = [RuleDefinition(field="email", check_type="is_complete", threshold=1.0)]
        report = validate(df, rules)
        assert report.results[0].status == ValidationStatus.FAIL

    def test_uniqueness_pass(self, df):
        rules = [RuleDefinition(field="user_id", check_type="is_unique", threshold=1.0)]
        report = validate(df, rules)
        assert report.results[0].status == ValidationStatus.PASS

    def test_is_positive_pass(self):
        df = pd.DataFrame({"revenue": [1.0, 2.0, 3.0]})
        rules = [
            RuleDefinition(field="revenue", check_type="is_positive", threshold=1.0)
        ]
        report = validate(df, rules)
        assert report.results[0].status == ValidationStatus.PASS

    def test_is_positive_fail(self):
        df = pd.DataFrame({"revenue": [1.0, -1.0, 3.0]})
        rules = [
            RuleDefinition(field="revenue", check_type="is_positive", threshold=1.0)
        ]
        report = validate(df, rules)
        assert report.results[0].status == ValidationStatus.FAIL

    def test_membership_pass(self, df):
        rules = [
            RuleDefinition(
                field="status",
                check_type="is_contained_in",
                value=["active", "inactive", "banned"],
                threshold=1.0,
            )
        ]
        report = validate(df, rules)
        assert report.results[0].status == ValidationStatus.PASS

    def test_aggregation_mean_pass(self):
        df = pd.DataFrame({"age": [20, 30, 40, 50, 60]})
        rules = [
            RuleDefinition(
                field="age", check_type="has_mean", value=40.0, threshold=0.0
            )
        ]
        report = validate(df, rules)
        assert report.results[0].status == ValidationStatus.PASS

    def test_empty_rules_returns_empty_report(self, df):
        report = validate(df, [])
        assert len(report.results) == 0

    def test_multiple_rules(self, df):
        rules = [
            RuleDefinition(field="user_id", check_type="is_unique", threshold=1.0),
            RuleDefinition(field="user_id", check_type="is_complete", threshold=1.0),
        ]
        report = validate(df, rules)
        assert len(report.results) == 2

    def test_string_table_name_without_connection_raises(self):
        rules = [RuleDefinition(field="user_id", check_type="is_unique")]
        report = validate("my_table", rules)
        assert report.error_message is not None

    def test_string_table_name_with_connection(self):
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE users AS SELECT 1 AS user_id, 'a@a.com' AS email")
        rules = [RuleDefinition(field="user_id", check_type="is_unique", threshold=1.0)]
        report = validate("users", rules, connection=con)
        assert report.results[0].status == ValidationStatus.PASS
        con.close()

    def test_nonexistent_table_returns_error_message(self):
        con = duckdb.connect(":memory:")
        rules = [RuleDefinition(field="user_id", check_type="is_unique")]
        report = validate("nonexistent_table", rules, connection=con)
        assert report.error_message is not None
        con.close()

    def test_execution_time_populated(self, df):
        rules = [RuleDefinition(field="user_id", check_type="is_unique")]
        report = validate(df, rules)
        assert report.execution_time_ms > 0
