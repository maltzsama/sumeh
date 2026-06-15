"""
Tests for sumeh/engines/sql_core/ — focused on what test_duckdb.py does NOT cover.

Scope:
- WeekdayAnalyzer (is_on_monday … is_on_sunday)
- SatisfiesAnalyzer
- ValidateDateFormatAnalyzer
- DateAnalyzer: is_t_minus_2, is_t_minus_3
- ColumnComparisonAnalyzer (is_equal_than)
- IS NULL fix: LegitAnalyzer and MultiFieldCompletenessAnalyzer
- NotImplementedError on unknown check_type
- Registry completeness (all declared keys resolve)
- Cross-dialect SQL output (bigquery vs duckdb)
- Manifest × registry coverage (declared SQL engines have analyzer)
"""

import json
import pytest

from pathlib import Path

import sqlglot.expressions as exp

from sumeh.core.rules.rule_model import RuleDefinition
from sumeh.engines.sql_core.analyzers import (
    WeekdayAnalyzer,
    SatisfiesAnalyzer,
    ValidateDateFormatAnalyzer,
    DateAnalyzer,
    ColumnComparisonAnalyzer,
    LegitAnalyzer,
    MultiFieldCompletenessAnalyzer,
    ComparisonAnalyzer,
    AggregationAnalyzer,
)
from sumeh.engines.sql_core.registry import (
    VALIDATION_REGISTRY,
    get_analyzer,
    get_constraint,
    list_implemented,
)
from sumeh.engines.sql_core.compiler import compile_rules_to_sql

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def is_expression(obj) -> bool:
    return isinstance(obj, exp.Expression)


def sql_bq(expr) -> str:
    return expr.sql(dialect="bigquery")


def sql_dk(expr) -> str:
    return expr.sql(dialect="duckdb")


# ---------------------------------------------------------------------------
# WeekdayAnalyzer
# ---------------------------------------------------------------------------


class TestWeekdayAnalyzer:
    """
    DOW convention: Sunday=1, Monday=2 … Saturday=7 (BigQuery/Spark/Trino).
    """

    @pytest.mark.parametrize(
        "check_type,expected_dow",
        [
            ("is_on_sunday", 1),
            ("is_on_monday", 2),
            ("is_on_tuesday", 3),
            ("is_on_wednesday", 4),
            ("is_on_thursday", 5),
            ("is_on_friday", 6),
            ("is_on_saturday", 7),
        ],
    )
    def test_returns_expression(self, check_type, expected_dow):
        rule = RuleDefinition(field="dt", check_type=check_type)
        assert is_expression(WeekdayAnalyzer.analyze(rule))

    @pytest.mark.parametrize(
        "check_type,expected_dow",
        [
            ("is_on_sunday", 1),
            ("is_on_monday", 2),
            ("is_on_tuesday", 3),
            ("is_on_wednesday", 4),
            ("is_on_thursday", 5),
            ("is_on_friday", 6),
            ("is_on_saturday", 7),
        ],
    )
    def test_sql_contains_correct_dow_number(self, check_type, expected_dow):
        rule = RuleDefinition(field="dt", check_type=check_type)
        sql = sql_bq(WeekdayAnalyzer.analyze(rule))
        assert str(expected_dow) in sql

    def test_bigquery_uses_day_of_week_function(self):
        rule = RuleDefinition(field="dt", check_type="is_on_monday")
        sql = sql_bq(WeekdayAnalyzer.analyze(rule))
        assert "DAY_OF_WEEK" in sql.upper()

    def test_duckdb_uses_dayofweek_function(self):
        rule = RuleDefinition(field="dt", check_type="is_on_monday")
        sql = sql_dk(WeekdayAnalyzer.analyze(rule))
        assert "DAYOFWEEK" in sql.upper()

    def test_sql_contains_field_name(self):
        rule = RuleDefinition(field="created_at", check_type="is_on_friday")
        sql = sql_bq(WeekdayAnalyzer.analyze(rule))
        assert "created_at" in sql

    def test_wrapped_in_avg_case(self):
        rule = RuleDefinition(field="dt", check_type="is_on_monday")
        sql = sql_bq(WeekdayAnalyzer.analyze(rule))
        assert "AVG" in sql.upper()
        assert "CASE" in sql.upper()

    def test_unknown_check_type_raises(self):
        class _F:
            check_type = "is_on_funday"
            field = "dt"
            value = None

        with pytest.raises(NotImplementedError):
            WeekdayAnalyzer.analyze(_F())

    def test_field_as_list_uses_first(self):
        rule = RuleDefinition(field=["dt", "other"], check_type="is_on_monday")
        sql = sql_bq(WeekdayAnalyzer.analyze(rule))
        assert "dt" in sql


# ---------------------------------------------------------------------------
# SatisfiesAnalyzer
# ---------------------------------------------------------------------------


class TestSatisfiesAnalyzer:

    def test_returns_expression(self):
        rule = RuleDefinition(field="age", check_type="satisfies", value="age > 0")
        assert is_expression(SatisfiesAnalyzer.analyze(rule))

    def test_sql_contains_condition(self):
        rule = RuleDefinition(field="age", check_type="satisfies", value="age > 0")
        sql = sql_bq(SatisfiesAnalyzer.analyze(rule))
        assert "age" in sql
        assert "0" in sql

    def test_compound_condition(self):
        rule = RuleDefinition(
            field="x",
            check_type="satisfies",
            value="age > 18 AND status = 'active'",
        )
        sql = sql_bq(SatisfiesAnalyzer.analyze(rule))
        assert "age" in sql
        assert "18" in sql
        assert "active" in sql
        assert "AND" in sql.upper()

    def test_wrapped_in_avg_case(self):
        rule = RuleDefinition(field="x", check_type="satisfies", value="x IS NOT NULL")
        sql = sql_bq(SatisfiesAnalyzer.analyze(rule))
        assert "AVG" in sql.upper()

    def test_consistent_across_dialects(self):
        rule = RuleDefinition(field="x", check_type="satisfies", value="revenue > 0")
        expr = SatisfiesAnalyzer.analyze(rule)
        assert "revenue" in sql_bq(expr)
        assert "revenue" in sql_dk(expr)


# ---------------------------------------------------------------------------
# ValidateDateFormatAnalyzer
# ---------------------------------------------------------------------------


class TestValidateDateFormatAnalyzer:

    def test_returns_expression(self):
        rule = RuleDefinition(field="dt", check_type="validate_date_format")
        assert is_expression(ValidateDateFormatAnalyzer.analyze(rule))

    def test_bigquery_uses_safe_cast(self):
        rule = RuleDefinition(field="dt", check_type="validate_date_format")
        sql = sql_bq(ValidateDateFormatAnalyzer.analyze(rule))
        assert "SAFE_CAST" in sql.upper()

    def test_duckdb_uses_try_cast(self):
        rule = RuleDefinition(field="dt", check_type="validate_date_format")
        sql = sql_dk(ValidateDateFormatAnalyzer.analyze(rule))
        assert "TRY_CAST" in sql.upper()

    def test_sql_contains_is_null(self):
        rule = RuleDefinition(field="dt", check_type="validate_date_format")
        sql = sql_bq(ValidateDateFormatAnalyzer.analyze(rule))
        assert "IS NULL" in sql.upper()
        assert "NOT" in sql.upper()

    def test_cast_target_is_date(self):
        rule = RuleDefinition(field="dt", check_type="validate_date_format")
        sql = sql_bq(ValidateDateFormatAnalyzer.analyze(rule))
        assert "DATE" in sql.upper()

    def test_field_name_present(self):
        rule = RuleDefinition(field="birth_date", check_type="validate_date_format")
        sql = sql_bq(ValidateDateFormatAnalyzer.analyze(rule))
        assert "birth_date" in sql

    def test_wrapped_in_avg_case(self):
        rule = RuleDefinition(field="dt", check_type="validate_date_format")
        sql = sql_bq(ValidateDateFormatAnalyzer.analyze(rule))
        assert "AVG" in sql.upper()


# ---------------------------------------------------------------------------
# DateAnalyzer — t_minus_2 / t_minus_3
# ---------------------------------------------------------------------------


class TestDateAnalyzerTMinus:

    @pytest.mark.parametrize(
        "check_type,days", [("is_t_minus_2", 2), ("is_t_minus_3", 3)]
    )
    def test_returns_expression(self, check_type, days):
        rule = RuleDefinition(field="dt", check_type=check_type)
        assert is_expression(DateAnalyzer.analyze(rule))

    @pytest.mark.parametrize(
        "check_type,days", [("is_t_minus_2", 2), ("is_t_minus_3", 3)]
    )
    def test_sql_contains_day_offset(self, check_type, days):
        rule = RuleDefinition(field="dt", check_type=check_type)
        sql = sql_bq(DateAnalyzer.analyze(rule))
        assert str(days) in sql

    def test_t_minus_2_bigquery_uses_date_sub(self):
        rule = RuleDefinition(field="dt", check_type="is_t_minus_2")
        sql = sql_bq(DateAnalyzer.analyze(rule))
        assert "DATE_SUB" in sql.upper()

    def test_t_minus_3_bigquery_uses_date_sub(self):
        rule = RuleDefinition(field="dt", check_type="is_t_minus_3")
        sql = sql_bq(DateAnalyzer.analyze(rule))
        assert "DATE_SUB" in sql.upper()

    def test_unknown_check_type_raises(self):
        class _F:
            check_type = "is_t_minus_99"
            field = "dt"
            value = None

        with pytest.raises(NotImplementedError):
            DateAnalyzer.analyze(_F())


# ---------------------------------------------------------------------------
# ColumnComparisonAnalyzer (is_equal_than)
# ---------------------------------------------------------------------------


class TestColumnComparisonAnalyzer:

    def test_returns_expression(self):
        rule = RuleDefinition(field="col_a", check_type="is_equal_than", value="col_b")
        assert is_expression(ColumnComparisonAnalyzer.analyze(rule))

    def test_sql_contains_both_columns(self):
        rule = RuleDefinition(field="col_a", check_type="is_equal_than", value="col_b")
        sql = sql_bq(ColumnComparisonAnalyzer.analyze(rule))
        assert "col_a" in sql
        assert "col_b" in sql

    def test_sql_uses_equality(self):
        rule = RuleDefinition(field="col_a", check_type="is_equal_than", value="col_b")
        sql = sql_bq(ColumnComparisonAnalyzer.analyze(rule))
        assert "=" in sql

    def test_field_as_list_uses_first(self):
        rule = RuleDefinition(
            field=["col_a", "col_b"], check_type="is_equal_than", value="col_c"
        )
        sql = sql_bq(ColumnComparisonAnalyzer.analyze(rule))
        assert "col_a" in sql


# ---------------------------------------------------------------------------
# IS NULL fix — expression=Null() (was kind=Null(), generated broken SQL)
# ---------------------------------------------------------------------------


class TestIsNullFix:

    def test_legit_sql_contains_is_null(self):
        rule = RuleDefinition(field="name", check_type="is_legit")
        sql = sql_bq(LegitAnalyzer.analyze(rule))
        assert "IS NULL" in sql.upper()

    def test_legit_sql_contains_not(self):
        rule = RuleDefinition(field="name", check_type="is_legit")
        sql = sql_bq(LegitAnalyzer.analyze(rule))
        assert "NOT" in sql.upper()

    def test_multi_completeness_sql_contains_is_null(self):
        rule = RuleDefinition(field=["a", "b"], check_type="are_complete")
        sql = sql_bq(MultiFieldCompletenessAnalyzer.analyze(rule))
        assert "IS NULL" in sql.upper()

    def test_multi_completeness_sql_well_formed(self):
        """Regression: kind= produced 'x IS' without NULL — verify full clause."""
        rule = RuleDefinition(field=["a", "b"], check_type="are_complete")
        sql = sql_bq(MultiFieldCompletenessAnalyzer.analyze(rule))
        # must not end IS without NULL (the old bug: "x IS")
        assert "IS NULL" in sql.upper() or "IS NOT NULL" in sql.upper()


# ---------------------------------------------------------------------------
# NotImplementedError — unknown check_types no longer return 0.0
# ---------------------------------------------------------------------------


class TestNotImplementedRaises:
    """
    RuleDefinition validates check_type against manifest in __post_init__.
    To test analyzer-level NotImplementedError, bypass RuleDefinition and use
    a simple namespace object so the analyzer receives an unknown check_type.
    """

    class _FakeRule:
        def __init__(self, check_type, field="x", value=None):
            self.check_type = check_type
            self.field = field
            self.value = value

    def test_comparison_unknown_raises(self):
        rule = self._FakeRule(check_type="is_purple", value=1)
        with pytest.raises(NotImplementedError):
            ComparisonAnalyzer.analyze(rule)

    def test_aggregation_unknown_raises(self):
        rule = self._FakeRule(check_type="has_vibes", value=1)
        with pytest.raises(NotImplementedError):
            AggregationAnalyzer.analyze(rule)

    def test_weekday_unknown_raises(self):
        rule = self._FakeRule(check_type="is_on_moonday", field="dt")
        with pytest.raises(NotImplementedError):
            WeekdayAnalyzer.analyze(rule)


# ---------------------------------------------------------------------------
# Registry completeness
# ---------------------------------------------------------------------------


class TestRegistry:

    def test_is_composite_key_registered(self):
        assert "is_composite_key" in VALIDATION_REGISTRY

    def test_is_equal_than_registered(self):
        assert "is_equal_than" in VALIDATION_REGISTRY

    def test_satisfies_registered(self):
        assert "satisfies" in VALIDATION_REGISTRY

    def test_validate_date_format_registered(self):
        assert "validate_date_format" in VALIDATION_REGISTRY

    @pytest.mark.parametrize(
        "check_type",
        [
            "is_on_monday",
            "is_on_tuesday",
            "is_on_wednesday",
            "is_on_thursday",
            "is_on_friday",
            "is_on_saturday",
            "is_on_sunday",
        ],
    )
    def test_weekdays_registered(self, check_type):
        assert check_type in VALIDATION_REGISTRY

    def test_get_analyzer_returns_none_for_unknown(self):
        assert get_analyzer("does_not_exist") is None

    def test_get_constraint_raises_for_unknown(self):
        with pytest.raises(KeyError):
            get_constraint("does_not_exist")

    def test_list_implemented_returns_list(self):
        result = list_implemented()
        assert isinstance(result, list)
        assert len(result) > 0

    @pytest.mark.parametrize("check_type", list(VALIDATION_REGISTRY.keys()))
    def test_all_registered_entries_have_valid_analyzer_and_constraint(
        self, check_type
    ):
        analyzer, constraint = VALIDATION_REGISTRY[check_type]
        assert hasattr(
            analyzer, "analyze"
        ), f"{check_type}: analyzer missing .analyze()"
        assert hasattr(
            constraint, "check"
        ), f"{check_type}: constraint missing .check()"


# ---------------------------------------------------------------------------
# Cross-dialect SQL output
# ---------------------------------------------------------------------------


class TestCrossDialect:
    """Same AST, different dialect output."""

    def test_satisfies_valid_in_both_dialects(self):
        rule = RuleDefinition(field="x", check_type="satisfies", value="revenue > 0")
        expr = SatisfiesAnalyzer.analyze(rule)
        assert "revenue" in sql_bq(expr)
        assert "revenue" in sql_dk(expr)

    def test_weekday_bigquery_vs_duckdb_different_function_name(self):
        rule = RuleDefinition(field="dt", check_type="is_on_monday")
        expr = WeekdayAnalyzer.analyze(rule)
        # BQ → DAY_OF_WEEK, DuckDB → DAYOFWEEK
        assert sql_bq(expr) != sql_dk(expr)

    def test_validate_date_format_bigquery_vs_duckdb_different_cast(self):
        rule = RuleDefinition(field="dt", check_type="validate_date_format")
        expr = ValidateDateFormatAnalyzer.analyze(rule)
        assert "SAFE_CAST" in sql_bq(expr).upper()
        assert "TRY_CAST" in sql_dk(expr).upper()

    def test_t_minus_2_valid_sql_in_both_dialects(self):
        rule = RuleDefinition(field="dt", check_type="is_t_minus_2")
        expr = DateAnalyzer.analyze(rule)
        # Both should be non-empty valid SQL
        assert len(sql_bq(expr)) > 0
        assert len(sql_dk(expr)) > 0

    def test_compile_rules_bigquery_dialect(self):
        rules = [
            RuleDefinition(field="dt", check_type="is_on_monday"),
            RuleDefinition(field="dt", check_type="validate_date_format"),
            RuleDefinition(field="x", check_type="satisfies", value="x > 0"),
        ]
        sql, rule_ids = compile_rules_to_sql(rules, "my_table", dialect="bigquery")
        assert "my_table" in sql
        assert len(rule_ids) == 3
        assert "DAY_OF_WEEK" in sql.upper()
        assert "SAFE_CAST" in sql.upper()


# ---------------------------------------------------------------------------
# Manifest × registry coverage
# ---------------------------------------------------------------------------


SQL_ENGINES = {"duckdb", "bigquery"}
_manifest_path = (
    Path(__file__).parent.parent.parent / "sumeh" / "core" / "rules" / "manifest.json"
)
_manifest_rules = json.loads(_manifest_path.read_text())["rules"]


@pytest.mark.parametrize("rule", _manifest_rules, ids=lambda r: r["check_type"])
def test_manifest_sql_engines_have_analyzer(rule):
    """
    Any rule that declares duckdb or bigquery in its engines list
    must have a corresponding entry in VALIDATION_REGISTRY.

    If this test is red: either add the analyzer, or remove the SQL engine
    from that rule's engines list in manifest.json.
    """
    if SQL_ENGINES & set(rule.get("engines", [])):
        assert rule["check_type"] in VALIDATION_REGISTRY, (
            f"'{rule['check_type']}' declares SQL engine support in manifest.json "
            f"but has no entry in sql_core/registry.py"
        )
