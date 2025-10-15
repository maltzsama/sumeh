# tests/engines/test_dask_engine.py

import dask.dataframe as dd
import pandas as pd
import pytest

from sumeh.core.rules.rule_model import RuleDef
# CORRIGIDO: importar validate do engine, não do sumeh
from sumeh.engines.dask_engine import (
    validate,  # <-- AQUI
    validate_row_level,
    validate_table_level,
    summarize,
    is_positive,
    is_negative,
    is_complete,
    is_unique,
    are_unique,
    is_greater_than,
    is_less_than,
    is_between,
    is_contained_in,
    not_contained_in,
    has_pattern,
    has_max,
    has_min,
    has_mean,
    has_std,
)


@pytest.fixture
def sample_data():
    data = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", None, "Eve"],
        "age": [25, -30, 35, 40, -45],
        "salary": [5000.0, 6000.0, 7000.0, 8000.0, 9000.0],
        "email": [
            "alice@example.com",
            "invalid",
            "charlie@test.com",
            "david@test.com",
            "eve@example.com"
        ],
        "department": ["HR", "HR", "IT", "IT", "Finance"],
    })
    return dd.from_pandas(data, npartitions=2)


@pytest.fixture
def empty_data():
    data = pd.DataFrame(columns=["id", "name", "value"])
    return dd.from_pandas(data, npartitions=1)


@pytest.fixture
def rule_factory():
    def _create(field, check_type, value="", threshold=1.0, level="ROW", **kwargs):
        return RuleDef.from_dict({
            "field": field,
            "check_type": check_type,
            "value": value,
            "threshold": threshold,
            "level": level,
            **kwargs
        })
    return _create


class TestRowLevelFunctions:

    def test_is_positive_finds_negative_values(self, sample_data, rule_factory):
        rule = rule_factory("age", "is_positive")
        result = is_positive(sample_data, rule).compute()

        assert len(result) == 2
        assert all(result["age"] < 0)
        assert "dq_status" in result.columns

    def test_is_positive_with_all_positive(self, rule_factory):
        data = pd.DataFrame({"value": [1, 2, 3, 4, 5]})
        dask_df = dd.from_pandas(data, npartitions=1)

        rule = rule_factory("value", "is_positive")
        result = is_positive(dask_df, rule).compute()

        assert len(result) == 0

    def test_is_negative_finds_positive_values(self, sample_data, rule_factory):
        rule = rule_factory("age", "is_negative")
        result = is_negative(sample_data, rule).compute()

        assert len(result) == 3
        assert all(result["age"] >= 0)

    def test_is_complete_finds_nulls(self, sample_data, rule_factory):
        rule = rule_factory("name", "is_complete")
        result = is_complete(sample_data, rule).compute()

        assert len(result) == 1
        assert result.iloc[0]["id"] == 4

    def test_is_unique_finds_duplicates(self, sample_data, rule_factory):
        rule = rule_factory("department", "is_unique")
        result = is_unique(sample_data, rule).compute()

        assert len(result) == 4
        assert set(result["department"].unique()) == {"HR", "IT"}

    def test_are_unique_multiple_fields(self, sample_data, rule_factory):
        rule = rule_factory(["department", "age"], "are_unique")
        result = are_unique(sample_data, rule).compute()

        assert len(result) == 0

    def test_is_greater_than(self, sample_data, rule_factory):
        rule = rule_factory("salary", "is_greater_than", value=7000)
        result = is_greater_than(sample_data, rule).compute()

        assert len(result) == 2
        assert all(result["salary"] > 7000)

    def test_is_less_than(self, sample_data, rule_factory):
        rule = rule_factory("salary", "is_less_than", value=6000)
        result = is_less_than(sample_data, rule).compute()

        assert len(result) == 4
        assert all(result["salary"] >= 6000)

    def test_is_between(self, sample_data, rule_factory):
        rule = rule_factory("age", "is_between", value="[20,40]")
        result = is_between(sample_data, rule).compute()

        assert len(result) == 2

    def test_is_contained_in(self, sample_data, rule_factory):
        rule = rule_factory("department", "is_contained_in", value="[HR,IT,Finance]")
        result = is_contained_in(sample_data, rule).compute()

        assert len(result) == 0

    def test_not_contained_in(self, sample_data, rule_factory):
        rule = rule_factory("department", "not_contained_in", value="[HR]")
        result = not_contained_in(sample_data, rule).compute()

        assert len(result) == 2
        assert all(result["department"] == "HR")

    def test_has_pattern(self, sample_data, rule_factory):
        rule = rule_factory("email", "has_pattern", value=r"^[\w\.-]+@[\w\.-]+\.\w+$")
        result = has_pattern(sample_data, rule).compute()

        assert len(result) >= 1


class TestTableLevelFunctions:

    def test_has_max(self, sample_data, rule_factory):
        rule = rule_factory("salary", "has_max", value=10000, level="TABLE")
        result = has_max(sample_data, rule)

        assert result["status"] == "PASS"
        assert result["actual"] == 9000.0
        assert result["expected"] == 10000.0

    def test_has_max_fail(self, sample_data, rule_factory):
        rule = rule_factory("salary", "has_max", value=8000, level="TABLE")
        result = has_max(sample_data, rule)

        assert result["status"] == "FAIL"
        assert result["actual"] == 9000.0

    def test_has_min(self, sample_data, rule_factory):
        rule = rule_factory("salary", "has_min", value=4000, level="TABLE")
        result = has_min(sample_data, rule)

        assert result["status"] == "PASS"
        assert result["actual"] == 5000.0

    def test_has_mean(self, sample_data, rule_factory):
        rule = rule_factory("salary", "has_mean", value=6000, level="TABLE")
        result = has_mean(sample_data, rule)

        assert result["status"] == "PASS"
        assert result["actual"] == 7000.0

    def test_has_std(self, sample_data, rule_factory):
        rule = rule_factory("salary", "has_std", value=1000, level="TABLE")
        result = has_std(sample_data, rule)

        assert result["status"] == "PASS"
        assert result["actual"] > 1000


class TestValidate:

    def test_validate_returns_three_values(self, sample_data, rule_factory):
        rules = [rule_factory("age", "is_positive", level="ROW")]

        result = validate(sample_data, rules)

        assert isinstance(result, tuple)
        assert len(result) == 3
        assert isinstance(result[0], dd.DataFrame)
        assert isinstance(result[1], dd.DataFrame)
        assert isinstance(result[2], pd.DataFrame)

    def test_validate_with_row_rules(self, sample_data, rule_factory):
        rules = [
            rule_factory("age", "is_positive", level="ROW"),
            rule_factory("name", "is_complete", level="ROW"),
        ]

        df_with_status, violations, table_summary = validate(sample_data, rules)

        viol_computed = violations.compute()
        assert len(viol_computed) > 0
        assert "dq_status" in viol_computed.columns

    def test_validate_with_table_rules(self, sample_data, rule_factory):
        rules = [
            rule_factory("salary", "has_max", value=10000, level="TABLE"),
            rule_factory("salary", "has_min", value=4000, level="TABLE"),
        ]

        _, _, table_summary = validate(sample_data, rules)

        assert len(table_summary) == 2
        assert all(table_summary["level"] == "TABLE")
        assert "status" in table_summary.columns

    def test_validate_with_mixed_rules(self, sample_data, rule_factory):
        rules = [
            rule_factory("age", "is_positive", level="ROW"),
            rule_factory("salary", "has_max", value=10000, level="TABLE"),
        ]

        df_with_status, violations, table_summary = validate(sample_data, rules)

        assert len(violations.compute()) > 0
        assert len(table_summary) == 1
        assert table_summary.iloc[0]["level"] == "TABLE"

    def test_validate_with_empty_data(self, empty_data, rule_factory):
        rules = [rule_factory("name", "is_complete", level="ROW")]

        df_with_status, violations, _ = validate(empty_data, rules)

        assert len(violations.compute()) == 0

    def test_validate_with_no_rules(self, sample_data):
        df_with_status, violations, table_summary = validate(sample_data, [])

        assert len(violations.compute()) == 0
        assert len(table_summary) == 0

    def test_validate_ignores_rules_without_level(self, sample_data, rule_factory):
        rule_dict = {
            "field": "age",
            "check_type": "is_positive",
            "value": "",
            "threshold": 1.0,
        }
        rule = RuleDef.from_dict(rule_dict)
        rule.level = None

        with pytest.warns(UserWarning, match="without level defined"):
            df_with_status, violations, _ = validate(sample_data, [rule])

        assert len(violations.compute()) == 0


class TestValidateRowLevel:

    def test_validate_row_level_basic(self, sample_data, rule_factory):
        rules = [rule_factory("age", "is_positive", level="ROW")]

        agg_df, raw_df = validate_row_level(sample_data, rules)

        assert isinstance(agg_df, dd.DataFrame)
        assert isinstance(raw_df, dd.DataFrame)
        assert "dq_status" in agg_df.columns

    def test_validate_row_level_with_execute_false(self, sample_data, rule_factory):
        rule = rule_factory("age", "is_positive", level="ROW", execute=False)

        with pytest.warns(UserWarning, match="rules ignored"):
            agg_df, raw_df = validate_row_level(sample_data, [rule])

        assert len(raw_df.compute()) == 0


class TestValidateTableLevel:

    def test_validate_table_level_basic(self, sample_data, rule_factory):
        rules = [rule_factory("salary", "has_max", value=10000, level="TABLE")]

        summary = validate_table_level(sample_data, rules)

        assert isinstance(summary, pd.DataFrame)
        assert len(summary) == 1
        assert "status" in summary.columns
        assert "expected" in summary.columns
        assert "actual" in summary.columns

    def test_validate_table_level_multiple_rules(self, sample_data, rule_factory):
        rules = [
            rule_factory("salary", "has_max", value=10000, level="TABLE"),
            rule_factory("salary", "has_min", value=4000, level="TABLE"),
            rule_factory("salary", "has_mean", value=6000, level="TABLE"),
        ]

        summary = validate_table_level(sample_data, rules)

        assert len(summary) == 3
        assert all(summary["status"].isin(["PASS", "FAIL", "ERROR"]))


class TestSummarize:

    def test_summarize_with_row_rules(self, sample_data, rule_factory):
        rules = [
            rule_factory("age", "is_positive", level="ROW"),
            rule_factory("name", "is_complete", threshold=0.8, level="ROW"),
        ]

        _, violations, _ = validate(sample_data, rules)
        total_rows = len(sample_data)

        summary = summarize(rules, total_rows, df_with_errors=violations)
        summary_computed = summary.compute()

        assert len(summary_computed) == 2
        assert "pass_rate" in summary_computed.columns
        assert "status" in summary_computed.columns
        assert "violations" in summary_computed.columns

    def test_summarize_calculates_pass_rate(self, sample_data, rule_factory):
        rules = [rule_factory("age", "is_positive", level="ROW")]

        _, violations, _ = validate(sample_data, rules)
        total_rows = len(sample_data)

        summary = summarize(rules, total_rows, df_with_errors=violations)
        summary_computed = summary.compute()

        row = summary_computed.iloc[0]
        assert row["rows"] == 5
        assert row["violations"] == 2
        assert row["pass_rate"] == 0.6

    def test_summarize_determines_status(self, sample_data, rule_factory):
        rules = [
            rule_factory("id", "is_positive", threshold=0.5, level="ROW"),
            rule_factory("age", "is_positive", threshold=0.8, level="ROW"),
        ]

        _, violations, _ = validate(sample_data, rules)
        total_rows = len(sample_data)

        summary = summarize(rules, total_rows, df_with_errors=violations)
        summary_computed = summary.compute()

        id_row = summary_computed[summary_computed["field"] == "id"].iloc[0]
        age_row = summary_computed[summary_computed["field"] == "age"].iloc[0]

        assert id_row["status"] == "PASS"
        assert age_row["status"] == "FAIL"

    def test_summarize_with_table_rules(self, sample_data, rule_factory):
        rules = [rule_factory("salary", "has_max", value=10000, level="TABLE")]

        _, _, table_summary = validate(sample_data, rules)
        total_rows = len(sample_data)

        summary = summarize(rules, total_rows, table_error=table_summary)
        summary_computed = summary.compute()

        assert len(summary_computed) == 1
        assert summary_computed.iloc[0]["level"] == "TABLE"

    def test_summarize_with_mixed_rules(self, sample_data, rule_factory):
        rules = [
            rule_factory("age", "is_positive", level="ROW"),
            rule_factory("salary", "has_max", value=10000, level="TABLE"),
        ]

        _, violations, table_summary = validate(sample_data, rules)
        total_rows = len(sample_data)

        summary = summarize(
            rules,
            total_rows,
            df_with_errors=violations,
            table_error=table_summary
        )
        summary_computed = summary.compute()

        assert len(summary_computed) == 2
        assert set(summary_computed["level"]) == {"ROW", "TABLE"}

    def test_summarize_sorts_by_status(self, sample_data, rule_factory):
        rules = [
            rule_factory("id", "is_positive", level="ROW"),
            rule_factory("age", "is_positive", level="ROW"),
        ]

        _, violations, _ = validate(sample_data, rules)
        total_rows = len(sample_data)

        summary = summarize(rules, total_rows, df_with_errors=violations)
        summary_computed = summary.compute()

        first_status = summary_computed.iloc[0]["status"]
        assert first_status in ["FAIL", "PASS"]

        if first_status == "FAIL":
            assert summary_computed.iloc[1]["status"] in ["PASS", "ERROR"]


class TestEdgeCases:

    def test_single_row_dataframe(self, rule_factory):
        data = pd.DataFrame({"value": [100]})
        dask_df = dd.from_pandas(data, npartitions=1)

        rules = [rule_factory("value", "is_positive", level="ROW")]

        df_with_status, violations, _ = validate(dask_df, rules)

        assert len(violations.compute()) == 0

    def test_all_null_column(self, rule_factory):
        data = pd.DataFrame({"value": [None, None, None]})
        dask_df = dd.from_pandas(data, npartitions=1)

        rules = [rule_factory("value", "is_complete", level="ROW")]

        _, violations, _ = validate(dask_df, rules)
        viol_computed = violations.compute()

        assert len(viol_computed) == 3

    def test_cannot_create_invalid_rule(self):
        with pytest.raises(ValueError, match="Invalid rule type"):
            RuleDef.from_dict({
                "field": "age",
                "check_type": "is_super_magic",
                "value": "",
                "level": "ROW",
            })

    def test_rule_without_implementation_is_skipped(self, sample_data):
        rule = RuleDef.from_dict({
            "field": "age",
            "check_type": "is_complete",
            "value": "",
            "level": "ROW",
        })

        rule.check_type = "nonexistent_function_xyz"

        with pytest.warns(UserWarning):
            df_with_status, violations, _ = validate(sample_data, [rule])

        assert len(violations.compute()) == 0


class TestIntegration:

    def test_full_pipeline(self, sample_data, rule_factory):
        rules = [
            rule_factory("age", "is_positive", level="ROW"),
            rule_factory("name", "is_complete", threshold=0.8, level="ROW"),
            rule_factory("salary", "has_max", value=10000, level="TABLE"),
        ]

        df_with_status, violations, table_summary = validate(sample_data, rules)

        total_rows = len(sample_data)
        summary = summarize(
            rules,
            total_rows,
            df_with_errors=violations,
            table_error=table_summary
        )
        summary_computed = summary.compute()

        assert len(summary_computed) == 3
        assert set(summary_computed["level"]) == {"ROW", "TABLE"}
        assert all(summary_computed["status"].isin(["PASS", "FAIL", "ERROR"]))

        row_rules = summary_computed[summary_computed["level"] == "ROW"]
        assert all(pd.notna(row_rules["violations"]))
        assert all(pd.notna(row_rules["pass_rate"]))

        table_rules = summary_computed[summary_computed["level"] == "TABLE"]
        assert all(pd.notna(table_rules["expected"]))
        assert all(pd.notna(table_rules["actual"]))
