import pytest
import polars as pl
from sumeh.engine.polars_engine import (
    validate,
    summarize,
    is_positive,
    is_negative,
    is_unique,
    are_unique,
    is_between,
    has_pattern,
    validate_date_format,
    is_future_date,
    is_past_date,
    is_date_between,
    is_date_after,
    is_date_before,
    all_date_checks,
    __build_rules_df,
)
from datetime import datetime, date, timedelta
import numpy as np


@pytest.fixture
def sample_data():
    return pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "age": [25, -30, 35, 40, -45],
            "salary": [5000, 6000, 7000, 8000, 9000],
            "email": [
                "alice@example.com",
                "bob@example",
                "charlie@example.com",
                "invalid-email",
                "eve@example.com",
            ],
            "department": ["HR", "HR", "IT", "IT", "Finance"],
        }
    )


@pytest.fixture
def sample_rules():
    return [
        {
            "field": "age",
            "check_type": "is_positive",
            "value": "",
            "threshold": 1.0,
            "execute": True,
        },
        {
            "field": "email",
            "check_type": "has_pattern",
            "value": r"^[\w\.-]+@[\w\.-]+\.\w+$",
            "threshold": 0.9,
        },
        {
            "field": "department",
            "check_type": "is_contained_in",
            "value": "[HR,IT,Finance,Sales]",
            "threshold": 1.0,
        },
        {
            "field": "salary",
            "check_type": "is_between",
            "value": "[3000,10000]",
            "threshold": 1.0,
        },
        {
            "field": ["name", "email"],
            "check_type": "are_unique",
            "value": "",
            "threshold": 1.0,
        },
    ]


def test_is_positive(sample_data):
    rule = {"field": "age", "check_type": "is_positive", "value": ""}
    result = is_positive(sample_data, rule)
    assert result.shape[0] == 2  # Should catch negative ages
    assert all(result["age"] < 0)


def test_is_negative(sample_data):
    rule = {"field": "age", "check_type": "is_negative", "value": ""}
    result = is_negative(sample_data, rule)
    assert result.shape[0] == 3  # Should catch positive ages
    assert all(result["age"] >= 0)


def test_is_unique(sample_data):
    # Make a duplicate in department
    data = sample_data.with_columns(
        pl.when(pl.col("id") == 5)
        .then(pl.lit("HR"))
        .otherwise(pl.col("department"))
        .alias("department")
    )
    rule = {"field": "department", "check_type": "is_unique", "value": ""}
    result = is_unique(data, rule)
    # Verifica apenas que há linhas retornadas e que contém os departamentos duplicados
    assert result.shape[0] > 0
    assert "HR" in result["department"].to_list()


def test_are_unique(sample_data):
    rule = {"field": ["name", "email"], "check_type": "are_unique", "value": ""}
    result = are_unique(sample_data, rule)
    assert result.shape[0] == 0  # All name+email combinations are unique


def test_has_pattern(sample_data):
    rule = {
        "field": "email",
        "check_type": "has_pattern",
        "value": r"^[\w\.-]+@[\w\.-]+\.\w+$",
    }
    result = has_pattern(sample_data, rule)
    assert result.shape[0] == 2  # Two invalid emails
    assert "bob@example" in result["email"].to_list()


def test_is_between(sample_data):
    rule = {"field": "salary", "check_type": "is_between", "value": "[3000,10000]"}
    result = is_between(sample_data, rule)
    assert result.shape[0] == 0  # All salaries are within range


def test_validate(sample_data, sample_rules):
    validated_df, violations = validate(sample_data, sample_rules)

    # Check the validated dataframe has the dq_status column
    assert "dq_status" in validated_df.columns

    # Check violations contain expected issues
    assert violations.shape[0] > 0
    assert "age:is_positive:" in violations["dq_status"].to_list()[0]


def test_summarize(sample_data, sample_rules):
    validated_df, violations = validate(sample_data, sample_rules)
    total_rows = sample_data.shape[0]
    summary = summarize(violations, sample_rules, total_rows)

    # Check summary structure
    expected_columns = [
        "id",
        "timestamp",
        "check",
        "level",
        "column",
        "rule",
        "value",
        "rows",
        "violations",
        "pass_rate",
        "pass_threshold",
        "status",
    ]
    assert all(col in summary.columns for col in expected_columns)

    # Check pass_rate calculation
    for row in summary.iter_rows(named=True):
        expected_pass_rate = (row["rows"] - row["violations"]) / row["rows"]
        assert (
            abs(row["pass_rate"] - expected_pass_rate) < 0.0001
        )  # Tolerância para float


def test__build_rules_df(sample_rules):
    rules_df = __build_rules_df(sample_rules)

    # Check DataFrame structure
    assert set(rules_df.columns) == {"column", "rule", "pass_threshold", "value"}

    # Check multi-field rule handling
    assert "name,email" in rules_df["column"].to_list()

    # Check threshold defaults
    assert all(rules_df["pass_threshold"] <= 1.0)


def test_empty_input():
    # Test with empty DataFrame
    empty_df = pl.DataFrame(schema={"id": pl.Int64, "name": pl.Utf8})
    rules = [{"field": "name", "check_type": "is_unique", "value": ""}]

    validated_df, violations = validate(empty_df, rules)
    assert validated_df.shape[0] == 0
    assert violations.shape[0] == 0

    summary = summarize(violations, rules, 0)
    assert summary.shape[0] == 1  # Should still produce a summary row


def test_validate_date_format():
    df = pl.DataFrame({"dt": ["2025-05-16", "16-05-2025", None]})
    rule = {"field": "dt", "check_type": "validate_date_format", "value": "YYYY-MM-DD"}
    result = validate_date_format(df, rule)
    # should catch the bad format and the null
    assert set(result["dt"].to_list()) == {"16-05-2025", None}


def test_is_future_date_and_is_past_date():
    today = date.today().isoformat()
    past = (date.today() - timedelta(days=1)).isoformat()
    future = (date.today() + timedelta(days=1)).isoformat()
    df = pl.DataFrame({"d": [past, today, future]})

    # future
    rule_fut = {"field": "d", "check_type": "is_future_date", "value": ""}
    fut = is_future_date(df, rule_fut)
    assert fut["d"].to_list() == [future]

    # past
    rule_past = {"field": "d", "check_type": "is_past_date", "value": ""}
    old = is_past_date(df, rule_past)
    assert old["d"].to_list() == [past]


def test_is_date_between():
    today = date.today().isoformat()
    past = (date.today() - timedelta(days=1)).isoformat()
    future = (date.today() + timedelta(days=1)).isoformat()
    df = pl.DataFrame({"d": [past, today, future]})

    rule = {
        "field": "d",
        "check_type": "is_date_between",
        "value": f"[{past},{future}]",
    }
    out = is_date_between(df, rule)
    # all three are within the inclusive [past,future], so nothing should remain
    assert out.is_empty()


def test_is_date_after_and_before():
    today = date.today().isoformat()
    past = (date.today() - timedelta(days=1)).isoformat()
    future = (date.today() + timedelta(days=1)).isoformat()
    df = pl.DataFrame({"d": [past, today, future]})

    # date_after: keep those before given date
    rule_after = {"field": "d", "check_type": "is_date_after", "value": today}
    aft = is_date_after(df, rule_after)
    assert aft["d"].to_list() == [past]

    # date_before: keep those after given date
    rule_before = {"field": "d", "check_type": "is_date_before", "value": today}
    bef = is_date_before(df, rule_before)
    assert bef["d"].to_list() == [future]


def test_all_date_checks_alias():
    today = date.today().isoformat()
    past = (date.today() - timedelta(days=1)).isoformat()
    future = (date.today() + timedelta(days=1)).isoformat()
    df = pl.DataFrame({"d": [past, today, future]})

    rule = {"field": "d", "check_type": "all_date_checks", "value": ""}
    out = all_date_checks(df, rule)
    # all_date_checks is alias for is_past_date
    assert out["d"].to_list() == [past]
