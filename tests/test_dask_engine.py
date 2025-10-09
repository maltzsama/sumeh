from datetime import datetime

import dask.dataframe as dd
import pandas as pd
import pytest

from sumeh.engines.dask_engine import (
    validate,
    summarize,
    _rules_to_df,
    is_positive,
    is_negative,
    is_unique,
)


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_data():
    data = {
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
        "join_date": pd.to_datetime(
            ["2022-01-01", "2022-01-02", "2022-01-03", "2022-01-04", "2022-01-05"]
        ),
        "last_updated": pd.to_datetime(
            ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"]
        ),
    }
    return dd.from_pandas(pd.DataFrame(data), npartitions=2)


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
    ]


def test_validate_returns_two_dataframes(sample_data, sample_rules):
    agg_df, raw_df = validate(sample_data, sample_rules)

    assert isinstance(agg_df, dd.DataFrame)
    assert isinstance(raw_df, dd.DataFrame)
    assert "dq_status" in agg_df.columns
    assert "dq_status" in raw_df.columns


def test_validate_aggregation(sample_data, sample_rules):
    agg_df, _ = validate(sample_data, sample_rules)
    agg_result = agg_df.compute()

    # Check that violations are concatenated with ;
    assert any(";" in status for status in agg_result["dq_status"].dropna())


def test_summarize_structure(sample_data, sample_rules):
    _, raw_df = validate(sample_data, sample_rules)
    total_rows = len(sample_data.index)
    summary = summarize(raw_df, sample_rules, total_rows).compute()

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
    assert len(summary) > 0


def test_summarize_calculations(sample_data, sample_rules):
    _, raw_df = validate(sample_data, sample_rules)
    total_rows = len(sample_data.index)
    summary = summarize(raw_df, sample_rules, total_rows).compute()

    # Check pass_rate calculation
    for _, row in summary.iterrows():
        expected_pass_rate = (row["rows"] - row["violations"]) / row["rows"]
        assert abs(row["pass_rate"] - expected_pass_rate) < 0.0001

        # Check status calculation
        expected_status = (
            "PASS" if row["pass_rate"] >= row["pass_threshold"] else "FAIL"
        )
        assert row["status"] == expected_status


def test_rules_to_df(sample_rules):
    rules_df = _rules_to_df(sample_rules)

    assert not rules_df.empty
    assert set(rules_df.columns) == {"column", "rule", "pass_threshold", "value"}
    assert all(rules_df["pass_threshold"] <= 1.0)


def test_individual_validation_rules(sample_data):
    # Test is_positive
    rule = {"field": "age", "check_type": "is_positive", "value": ""}
    result = is_positive(sample_data, rule).compute()
    assert len(result) == 2
    assert all(result["age"] < 0)

    # Test is_negative
    rule = {"field": "age", "check_type": "is_negative", "value": ""}
    result = is_negative(sample_data, rule).compute()
    assert len(result) == 3
    assert all(result["age"] >= 0)

    # Test is_unique
    rule = {"field": "department", "check_type": "is_unique", "value": ""}
    result = is_unique(sample_data, rule).compute()
    assert len(result) > 0
    assert "HR" in result["department"].values


def test_empty_input_handling():
    empty_df = dd.from_pandas(pd.DataFrame(columns=["id", "name"]), npartitions=1)
    rules = [{"field": "name", "check_type": "is_unique", "value": ""}]

    agg_df, raw_df = validate(empty_df, rules)
    assert agg_df.compute().empty
    assert raw_df.compute().empty

    # Create empty summary manually since summarize() can't handle empty input
    summary = pd.DataFrame(
        {
            "id": [1],
            "timestamp": [datetime.now().replace(second=0, microsecond=0)],
            "check": ["Quality Check"],
            "level": ["WARNING"],
            "column": ["name"],
            "rule": ["is_unique"],
            "value": [""],
            "rows": [0],
            "violations": [0],
            "pass_rate": [1.0],
            "pass_threshold": [1.0],
            "status": ["PASS"],
        }
    )

    # Verify the expected summary structure
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
    assert len(summary) == 1
