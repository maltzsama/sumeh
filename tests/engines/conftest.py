# tests/engines/conftest.py
"""
Shared fixtures for all engine tests
"""
from datetime import datetime, timedelta

import pandas as pd
import pytest

from sumeh.core.rules.rule_model import RuleDef


@pytest.fixture
def rule_factory():
    """Factory to create RuleDef instances easily"""

    def _create(field, check_type, value="", threshold=1.0, level="ROW", **kwargs):
        return RuleDef.from_dict(
            {
                "field": field,
                "check_type": check_type,
                "value": value,
                "threshold": threshold,
                "level": level,
                **kwargs,
            }
        )

    return _create


@pytest.fixture
def sample_pandas_df():
    """Standard test dataset as pandas DataFrame"""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", None, "Eve"],
            "age": [25, -30, 35, 40, -45],
            "salary": [5000.0, 6000.0, 7000.0, 8000.0, 9000.0],
            "email": [
                "alice@example.com",
                "invalid",
                "charlie@test.com",
                "david@test.com",
                "eve@example.com",
            ],
            "department": ["HR", "HR", "IT", "IT", "Finance"],
            "active": [True, True, False, True, True],
        }
    )


@pytest.fixture
def empty_pandas_df():
    """Empty DataFrame for edge case testing"""
    return pd.DataFrame(columns=["id", "name", "value"])


@pytest.fixture
def single_row_df():
    """Single row DataFrame for edge case testing"""
    return pd.DataFrame({"id": [1], "value": [100], "name": ["Test"]})


@pytest.fixture
def dates_df():
    """DataFrame with various date scenarios"""
    today = datetime.now().date()
    return pd.DataFrame(
        {
            "id": range(1, 8),
            "date_col": [
                today,
                today - timedelta(days=1),
                today - timedelta(days=2),
                today + timedelta(days=1),
                today - timedelta(days=365),
                pd.NaT,
                today - timedelta(days=3),
            ],
            "string_date": [
                "2024-01-01",
                "2024-02-15",
                "2024-03-30",
                "invalid",
                "2024-12-25",
                None,
                "2025-06-15",
            ],
        }
    )


@pytest.fixture
def numeric_edge_cases_df():
    """DataFrame with numeric edge cases"""
    return pd.DataFrame(
        {
            "positive": [1, 2, 3, 4, 5],
            "negative": [-1, -2, -3, -4, -5],
            "mixed": [1, -2, 3, -4, 5],
            "with_nulls": [1.0, None, 3.0, None, 5.0],
            "with_zeros": [0, 1, 0, 2, 0],
            "large_numbers": [1_000_000, 2_000_000, 500_000, 1_500_000, 3_000_000],
        }
    )


@pytest.fixture
def text_patterns_df():
    """DataFrame with text pattern scenarios"""
    return pd.DataFrame(
        {
            "email": [
                "valid@example.com",
                "invalid.email",
                "another@test.co.uk",
                "",
                None,
                "bad@",
            ],
            "phone": [
                "+1234567890",
                "123-456-7890",
                "invalid",
                "(123) 456-7890",
                None,
                "12345",
            ],
            "zipcode": ["12345", "12345-6789", "ABCDE", "1234", None, "00000"],
        }
    )


@pytest.fixture
def categorical_df():
    """DataFrame with categorical data for testing"""
    return pd.DataFrame(
        {
            "status": ["active", "inactive", "pending", "active", "invalid"],
            "priority": ["high", "medium", "low", "high", "urgent"],
            "category": ["A", "A", "B", "B", "C"],
        }
    )


@pytest.fixture
def basic_row_rules(rule_factory):
    """Standard set of ROW level rules"""
    return [
        rule_factory("age", "is_positive", level="ROW"),
        rule_factory("name", "is_complete", threshold=0.8, level="ROW"),
        rule_factory(
            "email",
            "has_pattern",
            value=r"^[\w\.-]+@[\w\.-]+\.\w+$",
            threshold=0.9,
            level="ROW",
        ),
    ]


@pytest.fixture
def basic_table_rules(rule_factory):
    """Standard set of TABLE level rules"""
    return [
        rule_factory("salary", "has_max", value=10000, level="TABLE"),
        rule_factory("salary", "has_min", value=4000, level="TABLE"),
        rule_factory("salary", "has_mean", value=6000, level="TABLE"),
    ]


@pytest.fixture
def mixed_rules(basic_row_rules, basic_table_rules):
    """Combination of ROW and TABLE rules"""
    return basic_row_rules + basic_table_rules


@pytest.fixture
def expected_schema():
    """Expected schema for schema validation tests"""
    return [
        {"field": "id", "data_type": "int64", "nullable": False},
        {"field": "name", "data_type": "object", "nullable": True},
        {"field": "age", "data_type": "int64", "nullable": False},
        {"field": "salary", "data_type": "float64", "nullable": False},
    ]


@pytest.fixture
def validation_scenarios():
    """Collection of validation scenarios for testing"""
    return {
        "all_pass": {
            "data": pd.DataFrame({"value": [1, 2, 3, 4, 5]}),
            "rule": {"field": "value", "check_type": "is_positive", "level": "ROW"},
            "expected_violations": 0,
        },
        "all_fail": {
            "data": pd.DataFrame({"value": [-1, -2, -3, -4, -5]}),
            "rule": {"field": "value", "check_type": "is_positive", "level": "ROW"},
            "expected_violations": 5,
        },
        "partial_fail": {
            "data": pd.DataFrame({"value": [1, -2, 3, -4, 5]}),
            "rule": {"field": "value", "check_type": "is_positive", "level": "ROW"},
            "expected_violations": 2,
        },
    }


def assert_validation_result(result, expected_violations=None, expected_status=None):
    """Helper to assert validation results consistently across engines"""
    if expected_violations is not None:
        assert len(result) == expected_violations

    if expected_status is not None:
        if hasattr(result, "iloc"):
            assert result.iloc[0]["status"] == expected_status
        elif isinstance(result, dict):
            assert result["status"] == expected_status


def compare_engine_results(result1, result2, tolerance=0.0001):
    """Helper to compare results from different engines"""
    if isinstance(result1, pd.DataFrame) and isinstance(result2, pd.DataFrame):
        assert len(result1) == len(result2)
        for col in result1.columns:
            if col in result2.columns:
                if pd.api.types.is_numeric_dtype(result1[col]):
                    assert all(abs(result1[col] - result2[col]) < tolerance)
                else:
                    assert all(result1[col] == result2[col])
    elif isinstance(result1, dict) and isinstance(result2, dict):
        for key in result1:
            if key in result2:
                if isinstance(result1[key], float):
                    assert abs(result1[key] - result2[key]) < tolerance
                else:
                    assert result1[key] == result2[key]
