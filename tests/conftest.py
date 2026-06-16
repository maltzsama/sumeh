"""
Shared fixtures for Sumeh test suite.
Covers all rule categories from manifest.json.
"""

from datetime import date, timedelta

import pandas as pd
import pytest

from sumeh.core.rules.rule_definition import RuleDefinition

# DataFrames


@pytest.fixture
def df_completeness():
    """DataFrame for completeness and uniqueness rules."""
    return pd.DataFrame(
        {
            "user_id": [1, 2, 3, 4, 5],
            "email": ["a@a.com", "b@b.com", None, "d@d.com", "e@e.com"],
            "name": ["Alice", "Bob", "Carol", None, "Eve"],
            "username": ["alice", "bob", "carol", "dan", "eve"],
        }
    )


@pytest.fixture
def df_uniqueness():
    """DataFrame with duplicate values for uniqueness rules."""
    return pd.DataFrame(
        {
            "user_id": [1, 2, 2, 4, 5],  # duplicate 2
            "email": [
                "a@a.com",
                "b@b.com",
                "b@b.com",
                "d@d.com",
                "e@e.com",
            ],  # duplicate
            "country": ["BR", "US", "US", "DE", "FR"],
            "city": ["SP", "NY", "NY", "BE", "PA"],
        }
    )


@pytest.fixture
def df_comparison():
    """DataFrame for numeric comparison rules."""
    return pd.DataFrame(
        {
            "age": [25, 17, 30, -1, 45],
            "score": [80, 55, 90, 100, 70],
            "revenue": [1_000.0, 2_000.0, -500.0, 4_000.0, 5_000.0],
            "balance": [100.0, 200.0, 300.0, 400.0, 500.0],
            "big_value": [1_000_000, 2_000_000, 500, 1_500_000, 2_500_000],
        }
    )


@pytest.fixture
def df_membership():
    """DataFrame for membership rules."""
    return pd.DataFrame(
        {
            "status": ["active", "inactive", "banned", "active", "pending"],
            "country": ["BR", "US", "XX", "DE", "FR"],
            "category": ["A", "B", "C", "D", "Z"],
        }
    )


@pytest.fixture
def df_pattern():
    """DataFrame for pattern and legit rules."""
    return pd.DataFrame(
        {
            "email": ["a@a.com", "not-an-email", None, "d@d.com", "  "],
            "phone": [
                "+5511999999999",
                "invalid",
                "+5521888888888",
                None,
                "+5531777777777",
            ],
            "zipcode": ["01310-100", "99999-000", "INVALID", "04538-132", "20040-020"],
            "notes": ["ok", "  ", None, "valid", "also ok"],
        }
    )


@pytest.fixture
def df_date():
    """DataFrame for date rules."""
    today = date.today()
    yesterday = today - timedelta(days=1)
    t_minus_2 = today - timedelta(days=2)
    t_minus_3 = today - timedelta(days=3)
    future = today + timedelta(days=5)

    return pd.DataFrame(
        {
            "created_at": [today, yesterday, t_minus_2, t_minus_3, today],
            "updated_at": [yesterday, yesterday, yesterday, yesterday, yesterday],
            "scheduled": [future, future, future, future, future],
            "past_date": [
                date(2020, 1, 1),
                date(2021, 6, 15),
                date(2019, 12, 31),
                date(2022, 3, 10),
                date(2023, 7, 4),
            ],
            "date_str": [
                today.strftime("%Y-%m-%d"),
                yesterday.strftime("%Y-%m-%d"),
                "not-a-date",
                t_minus_2.strftime("%Y-%m-%d"),
                t_minus_3.strftime("%Y-%m-%d"),
            ],
        }
    )


@pytest.fixture
def df_aggregation():
    """DataFrame for table-level aggregation rules."""
    return pd.DataFrame(
        {
            "age": [20, 30, 40, 50, 60],  # mean=40, min=20, max=60, sum=200
            "salary": [1000.0, 2000.0, 3000.0, 4000.0, 5000.0],  # mean=3000, sum=15000
            "category": ["A", "B", "A", "C", "B"],  # cardinality=3
            "score": [10.0, 20.0, 30.0, 40.0, 50.0],
        }
    )


@pytest.fixture
def df_all():
    """All-purpose DataFrame combining all column types."""
    today = date.today()
    return pd.DataFrame(
        {
            "user_id": [1, 2, 3, 4, 5],
            "email": ["a@a.com", "b@b.com", None, "d@d.com", "e@e.com"],
            "age": [25, 30, 17, 45, 60],
            "status": ["active", "inactive", "active", "banned", "active"],
            "revenue": [1000.0, 2000.0, 3000.0, 4000.0, 5000.0],
            "created_at": [today, today, today, today, today],
            "score": [80, 90, 70, 85, 95],
        }
    )


# Rule fixtures — Completeness


@pytest.fixture
def rule_is_complete():
    return RuleDefinition(field="email", check_type="is_complete", threshold=1.0)


@pytest.fixture
def rule_are_complete():
    return RuleDefinition(
        field=["name", "email"], check_type="are_complete", threshold=0.95
    )


# Rule fixtures — Uniqueness


@pytest.fixture
def rule_is_unique():
    return RuleDefinition(field="user_id", check_type="is_unique", threshold=1.0)


@pytest.fixture
def rule_are_unique():
    return RuleDefinition(
        field=["country", "city"], check_type="are_unique", threshold=1.0
    )


@pytest.fixture
def rule_is_primary_key():
    return RuleDefinition(field="user_id", check_type="is_primary_key", threshold=1.0)


@pytest.fixture
def rule_is_composite_key():
    return RuleDefinition(
        field=["country", "city"], check_type="is_composite_key", threshold=1.0
    )


# Rule fixtures — Comparison


@pytest.fixture
def rule_is_positive():
    return RuleDefinition(field="revenue", check_type="is_positive", threshold=1.0)


@pytest.fixture
def rule_is_negative():
    return RuleDefinition(field="revenue", check_type="is_negative", threshold=1.0)


@pytest.fixture
def rule_is_between():
    return RuleDefinition(
        field="age", check_type="is_between", value=[18, 120], threshold=1.0
    )


@pytest.fixture
def rule_is_greater_than():
    return RuleDefinition(
        field="score", check_type="is_greater_than", value=50, threshold=1.0
    )


@pytest.fixture
def rule_is_less_than():
    return RuleDefinition(
        field="age", check_type="is_less_than", value=100, threshold=1.0
    )


@pytest.fixture
def rule_is_greater_or_equal_than():
    return RuleDefinition(
        field="score", check_type="is_greater_or_equal_than", value=70, threshold=1.0
    )


@pytest.fixture
def rule_is_less_or_equal_than():
    return RuleDefinition(
        field="age", check_type="is_less_or_equal_than", value=120, threshold=1.0
    )


@pytest.fixture
def rule_is_equal():
    return RuleDefinition(
        field="score", check_type="is_equal", value=100, threshold=1.0
    )


@pytest.fixture
def rule_is_in_millions():
    return RuleDefinition(field="big_value", check_type="is_in_millions", threshold=1.0)


@pytest.fixture
def rule_is_in_billions():
    return RuleDefinition(field="big_value", check_type="is_in_billions", threshold=1.0)


# Rule fixtures — Membership


@pytest.fixture
def rule_is_contained_in():
    return RuleDefinition(
        field="status",
        check_type="is_contained_in",
        value=["active", "inactive", "pending"],
        threshold=1.0,
    )


@pytest.fixture
def rule_not_contained_in():
    return RuleDefinition(
        field="status",
        check_type="not_contained_in",
        value=["banned", "deleted"],
        threshold=1.0,
    )


@pytest.fixture
def rule_is_in():
    return RuleDefinition(
        field="country",
        check_type="is_in",
        value=["BR", "US", "DE", "FR"],
        threshold=1.0,
    )


@pytest.fixture
def rule_not_in():
    return RuleDefinition(
        field="country",
        check_type="not_in",
        value=["XX", "ZZ"],
        threshold=1.0,
    )


# Rule fixtures — Pattern


@pytest.fixture
def rule_has_pattern():
    return RuleDefinition(
        field="email",
        check_type="has_pattern",
        value=r"^[\w.-]+@[\w.-]+\.[a-zA-Z]{2,}$",
        threshold=1.0,
    )


@pytest.fixture
def rule_is_legit():
    return RuleDefinition(field="notes", check_type="is_legit", threshold=1.0)


# Rule fixtures — Date


@pytest.fixture
def rule_is_today():
    return RuleDefinition(field="created_at", check_type="is_today", threshold=1.0)


@pytest.fixture
def rule_is_yesterday():
    return RuleDefinition(field="updated_at", check_type="is_yesterday", threshold=1.0)


@pytest.fixture
def rule_is_t_minus_1():
    return RuleDefinition(field="updated_at", check_type="is_t_minus_1", threshold=1.0)


@pytest.fixture
def rule_is_past_date():
    return RuleDefinition(field="past_date", check_type="is_past_date", threshold=1.0)


@pytest.fixture
def rule_is_future_date():
    return RuleDefinition(field="scheduled", check_type="is_future_date", threshold=1.0)


@pytest.fixture
def rule_is_date_between():
    return RuleDefinition(
        field="past_date",
        check_type="is_date_between",
        value=[date(2019, 1, 1), date(2024, 12, 31)],
        threshold=1.0,
    )


@pytest.fixture
def rule_is_date_after():
    return RuleDefinition(
        field="past_date",
        check_type="is_date_after",
        value=date(2018, 1, 1),
        threshold=1.0,
    )


@pytest.fixture
def rule_is_date_before():
    return RuleDefinition(
        field="past_date",
        check_type="is_date_before",
        value=date(2025, 1, 1),
        threshold=1.0,
    )


@pytest.fixture
def rule_is_on_weekday():
    return RuleDefinition(field="created_at", check_type="is_on_weekday", threshold=1.0)


@pytest.fixture
def rule_validate_date_format():
    return RuleDefinition(
        field="date_str",
        check_type="validate_date_format",
        value="%Y-%m-%d",
        threshold=1.0,
    )


# Rule fixtures — Aggregation (TABLE level)


@pytest.fixture
def rule_has_min():
    return RuleDefinition(field="age", check_type="has_min", value=20.0, threshold=0.0)


@pytest.fixture
def rule_has_max():
    return RuleDefinition(field="age", check_type="has_max", value=60.0, threshold=0.0)


@pytest.fixture
def rule_has_mean():
    return RuleDefinition(
        field="age", check_type="has_mean", value=40.0, threshold=0.05
    )


@pytest.fixture
def rule_has_sum():
    return RuleDefinition(field="age", check_type="has_sum", value=200.0, threshold=0.0)


@pytest.fixture
def rule_has_std():
    return RuleDefinition(
        field="age", check_type="has_std", value=15.81, threshold=0.05
    )


@pytest.fixture
def rule_has_cardinality():
    return RuleDefinition(
        field="category", check_type="has_cardinality", value=3, threshold=0.0
    )


@pytest.fixture
def rule_has_entropy():
    return RuleDefinition(
        field="category", check_type="has_entropy", value=1.5, threshold=0.2
    )


# Rule fixtures — Custom SQL


@pytest.fixture
def rule_satisfies():
    return RuleDefinition(
        field="*",
        check_type="satisfies",
        value="age >= 18 AND status != 'banned'",
        threshold=1.0,
    )


# Composite rule sets


@pytest.fixture
def rules_basic():
    """Minimal rule set for smoke tests."""
    return [
        RuleDefinition(field="user_id", check_type="is_unique", threshold=1.0),
        RuleDefinition(field="email", check_type="is_complete", threshold=1.0),
        RuleDefinition(
            field="age", check_type="is_between", value=[18, 120], threshold=1.0
        ),
        RuleDefinition(
            field="status",
            check_type="is_contained_in",
            value=["active", "inactive", "pending"],
            threshold=1.0,
        ),
    ]


@pytest.fixture
def rules_aggregation():
    """Table-level rules only."""
    return [
        RuleDefinition(field="age", check_type="has_mean", value=40.0, threshold=0.05),
        RuleDefinition(field="age", check_type="has_min", value=20.0, threshold=0.0),
        RuleDefinition(field="age", check_type="has_max", value=60.0, threshold=0.0),
        RuleDefinition(
            field="category", check_type="has_cardinality", value=3, threshold=0.0
        ),
    ]


@pytest.fixture
def rules_all_categories():
    """One rule per category for broad coverage."""
    return [
        RuleDefinition(field="user_id", check_type="is_unique", threshold=1.0),
        RuleDefinition(field="email", check_type="is_complete", threshold=1.0),
        RuleDefinition(
            field="age", check_type="is_between", value=[18, 120], threshold=1.0
        ),
        RuleDefinition(
            field="status",
            check_type="is_contained_in",
            value=["active", "inactive", "pending", "banned"],
            threshold=1.0,
        ),
        RuleDefinition(
            field="email",
            check_type="has_pattern",
            value=r"^[\w.-]+@[\w.-]+\.[a-zA-Z]{2,}$",
            threshold=1.0,
        ),
        RuleDefinition(
            field="revenue", check_type="has_mean", value=3000.0, threshold=0.1
        ),
        RuleDefinition(field="created_at", check_type="is_today", threshold=1.0),
    ]
