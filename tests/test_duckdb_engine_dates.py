import pytest

from sumeh.engines.duckdb_engine import __RuleCtx
from sumeh.engines.duckdb_engine import (
    _is_in_millions,
    _is_in_billions,
    _is_today,
    _is_yesterday,
    _is_t_minus_1,
    _is_t_minus_2,
    _is_t_minus_3,
    _is_on_weekday,
    _is_on_weekend,
    _is_on_monday,
    _is_on_tuesday,
    _is_on_wednesday,
    _is_on_thursday,
    _is_on_friday,
    _is_on_saturday,
    _is_on_sunday,
)


@pytest.fixture
def ctx():
    # valor e name não importam para estas regras
    return lambda col, name: __RuleCtx(column=col, value=None, name=name)


@pytest.mark.parametrize(
    "column,sql",
    [
        ("rev", "rev >= 1000000"),
        ("x", "x >= 1000000"),
    ],
)
def test_is_in_millions(ctx, column, sql):
    assert _is_in_millions(ctx(column, "is_in_millions")) == sql


@pytest.mark.parametrize(
    "column,sql",
    [
        ("mc", "mc >= 1000000000"),
        ("y", "y >= 1000000000"),
    ],
)
def test_is_in_billions(ctx, column, sql):
    assert _is_in_billions(ctx(column, "is_in_billions")) == sql


@pytest.mark.parametrize(
    "fn,name,expected",
    [
        (_is_today, "is_today", "col = current_date"),
        (_is_yesterday, "is_yesterday", "col = current_date - 1"),
        (_is_t_minus_1, "is_t_minus_1", "col = current_date - 1"),
        (_is_t_minus_2, "is_t_minus_2", "col = current_date - 2"),
        (_is_t_minus_3, "is_t_minus_3", "col = current_date - 3"),
        (_is_on_weekday, "is_on_weekday", "EXTRACT(DOW FROM col) BETWEEN 1 AND 5"),
        (
            _is_on_weekend,
            "is_on_weekend",
            "(EXTRACT(DOW FROM col) = 0 OR EXTRACT(DOW FROM col) = 6)",
        ),
        (_is_on_monday, "is_on_monday", "EXTRACT(DOW FROM col) = 1"),
        (_is_on_tuesday, "is_on_tuesday", "EXTRACT(DOW FROM col) = 2"),
        (_is_on_wednesday, "is_on_wednesday", "EXTRACT(DOW FROM col) = 3"),
        (_is_on_thursday, "is_on_thursday", "EXTRACT(DOW FROM col) = 4"),
        (_is_on_friday, "is_on_friday", "EXTRACT(DOW FROM col) = 5"),
        (_is_on_saturday, "is_on_saturday", "EXTRACT(DOW FROM col) = 6"),
        (_is_on_sunday, "is_on_sunday", "EXTRACT(DOW FROM col) = 0"),
    ],
)
def test_date_and_weekday_rules(ctx, fn, name, expected):
    rule_ctx = ctx("col", name)
    assert fn(rule_ctx) == expected
