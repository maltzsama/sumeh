import pytest
import pandas as pd
import dask.dataframe as dd
from datetime import date, timedelta
from sumeh.engine.dask_engine import (
    validate_date_format,
    is_future_date,
    is_past_date,
    is_date_between,
    is_date_after,
    is_date_before,
    all_date_checks,
)
import re


@pytest.fixture
def date_df():
    today = date.today()
    data = {
        "d": [
            "2023-01-01",  # valid past
            "2025-12-31",  # valid future
            "not-a-date",  # invalid
            None,  # null
            (today - timedelta(days=1)).isoformat(),  # yesterday
            today.isoformat(),  # today
            (today + timedelta(days=1)).isoformat(),  # tomorrow
        ]
    }
    pdf = pd.DataFrame(data)
    return dd.from_pandas(pdf, npartitions=2)


def test_validate_date_format(date_df):
    rule = {"field": "d", "check_type": "validate_date_format", "value": "%Y-%m-%d"}
    result = validate_date_format(date_df, rule).compute()
    # should catch invalid and null
    vals = result["d"].tolist()
    assert "not-a-date" in vals
    # detect pandas NA for None
    assert result["d"].isna().any()


def test_is_future_date(date_df):
    rule = {"field": "d", "check_type": "is_future_date", "value": ""}
    result = is_future_date(date_df, rule).compute()
    # should include both far future and tomorrow
    expected = {
        "2025-12-31",
        (date.today() + timedelta(days=1)).isoformat(),
    }
    assert set(result["d"].tolist()) == expected
    # dq_status contains today's date string
    today_str = date.today().isoformat()
    assert all(today_str in s for s in result["dq_status"])


def test_is_past_date(date_df):
    rule = {"field": "d", "check_type": "is_past_date", "value": ""}
    result = is_past_date(date_df, rule).compute()
    # should catch everything strictly before today
    vals = set(result["d"].dropna().tolist())
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    assert {"2023-01-01", yesterday}.issubset(vals)


def test_is_date_between(date_df):
    start = "2023-01-01"
    end = date.today().isoformat()
    rule = {"field": "d", "check_type": "is_date_between", "value": f"[{start},{end}]"}
    result = is_date_between(date_df, rule).compute()
    vals = set(result["d"].dropna().tolist())
    # should catch out-of-range only: far future, invalid, null, tomorrow
    tomorrow = (date.today() + timedelta(days=1)).isoformat()
    assert {"2025-12-31", "not-a-date", tomorrow}.issubset(vals)
    # null rows: detect NA
    assert result["d"].isna().any()


def test_is_date_after(date_df):
    ref = "2023-01-02"
    rule = {"field": "d", "check_type": "is_date_after", "value": ref}
    result = is_date_after(date_df, rule).compute()
    # only rows strictly before ref and parseable
    assert set(result["d"].tolist()) == {"2023-01-01"}


def test_is_date_before(date_df):
    ref = "2025-05-17"
    rule = {"field": "d", "check_type": "is_date_before", "value": ref}
    result = is_date_before(date_df, rule).compute()

    # Drop nulls
    non_null = result["d"].dropna().tolist()
    assert non_null, "Expected at least one date greater than the reference"

    # Every returned value must parse and be strictly > ref
    for v in non_null:
        # ISO‐date format
        assert re.match(r"^\d{4}-\d{2}-\d{2}$", v), f"Invalid date format: {v}"
        assert pd.to_datetime(v) > pd.to_datetime(ref), f"{v} is not after {ref}"


def test_all_date_checks_alias(date_df):
    r = {"field": "d", "check_type": "all_date_checks", "value": ""}
    direct = is_past_date(date_df, r).compute()
    alias = all_date_checks(date_df, r).compute()
    pd.testing.assert_frame_equal(direct, alias)
