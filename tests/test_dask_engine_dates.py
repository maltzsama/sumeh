import pytest
import pandas as pd
import dask.dataframe as dd
from datetime import date, timedelta
from sumeh.engines.dask_engine import (
    validate_date_format,
    is_future_date,
    is_past_date,
    is_date_between,
    is_date_after,
    is_date_before,
    all_date_checks,
    is_in_millions,
    is_in_billions,
    is_t_minus_1,
    is_t_minus_2,
    is_t_minus_3,
    is_today,
    is_yesterday,
    is_on_weekday,
    is_on_weekend,
    is_on_monday,
    is_on_tuesday,
    is_on_wednesday,
    is_on_thursday,
    is_on_friday,
    is_on_saturday,
    is_on_sunday,
)
import re


@pytest.fixture
def date_df():
    today = date.today()
    data = {
        "d": pd.Series(
            [
                "2023-01-01",
                "2025-12-31",
                "not-a-date",
                None,
                (today - timedelta(days=1)).isoformat(),
                today.isoformat(),
                (today + timedelta(days=1)).isoformat(),
            ],
            dtype="object",
        ),
        "numeric": pd.Series(
            [
                999_999,
                1_000_000,
                1_000_001,
                999_999_999,
                1_000_000_000,
                1_000_000_001,
                None,
            ],
            dtype="Int64",
        ),
    }
    pdf = pd.DataFrame(data)
    return dd.from_pandas(pdf, npartitions=2)


def test_validate_date_format(date_df):
    rule = {"field": "d", "check_type": "validate_date_format", "value": "%Y-%m-%d"}
    result = validate_date_format(date_df, rule).compute()

    vals = result["d"].tolist()
    assert "not-a-date" in vals

    assert result["d"].isna().any()


def test_is_future_date(date_df):
    rule = {"field": "d", "check_type": "is_future_date", "value": ""}
    result = is_future_date(date_df, rule).compute()

    expected = {
        "2025-12-31",
        (date.today() + timedelta(days=1)).isoformat(),
    }
    assert set(result["d"].tolist()) == expected

    today_str = date.today().isoformat()
    assert all(today_str in s for s in result["dq_status"])


def test_is_past_date(date_df):
    rule = {"field": "d", "check_type": "is_past_date", "value": ""}
    result = is_past_date(date_df, rule).compute()

    vals = set(result["d"].dropna().tolist())
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    assert {"2023-01-01", yesterday}.issubset(vals)


def test_is_date_between(date_df):
    start = "2023-01-01"
    end = date.today().isoformat()
    rule = {"field": "d", "check_type": "is_date_between", "value": f"[{start},{end}]"}
    result = is_date_between(date_df, rule).compute()
    vals = set(result["d"].dropna().tolist())

    tomorrow = (date.today() + timedelta(days=1)).isoformat()
    assert {"2025-12-31", "not-a-date", tomorrow}.issubset(vals)

    assert result["d"].isna().any()


def test_is_date_after(date_df):
    ref = "2023-01-02"
    rule = {"field": "d", "check_type": "is_date_after", "value": ref}
    result = is_date_after(date_df, rule).compute()

    assert set(result["d"].tolist()) == {"2023-01-01"}


def test_is_date_before(date_df):
    ref = "2025-05-17"
    rule = {"field": "d", "check_type": "is_date_before", "value": ref}
    result = is_date_before(date_df, rule).compute()

    non_null = result["d"].dropna().tolist()
    assert non_null, "Expected at least one date greater than the reference"

    for v in non_null:

        assert re.match(r"^\d{4}-\d{2}-\d{2}$", v), f"Invalid date format: {v}"
        assert pd.to_datetime(v) > pd.to_datetime(ref), f"{v} is not after {ref}"


def test_all_date_checks_alias(date_df):
    r = {"field": "d", "check_type": "all_date_checks", "value": ""}
    direct = is_past_date(date_df, r).compute()
    alias = all_date_checks(date_df, r).compute()
    pd.testing.assert_frame_equal(direct, alias)


def test_is_in_millions(date_df):
    rule = {"field": "numeric", "check_type": "is_in_millions", "value": ""}
    result = is_in_millions(date_df, rule).compute()

    assert len(result) == 1
    assert result["numeric"].iloc[0] == 999_999
    assert all("numeric:is_in_millions:" in s for s in result["dq_status"])


def test_is_in_billions(date_df):
    rule = {"field": "numeric", "check_type": "is_in_billions", "value": ""}
    result = is_in_billions(date_df, rule).compute()

    assert len(result) == 4
    assert all(x < 1_000_000_000 for x in result["numeric"].dropna())
    assert all("numeric:is_in_billions:" in s for s in result["dq_status"])


def test_is_t_minus_1(date_df):
    rule = {"field": "d", "check_type": "is_t_minus_1", "value": ""}
    result = is_t_minus_1(date_df, rule).compute()

    yesterday = (date.today() - timedelta(days=1)).isoformat()
    valid_dates = [d for d in result["d"].dropna() if d != "not-a-date"]
    assert yesterday not in valid_dates
    assert all("d:is_t_minus_1:" in s for s in result["dq_status"])


def test_is_t_minus_2(date_df):
    rule = {"field": "d", "check_type": "is_t_minus_2", "value": ""}
    result = is_t_minus_2(date_df, rule).compute()

    two_days_ago = (date.today() - timedelta(days=2)).isoformat()
    valid_dates = [d for d in result["d"].dropna() if d != "not-a-date"]
    assert two_days_ago not in valid_dates
    assert all("d:is_t_minus_2:" in s for s in result["dq_status"])


def test_is_t_minus_3(date_df):
    rule = {"field": "d", "check_type": "is_t_minus_3", "value": ""}
    result = is_t_minus_3(date_df, rule).compute()

    three_days_ago = (date.today() - timedelta(days=3)).isoformat()
    valid_dates = [d for d in result["d"].dropna() if d != "not-a-date"]
    assert three_days_ago not in valid_dates
    assert all("d:is_t_minus_3:" in s for s in result["dq_status"])


def test_is_today(date_df):
    rule = {"field": "d", "check_type": "is_today", "value": ""}
    result = is_today(date_df, rule).compute()
    today = date.today().isoformat()
    valid_dates = [d for d in result["d"].dropna() if d != "not-a-date"]
    assert today not in valid_dates
    assert all("d:is_today:" in s for s in result["dq_status"])


def test_is_yesterday(date_df):
    rule = {"field": "d", "check_type": "is_yesterday", "value": ""}
    result = is_yesterday(date_df, rule).compute()
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    valid_dates = [d for d in result["d"].dropna() if d != "not-a-date"]
    assert yesterday not in valid_dates
    assert all("d:is_yesterday:" in s for s in result["dq_status"])


def test_is_on_weekday(date_df):
    rule = {"field": "d", "check_type": "is_on_weekday", "value": ""}
    result = is_on_weekday(date_df, rule).compute()
    valid_dates = [d for d in result["d"].dropna() if d != "not-a-date"]
    assert "2023-01-01" in valid_dates
    assert all("d:is_on_weekday:" in s for s in result["dq_status"])


def test_is_on_weekend(date_df):
    rule = {"field": "d", "check_type": "is_on_weekend", "value": ""}
    result = is_on_weekend(date_df, rule).compute()
    valid_dates = [d for d in result["d"].dropna() if d != "not-a-date"]
    assert "2023-01-01" not in valid_dates
    assert all("d:is_on_weekend:" in s for s in result["dq_status"])


@pytest.mark.parametrize(
    "day_func,day_num,day_name",
    [
        (is_on_monday, 0, "Monday"),
        (is_on_tuesday, 1, "Tuesday"),
        (is_on_wednesday, 2, "Wednesday"),
        (is_on_thursday, 3, "Thursday"),
        (is_on_friday, 4, "Friday"),
        (is_on_saturday, 5, "Saturday"),
        (is_on_sunday, 6, "Sunday"),
    ],
)
def test_day_specific_functions(date_df, day_func, day_num, day_name):
    rule = {"field": "d", "check_type": day_func.__name__, "value": ""}
    result = day_func(date_df, rule).compute()
    valid_dates = [d for d in result["d"].dropna() if d != "not-a-date"]
    if day_num == 6:
        assert "2023-01-01" not in valid_dates
    if day_num == 2:
        assert "2025-12-31" not in valid_dates

    assert all(f"d:{day_func.__name__}:" in s for s in result["dq_status"])
