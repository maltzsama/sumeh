from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from sumeh.engines.pandas_engine import (
    is_in_millions,
    is_in_billions,
    is_today,
    is_yesterday,
    is_t_minus_2,
    is_t_minus_3,
    is_on_weekday,
    is_on_weekend,
    is_on_monday,
    is_on_tuesday,
    is_on_wednesday,
    is_on_thursday,
    is_on_friday,
    is_on_saturday,
    is_on_sunday,
    is_complete,
)


@pytest.fixture
def test_df():
    """DataFrame de teste com vários tipos de dados"""
    return pd.DataFrame(
        {
            "numeric": [1000000, 500000, 2000000, 3000000000, np.nan],
            "date": pd.to_datetime(
                [
                    date.today(),
                    date.today() - timedelta(days=1),
                    date.today() - timedelta(days=2),
                    date.today() - timedelta(days=3),
                    np.nan,
                ]
            ),
            "text": ["A", "B", "C", "D", None],
        }
    )


@pytest.fixture
def weekday_df():
    """DataFrame com dias da semana para testes"""
    return pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2023-01-02",
                    "2023-01-03",
                    "2023-01-04",
                    "2023-01-05",
                    "2023-01-06",
                    "2023-01-07",
                    "2023-01-08",
                    np.nan,
                ]
            ),
            "day_name": [
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
                "Sunday",
                None,
            ],
        }
    )


def create_rule(field, check_type, value=None):
    """Cria um dicionário de regra para testes"""
    return {"field": field, "check_type": check_type, "value": value}


def test_is_in_millions(test_df):
    rule = create_rule("numeric", "is_in_millions", "1M")
    result = is_in_millions(test_df, rule)

    expected_count = len(
        [x for x in test_df["numeric"] if not pd.isna(x) and x < 1000000]
    )
    assert len(result) == expected_count
    assert all(result["numeric"] < 1000000)
    assert all(result["dq_status"] == "numeric:is_in_millions:1M")


def test_is_in_billions(test_df):
    rule = create_rule("numeric", "is_in_billions", "1B")
    result = is_in_billions(test_df, rule)

    assert len(result) == 3
    assert result.iloc[0]["numeric"] < 1000000000
    assert result.iloc[0]["dq_status"] == "numeric:is_in_billions:1B"


def test_is_today(test_df):
    rule = create_rule("date", "is_today", "today")
    result = is_today(test_df, rule)

    assert len(result) == 4
    assert result.iloc[0]["date"].date() != date.today()
    assert result.iloc[0]["dq_status"] == "date:is_today:today"


def test_is_yesterday(test_df):
    rule = create_rule("date", "is_yesterday", "yesterday")
    result = is_yesterday(test_df, rule)

    assert len(result) == 4
    assert result.iloc[0]["date"].date() != date.today() - timedelta(days=1)
    assert result.iloc[0]["dq_status"] == "date:is_yesterday:yesterday"


def test_is_t_minus_2(test_df):
    rule = create_rule("date", "is_t_minus_2", "t-2")
    result = is_t_minus_2(test_df, rule)

    assert len(result) == 4
    assert result.iloc[0]["date"].date() != date.today() - timedelta(days=2)
    assert result.iloc[0]["dq_status"] == "date:is_t_minus_2:t-2"


def test_is_t_minus_3(test_df):
    rule = create_rule("date", "is_t_minus_3", "t-3")
    result = is_t_minus_3(test_df, rule)

    assert len(result) == 4
    assert result.iloc[0]["date"].date() != date.today() - timedelta(days=3)
    assert result.iloc[0]["dq_status"] == "date:is_t_minus_3:t-3"


def test_is_on_weekday(weekday_df):
    rule = create_rule("date", "is_on_weekday", "weekday")
    result = is_on_weekday(weekday_df, rule)

    assert len(result) == 3
    assert all(result["dq_status"] == "date:is_on_weekday:weekday")


def test_is_on_weekend(weekday_df):
    rule = create_rule("date", "is_on_weekend", "weekend")
    result = is_on_weekend(weekday_df, rule)

    assert len(result) == 6
    assert all(result["dq_status"] == "date:is_on_weekend:weekend")


def test_is_on_monday(weekday_df):
    rule = create_rule("date", "is_on_monday", "monday")
    result = is_on_monday(weekday_df, rule)

    assert len(result) == 7

    assert result.iloc[0]["dq_status"] == "date:is_on_monday:monday"
    assert result.iloc[0]["day_name"] != "Monday"


def test_is_on_tuesday(weekday_df):
    rule = create_rule("date", "is_on_tuesday", "tuesday")
    result = is_on_tuesday(weekday_df, rule)

    assert len(result) == 7

    assert result.iloc[0]["dq_status"] == "date:is_on_tuesday:tuesday"
    assert result.iloc[0]["day_name"] != "Tuesday"


def test_is_on_wednesday(weekday_df):
    rule = create_rule("date", "is_on_wednesday", "wednesday")
    result = is_on_wednesday(weekday_df, rule)

    assert len(result) == 7

    assert result.iloc[0]["dq_status"] == "date:is_on_wednesday:wednesday"
    assert result.iloc[0]["day_name"] != "Wednesday"


def test_is_on_thursday(weekday_df):
    rule = create_rule("date", "is_on_thursday", "thursday")
    result = is_on_thursday(weekday_df, rule)

    assert len(result) == 7

    assert result.iloc[0]["dq_status"] == "date:is_on_thursday:thursday"
    assert result.iloc[0]["day_name"] != "Thursday"


def test_is_on_friday(weekday_df):
    rule = create_rule("date", "is_on_friday", "friday")
    result = is_on_friday(weekday_df, rule)

    assert len(result) == 7
    assert result.iloc[0]["dq_status"] == "date:is_on_friday:friday"
    assert result.iloc[0]["day_name"] != "Friday"


def test_is_on_saturday(weekday_df):
    rule = create_rule("date", "is_on_saturday", "saturday")
    result = is_on_saturday(weekday_df, rule)

    assert len(result) == 7
    assert result.iloc[0]["dq_status"] == "date:is_on_saturday:saturday"
    assert result.iloc[0]["day_name"] != "Saturday"


def test_is_on_sunday(weekday_df):
    rule = create_rule("date", "is_on_sunday", "sunday")
    result = is_on_sunday(weekday_df, rule)

    assert len(result) == 7
    assert result.iloc[0]["dq_status"] == "date:is_on_sunday:sunday"
    assert result.iloc[0]["day_name"] != "Sunday"


def test_empty_dataframe():
    empty_df = pd.DataFrame(columns=["numeric", "date", "text"])
    empty_df["date"] = pd.to_datetime(empty_df["date"])

    numeric_rule = create_rule("numeric", "is_in_millions", "1M")
    assert is_in_millions(empty_df, numeric_rule).empty

    date_rule = create_rule("date", "is_today", "today")
    assert is_today(empty_df, date_rule).empty

    complete_rule = create_rule("text", "is_complete", "not_null")
    assert is_complete(empty_df, complete_rule).empty


def test_missing_column(test_df):
    with pytest.raises(KeyError):
        is_in_millions(test_df, create_rule("nonexistent", "is_in_millions", "1M"))

    with pytest.raises(KeyError):
        is_today(test_df, create_rule("nonexistent", "is_today", "today"))

    with pytest.raises(KeyError):
        is_complete(test_df, create_rule("nonexistent", "is_complete", "not_null"))


def test_dq_status_format(test_df):
    """Verifica se o formato do dq_status está correto para todas as funções"""
    functions = [
        (is_in_millions, "numeric", "is_in_millions", "1M"),
        (is_in_billions, "numeric", "is_in_billions", "1B"),
        (is_today, "date", "is_today", "today"),
        (is_yesterday, "date", "is_yesterday", "yesterday"),
        (is_complete, "text", "is_complete", "not_null"),
    ]

    for func, field, check_type, value in functions:
        rule = create_rule(field, check_type, value)
        result = func(test_df, rule)

        if not result.empty:
            expected_status = f"{field}:{check_type}:{value}"
            assert all(result["dq_status"] == expected_status)
