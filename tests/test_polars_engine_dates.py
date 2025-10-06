import pytest
import polars as pl
from datetime import date, timedelta
from sumeh.engines.polars_engine import (
    is_in_millions,
    is_in_billions,
    is_today,
    is_t_minus_1,
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
)


@pytest.fixture
def test_data():
    """DataFrame de teste com dados numéricos e de data"""
    return pl.DataFrame(
        {
            "numeric": [
                999_999,
                1_000_000,
                2_000_000,
                999_999_999,
                1_000_000_000,
                None,
            ],
            "date": [
                date.today().isoformat(),
                (date.today() - timedelta(days=1)).isoformat(),
                (date.today() - timedelta(days=2)).isoformat(),
                (date.today() - timedelta(days=3)).isoformat(),
                "2023-01-02",
                None,
            ],
        }
    )


@pytest.fixture
def weekday_data():
    """DataFrame com dias da semana específicos"""
    return pl.DataFrame(
        {
            "date": [
                "2023-01-02",
                "2023-01-03",
                "2023-01-04",
                "2023-01-05",
                "2023-01-06",
                "2023-01-07",
                "2023-01-08",
                None,
            ],
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

    return {"field": field, "check_type": check_type, "value": value}


def assert_frame_equal(result, expected):

    assert result.schema == expected.schema

    assert result.equals(expected)


def test_is_in_millions(test_data):
    rule = create_rule("numeric", "is_in_millions", "1M")
    result = is_in_millions(test_data, rule)

    expected = test_data.filter(pl.col("numeric") < 1_000_000).with_columns(
        pl.lit("numeric:is_in_millions:1M").alias("dq_status")
    )
    assert_frame_equal(result, expected)


def test_is_in_billions(test_data):
    rule = create_rule("numeric", "is_in_billions", "1B")
    result = is_in_billions(test_data, rule)

    expected = test_data.filter(pl.col("numeric") < 1_000_000_000).with_columns(
        pl.lit("numeric:is_in_billions:1B").alias("dq_status")
    )
    assert_frame_equal(result, expected)


def test_is_today(test_data):
    rule = create_rule("date", "is_today", "today")
    result = is_today(test_data, rule)

    today_str = date.today().isoformat()
    expected = test_data.filter(pl.col("date") == today_str).with_columns(
        pl.lit("date:is_today:today").alias("dq_status")
    )
    assert_frame_equal(result, expected)


def test_is_t_minus_1(test_data):
    rule = create_rule("date", "is_t_minus_1", "t-1")
    result = is_t_minus_1(test_data, rule)

    yesterday = (date.today() - timedelta(days=1)).isoformat()
    expected = test_data.filter(pl.col("date") == yesterday).with_columns(
        pl.lit("date:is_t_minus_1:t-1").alias("dq_status")
    )
    assert_frame_equal(result, expected)


def test_is_t_minus_2(test_data):
    rule = create_rule("date", "is_t_minus_2", "t-2")
    result = is_t_minus_2(test_data, rule)

    target = (date.today() - timedelta(days=2)).isoformat()
    expected = test_data.filter(pl.col("date") == target).with_columns(
        pl.lit("date:is_t_minus_2:t-2").alias("dq_status")
    )
    assert_frame_equal(result, expected)


def test_is_t_minus_3(test_data):
    rule = create_rule("date", "is_t_minus_3", "t-3")
    result = is_t_minus_3(test_data, rule)

    target = (date.today() - timedelta(days=3)).isoformat()
    expected = test_data.filter(pl.col("date") == target).with_columns(
        pl.lit("date:is_t_minus_3:t-3").alias("dq_status")
    )
    assert_frame_equal(result, expected)


def test_is_on_weekday(weekday_data):
    rule = create_rule("date", "is_on_weekday", "weekday")
    result = is_on_weekday(weekday_data, rule)

    expected = weekday_data.filter(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d").dt.weekday() < 5
    ).with_columns(pl.lit("date:is_on_weekday:weekday").alias("dq_status"))
    assert_frame_equal(result, expected)


def test_is_on_weekend(weekday_data):
    rule = create_rule("date", "is_on_weekend", "weekend")
    result = is_on_weekend(weekday_data, rule)

    expected = weekday_data.filter(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d").dt.weekday() >= 5
    ).with_columns(pl.lit("date:is_on_weekend:weekend").alias("dq_status"))
    assert_frame_equal(result, expected)


def test_is_on_monday(weekday_data):
    rule = create_rule("date", "is_on_monday", "monday")
    result = is_on_monday(weekday_data, rule)

    expected = weekday_data.filter(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d").dt.weekday() == 0
    ).with_columns(pl.lit("date:is_on_monday:monday").alias("dq_status"))
    assert_frame_equal(result, expected)


def test_is_on_tuesday(weekday_data):
    rule = create_rule("date", "is_on_tuesday", "tuesday")
    result = is_on_tuesday(weekday_data, rule)

    expected = weekday_data.filter(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d").dt.weekday() == 1
    ).with_columns(pl.lit("date:is_on_tuesday:tuesday").alias("dq_status"))
    assert_frame_equal(result, expected)


def test_is_on_wednesday(weekday_data):
    rule = create_rule("date", "is_on_wednesday", "wednesday")
    result = is_on_wednesday(weekday_data, rule)

    expected = weekday_data.filter(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d").dt.weekday() == 2
    ).with_columns(pl.lit("date:is_on_wednesday:wednesday").alias("dq_status"))
    assert_frame_equal(result, expected)


def test_is_on_thursday(weekday_data):
    rule = create_rule("date", "is_on_thursday", "thursday")
    result = is_on_thursday(weekday_data, rule)

    expected = weekday_data.filter(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d").dt.weekday() == 3
    ).with_columns(pl.lit("date:is_on_thursday:thursday").alias("dq_status"))
    assert_frame_equal(result, expected)


def test_is_on_friday(weekday_data):
    rule = create_rule("date", "is_on_friday", "friday")
    result = is_on_friday(weekday_data, rule)

    expected = weekday_data.filter(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d").dt.weekday() == 4
    ).with_columns(pl.lit("date:is_on_friday:friday").alias("dq_status"))
    assert_frame_equal(result, expected)


def test_is_on_saturday(weekday_data):
    rule = create_rule("date", "is_on_saturday", "saturday")
    result = is_on_saturday(weekday_data, rule)

    expected = weekday_data.filter(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d").dt.weekday() == 5
    ).with_columns(pl.lit("date:is_on_saturday:saturday").alias("dq_status"))
    assert_frame_equal(result, expected)


def test_is_on_sunday(weekday_data):
    rule = create_rule("date", "is_on_sunday", "sunday")
    result = is_on_sunday(weekday_data, rule)

    expected = weekday_data.filter(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d").dt.weekday() == 6
    ).with_columns(pl.lit("date:is_on_sunday:sunday").alias("dq_status"))
    assert_frame_equal(result, expected)


def test_empty_dataframe():
    empty_df = pl.DataFrame(schema={"numeric": pl.Float64, "date": pl.Utf8})

    numeric_rule = create_rule("numeric", "is_in_millions", "1M")
    assert is_in_millions(empty_df, numeric_rule).is_empty()

    date_rule = create_rule("date", "is_today", "today")
    assert is_today(empty_df, date_rule).is_empty()


def test_missing_column(test_data):
    with pytest.raises(pl.exceptions.ColumnNotFoundError):
        is_in_millions(test_data, create_rule("missing", "is_in_millions", "1M"))

    with pytest.raises(pl.exceptions.ColumnNotFoundError):
        is_today(test_data, create_rule("missing", "is_today", "today"))


def test_dq_status_format(test_data):
    functions = [
        (is_in_millions, "numeric", "is_in_millions", "1M"),
        (is_in_billions, "numeric", "is_in_billions", "1B"),
        (is_today, "date", "is_today", "today"),
        (is_t_minus_1, "date", "is_t_minus_1", "t-1"),
    ]

    for func, field, check_type, value in functions:
        rule = create_rule(field, check_type, value)
        result = func(test_data, rule)

        if not result.is_empty():
            expected_status = f"{field}:{check_type}:{value}"
            assert result["dq_status"][0] == expected_status
