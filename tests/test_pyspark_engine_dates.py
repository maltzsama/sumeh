import pytest
from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sumeh.engine.pyspark_engine import (
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


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("dq_tests").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def df_dates(spark):
    today = date.today()
    data = [
        (1_500_000, 2_000_000_000, None),
        (0, 0, today),
    ]
    cols = ["in_millions", "in_billions", "dt"]
    return spark.createDataFrame(data, cols)


def test_is_in_millions(df_dates):
    df = is_in_millions(
        df_dates, {"field": "in_millions", "check_type": "is_in_millions", "value": ""}
    )
    assert df.filter(col("in_millions") < 1_000_000).count() == 0
    assert df.filter(col("in_millions") >= 1_000_000).count() == 1


def test_is_in_billions(df_dates):
    df = is_in_billions(
        df_dates, {"field": "in_billions", "check_type": "is_in_billions", "value": ""}
    )
    assert df.filter(col("in_billions") < 1_000_000_000).count() == 0
    assert df.filter(col("in_billions") >= 1_000_000_000).count() == 1


@pytest.fixture
def df_relatives(spark):
    today = date.today()
    rows = []
    for off in [0, 1, 2, 3]:
        rows.append((today - timedelta(days=off),))
    rows += [(today + timedelta(days=1),)]
    cols = ["dt"]
    return spark.createDataFrame(rows, cols)


def test_is_t_minus_1(df_relatives):
    df = is_t_minus_1(
        df_relatives, {"field": "dt", "check_type": "is_t_minus_1", "value": ""}
    )
    target = date.today() - timedelta(days=1)
    assert df.filter(col("dt") == target).count() == 1


def test_is_t_minus_2(df_relatives):
    df = is_t_minus_2(
        df_relatives, {"field": "dt", "check_type": "is_t_minus_2", "value": ""}
    )
    target = date.today() - timedelta(days=2)
    assert df.filter(col("dt") == target).count() == 1


def test_is_t_minus_3(df_relatives):
    df = is_t_minus_3(
        df_relatives, {"field": "dt", "check_type": "is_t_minus_3", "value": ""}
    )
    target = date.today() - timedelta(days=3)
    assert df.filter(col("dt") == target).count() == 1


def test_is_today_and_yesterday(df_relatives):
    df_today = is_today(
        df_relatives, {"field": "dt", "check_type": "is_today", "value": ""}
    )
    assert df_today.filter(col("dt") == date.today()).count() == 1

    df_yes = is_yesterday(
        df_relatives, {"field": "dt", "check_type": "is_yesterday", "value": ""}
    )
    assert df_yes.filter(col("dt") == date.today() - timedelta(days=1)).count() == 1


def test_weekday_and_weekend(df_relatives):
    df_wd = is_on_weekday(
        df_relatives, {"field": "dt", "check_type": "is_on_weekday", "value": ""}
    )
    wd_count = df_wd.count()
    assert wd_count >= 4

    df_we = is_on_weekend(
        df_relatives, {"field": "dt", "check_type": "is_on_weekend", "value": ""}
    )
    assert df_we.count() <= 2


@pytest.mark.parametrize(
    "fn,weekday",
    [
        (is_on_monday, 0),
        (is_on_tuesday, 1),
        (is_on_wednesday, 2),
        (is_on_thursday, 3),
        (is_on_friday, 4),
        (is_on_saturday, 5),
        (is_on_sunday, 6),
    ],
)
def test_each_weekday(df_relatives, fn, weekday):
    df = fn(df_relatives, {"field": "dt", "check_type": fn.__name__, "value": ""})
    vals = [row.dt.weekday() for row in df.collect()]
    assert all(v == weekday for v in vals)
