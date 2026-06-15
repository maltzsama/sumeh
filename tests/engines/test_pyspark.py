"""
PySpark engine tests.

Requires Java + PySpark installed. Skipped automatically if not available.
Run with: pytest tests/engines/test_pyspark.py -v
"""

import pytest
from datetime import date, timedelta

pyspark = pytest.importorskip("pyspark", reason="pyspark not installed")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from sumeh.core.rules.rule_model import RuleDefinition
from sumeh.core.models.validation import ValidationStatus
from sumeh.engines.pyspark import validate

# ---------------------------------------------------------------------------
# Session fixture — one SparkSession for the whole module
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("sumeh-test-pyspark")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# DataFrame fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def df_basic(spark):
    return spark.createDataFrame(
        [
            (1, "alice", 30, "active", 1_500_000.0),
            (2, "bob", 25, "inactive", 500.0),
            (3, None, 40, "active", 2_000_000.0),
            (4, "diana", 17, "pending", 3_000_000_000.0),
            (5, "eve", 35, "active", 800_000.0),
        ],
        ["id", "name", "age", "status", "revenue"],
    )


@pytest.fixture
def df_unique(spark):
    return spark.createDataFrame(
        [(1, "a"), (2, "b"), (3, "a"), (4, "c")],
        ["id", "category"],
    )


@pytest.fixture
def df_dates(spark):
    today = date.today()
    yesterday = today - timedelta(days=1)
    past = date(2020, 1, 1)
    future = date(2099, 12, 31)
    monday = date(2025, 6, 2)  # Monday
    saturday = date(2025, 6, 7)  # Saturday

    rows = [
        (str(today),),
        (str(yesterday),),
        (str(past),),
        (str(future),),
        (str(monday),),
        (str(saturday),),
    ]
    return spark.createDataFrame(rows, ["dt"])


# ---------------------------------------------------------------------------
# Completeness
# ---------------------------------------------------------------------------


class TestCompleteness:

    def test_complete_column_passes(self, df_basic):
        rules = [RuleDefinition(field="id", check_type="is_complete", threshold=1.0)]
        report = validate(df_basic, rules)
        assert report.results[0].status == ValidationStatus.PASS

    def test_incomplete_column_fails(self, df_basic):
        rules = [RuleDefinition(field="name", check_type="is_complete", threshold=1.0)]
        report = validate(df_basic, rules)
        assert report.results[0].status == ValidationStatus.FAIL

    def test_multi_field_completeness(self, df_basic):
        rules = [
            RuleDefinition(
                field=["id", "name"], check_type="are_complete", threshold=1.0
            )
        ]
        report = validate(df_basic, rules)
        assert report.results[0].status == ValidationStatus.FAIL

    def test_pass_rate_below_threshold_fails(self, df_basic):
        # name has 1/5 nulls = 0.8 completeness, threshold=1.0
        rules = [RuleDefinition(field="name", check_type="is_complete", threshold=1.0)]
        report = validate(df_basic, rules)
        result = report.results[0]
        assert result.status == ValidationStatus.FAIL
        assert result.actual_value < 1.0


# ---------------------------------------------------------------------------
# Uniqueness
# ---------------------------------------------------------------------------


class TestUniqueness:

    def test_unique_column_passes(self, df_basic):
        rules = [RuleDefinition(field="id", check_type="is_unique", threshold=1.0)]
        report = validate(df_basic, rules)
        assert report.results[0].status == ValidationStatus.PASS

    def test_non_unique_column_fails(self, df_unique):
        rules = [
            RuleDefinition(field="category", check_type="is_unique", threshold=1.0)
        ]
        report = validate(df_unique, rules)
        assert report.results[0].status == ValidationStatus.FAIL

    def test_primary_key_alias(self, df_basic):
        rules = [RuleDefinition(field="id", check_type="is_primary_key", threshold=1.0)]
        report = validate(df_basic, rules)
        assert report.results[0].status == ValidationStatus.PASS

    def test_composite_key(self, df_unique):
        rules = [
            RuleDefinition(
                field=["id", "category"], check_type="are_unique", threshold=1.0
            )
        ]
        report = validate(df_unique, rules)
        assert report.results[0].status == ValidationStatus.PASS


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


class TestComparison:

    def test_is_positive_passes(self, df_basic):
        rules = [RuleDefinition(field="age", check_type="is_positive", threshold=1.0)]
        report = validate(df_basic, rules)
        assert report.results[0].status == ValidationStatus.PASS

    def test_is_greater_than_passes(self, df_basic):
        rules = [
            RuleDefinition(
                field="age", check_type="is_greater_than", value=0, threshold=1.0
            )
        ]
        report = validate(df_basic, rules)
        assert report.results[0].status == ValidationStatus.PASS

    def test_is_greater_than_fails(self, df_basic):
        rules = [
            RuleDefinition(
                field="age", check_type="is_greater_than", value=18, threshold=1.0
            )
        ]
        report = validate(df_basic, rules)
        # age=17 fails
        assert report.results[0].status == ValidationStatus.FAIL

    def test_is_between_passes(self, df_basic):
        rules = [
            RuleDefinition(
                field="age", check_type="is_between", value=[0, 150], threshold=1.0
            )
        ]
        report = validate(df_basic, rules)
        assert report.results[0].status == ValidationStatus.PASS

    def test_is_in_millions_partial(self, df_basic):
        rules = [
            RuleDefinition(field="revenue", check_type="is_in_millions", threshold=1.0)
        ]
        report = validate(df_basic, rules)
        # 500.0 and 800_000.0 fail
        assert report.results[0].status == ValidationStatus.FAIL

    def test_is_in_billions_passes(self, df_basic):
        rules = [
            RuleDefinition(field="revenue", check_type="is_in_billions", threshold=0.1)
        ]
        report = validate(df_basic, rules)
        # at least 1 row passes (3_000_000_000)
        assert report.results[0].actual_value > 0


# ---------------------------------------------------------------------------
# Membership
# ---------------------------------------------------------------------------


class TestMembership:

    def test_is_contained_in_passes(self, df_basic):
        rules = [
            RuleDefinition(
                field="status",
                check_type="is_contained_in",
                value=["active", "inactive", "pending"],
                threshold=1.0,
            )
        ]
        report = validate(df_basic, rules)
        assert report.results[0].status == ValidationStatus.PASS

    def test_is_contained_in_fails(self, df_basic):
        rules = [
            RuleDefinition(
                field="status",
                check_type="is_contained_in",
                value=["active"],
                threshold=1.0,
            )
        ]
        report = validate(df_basic, rules)
        assert report.results[0].status == ValidationStatus.FAIL

    def test_not_contained_in(self, df_basic):
        rules = [
            RuleDefinition(
                field="status",
                check_type="not_contained_in",
                value=["banned", "deleted"],
                threshold=1.0,
            )
        ]
        report = validate(df_basic, rules)
        assert report.results[0].status == ValidationStatus.PASS


# ---------------------------------------------------------------------------
# Pattern
# ---------------------------------------------------------------------------


class TestPattern:

    def test_has_pattern_passes(self, df_basic):
        rules = [
            RuleDefinition(
                field="name",
                check_type="has_pattern",
                value=r"^[a-z]+$",
                threshold=0.7,  # name has 1 null
            )
        ]
        report = validate(df_basic, rules)
        assert report.results[0].status == ValidationStatus.PASS

    def test_is_legit_fails_on_nulls(self, df_basic):
        rules = [RuleDefinition(field="name", check_type="is_legit", threshold=1.0)]
        report = validate(df_basic, rules)
        assert report.results[0].status == ValidationStatus.FAIL


# ---------------------------------------------------------------------------
# Date
# ---------------------------------------------------------------------------


class TestDate:

    def test_is_past_date(self, df_dates):
        rules = [RuleDefinition(field="dt", check_type="is_past_date", threshold=0.4)]
        report = validate(df_dates, rules)
        # past (2020) and yesterday are in the past
        assert report.results[0].actual_value > 0

    def test_is_future_date(self, df_dates):
        rules = [RuleDefinition(field="dt", check_type="is_future_date", threshold=0.1)]
        report = validate(df_dates, rules)
        assert report.results[0].actual_value > 0

    def test_is_on_weekend(self, df_dates):
        rules = [RuleDefinition(field="dt", check_type="is_on_weekend", threshold=0.1)]
        report = validate(df_dates, rules)
        # saturday is in the dataset
        assert report.results[0].actual_value > 0

    def test_is_on_monday(self, df_dates):
        rules = [RuleDefinition(field="dt", check_type="is_on_monday", threshold=0.1)]
        report = validate(df_dates, rules)
        assert report.results[0].actual_value > 0


# ---------------------------------------------------------------------------
# Aggregations (table-level)
# ---------------------------------------------------------------------------


class TestAggregation:

    def test_has_mean_passes(self, df_basic):
        rules = [
            RuleDefinition(
                field="age",
                check_type="has_mean",
                value=29.4,
                threshold=0.1,
            )
        ]
        report = validate(df_basic, rules)
        assert report.results[0].status == ValidationStatus.PASS

    def test_has_min(self, df_basic):
        rules = [
            RuleDefinition(field="age", check_type="has_min", value=17, threshold=0.0)
        ]
        report = validate(df_basic, rules)
        assert report.results[0].status == ValidationStatus.PASS

    def test_has_max(self, df_basic):
        rules = [
            RuleDefinition(field="age", check_type="has_max", value=40, threshold=0.0)
        ]
        report = validate(df_basic, rules)
        assert report.results[0].status == ValidationStatus.PASS

    def test_has_cardinality(self, df_basic):
        rules = [
            RuleDefinition(
                field="status", check_type="has_cardinality", value=3, threshold=0.0
            )
        ]
        report = validate(df_basic, rules)
        assert report.results[0].status == ValidationStatus.PASS


# ---------------------------------------------------------------------------
# ValidationReport
# ---------------------------------------------------------------------------


class TestValidationReport:

    def test_returns_report(self, df_basic):
        rules = [RuleDefinition(field="id", check_type="is_complete", threshold=1.0)]
        report = validate(df_basic, rules)
        assert report is not None
        assert len(report.results) == 1
        assert report.engine == "pyspark"

    def test_pass_rate(self, df_basic):
        rules = [
            RuleDefinition(field="id", check_type="is_complete", threshold=1.0),
            RuleDefinition(field="name", check_type="is_complete", threshold=1.0),
        ]
        report = validate(df_basic, rules)
        assert 0.0 <= report.pass_rate <= 1.0

    def test_split_returns_two_dataframes(self, df_basic):
        rules = [RuleDefinition(field="name", check_type="is_complete", threshold=1.0)]
        report = validate(df_basic, rules)
        good, bad = report.split()
        assert good is not None
        assert bad is not None

    def test_execution_time_set(self, df_basic):
        rules = [RuleDefinition(field="id", check_type="is_complete", threshold=1.0)]
        report = validate(df_basic, rules)
        assert report.execution_time_ms > 0

    def test_multiple_rules(self, df_basic):
        rules = [
            RuleDefinition(field="id", check_type="is_complete", threshold=1.0),
            RuleDefinition(field="id", check_type="is_unique", threshold=1.0),
            RuleDefinition(field="age", check_type="is_positive", threshold=1.0),
            RuleDefinition(
                field="status",
                check_type="is_contained_in",
                value=["active", "inactive", "pending"],
                threshold=1.0,
            ),
        ]
        report = validate(df_basic, rules)
        assert len(report.results) == 4

    def test_unknown_field_produces_error_result(self, df_basic):
        rules = [
            RuleDefinition(field="nonexistent", check_type="is_complete", threshold=1.0)
        ]
        report = validate(df_basic, rules)
        assert report.results[0].status == ValidationStatus.ERROR

    def test_table_level_not_in_split(self, df_basic):
        """Table-level rules don't affect row bifurcation."""
        rules = [
            RuleDefinition(
                field="age", check_type="has_mean", value=29.4, threshold=0.1
            ),
        ]
        report = validate(df_basic, rules)
        assert len(report.results) == 1
