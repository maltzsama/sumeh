"""
Ray Data engine tests.

Two layers:
1. Analyzer batch logic — tested with a mock Dataset (no Ray required).
   The inner batch functions are pure pandas, so we exercise them directly.
2. Engine integration — skipped if Ray not installed.

Run with: pytest tests/engines/test_ray.py -v
"""

import pandas as pd
import pytest

from sumeh.core.rules.rule_definition import RuleDefinition
from sumeh.core.models.validation import ValidationStatus
from sumeh.engines.ray.analyzers import (
    CompletenessAnalyzer,
    MultiFieldCompletenessAnalyzer,
    ComparisonAnalyzer,
    BetweenAnalyzer,
    MembershipAnalyzer,
    PatternAnalyzer,
    LegitAnalyzer,
    DateAnalyzer,
    DateBetweenAnalyzer,
    DateComparisonAnalyzer,
    AggregationAnalyzer,
)

# ---------------------------------------------------------------------------
# Mock Ray Dataset
# Intercepts map_batches(fn) → calls fn(df) immediately on a pandas DataFrame.
# Intercepts sum/min/max/mean/std via direct pandas aggregation.
# ---------------------------------------------------------------------------


class MockDataset:
    """
    Minimal Ray Dataset mock for unit testing.
    Holds a pandas DataFrame internally and executes map_batches immediately.
    """

    def __init__(self, df: pd.DataFrame):
        self._df = df

    def map_batches(self, fn, batch_format="pandas"):
        result_df = fn(self._df)
        return MockDataset(result_df)

    def sum(self, col: str):
        val = self._df[col].sum()
        if pd.isna(val):
            return 0.0
        return float(val)

    def min(self, col: str):
        val = self._df[col].min()
        if pd.isna(val):
            return None
        return float(val)

    def max(self, col: str):
        val = self._df[col].max()
        if pd.isna(val):
            return None
        return float(val)

    def mean(self, col: str):
        val = self._df[col].mean()
        if pd.isna(val):
            return None
        return float(val)

    def std(self, col: str):
        val = self._df[col].std()
        if pd.isna(val):
            return None
        return float(val)

    def count(self):
        return len(self._df)


# ---------------------------------------------------------------------------
# Sample DataFrames
# ---------------------------------------------------------------------------


@pytest.fixture
def df_basic():
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["alice", "bob", None, "diana", ""],
            "age": [30, 25, 40, 17, 35],
            "status": ["active", "inactive", "active", "pending", "active"],
            "revenue": [1_500_000.0, 500.0, 2_000_000.0, 3_000_000_000.0, 800_000.0],
        }
    )


@pytest.fixture
def ds_basic(df_basic):
    return MockDataset(df_basic)


@pytest.fixture
def df_dates():
    # All fixed historical dates — no today/yesterday to avoid weekday drift
    return pd.DataFrame(
        {
            "dt": [
                "2025-06-02",  # Monday
                "2025-06-03",  # Tuesday
                "2025-06-07",  # Saturday
                "2025-06-08",  # Sunday
                "2020-01-01",  # past (Wednesday)
                "2099-12-31",  # future (Thursday)
            ]
        }
    )


@pytest.fixture
def ds_dates(df_dates):
    return MockDataset(df_dates)


# ---------------------------------------------------------------------------
# Completeness
# ---------------------------------------------------------------------------


class TestCompletenessAnalyzer:

    def test_no_nulls_returns_zero(self, ds_basic):
        rule = RuleDefinition(field="id", check_type="is_complete")
        result = CompletenessAnalyzer.analyze(ds_basic, rule)
        assert result == 0

    def test_counts_nulls(self, ds_basic):
        rule = RuleDefinition(field="name", check_type="is_complete")
        result = CompletenessAnalyzer.analyze(ds_basic, rule)
        assert result == 1  # one None

    def test_multi_field_counts_any_null(self, ds_basic):
        rule = RuleDefinition(field=["id", "name"], check_type="are_complete")
        result = MultiFieldCompletenessAnalyzer.analyze(ds_basic, rule)
        assert result == 1


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


class TestComparisonAnalyzer:

    @pytest.mark.parametrize(
        "check_type,value,expected_fails",
        [
            ("is_equal", 30, 4),        # only age=30 passes → 4 fail
            ("is_greater_than", 18, 1),  # age=17 fails → 1 fail
            ("is_less_than", 40, 1),     # age=40 fails → 1 fail
            ("is_greater_or_equal_than", 30, 2),  # age=25,17 fail → 2 fails
            ("is_less_or_equal_than", 30, 2),     # age=40,35 fail → 2 fails
            ("is_positive", None, 0),    # all ages > 0 → 0 fail
            ("is_negative", None, 5),    # no negative ages → 5 fail
        ],
    )
    def test_comparison_fail_counts(self, ds_basic, check_type, value, expected_fails):
        if check_type in ("is_positive", "is_negative"):
            rule = RuleDefinition(field="age", check_type=check_type)
        else:
            rule = RuleDefinition(field="age", check_type=check_type, value=value)
        result = ComparisonAnalyzer.analyze(ds_basic, rule)
        assert result == expected_fails

    def test_is_in_millions(self, ds_basic):
        """Test is_in_millions on revenue column (values >= 1_000_000)"""
        rule = RuleDefinition(field="revenue", check_type="is_in_millions")
        result = ComparisonAnalyzer.analyze(ds_basic, rule)
        # revenue: 1.5M, 500, 2M, 3B, 800k
        # Fails: 500, 800k (2 failures)
        assert result == 2

    def test_is_in_billions(self, ds_basic):
        rule = RuleDefinition(field="revenue", check_type="is_in_billions")
        result = ComparisonAnalyzer.analyze(ds_basic, rule)
        # only 3_000_000_000 passes → 4 fail
        assert result == 4


class TestBetweenAnalyzer:

    def test_all_in_range(self, ds_basic):
        rule = RuleDefinition(field="age", check_type="is_between", value=[0, 150])
        result = BetweenAnalyzer.analyze(ds_basic, rule)
        assert result == 0

    def test_some_out_of_range(self, ds_basic):
        rule = RuleDefinition(field="age", check_type="is_between", value=[18, 30])
        result = BetweenAnalyzer.analyze(ds_basic, rule)
        # age=40, age=35, age=17 fail → 3
        assert result == 3


# ---------------------------------------------------------------------------
# Membership
# ---------------------------------------------------------------------------


class TestMembershipAnalyzer:

    def test_is_contained_in_no_failures(self, ds_basic):
        rule = RuleDefinition(
            field="status",
            check_type="is_contained_in",
            value=["active", "inactive", "pending"],
        )
        result = MembershipAnalyzer.analyze(ds_basic, rule)
        assert result == 0

    def test_is_contained_in_failures(self, ds_basic):
        rule = RuleDefinition(
            field="status",
            check_type="is_contained_in",
            value=["active"],
        )
        result = MembershipAnalyzer.analyze(ds_basic, rule)
        # inactive and pending fail → 2
        assert result == 2

    def test_not_in_list(self, ds_basic):
        rule = RuleDefinition(
            field="status",
            check_type="not_in",
            value=["banned", "deleted"],
        )
        result = MembershipAnalyzer.analyze(ds_basic, rule)
        # No values in forbidden list → 0 failures
        assert result == 0

    def test_not_in_list_with_matches(self, ds_basic):
        rule = RuleDefinition(
            field="status",
            check_type="not_in",
            value=["active", "inactive"],
        )
        result = MembershipAnalyzer.analyze(ds_basic, rule)
        # active (rows 0,2,4) and inactive (row 1) are forbidden → 4 failures
        assert result == 4


# ---------------------------------------------------------------------------
# Pattern
# ---------------------------------------------------------------------------


class TestPatternAnalyzer:

    def test_pattern_matches(self, ds_basic):
        rule = RuleDefinition(
            field="status", check_type="has_pattern", value=r"^[a-z]+$"
        )
        result = PatternAnalyzer.analyze(ds_basic, rule)
        assert result == 0

    def test_pattern_failures(self, ds_basic):
        rule = RuleDefinition(
            field="name", check_type="has_pattern", value=r"^[a-z]{4,}$"
        )
        result = PatternAnalyzer.analyze(ds_basic, rule)
        # None→"None" fails pattern, "" fails, "bob" (3 chars) fails → 3 failures
        assert result == 3


class TestLegitAnalyzer:

    def test_legit_catches_null_and_empty(self, ds_basic):
        rule = RuleDefinition(field="name", check_type="is_legit")
        result = LegitAnalyzer.analyze(ds_basic, rule)
        # None and "" → 2 failures
        assert result == 2

    def test_legit_clean_column(self, ds_basic):
        rule = RuleDefinition(field="status", check_type="is_legit")
        result = LegitAnalyzer.analyze(ds_basic, rule)
        assert result == 0


# ---------------------------------------------------------------------------
# Date
# ---------------------------------------------------------------------------


class TestDateAnalyzer:
    # Fixture has: Mon(06-02), Tue(06-03), Sat(06-07), Sun(06-08), past(2020-01-01), future(2099-12-31)

    def test_is_past_date(self, ds_dates):
        rule = RuleDefinition(field="dt", check_type="is_past_date")
        result = DateAnalyzer.analyze(ds_dates, rule)
        # 2099-12-31 is not past → all other 5 rows are past → 1 fail
        assert result == 1

    def test_is_future_date(self, ds_dates):
        rule = RuleDefinition(field="dt", check_type="is_future_date")
        result = DateAnalyzer.analyze(ds_dates, rule)
        # only 2099-12-31 is future → 1 pass, 5 fail
        assert result == 5

    def test_is_on_monday(self, ds_dates):
        rule = RuleDefinition(field="dt", check_type="is_on_monday")
        result = DateAnalyzer.analyze(ds_dates, rule)
        # only 2025-06-02 is Monday → 1 pass, 5 fail
        assert result == 5

    def test_is_on_saturday(self, ds_dates):
        rule = RuleDefinition(field="dt", check_type="is_on_saturday")
        result = DateAnalyzer.analyze(ds_dates, rule)
        # only 2025-06-07 is Saturday → 1 pass, 5 fail
        assert result == 5

    def test_is_on_weekday(self, ds_dates):
        rule = RuleDefinition(field="dt", check_type="is_on_weekday")
        result = DateAnalyzer.analyze(ds_dates, rule)
        # Sat(06-07) and Sun(06-08) are weekend → 2 failures
        assert result == 2

    def test_is_on_weekend(self, ds_dates):
        rule = RuleDefinition(field="dt", check_type="is_on_weekend")
        result = DateAnalyzer.analyze(ds_dates, rule)
        # Mon,Tue,Wed(2020),Thu(2099) are weekdays → 4 failures
        assert result == 4


class TestDateBetweenAnalyzer:

    def test_date_between_catches_out_of_range(self, ds_dates):
        rule = RuleDefinition(
            field="dt",
            check_type="is_date_between",
            value=["2019-01-01", "2025-12-31"],
        )
        result = DateBetweenAnalyzer.analyze(ds_dates, rule)
        # 2099-12-31 fails → at least 1 failure
        assert result >= 1

    def test_date_between_all_pass(self, ds_dates):
        rule = RuleDefinition(
            field="dt",
            check_type="is_date_between",
            value=["2000-01-01", "2100-01-01"],
        )
        result = DateBetweenAnalyzer.analyze(ds_dates, rule)
        assert result == 0


class TestDateComparisonAnalyzer:

    def test_is_date_after(self, ds_dates):
        """is_date_after: value <= target? No — should be value > target to pass"""
        rule = RuleDefinition(
            field="dt",
            check_type="is_date_after",
            value="2010-01-01",
        )
        result = DateComparisonAnalyzer.analyze(ds_dates, rule)
        # All dates in fixture are > 2010-01-01, so all pass → 0 failures
        assert result == 0

    def test_is_date_before(self, ds_dates):
        rule = RuleDefinition(
            field="dt",
            check_type="is_date_before",
            value="2050-01-01",
        )
        result = DateComparisonAnalyzer.analyze(ds_dates, rule)
        # 2099-12-31 is NOT before 2050 → fails → 1 failure
        assert result == 1


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


class TestAggregationAnalyzer:

    def test_has_min(self, ds_basic):
        rule = RuleDefinition(field="age", check_type="has_min")
        result = AggregationAnalyzer.analyze(ds_basic, rule)
        assert result == 17.0

    def test_has_max(self, ds_basic):
        rule = RuleDefinition(field="age", check_type="has_max")
        result = AggregationAnalyzer.analyze(ds_basic, rule)
        assert result == 40.0

    def test_has_sum(self, ds_basic):
        rule = RuleDefinition(field="age", check_type="has_sum")
        result = AggregationAnalyzer.analyze(ds_basic, rule)
        assert result == 147.0  # 30+25+40+17+35

    def test_has_mean(self, ds_basic):
        rule = RuleDefinition(field="age", check_type="has_mean")
        result = AggregationAnalyzer.analyze(ds_basic, rule)
        assert abs(result - 29.4) < 0.01

    def test_has_std(self, ds_basic):
        rule = RuleDefinition(field="age", check_type="has_std")
        result = AggregationAnalyzer.analyze(ds_basic, rule)
        assert result > 0


# ---------------------------------------------------------------------------
# Engine integration — only if Ray installed
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def ray_init():
    ray = pytest.importorskip("ray", reason="ray not installed")
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
    return ray


@pytest.fixture
def ray_dataset(ray_init):
    ray = ray_init
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["alice", "bob", None, "diana", "eve"],
            "age": [30, 25, 40, 17, 35],
        }
    )
    return ray.data.from_pandas(df)


class TestRayEngineIntegration:

    def test_validate_returns_report(self, ray_dataset):
        from sumeh.engines.ray import validate as ray_validate

        rules = [RuleDefinition(field="id", check_type="is_complete", threshold=1.0)]
        report = ray_validate(ray_dataset, rules)
        assert report is not None
        assert report.engine == "ray"

    def test_completeness_pass(self, ray_dataset):
        from sumeh.engines.ray import validate as ray_validate

        rules = [RuleDefinition(field="id", check_type="is_complete", threshold=1.0)]
        report = ray_validate(ray_dataset, rules)
        assert report.results[0].status == ValidationStatus.PASS

    def test_completeness_fail(self, ray_dataset):
        from sumeh.engines.ray import validate as ray_validate

        rules = [RuleDefinition(field="name", check_type="is_complete", threshold=1.0)]
        report = ray_validate(ray_dataset, rules)
        assert report.results[0].status == ValidationStatus.FAIL

    def test_table_level_not_supported(self, ray_dataset):
        from sumeh.engines.ray import validate as ray_validate

        rules = [
            RuleDefinition(
                field="age", check_type="has_mean", value=29.4, threshold=0.1
            )
        ]
        report = ray_validate(ray_dataset, rules)
        # Ray engine marks table-level as ERROR
        assert report.results[0].status == ValidationStatus.ERROR

    def test_multiple_rules(self, ray_dataset):
        from sumeh.engines.ray import validate as ray_validate

        rules = [
            RuleDefinition(field="id", check_type="is_complete", threshold=1.0),
            RuleDefinition(field="age", check_type="is_positive", threshold=1.0),
            RuleDefinition(
                field="age", check_type="is_greater_than", value=0, threshold=1.0
            ),
        ]
        report = ray_validate(ray_dataset, rules)
        assert len(report.results) == 3
