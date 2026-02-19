"""
Ray Data Analyzers - ROW-LEVEL ONLY.

Ray Data has limited aggregation support (no groupBy).
We only support row-level validations (completeness, comparison, pattern).

Strategy:
    Uses .map_batches() to process Pandas DataFrames efficiently.
    Returns fail counts (not lazy - Ray Data computes immediately).
"""

import pandas as pd

from sumeh.core.rules.rule_model import RuleDef


# ============================================================================
# COMPLETENESS
# ============================================================================


class CompletenessAnalyzer:
    @staticmethod
    def analyze(ds, rule: RuleDef):
        """Count null values across dataset."""
        field = rule.field

        def count_nulls(batch: pd.DataFrame) -> pd.DataFrame:
            null_count = batch[field].isnull().sum()
            return pd.DataFrame({"null_count": [null_count]})

        # Aggregate null counts
        result = ds.map_batches(count_nulls, batch_format="pandas")
        total_nulls = result.sum("null_count")

        return total_nulls


# ============================================================================
# COMPARISON
# ============================================================================


class ComparisonAnalyzer:
    @staticmethod
    def analyze(ds, rule: RuleDef):
        """Count comparison failures."""
        field = rule.field
        check_type = rule.check_type
        threshold = rule.value

        def count_failures(batch: pd.DataFrame) -> pd.DataFrame:
            col = batch[field]

            if check_type == "is_equal":
                fail_mask = col != threshold
            elif check_type == "is_greater_than":
                fail_mask = col <= threshold
            elif check_type == "is_less_than":
                fail_mask = col >= threshold
            elif check_type == "is_greater_or_equal_than":
                fail_mask = col < threshold
            elif check_type == "is_less_or_equal_than":
                fail_mask = col > threshold
            elif check_type == "is_positive":
                fail_mask = col <= 0
            elif check_type == "is_negative":
                fail_mask = col >= 0
            elif check_type == "is_in_millions":
                fail_mask = col < 1_000_000
            elif check_type == "is_in_billions":
                fail_mask = col < 1_000_000_000
            else:
                fail_mask = pd.Series([False] * len(batch))

            fail_count = fail_mask.sum()
            return pd.DataFrame({"fail_count": [fail_count]})

        result = ds.map_batches(count_failures, batch_format="pandas")
        total_fails = result.sum("fail_count")

        return total_fails


class BetweenAnalyzer:
    @staticmethod
    def analyze(ds, rule: RuleDef):
        """Count values outside bounds."""
        field = rule.field
        min_val, max_val = rule.value

        def count_failures(batch: pd.DataFrame) -> pd.DataFrame:
            col = batch[field]
            fail_mask = (col < min_val) | (col > max_val)
            fail_count = fail_mask.sum()
            return pd.DataFrame({"fail_count": [fail_count]})

        result = ds.map_batches(count_failures, batch_format="pandas")
        total_fails = result.sum("fail_count")

        return total_fails


# ============================================================================
# MEMBERSHIP
# ============================================================================


class MembershipAnalyzer:
    @staticmethod
    def analyze(ds, rule: RuleDef):
        """Count membership failures."""
        field = rule.field
        allowed = rule.value
        check_type = rule.check_type

        def count_failures(batch: pd.DataFrame) -> pd.DataFrame:
            col = batch[field]

            if check_type in ["is_in", "is_contained_in"]:
                fail_mask = ~col.isin(allowed)
            else:  # not_in
                fail_mask = col.isin(allowed)

            fail_count = fail_mask.sum()
            return pd.DataFrame({"fail_count": [fail_count]})

        result = ds.map_batches(count_failures, batch_format="pandas")
        total_fails = result.sum("fail_count")

        return total_fails


# ============================================================================
# PATTERN
# ============================================================================


class PatternAnalyzer:
    @staticmethod
    def analyze(ds, rule: RuleDef):
        """Count pattern match failures."""
        field = rule.field
        pattern = rule.value

        def count_failures(batch: pd.DataFrame) -> pd.DataFrame:
            col = batch[field].astype(str)
            match_mask = col.str.match(pattern, na=False)
            fail_count = (~match_mask).sum()
            return pd.DataFrame({"fail_count": [fail_count]})

        result = ds.map_batches(count_failures, batch_format="pandas")
        total_fails = result.sum("fail_count")

        return total_fails


class LegitAnalyzer:
    @staticmethod
    def analyze(ds, rule: RuleDef):
        """Count null or empty string failures."""
        field = rule.field

        def count_failures(batch: pd.DataFrame) -> pd.DataFrame:
            col = batch[field]
            is_null = col.isnull()
            is_empty = col.astype(str).str.strip() == ""
            fail_mask = is_null | is_empty
            fail_count = fail_mask.sum()
            return pd.DataFrame({"fail_count": [fail_count]})

        result = ds.map_batches(count_failures, batch_format="pandas")
        total_fails = result.sum("fail_count")

        return total_fails


# ============================================================================
# DATE
# ============================================================================


class DateAnalyzer:
    @staticmethod
    def analyze(ds, rule: RuleDef):
        """Count date validation failures."""
        field = rule.field
        check_type = rule.check_type

        def count_failures(batch: pd.DataFrame) -> pd.DataFrame:
            col = pd.to_datetime(batch[field], errors="coerce")
            col_normalized = col.dt.normalize()
            today = pd.Timestamp.now().normalize()

            if check_type == "is_today":
                fail_mask = col_normalized != today
            elif check_type in ["is_yesterday", "is_t_minus_1"]:
                fail_mask = col_normalized != (today - pd.Timedelta(days=1))
            elif check_type == "is_t_minus_2":
                fail_mask = col_normalized != (today - pd.Timedelta(days=2))
            elif check_type == "is_t_minus_3":
                fail_mask = col_normalized != (today - pd.Timedelta(days=3))
            elif check_type == "is_past_date":
                fail_mask = col_normalized >= today
            elif check_type == "is_future_date":
                fail_mask = col_normalized <= today
            elif check_type == "is_on_weekday":
                fail_mask = col.dt.weekday >= 5
            elif check_type == "is_on_weekend":
                fail_mask = col.dt.weekday < 5
            elif check_type == "is_on_monday":
                fail_mask = col.dt.weekday != 0
            elif check_type == "is_on_tuesday":
                fail_mask = col.dt.weekday != 1
            elif check_type == "is_on_wednesday":
                fail_mask = col.dt.weekday != 2
            elif check_type == "is_on_thursday":
                fail_mask = col.dt.weekday != 3
            elif check_type == "is_on_friday":
                fail_mask = col.dt.weekday != 4
            elif check_type == "is_on_saturday":
                fail_mask = col.dt.weekday != 5
            elif check_type == "is_on_sunday":
                fail_mask = col.dt.weekday != 6
            else:
                fail_mask = pd.Series([False] * len(batch))

            fail_count = fail_mask.sum()
            return pd.DataFrame({"fail_count": [fail_count]})

        result = ds.map_batches(count_failures, batch_format="pandas")
        total_fails = result.sum("fail_count")

        return total_fails


class DateBetweenAnalyzer:
    @staticmethod
    def analyze(ds, rule: RuleDef):
        """Count dates outside range."""
        field = rule.field
        start, end = rule.value

        def count_failures(batch: pd.DataFrame) -> pd.DataFrame:
            col = pd.to_datetime(batch[field], errors="coerce")
            start_ts = pd.Timestamp(start)
            end_ts = pd.Timestamp(end)
            fail_mask = (col < start_ts) | (col > end_ts)
            fail_count = fail_mask.sum()
            return pd.DataFrame({"fail_count": [fail_count]})

        result = ds.map_batches(count_failures, batch_format="pandas")
        total_fails = result.sum("fail_count")

        return total_fails


class DateComparisonAnalyzer:
    @staticmethod
    def analyze(ds, rule: RuleDef):
        """Count date comparison failures."""
        field = rule.field
        check_type = rule.check_type
        target = rule.value

        def count_failures(batch: pd.DataFrame) -> pd.DataFrame:
            col = pd.to_datetime(batch[field], errors="coerce")
            target_ts = pd.Timestamp(target)

            if check_type == "is_date_after":
                fail_mask = col <= target_ts
            else:  # is_date_before
                fail_mask = col >= target_ts

            fail_count = fail_mask.sum()
            return pd.DataFrame({"fail_count": [fail_count]})

        result = ds.map_batches(count_failures, batch_format="pandas")
        total_fails = result.sum("fail_count")

        return total_fails


# ============================================================================
# MULTI-FIELD
# ============================================================================


class MultiFieldCompletenessAnalyzer:
    @staticmethod
    def analyze(ds, rule: RuleDef):
        """Count rows where ANY field is null."""
        fields = rule.field if isinstance(rule.field, list) else [rule.field]

        def count_failures(batch: pd.DataFrame) -> pd.DataFrame:
            # Row fails if ANY field is null
            fail_mask = batch[fields].isnull().any(axis=1)
            fail_count = fail_mask.sum()
            return pd.DataFrame({"fail_count": [fail_count]})

        result = ds.map_batches(count_failures, batch_format="pandas")
        total_fails = result.sum("fail_count")

        return total_fails


# ============================================================================
# TABLE-LEVEL AGGREGATIONS
# ============================================================================


class AggregationAnalyzer:
    @staticmethod
    def analyze(ds, rule: RuleDef):
        """Compute table-level aggregations."""
        field = rule.field
        check_type = rule.check_type

        if check_type == "has_min":
            return ds.min(field)
        elif check_type == "has_max":
            return ds.max(field)
        elif check_type == "has_sum":
            return ds.sum(field)
        elif check_type == "has_mean":
            return ds.mean(field)
        elif check_type == "has_std":
            return ds.std(field)
        elif check_type == "has_cardinality":
            # Cardinality = distinct count (expensive!)
            def count_unique(batch: pd.DataFrame) -> pd.DataFrame:
                unique_vals = batch[field].nunique()
                return pd.DataFrame({"unique_count": [unique_vals]})

            result = ds.map_batches(count_unique, batch_format="pandas")
            # Note: This is approximate (counts per batch, not global)
            # For exact, would need groupby (not supported)
            return result.sum("unique_count")

        return 0
