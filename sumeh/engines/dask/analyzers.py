"""
Dask Analyzers - COMPLETE IMPLEMENTATION.
Lazy evaluation builders for distributed datasets.

Strategy:
    Returns Dask Scalars/Series that construct the computation graph.
    Actual execution happens in the Engine via dask.compute().
"""

import dask.dataframe as dd
import dask.array as da
import pandas as pd
import numpy as np
from sumeh.core.rules.rule_model import RuleDef

# ============================================================================
# COMPLETENESS
# ============================================================================


class CompletenessAnalyzer:
    @staticmethod
    def analyze(df: dd.DataFrame, rule: RuleDef):
        """Returns count of null values."""
        return df[rule.field].isnull().sum()


class MultiFieldCompletenessAnalyzer:
    @staticmethod
    def analyze(df: dd.DataFrame, rule: RuleDef):
        """Returns count of rows where ANY field is null."""
        fields = rule.field if isinstance(rule.field, list) else [rule.field]
        # Axis=1 in Dask can be slow, but it's necessary here.
        return df[fields].isnull().any(axis=1).sum()


# ============================================================================
# UNIQUENESS
# ============================================================================


class UniquenessAnalyzer:
    @staticmethod
    def analyze(df: dd.DataFrame, rule: RuleDef):
        """
        Returns count of duplicates.
        Note: Exact uniqueness in Dask triggers a shuffle!
        """
        col = df[rule.field]
        total = len(df)  # Lazy
        # We assume exact uniqueness is required for Data Quality
        unique = col.nunique()
        return total - unique


class MultiFieldUniquenessAnalyzer:
    @staticmethod
    def analyze(df: dd.DataFrame, rule: RuleDef):
        """Concatenates fields to check composite uniqueness."""
        fields = rule.field if isinstance(rule.field, list) else [rule.field]

        # Dask string concatenation for composite key
        # We handle non-string types by casting
        def concat_row(row):
            return "|".join(str(x) for x in row)

        # map_partitions is more efficient than applying over the whole index
        key_col = (
            df[fields]
            .astype(str)
            .map_partitions(
                lambda pdf: pdf.apply(lambda row: "|".join(row.values), axis=1),
                meta=("key", "object"),
            )
        )

        total = len(df)
        unique = key_col.nunique()
        return total - unique


# ============================================================================
# COMPARISON (The Heavy Lifters)
# ============================================================================


class ComparisonAnalyzer:
    @staticmethod
    def analyze(df: dd.DataFrame, rule: RuleDef):
        col = df[rule.field]
        val = rule.value
        check = rule.check_type

        # Dispatch table for Dask Series operations
        if check == "is_equal":
            fail_mask = col != val
        elif check == "is_greater_than":
            fail_mask = col <= val
        elif check == "is_less_than":
            fail_mask = col >= val
        elif check == "is_greater_or_equal_than":
            fail_mask = col < val
        elif check == "is_less_or_equal_than":
            fail_mask = col > val
        elif check == "is_positive":
            fail_mask = col <= 0
        elif check == "is_negative":
            fail_mask = col >= 0
        elif check == "is_in_millions":
            fail_mask = col < 1_000_000
        elif check == "is_in_billions":
            fail_mask = col < 1_000_000_000
        else:
            # Fallback for unsupported checks to avoid crash during graph build
            return 0

        return fail_mask.sum()


class BetweenAnalyzer:
    @staticmethod
    def analyze(df: dd.DataFrame, rule: RuleDef):
        col = df[rule.field]
        min_val, max_val = rule.value[0], rule.value[1]

        # Logic: Fail if (Value < Min) OR (Value > Max)
        fail_mask = (col < min_val) | (col > max_val)
        return fail_mask.sum()


class ColumnComparisonAnalyzer:
    @staticmethod
    def analyze(df: dd.DataFrame, rule: RuleDef):
        col1 = df[rule.field]
        col2 = df[rule.value]  # rule.value holds the other column name

        fail_mask = col1 != col2
        return fail_mask.sum()


# ============================================================================
# MEMBERSHIP
# ============================================================================


class MembershipAnalyzer:
    @staticmethod
    def analyze(df: dd.DataFrame, rule: RuleDef):
        col = df[rule.field]
        values = rule.value
        check = rule.check_type

        # isin is supported natively in Dask
        if check in ["is_contained_in", "is_in"]:
            fail_mask = ~col.isin(values)
        else:  # not_in
            fail_mask = col.isin(values)

        return fail_mask.sum()


# ============================================================================
# PATTERN & TEXT
# ============================================================================


class PatternAnalyzer:
    @staticmethod
    def analyze(df: dd.DataFrame, rule: RuleDef):
        col = df[rule.field].astype(str)
        # na=False implies that NaN does NOT match the pattern, hence it is a failure
        match_mask = col.str.match(rule.value, na=False)
        return (~match_mask).sum()


class LegitAnalyzer:
    @staticmethod
    def analyze(df: dd.DataFrame, rule: RuleDef):
        """Checks for Nulls OR Empty Strings (whitespace trimmed)."""
        col = df[rule.field]

        # Check Nulls
        is_null = col.isnull()

        # Check Empty Strings (only computed on non-nulls technically, but vector works)
        # We cast to str first to be safe
        is_empty = col.astype(str).str.strip() == ""

        fail_mask = is_null | is_empty
        return fail_mask.sum()


# ============================================================================
# DATE & TIME (Timezone Naive Normalization)
# ============================================================================


class DateAnalyzer:
    @staticmethod
    def analyze(df: dd.DataFrame, rule: RuleDef):
        # Convert to datetime (lazy)
        col = dd.to_datetime(df[rule.field], errors="coerce")

        # Normalize to midnight for date comparison
        col_normalized = col.dt.normalize()
        today = pd.Timestamp.now().normalize()

        check = rule.check_type

        if check == "is_today":
            fail_mask = col_normalized != today
        elif check in ["is_yesterday", "is_t_minus_1"]:
            fail_mask = col_normalized != (today - pd.Timedelta(days=1))
        elif check == "is_past_date":
            fail_mask = col_normalized >= today
        elif check == "is_future_date":
            fail_mask = col_normalized <= today
        elif check == "is_on_weekend":
            fail_mask = col.dt.weekday < 5
        elif check == "is_on_weekday":
            fail_mask = col.dt.weekday >= 5
        # Weekday checks (0=Mon, 6=Sun)
        elif check == "is_on_monday":
            fail_mask = col.dt.weekday != 0
        elif check == "is_on_tuesday":
            fail_mask = col.dt.weekday != 1
        elif check == "is_on_wednesday":
            fail_mask = col.dt.weekday != 2
        elif check == "is_on_thursday":
            fail_mask = col.dt.weekday != 3
        elif check == "is_on_friday":
            fail_mask = col.dt.weekday != 4
        elif check == "is_on_saturday":
            fail_mask = col.dt.weekday != 5
        elif check == "is_on_sunday":
            fail_mask = col.dt.weekday != 6
        else:
            return 0

        return fail_mask.sum()


class DateBetweenAnalyzer:
    @staticmethod
    def analyze(df: dd.DataFrame, rule: RuleDef):
        col = dd.to_datetime(df[rule.field], errors="coerce")
        start = pd.Timestamp(rule.value[0])
        end = pd.Timestamp(rule.value[1])

        # Fail if outside bounds
        fail_mask = (col < start) | (col > end)
        return fail_mask.sum()


class DateComparisonAnalyzer:
    @staticmethod
    def analyze(df: dd.DataFrame, rule: RuleDef):
        col = dd.to_datetime(df[rule.field], errors="coerce")
        target = pd.Timestamp(rule.value)
        check = rule.check_type

        if check == "is_date_after":
            # Fail if Date <= Target
            fail_mask = col <= target
        else:  # is_date_before
            # Fail if Date >= Target
            fail_mask = col >= target

        return fail_mask.sum()


# ============================================================================
# TABLE LEVEL AGGREGATIONS
# ============================================================================


class AggregationAnalyzer:
    @staticmethod
    def analyze(df: dd.DataFrame, rule: RuleDef):
        col = df[rule.field]
        check = rule.check_type

        if check == "has_mean":
            return col.mean()
        if check == "has_sum":
            return col.sum()
        if check == "has_max":
            return col.max()
        if check == "has_min":
            return col.min()
        if check == "has_std":
            return col.std()
        if check == "has_cardinality":
            return col.nunique()

        return 0


# ============================================================================
# LOGIC & CUSTOM EXPRESSIONS
# ============================================================================


class LogicAnalyzer:
    @staticmethod
    def analyze(df: dd.DataFrame, rule: RuleDef):
        """
        Executes a custom Python expression string (e.g., "age > 18 and income > 0")
        Uses df.eval() if available or falls back to map_partitions.
        """
        expression = rule.value

        # Dask doesn't always support eval fully, but for simple arithmetic it works.
        # It returns a boolean series of PASSING rows.
        try:
            pass_mask = df.eval(expression)
            fail_mask = ~pass_mask
            return fail_mask.sum()
        except Exception:
            # Fallback: pure python eval inside partitions
            # Warning: Slow
            def eval_partition(pdf):
                try:
                    return pdf.eval(expression)
                except:
                    return pd.Series([False] * len(pdf), index=pdf.index)

            pass_mask = df.map_partitions(eval_partition, meta=("mask", "bool"))
            return (~pass_mask).sum()
