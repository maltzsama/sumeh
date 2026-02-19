"""
Pandas-specific analyzers - COMPLETE IMPLEMENTATION.

All row-level + table-level analyzers for pandas engine.
"""

import numpy as np
import pandas as pd

from sumeh.core.models import MetricResult
from sumeh.core.rules.rule_model import RuleDef


# ============================================================================
# COMPLETENESS ANALYZERS
# ============================================================================


class CompletenessAnalyzer:
    @staticmethod
    def analyze(df: pd.DataFrame, rule: RuleDef) -> MetricResult:
        field = rule.field
        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")

        total = len(df)
        null_mask = df[field].isna()
        null_count = int(null_mask.sum())
        null_row_ids = df.index[null_mask].tolist()
        completeness_rate = (total - null_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="completeness",
            field=field,
            value=completeness_rate,
            total_rows=total,
            affected_row_ids=null_row_ids,
            metadata={"null_count": null_count, "total_count": total},
        )


class MultiFieldCompletenessAnalyzer:
    @staticmethod
    def analyze(df: pd.DataFrame, rule: RuleDef) -> MetricResult:
        fields = rule.field if isinstance(rule.field, list) else [rule.field]
        for field in fields:
            if field not in df.columns:
                raise KeyError(f"Field '{field}' not found")

        total = len(df)
        any_null_mask = df[fields].isna().any(axis=1)
        incomplete_count = int(any_null_mask.sum())
        incomplete_row_ids = df.index[any_null_mask].tolist()
        completeness_rate = (total - incomplete_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="multi_field_completeness",
            field=",".join(fields),
            value=completeness_rate,
            total_rows=total,
            affected_row_ids=incomplete_row_ids,
            metadata={
                "incomplete_count": incomplete_count,
                "total_count": total,
                "fields": fields,
            },
        )


# ============================================================================
# UNIQUENESS ANALYZERS
# ============================================================================


class UniquenessAnalyzer:
    @staticmethod
    def analyze(df: pd.DataFrame, rule: RuleDef) -> MetricResult:
        field = rule.field
        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")

        total = len(df)
        duplicate_mask = df[field].duplicated(keep=False)
        duplicate_count = int(duplicate_mask.sum())
        duplicate_row_ids = df.index[duplicate_mask].tolist()
        uniqueness_rate = (total - duplicate_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="uniqueness",
            field=field,
            value=uniqueness_rate,
            total_rows=total,
            affected_row_ids=duplicate_row_ids,
            metadata={"duplicate_count": duplicate_count, "total_count": total},
        )


class MultiFieldUniquenessAnalyzer:
    @staticmethod
    def analyze(df: pd.DataFrame, rule: RuleDef) -> MetricResult:
        fields = rule.field if isinstance(rule.field, list) else [rule.field]
        for field in fields:
            if field not in df.columns:
                raise KeyError(f"Field '{field}' not found")

        total = len(df)
        duplicate_mask = df[fields].duplicated(keep=False)
        duplicate_count = int(duplicate_mask.sum())
        duplicate_row_ids = df.index[duplicate_mask].tolist()
        uniqueness_rate = (total - duplicate_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="multi_field_uniqueness",
            field=",".join(fields),
            value=uniqueness_rate,
            total_rows=total,
            affected_row_ids=duplicate_row_ids,
            metadata={
                "duplicate_count": duplicate_count,
                "total_count": total,
                "fields": fields,
            },
        )


# ============================================================================
# COMPARISON ANALYZERS
# ============================================================================


class ComparisonAnalyzer:
    @staticmethod
    def analyze(df: pd.DataFrame, rule: RuleDef) -> MetricResult:
        field = rule.field
        check_type = rule.check_type
        threshold = rule.value

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")

        total = len(df)

        # Determine comparison
        if check_type == "is_equal":
            fail_mask = df[field] != threshold
        elif check_type == "is_greater_than":
            fail_mask = df[field] <= threshold
        elif check_type == "is_less_than":
            fail_mask = df[field] >= threshold
        elif check_type == "is_greater_or_equal_than":
            fail_mask = df[field] < threshold
        elif check_type == "is_less_or_equal_than":
            fail_mask = df[field] > threshold
        elif check_type == "is_positive":
            fail_mask = df[field] <= 0
        elif check_type == "is_negative":
            fail_mask = df[field] >= 0
        elif check_type == "is_in_millions":
            fail_mask = df[field] < 1_000_000
        elif check_type == "is_in_billions":
            fail_mask = df[field] < 1_000_000_000
        else:
            raise ValueError(f"Unknown comparison: {check_type}")

        fail_count = int(fail_mask.sum())
        fail_row_ids = df.index[fail_mask].tolist()
        pass_rate = (total - fail_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="comparison",
            field=field,
            value=pass_rate,
            total_rows=total,
            affected_row_ids=fail_row_ids,
            metadata={
                "fail_count": fail_count,
                "total_count": total,
                "threshold": threshold,
            },
        )


class BetweenAnalyzer:
    @staticmethod
    def analyze(df: pd.DataFrame, rule: RuleDef) -> MetricResult:
        field = rule.field
        bounds = rule.value

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")
        if not isinstance(bounds, list) or len(bounds) != 2:
            raise ValueError("is_between requires value=[min, max]")

        min_val, max_val = bounds
        total = len(df)
        fail_mask = ~df[field].between(min_val, max_val)
        fail_count = int(fail_mask.sum())
        fail_row_ids = df.index[fail_mask].tolist()
        pass_rate = (total - fail_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="between",
            field=field,
            value=pass_rate,
            total_rows=total,
            affected_row_ids=fail_row_ids,
            metadata={
                "fail_count": fail_count,
                "total_count": total,
                "min": min_val,
                "max": max_val,
            },
        )


class ColumnComparisonAnalyzer:
    @staticmethod
    def analyze(df: pd.DataFrame, rule: RuleDef) -> MetricResult:
        field = rule.field
        other_field = rule.value

        if field not in df.columns or other_field not in df.columns:
            raise KeyError(f"Fields not found")

        total = len(df)
        fail_mask = df[field] != df[other_field]
        fail_count = int(fail_mask.sum())
        fail_row_ids = df.index[fail_mask].tolist()
        pass_rate = (total - fail_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="column_comparison",
            field=field,
            value=pass_rate,
            total_rows=total,
            affected_row_ids=fail_row_ids,
            metadata={
                "fail_count": fail_count,
                "total_count": total,
                "compared_to": other_field,
            },
        )


# ============================================================================
# MEMBERSHIP ANALYZERS
# ============================================================================


class MembershipAnalyzer:
    @staticmethod
    def analyze(df: pd.DataFrame, rule: RuleDef) -> MetricResult:
        field = rule.field
        allowed_values = rule.value
        check_type = rule.check_type

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")
        if not isinstance(allowed_values, list):
            raise ValueError("Membership check requires list of values")

        total = len(df)

        if check_type in ["is_contained_in", "is_in"]:
            fail_mask = ~df[field].isin(allowed_values)
        else:  # not_contained_in, not_in
            fail_mask = df[field].isin(allowed_values)

        fail_count = int(fail_mask.sum())
        fail_row_ids = df.index[fail_mask].tolist()
        pass_rate = (total - fail_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="membership",
            field=field,
            value=pass_rate,
            total_rows=total,
            affected_row_ids=fail_row_ids,
            metadata={
                "fail_count": fail_count,
                "total_count": total,
                "allowed_values": allowed_values,
            },
        )


# ============================================================================
# PATTERN ANALYZERS
# ============================================================================


class PatternAnalyzer:
    @staticmethod
    def analyze(df: pd.DataFrame, rule: RuleDef) -> MetricResult:
        field = rule.field
        pattern = rule.value

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")
        if not pattern:
            raise ValueError("Pattern required")

        total = len(df)
        fail_mask = ~df[field].astype(str).str.match(pattern, na=False)
        fail_count = int(fail_mask.sum())
        fail_row_ids = df.index[fail_mask].tolist()
        pass_rate = (total - fail_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="pattern",
            field=field,
            value=pass_rate,
            total_rows=total,
            affected_row_ids=fail_row_ids,
            metadata={
                "fail_count": fail_count,
                "total_count": total,
                "pattern": pattern,
            },
        )


class LegitAnalyzer:
    @staticmethod
    def analyze(df: pd.DataFrame, rule: RuleDef) -> MetricResult:
        field = rule.field

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")

        total = len(df)
        fail_mask = df[field].isna() | (df[field].astype(str).str.strip() == "")
        fail_count = int(fail_mask.sum())
        fail_row_ids = df.index[fail_mask].tolist()
        pass_rate = (total - fail_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="legit",
            field=field,
            value=pass_rate,
            total_rows=total,
            affected_row_ids=fail_row_ids,
            metadata={"fail_count": fail_count, "total_count": total},
        )


# ============================================================================
# DATE ANALYZERS
# ============================================================================


class DateAnalyzer:
    @staticmethod
    def analyze(df: pd.DataFrame, rule: RuleDef) -> MetricResult:
        field = rule.field
        check_type = rule.check_type

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")

        # Convert to datetime
        dates = pd.to_datetime(df[field], errors="coerce")
        today = pd.Timestamp.now().normalize()

        total = len(df)

        if check_type == "is_today":
            fail_mask = dates.dt.normalize() != today
        elif check_type in ["is_t_minus_1", "is_yesterday"]:
            fail_mask = dates.dt.normalize() != (today - pd.Timedelta(days=1))
        elif check_type == "is_t_minus_2":
            fail_mask = dates.dt.normalize() != (today - pd.Timedelta(days=2))
        elif check_type == "is_t_minus_3":
            fail_mask = dates.dt.normalize() != (today - pd.Timedelta(days=3))
        elif check_type == "is_past_date":
            fail_mask = dates.dt.normalize() >= today
        elif check_type == "is_future_date":
            fail_mask = dates.dt.normalize() <= today
        elif check_type == "is_on_weekday":
            fail_mask = dates.dt.weekday >= 5
        elif check_type == "is_on_weekend":
            fail_mask = dates.dt.weekday < 5
        elif check_type == "is_on_monday":
            fail_mask = dates.dt.weekday != 0
        elif check_type == "is_on_tuesday":
            fail_mask = dates.dt.weekday != 1
        elif check_type == "is_on_wednesday":
            fail_mask = dates.dt.weekday != 2
        elif check_type == "is_on_thursday":
            fail_mask = dates.dt.weekday != 3
        elif check_type == "is_on_friday":
            fail_mask = dates.dt.weekday != 4
        elif check_type == "is_on_saturday":
            fail_mask = dates.dt.weekday != 5
        elif check_type == "is_on_sunday":
            fail_mask = dates.dt.weekday != 6
        else:
            raise ValueError(f"Unknown date check: {check_type}")

        fail_count = int(fail_mask.sum())
        fail_row_ids = df.index[fail_mask].tolist()
        pass_rate = (total - fail_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="date",
            field=field,
            value=pass_rate,
            total_rows=total,
            affected_row_ids=fail_row_ids,
            metadata={"fail_count": fail_count, "total_count": total},
        )


class DateBetweenAnalyzer:
    @staticmethod
    def analyze(df: pd.DataFrame, rule: RuleDef) -> MetricResult:
        field = rule.field
        bounds = rule.value

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")
        if not isinstance(bounds, list) or len(bounds) != 2:
            raise ValueError("is_date_between requires [start, end]")

        dates = pd.to_datetime(df[field], errors="coerce")
        start = pd.to_datetime(bounds[0])
        end = pd.to_datetime(bounds[1])

        total = len(df)
        fail_mask = ~dates.between(start, end)
        fail_count = int(fail_mask.sum())
        fail_row_ids = df.index[fail_mask].tolist()
        pass_rate = (total - fail_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="date_between",
            field=field,
            value=pass_rate,
            total_rows=total,
            affected_row_ids=fail_row_ids,
            metadata={
                "fail_count": fail_count,
                "total_count": total,
                "start": str(start),
                "end": str(end),
            },
        )


class DateComparisonAnalyzer:
    @staticmethod
    def analyze(df: pd.DataFrame, rule: RuleDef) -> MetricResult:
        field = rule.field
        check_type = rule.check_type
        target_date = pd.to_datetime(rule.value)

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")

        dates = pd.to_datetime(df[field], errors="coerce")
        total = len(df)

        if check_type == "is_date_after":
            fail_mask = dates <= target_date
        else:  # is_date_before
            fail_mask = dates >= target_date

        fail_count = int(fail_mask.sum())
        fail_row_ids = df.index[fail_mask].tolist()
        pass_rate = (total - fail_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="date_comparison",
            field=field,
            value=pass_rate,
            total_rows=total,
            affected_row_ids=fail_row_ids,
            metadata={
                "fail_count": fail_count,
                "total_count": total,
                "target": str(target_date),
            },
        )


# ============================================================================
# TABLE-LEVEL ANALYZERS
# ============================================================================


class AggregationAnalyzer:
    @staticmethod
    def analyze(df: pd.DataFrame, rule: RuleDef) -> MetricResult:
        field = rule.field
        check_type = rule.check_type

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")

        if check_type == "has_min":
            actual = df[field].min()
        elif check_type == "has_max":
            actual = df[field].max()
        elif check_type == "has_sum":
            actual = df[field].sum()
        elif check_type == "has_mean":
            actual = df[field].mean()
        elif check_type == "has_std":
            actual = df[field].std()
        elif check_type == "has_cardinality":
            actual = df[field].nunique()
        else:
            raise ValueError(f"Unknown aggregation: {check_type}")

        return MetricResult(
            metric_type="aggregation",
            field=field,
            value=float(actual),
            total_rows=len(df),
            affected_row_ids=[],
            metadata={"metric": check_type, "value": float(actual)},
        )
