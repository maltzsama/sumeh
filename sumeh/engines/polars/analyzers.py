"""
Polars-specific analyzers - COMPLETE IMPLEMENTATION.

All row-level + table-level analyzers for Polars engine.
"""

from datetime import datetime

import polars as pl

from sumeh.core.models.metrics import MetricResult
from sumeh.core.rules.rule_model import RuleDefinition

# ============================================================================
# COMPLETENESS ANALYZERS
# ============================================================================


class CompletenessAnalyzer:
    @staticmethod
    def analyze(df: pl.DataFrame, rule: RuleDefinition) -> MetricResult:
        field = rule.field
        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")

        total = len(df)
        null_count = df.select(pl.col(field).is_null().sum()).item()
        null_row_ids = (
            df.with_row_count()
            .filter(pl.col(field).is_null())
            .select("row_nr")
            .to_series()
            .to_list()
        )

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
    def analyze(df: pl.DataFrame, rule: RuleDefinition) -> MetricResult:
        fields = rule.field if isinstance(rule.field, list) else [rule.field]

        for field in fields:
            if field not in df.columns:
                raise KeyError(f"Field '{field}' not found")

        total = len(df)

        # Row has incomplete data if ANY field is null
        any_null = pl.any_horizontal([pl.col(f).is_null() for f in fields])
        incomplete_count = df.select(any_null.sum()).item()
        incomplete_row_ids = (
            df.with_row_count().filter(any_null).select("row_nr").to_series().to_list()
        )

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
    def analyze(df: pl.DataFrame, rule: RuleDefinition) -> MetricResult:
        field = rule.field
        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")

        total = len(df)

        # Find duplicates
        duplicate_mask = df.select(pl.col(field).is_duplicated()).to_series()
        duplicate_count = duplicate_mask.sum()
        duplicate_row_ids = (
            df.with_row_count()
            .filter(duplicate_mask)
            .select("row_nr")
            .to_series()
            .to_list()
        )

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
    def analyze(df: pl.DataFrame, rule: RuleDefinition) -> MetricResult:
        fields = rule.field if isinstance(rule.field, list) else [rule.field]

        for field in fields:
            if field not in df.columns:
                raise KeyError(f"Field '{field}' not found")

        total = len(df)

        # Check duplicates on combination
        duplicate_mask = df.select(pl.struct(fields).is_duplicated()).to_series()
        duplicate_count = duplicate_mask.sum()
        duplicate_row_ids = (
            df.with_row_count()
            .filter(duplicate_mask)
            .select("row_nr")
            .to_series()
            .to_list()
        )

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
    def analyze(df: pl.DataFrame, rule: RuleDefinition) -> MetricResult:
        field = rule.field
        check_type = rule.check_type
        threshold = rule.value

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")

        total = len(df)

        # Build condition based on check_type
        if check_type == "is_equal":
            fail_condition = pl.col(field) != threshold
        elif check_type == "is_greater_than":
            fail_condition = pl.col(field) <= threshold
        elif check_type == "is_less_than":
            fail_condition = pl.col(field) >= threshold
        elif check_type == "is_greater_or_equal_than":
            fail_condition = pl.col(field) < threshold
        elif check_type == "is_less_or_equal_than":
            fail_condition = pl.col(field) > threshold
        elif check_type == "is_positive":
            fail_condition = pl.col(field) <= 0
        elif check_type == "is_negative":
            fail_condition = pl.col(field) >= 0
        elif check_type == "is_in_millions":
            fail_condition = pl.col(field) < 1_000_000
        elif check_type == "is_in_billions":
            fail_condition = pl.col(field) < 1_000_000_000
        else:
            raise ValueError(f"Unknown comparison: {check_type}")

        fail_count = df.select(fail_condition.sum()).item()
        fail_row_ids = (
            df.with_row_count()
            .filter(fail_condition)
            .select("row_nr")
            .to_series()
            .to_list()
        )
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
    def analyze(df: pl.DataFrame, rule: RuleDefinition) -> MetricResult:
        field = rule.field
        bounds = rule.value

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")
        if not isinstance(bounds, list) or len(bounds) != 2:
            raise ValueError("is_between requires value=[min, max]")

        min_val, max_val = bounds
        total = len(df)

        fail_condition = (pl.col(field) < min_val) | (pl.col(field) > max_val)
        fail_count = df.select(fail_condition.sum()).item()
        fail_row_ids = (
            df.with_row_count()
            .filter(fail_condition)
            .select("row_nr")
            .to_series()
            .to_list()
        )
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
    def analyze(df: pl.DataFrame, rule: RuleDefinition) -> MetricResult:
        field = rule.field
        other_field = rule.value

        if field not in df.columns or other_field not in df.columns:
            raise KeyError(f"Fields not found")

        total = len(df)
        fail_condition = pl.col(field) != pl.col(other_field)
        fail_count = df.select(fail_condition.sum()).item()
        fail_row_ids = (
            df.with_row_count()
            .filter(fail_condition)
            .select("row_nr")
            .to_series()
            .to_list()
        )
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
    def analyze(df: pl.DataFrame, rule: RuleDefinition) -> MetricResult:
        field = rule.field
        allowed_values = rule.value
        check_type = rule.check_type

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")
        if not isinstance(allowed_values, list):
            raise ValueError("Membership check requires list of values")

        total = len(df)

        if check_type in ["is_contained_in", "is_in"]:
            fail_condition = ~pl.col(field).is_in(allowed_values)
        else:  # not_contained_in, not_in
            fail_condition = pl.col(field).is_in(allowed_values)

        fail_count = df.select(fail_condition.sum()).item()
        fail_row_ids = (
            df.with_row_count()
            .filter(fail_condition)
            .select("row_nr")
            .to_series()
            .to_list()
        )
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
    def analyze(df: pl.DataFrame, rule: RuleDefinition) -> MetricResult:
        field = rule.field
        pattern = rule.value

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")
        if not pattern:
            raise ValueError("Pattern required")

        total = len(df)

        # Polars regex matching
        fail_condition = ~pl.col(field).str.contains(pattern)
        fail_count = df.select(fail_condition.sum()).item()
        fail_row_ids = (
            df.with_row_count()
            .filter(fail_condition)
            .select("row_nr")
            .to_series()
            .to_list()
        )
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
    def analyze(df: pl.DataFrame, rule: RuleDefinition) -> MetricResult:
        field = rule.field

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")

        total = len(df)
        fail_condition = pl.col(field).is_null() | (
            pl.col(field).cast(pl.Utf8).str.strip_chars() == ""
        )
        fail_count = df.select(fail_condition.sum()).item()
        fail_row_ids = (
            df.with_row_count()
            .filter(fail_condition)
            .select("row_nr")
            .to_series()
            .to_list()
        )
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
    def analyze(df: pl.DataFrame, rule: RuleDefinition) -> MetricResult:
        field = rule.field
        check_type = rule.check_type

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")

        # Convert to datetime and get current date components
        today = datetime.now().date()
        weekday_map = {
            "is_on_monday": 0,
            "is_on_tuesday": 1,
            "is_on_wednesday": 2,
            "is_on_thursday": 3,
            "is_on_friday": 4,
            "is_on_saturday": 5,
            "is_on_sunday": 6,
        }

        total = len(df)

        # Handle different date checks
        if check_type == "is_today":
            fail_condition = pl.col(field).dt.date() != today
        elif check_type == "is_yesterday":
            fail_condition = pl.col(field).dt.date() != (
                today - datetime.timedelta(days=1)
            )
        elif check_type == "is_t_minus_1":
            fail_condition = pl.col(field).dt.date() != (
                today - datetime.timedelta(days=1)
            )
        elif check_type == "is_t_minus_2":
            fail_condition = pl.col(field).dt.date() != (
                today - datetime.timedelta(days=2)
            )
        elif check_type == "is_t_minus_3":
            fail_condition = pl.col(field).dt.date() != (
                today - datetime.timedelta(days=3)
            )
        elif check_type == "is_past_date":
            fail_condition = pl.col(field).dt.date() >= today
        elif check_type == "is_future_date":
            fail_condition = pl.col(field).dt.date() <= today
        elif check_type == "is_on_weekday":
            fail_condition = (
                pl.col(field).dt.weekday().is_in([5, 6])
            )  # Sat/Sun are weekend
        elif check_type == "is_on_weekend":
            fail_condition = (
                pl.col(field).dt.weekday().is_in([0, 1, 2, 3, 4])
            )  # Mon-Fri
        elif check_type in weekday_map:
            fail_condition = pl.col(field).dt.weekday() != weekday_map[check_type]
        else:
            raise ValueError(f"Unknown date check: {check_type}")

        fail_count = df.select(fail_condition.sum()).item()
        fail_row_ids = (
            df.with_row_count()
            .filter(fail_condition)
            .select("row_nr")
            .to_series()
            .to_list()
        )
        pass_rate = (total - fail_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="date",
            field=field,
            value=pass_rate,
            total_rows=total,
            affected_row_ids=fail_row_ids,
            metadata={
                "fail_count": fail_count,
                "total_count": total,
                "check_type": check_type,
            },
        )


class DateBetweenAnalyzer:
    @staticmethod
    def analyze(df: pl.DataFrame, rule: RuleDefinition) -> MetricResult:
        field = rule.field
        bounds = rule.value

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")
        if not isinstance(bounds, list) or len(bounds) != 2:
            raise ValueError("is_date_between requires [start, end]")

        start = datetime.strptime(bounds[0], "%Y-%m-%d").date()
        end = datetime.strptime(bounds[1], "%Y-%m-%d").date()

        total = len(df)
        fail_condition = (pl.col(field).dt.date() < start) | (
            pl.col(field).dt.date() > end
        )
        fail_count = df.select(fail_condition.sum()).item()
        fail_row_ids = (
            df.with_row_count()
            .filter(fail_condition)
            .select("row_nr")
            .to_series()
            .to_list()
        )
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
    def analyze(df: pl.DataFrame, rule: RuleDefinition) -> MetricResult:
        field = rule.field
        check_type = rule.check_type
        target_date = datetime.strptime(rule.value, "%Y-%m-%d").date()

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")

        total = len(df)

        if check_type == "is_date_after":
            fail_condition = pl.col(field).dt.date() <= target_date
        else:  # is_date_before
            fail_condition = pl.col(field).dt.date() >= target_date

        fail_count = df.select(fail_condition.sum()).item()
        fail_row_ids = (
            df.with_row_count()
            .filter(fail_condition)
            .select("row_nr")
            .to_series()
            .to_list()
        )
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
    def analyze(df: pl.DataFrame, rule: RuleDefinition) -> MetricResult:
        field = rule.field
        check_type = rule.check_type

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")

        if check_type == "has_min":
            actual = df.select(pl.col(field).min()).item()
        elif check_type == "has_max":
            actual = df.select(pl.col(field).max()).item()
        elif check_type == "has_sum":
            actual = df.select(pl.col(field).sum()).item()
        elif check_type == "has_mean":
            actual = df.select(pl.col(field).mean()).item()
        elif check_type == "has_std":
            actual = df.select(pl.col(field).std()).item()
        elif check_type == "has_cardinality":
            actual = df.select(pl.col(field).n_unique()).item()
        else:
            raise ValueError(f"Unknown aggregation: {check_type}")

        return MetricResult(
            metric_type="aggregation",
            field=field,
            value=float(actual) if actual is not None else None,
            total_rows=len(df),
            affected_row_ids=[],
            metadata={
                "metric": check_type,
                "value": float(actual) if actual is not None else None,
            },
        )
