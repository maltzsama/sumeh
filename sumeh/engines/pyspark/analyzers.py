"""
PySpark analyzers using Column API (zero UDFs) - COMPLETE.

Pure JVM operations for maximum performance.
All 48+ validation rules implemented.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from sumeh.core.models.metrics import MetricResult
from sumeh.core.rules.rule_model import RuleDefinition


# COMPLETENESS ANALYZERS
class CompletenessAnalyzer:
    @staticmethod
    def analyze(df: DataFrame, rule: RuleDefinition) -> MetricResult:
        """Analyze completeness using PySpark Column API."""
        field = rule.field

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")

        # Compute metrics in single pass
        result = df.agg(
            F.count(F.lit(1)).alias("total"),
            F.sum(F.when(F.col(field).isNull(), 1).otherwise(0)).alias("null_count"),
        ).collect()[0]

        total = result["total"]
        null_count = result["null_count"]
        completeness_rate = (total - null_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="completeness",
            field=field,
            value=completeness_rate,
            total_rows=total,
            affected_row_ids=[],
            metadata={"null_count": null_count, "total_count": total},
        )


class MultiFieldCompletenessAnalyzer:
    @staticmethod
    def analyze(df: DataFrame, rule: RuleDefinition) -> MetricResult:
        """Multi-field completeness using PySpark."""
        fields = rule.field if isinstance(rule.field, list) else [rule.field]

        for field in fields:
            if field not in df.columns:
                raise KeyError(f"Field '{field}' not found")

        # Row has incomplete data if ANY field is null
        any_null_condition = F.coalesce(*[F.col(f).isNull() for f in fields])

        result = df.agg(
            F.count(F.lit(1)).alias("total"),
            F.sum(F.when(any_null_condition, 1).otherwise(0)).alias("incomplete_count"),
        ).collect()[0]

        total = result["total"]
        incomplete_count = result["incomplete_count"]
        completeness_rate = (total - incomplete_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="multi_field_completeness",
            field=",".join(fields),
            value=completeness_rate,
            total_rows=total,
            affected_row_ids=[],
            metadata={
                "incomplete_count": incomplete_count,
                "total_count": total,
                "fields": fields,
            },
        )


# UNIQUENESS ANALYZERS
class UniquenessAnalyzer:
    @staticmethod
    def analyze(df: DataFrame, rule: RuleDefinition) -> MetricResult:
        """Uniqueness using window functions (no UDFs)."""
        field = rule.field

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")

        total = df.count()

        # Find duplicates using aggregation
        duplicate_count = (
            df.groupBy(field)
            .count()
            .filter(F.col("count") > 1)
            .agg(F.sum("count").alias("dup_count"))
            .collect()[0]["dup_count"]
            or 0
        )

        uniqueness_rate = (total - duplicate_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="uniqueness",
            field=field,
            value=uniqueness_rate,
            total_rows=total,
            affected_row_ids=[],
            metadata={"duplicate_count": duplicate_count, "total_count": total},
        )


class MultiFieldUniquenessAnalyzer:
    @staticmethod
    def analyze(df: DataFrame, rule: RuleDefinition) -> MetricResult:
        """Multi-field uniqueness (composite key)."""
        fields = rule.field if isinstance(rule.field, list) else [rule.field]

        for field in fields:
            if field not in df.columns:
                raise KeyError(f"Field '{field}' not found")

        total = df.count()

        # Find duplicate combinations
        duplicate_count = (
            df.groupBy(*fields)
            .count()
            .filter(F.col("count") > 1)
            .agg(F.sum("count").alias("dup_count"))
            .collect()[0]["dup_count"]
            or 0
        )

        uniqueness_rate = (total - duplicate_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="multi_field_uniqueness",
            field=",".join(fields),
            value=uniqueness_rate,
            total_rows=total,
            affected_row_ids=[],
            metadata={
                "duplicate_count": duplicate_count,
                "total_count": total,
                "fields": fields,
            },
        )


# COMPARISON ANALYZERS
class ComparisonAnalyzer:
    @staticmethod
    def analyze(df: DataFrame, rule: RuleDefinition) -> MetricResult:
        """Comparison checks using Column API."""
        field = rule.field
        check_type = rule.check_type
        threshold = rule.value

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")

        # Build condition
        if check_type == "is_equal":
            fail_condition = F.col(field) != threshold
        elif check_type == "is_greater_than":
            fail_condition = F.col(field) <= threshold
        elif check_type == "is_less_than":
            fail_condition = F.col(field) >= threshold
        elif check_type == "is_greater_or_equal_than":
            fail_condition = F.col(field) < threshold
        elif check_type == "is_less_or_equal_than":
            fail_condition = F.col(field) > threshold
        elif check_type == "is_positive":
            fail_condition = F.col(field) <= 0
        elif check_type == "is_negative":
            fail_condition = F.col(field) >= 0
        elif check_type == "is_in_millions":
            fail_condition = F.col(field) < 1_000_000
        elif check_type == "is_in_billions":
            fail_condition = F.col(field) < 1_000_000_000
        else:
            raise ValueError(f"Unknown comparison: {check_type}")

        result = df.agg(
            F.count(F.lit(1)).alias("total"),
            F.sum(F.when(fail_condition, 1).otherwise(0)).alias("fail_count"),
        ).collect()[0]

        total = result["total"]
        fail_count = result["fail_count"]
        pass_rate = (total - fail_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="comparison",
            field=field,
            value=pass_rate,
            total_rows=total,
            affected_row_ids=[],
            metadata={
                "fail_count": fail_count,
                "total_count": total,
                "threshold": threshold,
            },
        )


class BetweenAnalyzer:
    @staticmethod
    def analyze(df: DataFrame, rule: RuleDefinition) -> MetricResult:
        """Between check using Column API."""
        field = rule.field
        bounds = rule.value

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")
        if not isinstance(bounds, list) or len(bounds) != 2:
            raise ValueError("is_between requires value=[min, max]")

        min_val, max_val = bounds
        fail_condition = (F.col(field) < min_val) | (F.col(field) > max_val)

        result = df.agg(
            F.count(F.lit(1)).alias("total"),
            F.sum(F.when(fail_condition, 1).otherwise(0)).alias("fail_count"),
        ).collect()[0]

        total = result["total"]
        fail_count = result["fail_count"]
        pass_rate = (total - fail_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="between",
            field=field,
            value=pass_rate,
            total_rows=total,
            affected_row_ids=[],
            metadata={
                "fail_count": fail_count,
                "total_count": total,
                "min": min_val,
                "max": max_val,
            },
        )


class ColumnComparisonAnalyzer:
    @staticmethod
    def analyze(df: DataFrame, rule: RuleDefinition) -> MetricResult:
        """Column-to-column comparison."""
        field = rule.field
        other_field = rule.value

        if field not in df.columns or other_field not in df.columns:
            raise KeyError(f"Fields not found")

        fail_condition = F.col(field) != F.col(other_field)

        result = df.agg(
            F.count(F.lit(1)).alias("total"),
            F.sum(F.when(fail_condition, 1).otherwise(0)).alias("fail_count"),
        ).collect()[0]

        total = result["total"]
        fail_count = result["fail_count"]
        pass_rate = (total - fail_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="column_comparison",
            field=field,
            value=pass_rate,
            total_rows=total,
            affected_row_ids=[],
            metadata={
                "fail_count": fail_count,
                "total_count": total,
                "compared_to": other_field,
            },
        )


# MEMBERSHIP ANALYZERS
class MembershipAnalyzer:
    @staticmethod
    def analyze(df: DataFrame, rule: RuleDefinition) -> MetricResult:
        """Membership check using isin()."""
        field = rule.field
        allowed_values = rule.value
        check_type = rule.check_type

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")
        if not isinstance(allowed_values, list):
            raise ValueError("Membership requires list of values")

        if check_type in ["is_contained_in", "is_in"]:
            fail_condition = ~F.col(field).isin(allowed_values)
        else:  # not_contained_in, not_in
            fail_condition = F.col(field).isin(allowed_values)

        result = df.agg(
            F.count(F.lit(1)).alias("total"),
            F.sum(F.when(fail_condition, 1).otherwise(0)).alias("fail_count"),
        ).collect()[0]

        total = result["total"]
        fail_count = result["fail_count"]
        pass_rate = (total - fail_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="membership",
            field=field,
            value=pass_rate,
            total_rows=total,
            affected_row_ids=[],
            metadata={
                "fail_count": fail_count,
                "total_count": total,
                "allowed_values": allowed_values,
            },
        )


# PATTERN ANALYZERS
class PatternAnalyzer:
    @staticmethod
    def analyze(df: DataFrame, rule: RuleDefinition) -> MetricResult:
        """Pattern matching using rlike()."""
        field = rule.field
        pattern = rule.value

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")
        if not pattern:
            raise ValueError("Pattern required")

        # PySpark regex: rlike()
        fail_condition = ~F.col(field).rlike(pattern)

        result = df.agg(
            F.count(F.lit(1)).alias("total"),
            F.sum(F.when(fail_condition, 1).otherwise(0)).alias("fail_count"),
        ).collect()[0]

        total = result["total"]
        fail_count = result["fail_count"]
        pass_rate = (total - fail_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="pattern",
            field=field,
            value=pass_rate,
            total_rows=total,
            affected_row_ids=[],
            metadata={
                "fail_count": fail_count,
                "total_count": total,
                "pattern": pattern,
            },
        )


class LegitAnalyzer:
    @staticmethod
    def analyze(df: DataFrame, rule: RuleDefinition) -> MetricResult:
        """Check for null or empty strings."""
        field = rule.field

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")

        fail_condition = F.col(field).isNull() | (F.trim(F.col(field)) == "")

        result = df.agg(
            F.count(F.lit(1)).alias("total"),
            F.sum(F.when(fail_condition, 1).otherwise(0)).alias("fail_count"),
        ).collect()[0]

        total = result["total"]
        fail_count = result["fail_count"]
        pass_rate = (total - fail_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="legit",
            field=field,
            value=pass_rate,
            total_rows=total,
            affected_row_ids=[],
            metadata={"fail_count": fail_count, "total_count": total},
        )


# DATE ANALYZERS
class DateAnalyzer:
    @staticmethod
    def analyze(df: DataFrame, rule: RuleDefinition) -> MetricResult:
        """Date validations using PySpark date functions."""
        field = rule.field
        check_type = rule.check_type

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")

        # Convert to date if needed
        date_col = F.to_date(F.col(field))
        today = F.current_date()

        # Build condition
        if check_type == "is_today":
            fail_condition = date_col != today
        elif check_type in ["is_t_minus_1", "is_yesterday"]:
            fail_condition = date_col != F.date_sub(today, 1)
        elif check_type == "is_t_minus_2":
            fail_condition = date_col != F.date_sub(today, 2)
        elif check_type == "is_t_minus_3":
            fail_condition = date_col != F.date_sub(today, 3)
        elif check_type == "is_past_date":
            fail_condition = date_col >= today
        elif check_type == "is_future_date":
            fail_condition = date_col <= today
        elif check_type == "is_on_weekday":
            fail_condition = F.dayofweek(date_col).isin([1, 7])  # Sunday=1, Saturday=7
        elif check_type == "is_on_weekend":
            fail_condition = ~F.dayofweek(date_col).isin([1, 7])
        elif check_type == "is_on_monday":
            fail_condition = F.dayofweek(date_col) != 2
        elif check_type == "is_on_tuesday":
            fail_condition = F.dayofweek(date_col) != 3
        elif check_type == "is_on_wednesday":
            fail_condition = F.dayofweek(date_col) != 4
        elif check_type == "is_on_thursday":
            fail_condition = F.dayofweek(date_col) != 5
        elif check_type == "is_on_friday":
            fail_condition = F.dayofweek(date_col) != 6
        elif check_type == "is_on_saturday":
            fail_condition = F.dayofweek(date_col) != 7
        elif check_type == "is_on_sunday":
            fail_condition = F.dayofweek(date_col) != 1
        else:
            raise ValueError(f"Unknown date check: {check_type}")

        result = df.agg(
            F.count(F.lit(1)).alias("total"),
            F.sum(F.when(fail_condition, 1).otherwise(0)).alias("fail_count"),
        ).collect()[0]

        total = result["total"]
        fail_count = result["fail_count"]
        pass_rate = (total - fail_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="date",
            field=field,
            value=pass_rate,
            total_rows=total,
            affected_row_ids=[],
            metadata={"fail_count": fail_count, "total_count": total},
        )


class DateBetweenAnalyzer:
    @staticmethod
    def analyze(df: DataFrame, rule: RuleDefinition) -> MetricResult:
        """Date between check."""
        field = rule.field
        bounds = rule.value

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")
        if not isinstance(bounds, list) or len(bounds) != 2:
            raise ValueError("is_date_between requires [start, end]")

        date_col = F.to_date(F.col(field))
        start = F.to_date(F.lit(bounds[0]))
        end = F.to_date(F.lit(bounds[1]))

        fail_condition = (date_col < start) | (date_col > end)

        result = df.agg(
            F.count(F.lit(1)).alias("total"),
            F.sum(F.when(fail_condition, 1).otherwise(0)).alias("fail_count"),
        ).collect()[0]

        total = result["total"]
        fail_count = result["fail_count"]
        pass_rate = (total - fail_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="date_between",
            field=field,
            value=pass_rate,
            total_rows=total,
            affected_row_ids=[],
            metadata={
                "fail_count": fail_count,
                "total_count": total,
                "start": bounds[0],
                "end": bounds[1],
            },
        )


class DateComparisonAnalyzer:
    @staticmethod
    def analyze(df: DataFrame, rule: RuleDefinition) -> MetricResult:
        """Date comparison (before/after)."""
        field = rule.field
        check_type = rule.check_type
        target = rule.value

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")

        date_col = F.to_date(F.col(field))
        target_date = F.to_date(F.lit(target))

        if check_type == "is_date_after":
            fail_condition = date_col <= target_date
        else:  # is_date_before
            fail_condition = date_col >= target_date

        result = df.agg(
            F.count(F.lit(1)).alias("total"),
            F.sum(F.when(fail_condition, 1).otherwise(0)).alias("fail_count"),
        ).collect()[0]

        total = result["total"]
        fail_count = result["fail_count"]
        pass_rate = (total - fail_count) / total if total > 0 else 1.0

        return MetricResult(
            metric_type="date_comparison",
            field=field,
            value=pass_rate,
            total_rows=total,
            affected_row_ids=[],
            metadata={"fail_count": fail_count, "total_count": total, "target": target},
        )


# TABLE-LEVEL AGGREGATION ANALYZERS
class AggregationAnalyzer:
    @staticmethod
    def analyze(df: DataFrame, rule: RuleDefinition) -> MetricResult:
        """Table-level aggregations."""
        field = rule.field
        check_type = rule.check_type

        if field not in df.columns:
            raise KeyError(f"Field '{field}' not found")

        if check_type == "has_min":
            actual = df.agg(F.min(field).alias("value")).collect()[0]["value"]
        elif check_type == "has_max":
            actual = df.agg(F.max(field).alias("value")).collect()[0]["value"]
        elif check_type == "has_sum":
            actual = df.agg(F.sum(field).alias("value")).collect()[0]["value"]
        elif check_type == "has_mean":
            actual = df.agg(F.mean(field).alias("value")).collect()[0]["value"]
        elif check_type == "has_std":
            actual = df.agg(F.stddev(field).alias("value")).collect()[0]["value"]
        elif check_type == "has_cardinality":
            actual = df.agg(F.countDistinct(field).alias("value")).collect()[0]["value"]
        else:
            raise ValueError(f"Unknown aggregation: {check_type}")

        return MetricResult(
            metric_type="aggregation",
            field=field,
            value=float(actual) if actual is not None else 0.0,
            total_rows=df.count(),
            affected_row_ids=[],
            metadata={
                "metric": check_type,
                "value": float(actual) if actual is not None else 0.0,
            },
        )
