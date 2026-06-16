"""
SQLCore Analyzers - COMPLETE IMPLEMENTATION.

Maps business rules to SQLGlot Abstract Syntax Trees (AST).
Achieves parity with Pandas engine features (Dates, Patterns, Multi-field).
"""

import sqlglot
import sqlglot.expressions as exp

from sumeh.core.rules.rule_definition import RuleDefinition

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def _to_col(field: str) -> exp.Column:
    """Safely converts string field to Column expression."""
    return exp.to_column(field)


def _pass_rate(condition: exp.Expression) -> exp.Expression:
    """
    Generates: AVG(CASE WHEN <condition> THEN 1.0 ELSE 0.0 END)
    This calculates the Pass Rate logic used in row-level checks.
    """
    case_expr = exp.Case(
        ifs=[exp.If(this=condition, true=exp.Literal.number(1.0))],
        default=exp.Literal.number(0.0),
    )
    return exp.Avg(this=case_expr)


def _count_all() -> exp.Expression:
    """Generates: COUNT(*)"""
    return exp.Count(this=exp.Star())


# ============================================================================
# COMPLETENESS ANALYZERS
# ============================================================================


class CompletenessAnalyzer:
    @staticmethod
    def analyze(rule: RuleDefinition) -> exp.Expression:
        field = rule.field
        if isinstance(field, list):
            field = field[0]
        col = _to_col(field)
        count_val = exp.Count(this=col)
        count_all = _count_all()
        return exp.Cast(this=count_val, to=exp.DataType.build("double")) / count_all


class MultiFieldCompletenessAnalyzer:
    @staticmethod
    def analyze(rule: RuleDefinition) -> exp.Expression:
        fields = rule.field if isinstance(rule.field, list) else [rule.field]
        # FIX: expression=exp.Null() — not kind=
        conditions = [
            exp.Not(this=exp.Is(this=_to_col(f), expression=exp.Null())) for f in fields
        ]
        final_condition: exp.Expression = conditions[0]
        for c in conditions[1:]:
            final_condition = exp.And(this=final_condition, expression=c)
        return _pass_rate(final_condition)


# ============================================================================
# UNIQUENESS ANALYZERS
# ============================================================================


class UniquenessAnalyzer:
    @staticmethod
    def analyze(rule: RuleDefinition) -> exp.Expression:
        field = rule.field
        if isinstance(field, list):
            field = field[0]
        col = _to_col(field)
        count_distinct: exp.Expression = exp.Count(this=exp.Distinct(expressions=[col]))
        count_all = _count_all()
        return (
            exp.Cast(this=count_distinct, to=exp.DataType.build("double")) / count_all
        )


class MultiFieldUniquenessAnalyzer:
    @staticmethod
    def analyze(rule: RuleDefinition) -> exp.Expression:
        fields = rule.field if isinstance(rule.field, list) else [rule.field]
        cols = [_to_col(f) for f in fields]
        count_distinct: exp.Expression = exp.Count(this=exp.Distinct(expressions=cols))
        count_all = _count_all()
        return (
            exp.Cast(this=count_distinct, to=exp.DataType.build("double")) / count_all
        )


# ============================================================================
# COMPARISON ANALYZERS
# ============================================================================


class ComparisonAnalyzer:
    @staticmethod
    def analyze(rule: RuleDefinition) -> exp.Expression:
        field = rule.field
        if isinstance(field, list):
            field = field[0]
        col = _to_col(field)
        val = exp.convert(rule.value)
        check_type = rule.check_type

        cond: exp.Expression

        if check_type == "is_equal":
            cond = exp.EQ(this=col, expression=val)
        elif check_type == "is_greater_than":
            cond = exp.GT(this=col, expression=val)
        elif check_type == "is_less_than":
            cond = exp.LT(this=col, expression=val)
        elif check_type == "is_greater_or_equal_than":
            cond = exp.GTE(this=col, expression=val)
        elif check_type == "is_less_or_equal_than":
            cond = exp.LTE(this=col, expression=val)
        elif check_type == "is_positive":
            cond = exp.GT(this=col, expression=exp.Literal.number(0))
        elif check_type == "is_negative":
            cond = exp.LT(this=col, expression=exp.Literal.number(0))
        elif check_type == "is_in_millions":
            cond = exp.GTE(this=col, expression=exp.Literal.number(1_000_000))
        elif check_type == "is_in_billions":
            cond = exp.GTE(this=col, expression=exp.Literal.number(1_000_000_000))
        else:
            raise NotImplementedError(
                f"ComparisonAnalyzer: check_type '{check_type}' not implemented"
            )

        return _pass_rate(cond)


class BetweenAnalyzer:
    @staticmethod
    def analyze(rule: RuleDefinition) -> exp.Expression:
        field = rule.field
        if isinstance(field, list):
            field = field[0]
        col = _to_col(field)
        min_val = exp.convert(rule.value[0])
        max_val = exp.convert(rule.value[1])
        cond: exp.Expression = exp.Between(this=col, low=min_val, high=max_val)
        return _pass_rate(cond)


class ColumnComparisonAnalyzer:
    @staticmethod
    def analyze(rule: RuleDefinition) -> exp.Expression:
        field = rule.field
        if isinstance(field, list):
            field = field[0]
        col1 = _to_col(field)
        col2 = _to_col(str(rule.value))
        cond: exp.Expression = exp.EQ(this=col1, expression=col2)
        return _pass_rate(cond)


# ============================================================================
# MEMBERSHIP ANALYZERS
# ============================================================================


class MembershipAnalyzer:
    @staticmethod
    def analyze(rule: RuleDefinition) -> exp.Expression:
        field = rule.field
        if isinstance(field, list):
            field = field[0]
        col = _to_col(field)
        values = [exp.convert(v) for v in rule.value]

        if rule.check_type in ("is_contained_in", "is_in"):
            cond: exp.Expression = exp.In(this=col, expressions=values)
        else:
            cond = exp.Not(this=exp.In(this=col, expressions=values))

        return _pass_rate(cond)


# ============================================================================
# PATTERN ANALYZERS
# ============================================================================


class PatternAnalyzer:
    @staticmethod
    def analyze(rule: RuleDefinition) -> exp.Expression:
        field = rule.field
        if isinstance(field, list):
            field = field[0]
        col = _to_col(field)
        pattern = exp.Literal.string(str(rule.value))
        cond = exp.RegexpLike(this=col, expression=pattern)
        return _pass_rate(cond)


class LegitAnalyzer:
    @staticmethod
    def analyze(rule: RuleDefinition) -> exp.Expression:
        field = rule.field
        if isinstance(field, list):
            field = field[0]
        col = _to_col(field)
        # FIX: expression=exp.Null() — not kind=
        is_not_null = exp.Not(this=exp.Is(this=col, expression=exp.Null()))
        is_not_empty = exp.NEQ(this=col, expression=exp.Literal.string(""))
        cond: exp.Expression = exp.And(this=is_not_null, expression=is_not_empty)
        return _pass_rate(cond)


# ============================================================================
# CUSTOM SQL ANALYZER
# ============================================================================


class SatisfiesAnalyzer:
    @staticmethod
    def analyze(rule: RuleDefinition) -> exp.Expression:
        """
        Parses rule.value as a raw SQL condition via SQLGlot.
        Input sanitization is the caller's responsibility.
        """
        cond = sqlglot.condition(str(rule.value))
        return _pass_rate(cond)


# ============================================================================
# DATE ANALYZERS
# ============================================================================


class DateAnalyzer:
    @staticmethod
    def analyze(rule: RuleDefinition) -> exp.Expression:
        field = rule.field
        if isinstance(field, list):
            field = field[0]
        col = _to_col(field)
        check_type = rule.check_type
        now = exp.CurrentDate()

        cond: exp.Expression

        if check_type == "is_today":
            cond = exp.EQ(
                this=exp.DateTrunc(unit=exp.var("DAY"), this=col), expression=now
            )

        elif check_type in ("is_t_minus_1", "is_yesterday"):
            target = exp.DateSub(this=now, expression=exp.Literal.number(1))
            cond = exp.EQ(
                this=exp.DateTrunc(unit=exp.var("DAY"), this=col), expression=target
            )

        elif check_type == "is_t_minus_2":
            target = exp.DateSub(this=now, expression=exp.Literal.number(2))
            cond = exp.EQ(
                this=exp.DateTrunc(unit=exp.var("DAY"), this=col), expression=target
            )

        elif check_type == "is_t_minus_3":
            target = exp.DateSub(this=now, expression=exp.Literal.number(3))
            cond = exp.EQ(
                this=exp.DateTrunc(unit=exp.var("DAY"), this=col), expression=target
            )

        elif check_type == "is_past_date":
            cond = exp.LT(
                this=exp.DateTrunc(unit=exp.var("DAY"), this=col), expression=now
            )

        elif check_type == "is_future_date":
            cond = exp.GT(
                this=exp.DateTrunc(unit=exp.var("DAY"), this=col), expression=now
            )

        elif check_type == "is_on_weekend":
            dow = exp.DayOfWeek(this=col)
            cond = exp.In(
                this=dow, expressions=[exp.Literal.number(1), exp.Literal.number(7)]
            )

        elif check_type == "is_on_weekday":
            dow = exp.DayOfWeek(this=col)
            cond = exp.Not(
                this=exp.In(
                    this=dow,
                    expressions=[exp.Literal.number(1), exp.Literal.number(7)],
                )
            )

        else:
            raise NotImplementedError(
                f"DateAnalyzer: check_type '{check_type}' not implemented"
            )

        return _pass_rate(cond)


class WeekdayAnalyzer:
    """
    Handles is_on_monday … is_on_sunday.

    DOW convention: Sunday=1, Monday=2 … Saturday=7
    (BigQuery / Spark / Trino native convention).

    DuckDB and Snowflake use Sunday=0 — on those engines the result will be
    off-by-one. Use `satisfies` with engine-specific SQL for exact checks on
    those dialects.
    """

    _DOW = {
        "is_on_sunday": 1,
        "is_on_monday": 2,
        "is_on_tuesday": 3,
        "is_on_wednesday": 4,
        "is_on_thursday": 5,
        "is_on_friday": 6,
        "is_on_saturday": 7,
    }

    @staticmethod
    def analyze(rule: RuleDefinition) -> exp.Expression:
        field = rule.field
        if isinstance(field, list):
            field = field[0]
        col = _to_col(field)
        day_num = WeekdayAnalyzer._DOW.get(rule.check_type)
        if day_num is None:
            raise NotImplementedError(
                f"WeekdayAnalyzer: unknown check_type '{rule.check_type}'"
            )
        dow = exp.DayOfWeek(this=col)
        cond: exp.Expression = exp.EQ(this=dow, expression=exp.Literal.number(day_num))
        return _pass_rate(cond)


class ValidateDateFormatAnalyzer:
    """
    Checks that a column can be safely parsed as DATE.
    Transpiles to:
        BigQuery  → NOT SAFE_CAST(col AS DATE) IS NULL
        DuckDB    → NOT TRY_CAST(col AS DATE) IS NULL
        Snowflake → NOT CAST(col AS DATE) IS NULL   (raises on bad input)
        Trino     → NOT TRY_CAST(col AS DATE) IS NULL
    """

    @staticmethod
    def analyze(rule: RuleDefinition) -> exp.Expression:
        field = rule.field
        if isinstance(field, list):
            field = field[0]
        col = _to_col(field)
        try_cast = exp.TryCast(this=col, to=exp.DataType.build("date"))
        is_null = exp.Is(this=try_cast, expression=exp.Null())
        return _pass_rate(exp.Not(this=is_null))


class DateBetweenAnalyzer:
    @staticmethod
    def analyze(rule: RuleDefinition) -> exp.Expression:
        field = rule.field
        if isinstance(field, list):
            field = field[0]
        col = _to_col(field)
        start = exp.Cast(
            this=exp.Literal.string(str(rule.value[0])), to=exp.DataType.build("date")
        )
        end = exp.Cast(
            this=exp.Literal.string(str(rule.value[1])), to=exp.DataType.build("date")
        )
        cond: exp.Expression = exp.Between(this=col, low=start, high=end)
        return _pass_rate(cond)


class DateComparisonAnalyzer:
    @staticmethod
    def analyze(rule: RuleDefinition) -> exp.Expression:
        field = rule.field
        if isinstance(field, list):
            field = field[0]
        col = _to_col(field)
        target = exp.Cast(
            this=exp.Literal.string(str(rule.value)), to=exp.DataType.build("date")
        )
        if rule.check_type == "is_date_after":
            cond: exp.Expression = exp.GT(this=col, expression=target)
        else:
            cond = exp.LT(this=col, expression=target)
        return _pass_rate(cond)


# ============================================================================
# TABLE-LEVEL ANALYZERS
# ============================================================================


class AggregationAnalyzer:
    @staticmethod
    def analyze(rule: RuleDefinition) -> exp.Expression:
        field = rule.field
        if isinstance(field, list):
            field = field[0]
        col = _to_col(field)
        check_type = rule.check_type

        if check_type == "has_min":
            return exp.Min(this=col)
        elif check_type == "has_max":
            return exp.Max(this=col)
        elif check_type == "has_sum":
            return exp.Sum(this=col)
        elif check_type == "has_mean":
            return exp.Avg(this=col)
        elif check_type == "has_std":
            return exp.Stddev(this=col)
        elif check_type == "has_cardinality":
            return exp.Count(this=exp.Distinct(expressions=[col]))
        else:
            raise NotImplementedError(
                f"AggregationAnalyzer: check_type '{check_type}' not implemented"
            )
