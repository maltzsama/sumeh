#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
BigQuery data quality validation engine for Sumeh.

This module provides validation functions for data quality rules in BigQuery using SQLGlot
for SQL generation. It supports various validation types including completeness, uniqueness,
pattern matching, date validations, and numeric comparisons.
"""

from google.cloud import bigquery
from typing import List, Dict, Any, Tuple, Callable
import warnings
from dataclasses import dataclass
import uuid
from datetime import datetime
import sqlglot
from sqlglot import exp

from sumeh.core.utils import __compare_schemas


@dataclass(slots=True)
class __RuleCtx:
    """
        Context for validation rule execution.

        Attributes:
            column: Column name(s) to validate (str or list of str)
            value: Threshold or comparison value for the rule
            name: Check type identifier
    """
    column: Any
    value: Any
    name: str


def _parse_table_ref(table_ref: str) -> exp.Table:
    """
    Parses a table reference string into a SQLGlot Table expression.

    Args:
        table_ref: Table reference in format "project.dataset.table", "dataset.table", or "table"

    Returns:
        SQLGlot Table expression with appropriate catalog, database, and table identifiers

    Examples:
        >>> _parse_table_ref("my-project.my_dataset.my_table")
        Table(catalog=Identifier("my-project"), db=Identifier("my_dataset"), this=Identifier("my_table"))
    """
    parts = table_ref.split(".")

    if len(parts) == 3:
        return exp.Table(
            catalog=exp.Identifier(this=parts[0], quoted=False),
            db=exp.Identifier(this=parts[1], quoted=False),
            this=exp.Identifier(this=parts[2], quoted=False),
        )
    elif len(parts) == 2:
        return exp.Table(
            db=exp.Identifier(this=parts[0], quoted=False),
            this=exp.Identifier(this=parts[1], quoted=False),
        )
    else:
        return exp.Table(this=exp.Identifier(this=parts[0], quoted=False))


def _is_complete(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to validate column completeness (non-null).

    Args:
        r: Rule context containing the column to validate

    Returns:
        SQLGlot expression checking if column IS NOT NULL
    """
    return exp.Is(this=exp.Column(this=r.column), expression=exp.Not(this=exp.Null()))


def _are_complete(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to validate multiple columns are complete (all non-null).

    Args:
        r: Rule context with column list to validate

    Returns:
        SQLGlot AND expression checking all columns are NOT NULL
    """
    conditions = [
        exp.Is(this=exp.Column(this=c), expression=exp.Not(this=exp.Null()))
        for c in r.column
    ]
    return exp.And(expressions=conditions)


def _is_unique(r: __RuleCtx, table_expr: exp.Table) -> exp.Expression:
    """
    Creates a subquery expression to verify column uniqueness.

    Args:
        r: Rule context containing column and validation parameters
        table_expr: SQLGlot table expression for the source table

    Returns:
        exp.Expression: SQLGlot expression for uniqueness validation
    """
    subquery = (
        exp.Select(expressions=[exp.Count(this=exp.Star())])
        .from_(exp.alias_(table_expr, "d2", copy=True))
        .where(
            exp.EQ(
                this=exp.Column(this=r.column, table="d2"),
                expression=exp.Column(this=r.column, table="tbl"),
            )
        )
    )
    return exp.EQ(this=exp.Paren(this=subquery), expression=exp.Literal.number(1))


def _are_unique(r: __RuleCtx, table_expr: exp.Table) -> exp.Expression:
    """
    Generates SQL subquery expression to verify composite key uniqueness.

    Concatenates multiple columns with '|' separator and checks for uniqueness.

    Args:
        r: Rule context containing list of columns forming composite key
        table_expr: SQLGlot table expression for source table

    Returns:
        SQLGlot expression checking concatenated columns are unique
    """

    def concat_cols(table_alias):
        parts = [
            exp.Cast(
                this=exp.Column(this=c, table=table_alias),
                to=exp.DataType.build("STRING"),
            )
            for c in r.column
        ]

        if len(parts) == 1:
            return parts[0]

        result = parts[0]
        for part in parts[1:]:
            result = exp.DPipe(this=result, expression=exp.Literal.string("|"))
            result = exp.DPipe(this=result, expression=part)

        return result

    subquery = (
        exp.Select(expressions=[exp.Count(this=exp.Star())])
        .from_(exp.alias_(table_expr, "d2", copy=True))
        .where(exp.EQ(this=concat_cols("d2"), expression=concat_cols("tbl")))
    )

    return exp.EQ(this=exp.Paren(this=subquery), expression=exp.Literal.number(1))


def _is_positive(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect negative values (violation for positive rule).

    Args:
        r: Rule context containing column to validate

    Returns:
        SQLGlot expression checking if column < 0 (violation condition)
    """
    return exp.LT(this=exp.Column(this=r.column), expression=exp.Literal.number(0))


def _is_negative(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect non-negative values (violation for negative rule).

    Args:
        r: Rule context containing column to validate

    Returns:
        SQLGlot expression checking if column >= 0 (violation condition)
    """
    return exp.GTE(this=exp.Column(this=r.column), expression=exp.Literal.number(0))


def _is_greater_than(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect values not greater than threshold (violation).

    Args:
        r: Rule context containing column and threshold value

    Returns:
        SQLGlot expression checking if column <= threshold (violation condition)
    """
    return exp.LTE(
        this=exp.Column(this=r.column), expression=exp.Literal.number(r.value)
    )


def _is_less_than(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect values not less than threshold (violation).

    Args:
        r: Rule context containing column and threshold value

    Returns:
        SQLGlot expression checking if column >= threshold (violation condition)
    """
    return exp.GTE(
        this=exp.Column(this=r.column), expression=exp.Literal.number(r.value)
    )


def _is_greater_or_equal_than(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect values less than threshold (violation).

    Args:
        r: Rule context containing column and threshold value

    Returns:
        SQLGlot expression checking if column < threshold (violation condition)
    """
    return exp.LT(
        this=exp.Column(this=r.column), expression=exp.Literal.number(r.value)
    )


def _is_less_or_equal_than(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect values greater than threshold (violation).

    Args:
        r: Rule context containing column and threshold value

    Returns:
        SQLGlot expression checking if column > threshold (violation condition)
    """
    return exp.GT(
        this=exp.Column(this=r.column), expression=exp.Literal.number(r.value)
    )


def _is_equal_than(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect values not equal to specified value (violation).

    Args:
        r: Rule context containing column and comparison value

    Returns:
        SQLGlot expression checking if column != value (violation condition)
    """
    return exp.EQ(
        this=exp.Column(this=r.column), expression=exp.Literal.number(r.value)
    )


def _is_in_millions(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect values less than one million (violation).

    Args:
        r: Rule context containing column to validate

    Returns:
        SQLGlot expression checking if column < 1,000,000 (violation condition)
    """
    return exp.GTE(
        this=exp.Column(this=r.column), expression=exp.Literal.number(1000000)
    )


def _is_in_billions(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect values less than one billion (violation).

    Args:
        r: Rule context containing column to validate

    Returns:
        SQLGlot expression checking if column < 1,000,000,000 (violation condition)
    """
    return exp.GTE(
        this=exp.Column(this=r.column), expression=exp.Literal.number(1000000000)
    )


def _is_between(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to validate column value is within specified range.

    Args:
        r: Rule context containing column and range values (as list/tuple or comma-separated string)

    Returns:
        SQLGlot BETWEEN expression checking if value is in [low, high] range
    """
    val = r.value
    if isinstance(val, (list, tuple)):
        lo, hi = val
    else:
        lo, hi, *_ = [v.strip(" []()'\"") for v in str(val).split(",")]

    return exp.Between(
        this=exp.Column(this=r.column),
        low=exp.Literal.number(float(lo)),
        high=exp.Literal.number(float(hi)),
    )


def _has_pattern(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to validate column matches regex pattern.

    Args:
        r: Rule context containing column and regex pattern

    Returns:
        SQLGlot REGEXP_CONTAINS expression for pattern matching
    """
    return exp.RegexpLike(
        this=exp.Cast(this=exp.Column(this=r.column), to=exp.DataType.build("STRING")),
        expression=exp.Literal.string(str(r.value)),
    )


def _is_contained_in(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to validate column value is in allowed list.

    Args:
        r: Rule context containing column and allowed values (as list/tuple or comma-separated string)

    Returns:
        SQLGlot IN expression checking if value is in allowed list
    """
    if isinstance(r.value, (list, tuple)):
        seq = r.value
    else:
        seq = [v.strip() for v in str(r.value).split(",")]

    literals = [exp.Literal.string(str(x)) for x in seq if x]
    return exp.In(this=exp.Column(this=r.column), expressions=literals)


def _is_in(r: __RuleCtx) -> exp.Expression:
    """
    Alias for _is_contained_in. Validates column value is in allowed list.

    Args:
        r: Rule context containing column and allowed values

    Returns:
        SQLGlot IN expression
    """
    return _is_contained_in(r)


def _not_contained_in(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to validate column value is not in blocked list.

    Args:
        r: Rule context containing column and blocked values (as list/tuple or comma-separated string)

    Returns:
        SQLGlot NOT IN expression checking if value is not in blocked list
    """
    if isinstance(r.value, (list, tuple)):
        seq = r.value
    else:
        seq = [v.strip() for v in str(r.value).split(",")]

    literals = [exp.Literal.string(str(x)) for x in seq if x]
    return exp.Not(this=exp.In(this=exp.Column(this=r.column), expressions=literals))


def _not_in(r: __RuleCtx) -> exp.Expression:
    """
    Alias for _not_contained_in. Validates column value is not in blocked list.

    Args:
        r: Rule context containing column and blocked values

    Returns:
        SQLGlot NOT IN expression
    """
    return _not_contained_in(r)


def _satisfies(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression from custom SQL condition string.

    Args:
        r: Rule context containing custom SQL expression string in value attribute

    Returns:
        SQLGlot expression parsed from custom SQL string
    """
    return sqlglot.parse_one(str(r.value), dialect="bigquery")


def _validate_date_format(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to validate date string format using regex.

    Supports tokens: DD (day), MM (month), YYYY (4-digit year), YY (2-digit year)

    Args:
        r: Rule context containing column and date format pattern (e.g., "DD/MM/YYYY")

    Returns:
        SQLGlot expression checking if column IS NULL or doesn't match format (violation)
    """
    fmt = r.value
    token_map = {
        "DD": r"(0[1-9]|[12][0-9]|3[01])",
        "MM": r"(0[1-9]|1[0-2])",
        "YYYY": r"(19|20)\d\d",
        "YY": r"\d\d",
    }
    regex = fmt
    for tok, pat in token_map.items():
        regex = regex.replace(tok, pat)
    regex = regex.replace(".", r"\.").replace(" ", r"\s")

    return exp.Or(
        this=exp.Is(this=exp.Column(this=r.column), expression=exp.Null()),
        expression=exp.Not(
            this=exp.RegexpLike(
                this=exp.Column(this=r.column),
                expression=exp.Literal.string(f"^{regex}$"),
            )
        ),
    )


def _is_future_date(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect future dates (violation).

    Args:
        r: Rule context containing date column to validate

    Returns:
        SQLGlot expression checking if column > CURRENT_DATE() (violation)
    """
    return exp.GT(this=exp.Column(this=r.column), expression=exp.CurrentDate())


def _is_past_date(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect past dates (violation).

    Args:
        r: Rule context containing date column to validate

    Returns:
        SQLGlot expression checking if column < CURRENT_DATE() (violation)
    """
    return exp.LT(this=exp.Column(this=r.column), expression=exp.CurrentDate())


def _is_date_after(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect dates not after specified date (violation).

    Args:
        r: Rule context containing column and threshold date string

    Returns:
        SQLGlot expression checking if column < threshold_date (violation)
    """
    return exp.LT(
        this=exp.Column(this=r.column),
        expression=exp.Anonymous(
            this="DATE", expressions=[exp.Literal.string(r.value)]
        ),
    )


def _is_date_before(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect dates not before specified date (violation).

    Args:
        r: Rule context containing column and threshold date string

    Returns:
        SQLGlot expression checking if column > threshold_date (violation)
    """
    return exp.GT(
        this=exp.Column(this=r.column),
        expression=exp.Anonymous(
            this="DATE", expressions=[exp.Literal.string(r.value)]
        ),
    )


def _is_date_between(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect dates outside specified range (violation).

    Args:
        r: Rule context containing column and date range as "[start,end]"

    Returns:
        SQLGlot expression checking if column NOT BETWEEN start AND end (violation)
    """
    start, end = [d.strip() for d in r.value.strip("[]").split(",")]
    return exp.Not(
        this=exp.Between(
            this=exp.Column(this=r.column),
            low=exp.Anonymous(this="DATE", expressions=[exp.Literal.string(start)]),
            high=exp.Anonymous(this="DATE", expressions=[exp.Literal.string(end)]),
        )
    )


def _all_date_checks(r: __RuleCtx) -> exp.Expression:
    """
    Applies all standard date validations. Currently defaults to past date check.

    Args:
        r: Rule context containing date column to validate

    Returns:
        SQLGlot expression for comprehensive date validation
    """
    return _is_past_date(r)


def _is_today(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect dates that are not today (violation).

    Args:
        r: Rule context containing date column to validate

    Returns:
        SQLGlot expression checking if column != CURRENT_DATE() (violation)
    """
    return exp.EQ(this=exp.Column(this=r.column), expression=exp.CurrentDate())


def _is_yesterday(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect dates that are not yesterday (violation).

    Args:
        r: Rule context containing date column to validate

    Returns:
        SQLGlot expression checking if column != CURRENT_DATE() - 1 DAY (violation)
    """
    return exp.EQ(
        this=exp.Column(this=r.column),
        expression=exp.DateSub(
            this=exp.CurrentDate(),
            expression=exp.Interval(
                this=exp.Literal.number(1), unit=exp.Var(this="DAY")
            ),
        ),
    )


def _is_t_minus_1(r: __RuleCtx) -> exp.Expression:
    """
    Alias for _is_yesterday. Validates date is T-1 (yesterday).

    Args:
        r: Rule context containing date column to validate

    Returns:
        SQLGlot expression for yesterday validation
    """
    return _is_yesterday(r)


def _is_t_minus_2(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect dates that are not T-2 (2 days ago).

    Args:
        r: Rule context containing date column to validate

    Returns:
        SQLGlot expression checking if column != CURRENT_DATE() - 2 DAYS (violation)
    """
    return exp.EQ(
        this=exp.Column(this=r.column),
        expression=exp.DateSub(
            this=exp.CurrentDate(),
            expression=exp.Interval(
                this=exp.Literal.number(2), unit=exp.Var(this="DAY")
            ),
        ),
    )


def _is_t_minus_3(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect dates that are not T-3 (3 days ago).

    Args:
        r: Rule context containing date column to validate

    Returns:
        SQLGlot expression checking if column != CURRENT_DATE() - 3 DAYS (violation)
    """
    return exp.EQ(
        this=exp.Column(this=r.column),
        expression=exp.DateSub(
            this=exp.CurrentDate(),
            expression=exp.Interval(
                this=exp.Literal.number(3), unit=exp.Var(this="DAY")
            ),
        ),
    )


def _is_on_weekday(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect weekend dates (violation for weekday rule).

    Args:
        r: Rule context containing date column to validate

    Returns:
        SQLGlot expression checking if DAYOFWEEK NOT BETWEEN 2 AND 6 (violation)
    """
    dayofweek = exp.Extract(
        this=exp.Var(this="DAYOFWEEK"), expression=exp.Column(this=r.column)
    )
    return exp.Between(
        this=dayofweek, low=exp.Literal.number(2), high=exp.Literal.number(6)
    )


def _is_on_weekend(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect weekday dates (violation for weekend rule).

    Args:
        r: Rule context containing date column to validate

    Returns:
        SQLGlot expression checking if DAYOFWEEK is 1 (Sunday) or 7 (Saturday)
    """
    dayofweek = exp.Extract(
        this=exp.Var(this="DAYOFWEEK"), expression=exp.Column(this=r.column)
    )
    return exp.Or(
        this=exp.EQ(this=dayofweek, expression=exp.Literal.number(1)),
        expression=exp.EQ(this=dayofweek, expression=exp.Literal.number(7)),
    )


def _is_on_monday(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect non-Monday dates (violation).

    Args:
        r: Rule context containing date column to validate

    Returns:
        SQLGlot expression checking if DAYOFWEEK != 2 (violation)
    """
    return exp.EQ(
        this=exp.Extract(
            this=exp.Var(this="DAYOFWEEK"), expression=exp.Column(this=r.column)
        ),
        expression=exp.Literal.number(2),
    )


def _is_on_tuesday(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect non-Tuesday dates (violation).

    Args:
        r: Rule context containing date column to validate

    Returns:
        SQLGlot expression checking if DAYOFWEEK != 3 (violation)
    """
    return exp.EQ(
        this=exp.Extract(
            this=exp.Var(this="DAYOFWEEK"), expression=exp.Column(this=r.column)
        ),
        expression=exp.Literal.number(3),
    )


def _is_on_wednesday(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect non-Wednesday dates (violation).

    Args:
        r: Rule context containing date column to validate

    Returns:
        SQLGlot expression checking if DAYOFWEEK != 4 (violation)
    """
    return exp.EQ(
        this=exp.Extract(
            this=exp.Var(this="DAYOFWEEK"), expression=exp.Column(this=r.column)
        ),
        expression=exp.Literal.number(4),
    )


def _is_on_thursday(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect non-Thursday dates (violation).

    Args:
        r: Rule context containing date column to validate

    Returns:
        SQLGlot expression checking if DAYOFWEEK != 5 (violation)
    """
    return exp.EQ(
        this=exp.Extract(
            this=exp.Var(this="DAYOFWEEK"), expression=exp.Column(this=r.column)
        ),
        expression=exp.Literal.number(5),
    )


def _is_on_friday(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect non-Friday dates (violation).

    Args:
        r: Rule context containing date column to validate

    Returns:
        SQLGlot expression checking if DAYOFWEEK != 6 (violation)
    """
    return exp.EQ(
        this=exp.Extract(
            this=exp.Var(this="DAYOFWEEK"), expression=exp.Column(this=r.column)
        ),
        expression=exp.Literal.number(6),
    )


def _is_on_saturday(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect non-Saturday dates (violation).

    Args:
        r: Rule context containing date column to validate

    Returns:
        SQLGlot expression checking if DAYOFWEEK != 7 (violation)
    """
    return exp.EQ(
        this=exp.Extract(
            this=exp.Var(this="DAYOFWEEK"), expression=exp.Column(this=r.column)
        ),
        expression=exp.Literal.number(7),
    )


def _is_on_sunday(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to detect non-Sunday dates (violation).

    Args:
        r: Rule context containing date column to validate

    Returns:
        SQLGlot expression checking if DAYOFWEEK != 1 (violation)
    """
    return exp.EQ(
        this=exp.Extract(
            this=exp.Var(this="DAYOFWEEK"), expression=exp.Column(this=r.column)
        ),
        expression=exp.Literal.number(1),
    )


__RULE_DISPATCH_SIMPLE: dict[str, Callable[[__RuleCtx], exp.Expression]] = {
    "is_complete": _is_complete,
    "are_complete": _are_complete,
    "is_greater_than": _is_greater_than,
    "is_less_than": _is_less_than,
    "is_greater_or_equal_than": _is_greater_or_equal_than,
    "is_less_or_equal_than": _is_less_or_equal_than,
    "is_equal_than": _is_equal_than,
    "is_in_millions": _is_in_millions,
    "is_in_billions": _is_in_billions,
    "is_between": _is_between,
    "has_pattern": _has_pattern,
    "is_contained_in": _is_contained_in,
    "is_in": _is_in,
    "not_contained_in": _not_contained_in,
    "not_in": _not_in,
    "satisfies": _satisfies,
    "validate_date_format": _validate_date_format,
    "is_future_date": _is_future_date,
    "is_past_date": _is_past_date,
    "is_date_after": _is_date_after,
    "is_date_before": _is_date_before,
    "is_date_between": _is_date_between,
    "all_date_checks": _all_date_checks,
    "is_positive": _is_positive,
    "is_negative": _is_negative,
    "is_on_weekday": _is_on_weekday,
    "is_on_weekend": _is_on_weekend,
    "is_on_monday": _is_on_monday,
    "is_on_tuesday": _is_on_tuesday,
    "is_on_wednesday": _is_on_wednesday,
    "is_on_thursday": _is_on_thursday,
    "is_on_friday": _is_on_friday,
    "is_on_saturday": _is_on_saturday,
    "is_on_sunday": _is_on_sunday,
}

__RULE_DISPATCH_WITH_TABLE: dict[
    str, Callable[[__RuleCtx, exp.Table], exp.Expression]
] = {
    "is_unique": _is_unique,
    "are_unique": _are_unique,
    "is_primary_key": _is_unique,
    "is_composite_key": _are_unique,
}


def _build_union_sql(rules: List[Dict], table_ref: str) -> str:
    """
    Constructs UNION ALL SQL query for all validation rules using SQLGlot.

    Generates a SQL query that checks each rule and returns violating rows with
    a dq_status column indicating which rule was violated.

    Args:
        rules: List of validation rule dictionaries containing field, check_type, value, execute
        table_ref: Fully qualified BigQuery table reference (project.dataset.table)

    Returns:
        SQL query string with UNION ALL of all rule validations

    Warnings:
        UserWarning: Issued for unknown rule types
    """

    table_expr = _parse_table_ref(table_ref)

    queries = []

    for r in rules:
        if not r.get("execute", True):
            continue

        check = r["check_type"]

        if check in __RULE_DISPATCH_SIMPLE:
            builder = __RULE_DISPATCH_SIMPLE[check]
            needs_table = False
        elif check in __RULE_DISPATCH_WITH_TABLE:
            builder = __RULE_DISPATCH_WITH_TABLE[check]
            needs_table = True
        else:
            warnings.warn(f"Unknown rule: {check}")
            continue

        ctx = __RuleCtx(
            column=r["field"],
            value=r.get("value"),
            name=check,
        )

        if needs_table:
            expr_ok = builder(ctx, table_expr)
        else:
            expr_ok = builder(ctx)

        dq_tag = f"{ctx.column}:{check}:{ctx.value}"

        query = (
            exp.Select(
                expressions=[
                    exp.Star(),
                    exp.alias_(exp.Literal.string(dq_tag), "dq_status"),
                ]
            )
            .from_(exp.alias_(table_expr, "tbl", copy=True))
            .where(exp.Not(this=expr_ok))
        )

        queries.append(query)

    if not queries:
        empty = (
            exp.Select(
                expressions=[
                    exp.Star(),
                    exp.alias_(exp.Literal.string(""), "dq_status"),
                ]
            )
            .from_(table_expr)
            .where(exp.false())
        )

        return empty.sql(dialect="bigquery")

    union_query = queries[0]
    for q in queries[1:]:
        union_query = exp.union(union_query, q, distinct=False)

    return union_query.sql(dialect="bigquery")


def validate(
    client: bigquery.Client, table_ref: str, rules: List[Dict]
) -> Tuple[bigquery.table.RowIterator, bigquery.table.RowIterator]:
    """
    Validates BigQuery table data against specified quality rules.

    Executes two queries:
    1. Raw violations - all violating rows with individual dq_status
    2. Aggregated violations - rows grouped with concatenated dq_status

    Args:
        client: Authenticated BigQuery client instance
        table_ref: Fully qualified table reference (project.dataset.table)
        rules: List of validation rule dictionaries

    Returns:
        Tuple containing:
            - Aggregated results with grouped violations
            - Raw results with individual violations
    """

    union_sql = _build_union_sql(rules, table_ref)

    violations_subquery = sqlglot.parse_one(union_sql, dialect="bigquery")

    table = client.get_table(table_ref)
    cols = [exp.Column(this=f.name) for f in table.schema]

    raw_query = (
        exp.Select(expressions=cols + [exp.Column(this="dq_status")])
        .with_("violations", as_=violations_subquery)
        .from_("violations")
    )

    raw_sql = raw_query.sql(dialect="bigquery")

    final_query = (
        exp.Select(
            expressions=cols
            + [
                exp.alias_(
                    exp.Anonymous(
                        this="STRING_AGG",
                        expressions=[
                            exp.Column(this="dq_status"),
                            exp.Literal.string(";"),
                        ],
                    ),
                    "dq_status",
                )
            ]
        )
        .with_("violations", as_=violations_subquery)
        .from_("violations")
        .group_by(*cols)
    )

    final_sql = final_query.sql(dialect="bigquery")

    raw = client.query(raw_sql).result()
    final = client.query(final_sql).result()

    return final, raw


def __rules_to_bq_sql(rules: List[Dict]) -> str:
    """
    Converts rule definitions into SQL representation using SQLGlot.

    Generates SQL query that represents each rule as a row with columns:
    col, rule, pass_threshold, value

    Args:
        rules: List of validation rule dictionaries

    Returns:
        SQL query string with DISTINCT rule definitions
    """

    queries = []

    for r in rules:
        if not r.get("execute", True):
            continue

        ctx = __RuleCtx(column=r["field"], value=r.get("value"), name=r["check_type"])

        col = ", ".join(ctx.column) if isinstance(ctx.column, list) else ctx.column

        try:
            thr = float(r.get("threshold", 1.0))
        except (TypeError, ValueError):
            thr = 1.0

        if ctx.value is None:
            val_literal = exp.Null()
        elif isinstance(ctx.value, str):
            val_literal = exp.Literal.string(ctx.value)
        elif isinstance(ctx.value, (list, tuple)):
            val_literal = exp.Literal.string(",".join(str(x) for x in ctx.value))
        else:
            val_literal = exp.Literal.number(ctx.value)

        query = exp.Select(
            expressions=[
                exp.alias_(exp.Literal.string(col.strip()), "col"),
                exp.alias_(exp.Literal.string(ctx.name), "rule"),
                exp.alias_(exp.Literal.number(thr), "pass_threshold"),
                exp.alias_(val_literal, "value"),
            ]
        )

        queries.append(query)

    if not queries:

        empty = exp.Select(
            expressions=[
                exp.alias_(exp.Null(), "col"),
                exp.alias_(exp.Null(), "rule"),
                exp.alias_(exp.Null(), "pass_threshold"),
                exp.alias_(exp.Null(), "value"),
            ]
        ).limit(0)
        return empty.sql(dialect="bigquery")

    union_query = queries[0]
    for q in queries[1:]:
        union_query = exp.union(union_query, q, distinct=False)

    final_query = (
        exp.Select(
            expressions=[
                exp.Column(this="col"),
                exp.Column(this="rule"),
                exp.Column(this="pass_threshold"),
                exp.Column(this="value"),
            ]
        )
        .from_(exp.alias_(exp.Subquery(this=union_query), "t"))
        .distinct()
    )

    return final_query.sql(dialect="bigquery")


def summarize(
    df: bigquery.table.RowIterator,
    rules: List[Dict],
    total_rows: int,
    client: bigquery.Client,
) -> List[Dict[str, Any]]:
    """
    Generates validation summary report with pass/fail status for each rule.

    Analyzes violation results and compares against rule thresholds to determine
    pass/fail status for each validation rule.

    Args:
        df: Row iterator containing validation violations from validate()
        rules: List of validation rules that were executed
        total_rows: Total number of rows in validated table
        client: BigQuery client instance (for compatibility, not actively used)

    Returns:
        List of summary dictionaries, each containing:
            - id: Unique identifier for the summary record
            - timestamp: Validation execution timestamp
            - check: Check category (always "Quality Check")
            - level: Severity level (always "WARNING")
            - column: Column name(s) validated
            - rule: Rule type applied
            - value: Rule threshold/comparison value
            - rows: Total rows evaluated
            - violations: Number of violating rows
            - pass_rate: Percentage of passing rows (0.0-1.0)
            - pass_threshold: Required pass rate from rule
            - status: "PASS" or "FAIL" based on pass_rate vs pass_threshold
    """

    violations_count = {}
    for row in df:
        dq_status = row.get("dq_status", "")
        if dq_status and dq_status.strip():
            parts = dq_status.split(":", 2)
            if len(parts) >= 2:
                col, rule = parts[0], parts[1]
                val = parts[2] if len(parts) > 2 else "N/A"
                key = (col, rule, val)
                violations_count[key] = violations_count.get(key, 0) + 1

    results = []
    for r in rules:
        if not r.get("execute", True):
            continue

        col = r["field"]
        if isinstance(col, list):
            col = ", ".join(col)

        rule = r["check_type"]
        val = str(r.get("value")) if r.get("value") is not None else "N/A"
        threshold = float(r.get("threshold", 1.0))

        key = (str(col), rule, val)
        violations = violations_count.get(key, 0)

        pass_rate = (total_rows - violations) / total_rows if total_rows > 0 else 1.0
        status = "PASS" if pass_rate >= threshold else "FAIL"

        results.append(
            {
                "id": str(uuid.uuid4()),
                "timestamp": datetime.now(),
                "check": "Quality Check",
                "level": "WARNING",
                "column": col,
                "rule": rule,
                "value": val,
                "rows": total_rows,
                "violations": violations,
                "pass_rate": pass_rate,
                "pass_threshold": threshold,
                "status": status,
            }
        )

    return results


def count_rows(client: bigquery.Client, table_ref: str) -> int:
    """
    Counts total number of rows in a BigQuery table using SQLGlot.

    Args:
        client: Authenticated BigQuery client instance
        table_ref: Fully qualified table reference (project.dataset.table)

    Returns:
        Total row count as integer
    """

    table_expr = _parse_table_ref(table_ref)

    query = exp.Select(
        expressions=[exp.alias_(exp.Count(this=exp.Star()), "total")]
    ).from_(table_expr)

    sql = query.sql(dialect="bigquery")
    result = client.query(sql).result()
    return list(result)[0]["total"]


def extract_schema(table: bigquery.Table) -> List[Dict[str, Any]]:
    """
    Extracts schema definition from BigQuery table object.

    Args:
        table: BigQuery Table object with schema information

    Returns:
        List of schema field dictionaries, each containing:
            - field: Field name
            - data_type: BigQuery data type
            - nullable: Whether field allows NULL values
            - max_length: Always None (reserved for future use)
    """
    return [
        {
            "field": fld.name,
            "data_type": fld.field_type,
            "nullable": fld.is_nullable,
            "max_length": None,
        }
        for fld in table.schema
    ]


def validate_schema(
    client: bigquery.Client, expected: List[Dict[str, Any]], table_ref: str
) -> tuple[bool, list[dict[str, Any]]]:
    """
    Validates BigQuery table schema against expected schema definition.

    Compares actual table schema with expected schema and identifies mismatches
    in field names, data types, and nullability constraints.

    Args:
        client: Authenticated BigQuery client instance
        expected: List of expected schema field dictionaries
        table_ref: Fully qualified table reference (project.dataset.table)

    Returns:
        Tuple containing:
            - Boolean indicating if schemas match exactly
            - List of error dictionaries describing any mismatches
    """

    table = client.get_table(table_ref)
    actual = extract_schema(table)

    result, errors = __compare_schemas(actual, expected)
    return result, errors
