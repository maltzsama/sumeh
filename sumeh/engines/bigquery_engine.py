#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
BigQuery data quality validation engine for Sumeh.

This module provides a complete BigQuery implementation of the Sumeh data quality framework,
using SQLGlot for SQL generation and BigQuery client for execution. It supports all standard
Sumeh validation rules through SQL-based validation queries.

Key Features:
    - SQL-based validation using SQLGlot AST generation
    - Support for all Sumeh validation rules (completeness, uniqueness, ranges, patterns, dates)
    - Schema validation against expected schemas
    - Efficient UNION-based query generation for multiple rules
    - BigQuery-specific SQL dialect optimization

Functions:
    validate(client, table_ref, rules): Validates data against quality rules
    summarize(df, rules, total_rows, client): Generates validation summary report
    count_rows(client, table_ref): Counts total rows in a table
    extract_schema(table): Extracts schema from BigQuery table
    validate_schema(client, expected, table_ref): Validates table schema

Supported Rules:
    - Completeness: is_complete, are_complete
    - Uniqueness: is_unique, are_unique, is_primary_key, is_composite_key
    - Numeric: is_positive, is_negative, is_greater_than, is_less_than, is_between
    - Patterns: has_pattern, is_contained_in, not_contained_in
    - Dates: is_future_date, is_past_date, is_date_between, validate_date_format
    - Custom: satisfies (custom SQL expressions)

Dependencies:
    - google.cloud.bigquery: BigQuery client and table operations
    - sqlglot: SQL AST generation and dialect conversion
    - sumeh.core.utils: Schema comparison utilities

Example:
    >>> from google.cloud import bigquery
    >>> from sumeh.engines.bigquery_engine import validate, summarize
    >>>
    >>> client = bigquery.Client()
    >>> rules = [{"field": "email", "check_type": "is_complete", "threshold": 0.95}]
    >>> result, raw = validate(client, "project.dataset.table", rules)
    >>> summary = summarize(raw, rules, 1000, client)
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
    column: Any
    value: Any
    name: str


def _parse_table_ref(table_ref: str) -> exp.Table:
    """
    Parses a table reference string into a sqlglot Table expression.

    Args:
        table_ref (str): String containing the table reference in one of these formats:
            - "project.dataset.table"
            - "dataset.table"
            - "table"

    Returns:
        exp.Table: A sqlglot Table expression representing the parsed table reference

    Examples:
        >>> _parse_table_ref("project.dataset.table")
        Table(catalog=Identifier("project"), db=Identifier("dataset"), this=Identifier("table"))

        >>> _parse_table_ref("dataset.table")
        Table(db=Identifier("dataset"), this=Identifier("table"))

        >>> _parse_table_ref("table")
        Table(this=Identifier("table"))
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
    Generates a SQL expression to check if a column is complete (non-null).

    Args:
        r (__RuleCtx): Rule context containing the column to validate.

    Returns:
        exp.Expression: SQLGlot expression for completeness validation.
    """
    return exp.Is(this=exp.Column(this=r.column), expression=exp.Not(this=exp.Null()))


def _are_complete(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if multiple columns are complete (non-null).

    Args:
        r (__RuleCtx): Rule context containing the columns to validate. The `column` attribute
            should be a list of column names to check for completeness.

    Returns:
        exp.Expression: SQLGlot expression that evaluates to True if all specified columns are non-null.
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
    Creates a subquery expression to verify composite key uniqueness.

    Args:
        r: Rule context containing columns and validation parameters
        table_expr: SQLGlot table expression for the source table

    Returns:
        exp.Expression: SQLGlot expression for composite uniqueness validation
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
    Generates a SQL expression to check if a column contains positive values.

    Args:
        r (__RuleCtx): Rule context containing the column to validate.

    Returns:
        exp.Expression: SQLGlot expression for positivity validation.
    """
    return exp.LT(this=exp.Column(this=r.column), expression=exp.Literal.number(0))


def _is_negative(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if a column contains negative values.

    Args:
        r (__RuleCtx): Rule context containing the column to validate.

    Returns:
        exp.Expression: SQLGlot expression for negativity validation.
    """
    return exp.GTE(this=exp.Column(this=r.column), expression=exp.Literal.number(0))


def _is_greater_than(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if a column's value is greater than a specified value.

    Args:
        r (__RuleCtx): Rule context containing the column and the value to compare.

    Returns:
        exp.Expression: SQLGlot expression for the greater-than validation.
    """
    return exp.LTE(
        this=exp.Column(this=r.column), expression=exp.Literal.number(r.value)
    )


def _is_less_than(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if a column's value is less than a specified value.

    Args:
        r (__RuleCtx): Rule context containing the column and the value to compare.

    Returns:
        exp.Expression: SQLGlot expression for the less-than validation.
    """
    return exp.GTE(
        this=exp.Column(this=r.column), expression=exp.Literal.number(r.value)
    )


def _is_greater_or_equal_than(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if a column's value is greater than or equal to a specified value.

    Args:
        r (__RuleCtx): Rule context containing the column and the value to compare.

    Returns:
        exp.Expression: SQLGlot expression for the greater-or-equal-than validation.
    """
    return exp.LT(
        this=exp.Column(this=r.column), expression=exp.Literal.number(r.value)
    )


def _is_less_or_equal_than(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if a column's value is less than or equal to a specified value.

    Args:
        r (__RuleCtx): Rule context containing the column and the value to compare.

    Returns:
        exp.Expression: SQLGlot expression for the less-or-equal-than validation.
    """
    return exp.GT(
        this=exp.Column(this=r.column), expression=exp.Literal.number(r.value)
    )


def _is_equal_than(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if a column's value is equal to a specified value.

    Args:
        r (__RuleCtx): Rule context containing the column and the value to compare.

    Returns:
        exp.Expression: SQLGlot expression for the equal-than validation.
    """
    return exp.EQ(
        this=exp.Column(this=r.column), expression=exp.Literal.number(r.value)
    )


def _is_in_millions(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if a column's value is in the millions (>= 1,000,000).

    Args:
        r (__RuleCtx): Rule context containing the column to validate.

    Returns:
        exp.Expression: SQLGlot expression for the millions' validation.
    """
    return exp.GTE(
        this=exp.Column(this=r.column), expression=exp.Literal.number(1000000)
    )


def _is_in_billions(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if a column's value is in the billions (>= 1,000,000,000).

    Args:
        r (__RuleCtx): Rule context containing the column to validate.

    Returns:
        exp.Expression: SQLGlot expression for the billions' validation.
    """
    return exp.GTE(
        this=exp.Column(this=r.column), expression=exp.Literal.number(1000000000)
    )


def _is_between(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if a column's value is between two specified values.

    Args:
        r (__RuleCtx): Rule context containing the column and the range values.

    Returns:
        exp.Expression: SQLGlot expression for the between validation.
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
    Generates a SQL expression to check if a column's value matches a specified pattern.

    Args:
        r (__RuleCtx): Rule context containing the column and the pattern to match.

    Returns:
        exp.Expression: SQLGlot expression for the pattern validation.
    """
    return exp.RegexpLike(
        this=exp.Cast(this=exp.Column(this=r.column), to=exp.DataType.build("STRING")),
        expression=exp.Literal.string(str(r.value)),
    )


def _is_contained_in(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if a column's value is contained in a specified list of values.

    Args:
        r (__RuleCtx): Rule context containing the column and the list of values.

    Returns:
        exp.Expression: SQLGlot expression for the contained-in validation.
    """
    if isinstance(r.value, (list, tuple)):
        seq = r.value
    else:
        seq = [v.strip() for v in str(r.value).split(",")]

    literals = [exp.Literal.string(str(x)) for x in seq if x]
    return exp.In(this=exp.Column(this=r.column), expressions=literals)


def _is_in(r: __RuleCtx) -> exp.Expression:
    """
    Alias for _is_contained_in. Checks if a column's value is in a specified list of values.
    """
    return _is_contained_in(r)


def _not_contained_in(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if a column's value is not contained in a specified list of values.

    Args:
        r (__RuleCtx): Rule context containing the column and the list of values.

    Returns:
        exp.Expression: SQLGlot expression for the not-contained-in validation.
    """
    if isinstance(r.value, (list, tuple)):
        seq = r.value
    else:
        seq = [v.strip() for v in str(r.value).split(",")]

    literals = [exp.Literal.string(str(x)) for x in seq if x]
    return exp.Not(this=exp.In(this=exp.Column(this=r.column), expressions=literals))


def _not_in(r: __RuleCtx) -> exp.Expression:
    """
    Alias for _not_contained_in. Checks if a column's value is not in a specified list of values.
    """
    return _not_contained_in(r)


def _satisfies(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression from a custom SQL string provided in the rule context.

    Args:
        r (__RuleCtx): Rule context containing the custom SQL string in the value attribute.

    Returns:
        exp.Expression: SQLGlot expression parsed from the custom SQL string.
    """
    return sqlglot.parse_one(str(r.value), dialect="bigquery")


def _validate_date_format(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to validate the date format of a column using a specified format string.

    Args:
        r (__RuleCtx): Rule context containing the column and the date format string.

    Returns:
        exp.Expression: SQLGlot expression for the date format validation.
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
    Generates a SQL expression to check if a column's date value is in the future.

    Args:
        r (__RuleCtx): Rule context containing the column to validate.

    Returns:
        exp.Expression: SQLGlot expression for the future date validation.
    """
    return exp.GT(this=exp.Column(this=r.column), expression=exp.CurrentDate())


def _is_past_date(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if a column's date value is in the past.

    Args:
        r (__RuleCtx): Rule context containing the column to validate.

    Returns:
        exp.Expression: SQLGlot expression for the past date validation.
    """
    return exp.LT(this=exp.Column(this=r.column), expression=exp.CurrentDate())


def _is_date_after(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if a column's date value is after a specified date.

    Args:
        r (__RuleCtx): Rule context containing the column and the date to compare.

    Returns:
        exp.Expression: SQLGlot expression for the date-after validation.
    """
    return exp.LT(
        this=exp.Column(this=r.column),
        expression=exp.Anonymous(
            this="DATE", expressions=[exp.Literal.string(r.value)]
        ),
    )


def _is_date_before(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if a column's date value is before a specified date.

    Args:
        r (__RuleCtx): Rule context containing the column and the date to compare.

    Returns:
        exp.Expression: SQLGlot expression for the date-before validation.
    """
    return exp.GT(
        this=exp.Column(this=r.column),
        expression=exp.Anonymous(
            this="DATE", expressions=[exp.Literal.string(r.value)]
        ),
    )


def _is_date_between(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if a column's date value is between two specified dates.

    Args:
        r (__RuleCtx): Rule context containing the column and the date range.

    Returns:
        exp.Expression: SQLGlot expression for the date-between validation.
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
    Generates a SQL expression to check if a column's date value passes all date checks (default: is_past_date).

    Args:
        r (__RuleCtx): Rule context containing the column to validate.

    Returns:
        exp.Expression: SQLGlot expression for the all-date-checks validation.
    """
    return _is_past_date(r)


def _is_today(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if a column's date value is today.

    Args:
        r (__RuleCtx): Rule context containing the column to validate.

    Returns:
        exp.Expression: SQLGlot expression for the today validation.
    """
    return exp.EQ(this=exp.Column(this=r.column), expression=exp.CurrentDate())


def _is_yesterday(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if a column's date value is yesterday.

    Args:
        r (__RuleCtx): Rule context containing the column to validate.

    Returns:
        exp.Expression: SQLGlot expression for the yesterday validation.
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
    Alias for _is_yesterday. Checks if a column's date value is yesterday.
    """
    return _is_yesterday(r)


def _is_t_minus_2(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if a column's date value is two days ago (T-2).

    Args:
        r (__RuleCtx): Rule context containing the column to validate.

    Returns:
        exp.Expression: SQLGlot expression for the T-2 validation.
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
    Generates a SQL expression to check if a column's date value is three days ago (T-3).

    Args:
        r (__RuleCtx): Rule context containing the column to validate.

    Returns:
        exp.Expression: SQLGlot expression for the T-3 validation.
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
    Generates a SQL expression to check if a column's date value falls on a weekday (Monday to Friday).

    Args:
        r (__RuleCtx): Rule context containing the column to validate.

    Returns:
        exp.Expression: SQLGlot expression for the weekday validation.
    """
    dayofweek = exp.Extract(
        this=exp.Var(this="DAYOFWEEK"), expression=exp.Column(this=r.column)
    )
    return exp.Between(
        this=dayofweek, low=exp.Literal.number(2), high=exp.Literal.number(6)
    )


def _is_on_weekend(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if a column's date value falls on a weekend (Saturday or Sunday).

    Args:
        r (__RuleCtx): Rule context containing the column to validate.

    Returns:
        exp.Expression: SQLGlot expression for the weekend validation.
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
    Generates a SQL expression to check if a column's date value falls on a Monday.

    Args:
        r (__RuleCtx): Rule context containing the column to validate.

    Returns:
        exp.Expression: SQLGlot expression for the Monday validation.
    """
    return exp.EQ(
        this=exp.Extract(
            this=exp.Var(this="DAYOFWEEK"), expression=exp.Column(this=r.column)
        ),
        expression=exp.Literal.number(2),
    )


def _is_on_tuesday(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if a column's date value falls on a Tuesday.

    Args:
        r (__RuleCtx): Rule context containing the column to validate.

    Returns:
        exp.Expression: SQLGlot expression for the Tuesday validation.
    """
    return exp.EQ(
        this=exp.Extract(
            this=exp.Var(this="DAYOFWEEK"), expression=exp.Column(this=r.column)
        ),
        expression=exp.Literal.number(3),
    )


def _is_on_wednesday(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if a column's date value falls on a Wednesday.

    Args:
        r (__RuleCtx): Rule context containing the column to validate.

    Returns:
        exp.Expression: SQLGlot expression for the Wednesday validation.
    """
    return exp.EQ(
        this=exp.Extract(
            this=exp.Var(this="DAYOFWEEK"), expression=exp.Column(this=r.column)
        ),
        expression=exp.Literal.number(4),
    )


def _is_on_thursday(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if a column's date value falls on a Thursday.

    Args:
        r (__RuleCtx): Rule context containing the column to validate.

    Returns:
        exp.Expression: SQLGlot expression for the Thursday validation.
    """
    return exp.EQ(
        this=exp.Extract(
            this=exp.Var(this="DAYOFWEEK"), expression=exp.Column(this=r.column)
        ),
        expression=exp.Literal.number(5),
    )


def _is_on_friday(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if a column's date value falls on a Friday.

    Args:
        r (__RuleCtx): Rule context containing the column to validate.

    Returns:
        exp.Expression: SQLGlot expression for the Friday validation.
    """
    return exp.EQ(
        this=exp.Extract(
            this=exp.Var(this="DAYOFWEEK"), expression=exp.Column(this=r.column)
        ),
        expression=exp.Literal.number(6),
    )


def _is_on_saturday(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if a column's date value falls on a Saturday.

    Args:
        r (__RuleCtx): Rule context containing the column to validate.

    Returns:
        exp.Expression: SQLGlot expression for the Saturday validation.
    """
    return exp.EQ(
        this=exp.Extract(
            this=exp.Var(this="DAYOFWEEK"), expression=exp.Column(this=r.column)
        ),
        expression=exp.Literal.number(7),
    )


def _is_on_sunday(r: __RuleCtx) -> exp.Expression:
    """
    Generates a SQL expression to check if a column's date value falls on a Sunday.

    Args:
        r (__RuleCtx): Rule context containing the column to validate.

    Returns:
        exp.Expression: SQLGlot expression for the Sunday validation.
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
    Builds validation SQL using SQLGlot AST generation.

    Args:
        rules: List of validation rules
        table_ref: BigQuery table reference

    Returns:
        str: Generated SQL query with UNION of all rule violations
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
    Validates data using specified quality rules.

    Args:
        client: BigQuery client instance
        table_ref: Table reference (project.dataset.table)
        rules: List of validation rule dictionaries

    Returns:
        Tuple[RowIterator, RowIterator]: (aggregated_results, raw_violations)
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
    Generates SQL representation of rules using SQLGlot.

    Args:
        rules: List of validation rules

    Returns:
        str: SQL query representing all rules
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
    Generates validation summary report from violation results.

    Args:
        df: Row iterator containing validation violations
        rules: List of validation rules that were applied
        total_rows: Total number of rows in the dataset
        client: BigQuery client instance

    Returns:
        List[Dict]: Summary report with pass/fail status for each rule
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
    Counts total rows in a BigQuery table.

    Args:
        client: BigQuery client instance
        table_ref: Table reference (project.dataset.table)

    Returns:
        int: Total number of rows in the table
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
    Converts a BigQuery table schema into a list of dictionaries representing the schema fields.

    Args:
        table (bigquery.Table): The BigQuery table whose schema is to be converted.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries where each dictionary represents a field in the schema.
            Each dictionary contains the following keys:
                - "field": The name of the field.
                - "data_type": The data type of the field, converted to lowercase.
                - "nullable": A boolean indicating whether the field is nullable.
                - "max_length": Always set to None (reserved for future use).
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
    Validates the schema of a BigQuery table against an expected schema.

    Args:
        client (bigquery.Client): The BigQuery client used to interact with the BigQuery service.
        expected (List[Dict[str, Any]]): The expected schema as a list of dictionaries, where each dictionary
            represents a field with its attributes (e.g., name, type, mode).
        table_ref (str): The reference to the BigQuery table (e.g., "project.dataset.table").

    Returns:
        Tuple[bool, List[Tuple[str, str]]]: A tuple where the first element is a boolean indicating whether
            the actual schema matches the expected schema, and the second element is a list of tuples
            describing the differences (if any) between the schemas. Each tuple contains a description
            of the difference and the corresponding field name.
    """

    table = client.get_table(table_ref)
    actual = extract_schema(table)

    result, errors = __compare_schemas(actual, expected)
    return result, errors
