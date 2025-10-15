#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
BigQuery data quality validation engine for Sumeh.

This module provides validation functions for data quality rules in BigQuery using SQLGlot
for SQL generation. It supports various validation types including completeness, uniqueness,
pattern matching, date validations, and numeric comparisons.
"""

import uuid
import warnings
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any, Tuple, Callable, Optional

import pandas as pd
import sqlglot
from google.cloud import bigquery
from sqlglot import exp

from sumeh.core.rules.rule_model import RuleDef
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


def _is_legit(r: __RuleCtx) -> exp.Expression:
    """
    Generates SQL expression to validate that a column contains non-null, non-whitespace values.
    Args:
        r: Rule context containing the column to validate
    Returns:
        SQLGlot expression checking if column is NOT NULL and NOT empty/whitespace only
    """
    # Cast to STRING to handle non-string types safely
    col_str = exp.Cast(this=exp.Column(this=r.column), to=exp.DataType.build("STRING"))
    # Check: NOT NULL AND TRIM(column) != ''
    return exp.Or(
        this=exp.Is(this=col_str, expression=exp.Null()),
        expression=exp.EQ(
            this=exp.Trim(this=col_str), expression=exp.Literal.string("")
        ),
    )


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


def has_std(client: bigquery.Client, table_ref: str, rule: RuleDef) -> dict:
    """Checks if standard deviation meets expectations."""
    field = rule.field
    expected = rule.value
    threshold = rule.threshold if rule.threshold else 1.0

    if expected is None:
        return {
            "status": "ERROR",
            "expected": None,
            "actual": None,
            "message": "Expected value not defined for has_std",
        }

    try:
        sql = f"SELECT STDDEV({field}) as std_val FROM `{table_ref}`"
        result = client.query(sql).result()
        actual = float(list(result)[0]["std_val"] or 0.0)
        expected = float(expected)

        if threshold < 1.0:
            min_val = expected * threshold
            max_val = expected * (2 - threshold)
            passed = min_val <= actual <= max_val
            msg = f"Std {actual:.2f} outside range [{min_val:.2f}, {max_val:.2f}]"
        else:
            passed = actual >= expected
            msg = f"Std {actual:.2f} < expected {expected:.2f}"

        return {
            "status": "PASS" if passed else "FAIL",
            "expected": float(expected),
            "actual": float(actual),
            "message": None if passed else msg,
        }
    except Exception as e:
        return {
            "status": "ERROR",
            "expected": float(expected) if expected is not None else None,
            "actual": None,
            "message": f"Error: {str(e)}",
        }


def has_mean(client: bigquery.Client, table_ref: str, rule: RuleDef) -> dict:
    """Checks if mean meets expectations."""
    field = rule.field
    expected = rule.value
    threshold = rule.threshold if rule.threshold else 1.0

    if expected is None:
        return {
            "status": "ERROR",
            "expected": None,
            "actual": None,
            "message": "Expected value not defined for has_mean",
        }

    try:
        sql = f"SELECT AVG({field}) as mean_val FROM `{table_ref}`"
        result = client.query(sql).result()
        actual = float(list(result)[0]["mean_val"] or 0.0)
        expected = float(expected)

        if threshold < 1.0:
            min_expected = expected * threshold
            passed = actual >= min_expected
            msg = f"Mean {actual:.2f} < minimum {min_expected:.2f} ({threshold * 100}% of {expected})"
        else:
            passed = actual >= expected
            msg = f"Mean {actual:.2f} < expected {expected:.2f}"

        return {
            "status": "PASS" if passed else "FAIL",
            "expected": float(expected),
            "actual": float(actual),
            "message": None if passed else msg,
        }
    except Exception as e:
        return {
            "status": "ERROR",
            "expected": float(expected) if expected is not None else None,
            "actual": None,
            "message": f"Error: {str(e)}",
        }


def has_sum(client: bigquery.Client, table_ref: str, rule: RuleDef) -> dict:
    """Checks if sum meets expectations."""
    field = rule.field
    expected = rule.value
    threshold = rule.threshold if rule.threshold else 1.0

    if expected is None:
        return {
            "status": "ERROR",
            "expected": None,
            "actual": None,
            "message": "Expected value not defined for has_sum",
        }

    try:
        sql = f"SELECT SUM({field}) as sum_val FROM `{table_ref}`"
        result = client.query(sql).result()
        actual = float(list(result)[0]["sum_val"] or 0.0)
        expected = float(expected)

        if threshold < 1.0:
            min_expected = expected * threshold
            passed = actual >= min_expected
            msg = f"Sum {actual:.2f} < minimum {min_expected:.2f} ({threshold * 100}% of {expected})"
        else:
            passed = actual >= expected
            msg = f"Sum {actual:.2f} < expected {expected:.2f}"

        return {
            "status": "PASS" if passed else "FAIL",
            "expected": float(expected),
            "actual": float(actual),
            "message": None if passed else msg,
        }
    except Exception as e:
        return {
            "status": "ERROR",
            "expected": float(expected) if expected is not None else None,
            "actual": None,
            "message": f"Error: {str(e)}",
        }


def has_min(client: bigquery.Client, table_ref: str, rule: RuleDef) -> dict:
    """Checks if minimum value meets expectations."""
    field = rule.field
    expected = rule.value
    threshold = rule.threshold if rule.threshold else 1.0

    if expected is None:
        return {
            "status": "ERROR",
            "expected": None,
            "actual": None,
            "message": "Expected value not defined for has_min",
        }

    try:
        sql = f"SELECT MIN({field}) as min_val FROM `{table_ref}`"
        result = client.query(sql).result()
        actual = float(list(result)[0]["min_val"] or 0.0)
        expected = float(expected)

        if threshold < 1.0:
            min_expected = expected * threshold
            passed = actual >= min_expected
            msg = f"Min {actual:.2f} < minimum {min_expected:.2f} ({threshold * 100}% of {expected})"
        else:
            passed = actual >= expected
            msg = f"Min {actual:.2f} < expected {expected:.2f}"

        return {
            "status": "PASS" if passed else "FAIL",
            "expected": float(expected),
            "actual": float(actual),
            "message": None if passed else msg,
        }
    except Exception as e:
        return {
            "status": "ERROR",
            "expected": float(expected) if expected is not None else None,
            "actual": None,
            "message": f"Error: {str(e)}",
        }


def has_max(client: bigquery.Client, table_ref: str, rule: RuleDef) -> dict:
    """Checks if maximum value meets expectations."""
    field = rule.field
    expected = rule.value
    threshold = rule.threshold if rule.threshold else 1.0

    if expected is None:
        return {
            "status": "ERROR",
            "expected": None,
            "actual": None,
            "message": "Expected value not defined for has_max",
        }

    try:
        sql = f"SELECT MAX({field}) as max_val FROM `{table_ref}`"
        result = client.query(sql).result()
        actual = float(list(result)[0]["max_val"] or 0.0)
        expected = float(expected)

        if threshold < 1.0:
            max_expected = expected * threshold
            passed = actual <= max_expected
            msg = f"Max {actual:.2f} > maximum {max_expected:.2f} ({threshold * 100}% of {expected})"
        else:
            passed = actual <= expected
            msg = f"Max {actual:.2f} > expected {expected:.2f}"

        return {
            "status": "PASS" if passed else "FAIL",
            "expected": float(expected),
            "actual": float(actual),
            "message": None if passed else msg,
        }
    except Exception as e:
        return {
            "status": "ERROR",
            "expected": float(expected) if expected is not None else None,
            "actual": None,
            "message": f"Error: {str(e)}",
        }


def has_cardinality(client: bigquery.Client, table_ref: str, rule: RuleDef) -> dict:
    """Checks if cardinality meets expectations."""
    field = rule.field
    expected = rule.value
    threshold = rule.threshold if rule.threshold else 1.0

    if expected is None:
        return {
            "status": "ERROR",
            "expected": None,
            "actual": None,
            "message": "Expected value not defined for has_cardinality",
        }

    try:
        sql = f"SELECT COUNT(DISTINCT {field}) as card_val FROM `{table_ref}`"
        result = client.query(sql).result()
        actual = int(list(result)[0]["card_val"] or 0)
        expected = int(expected)

        if threshold < 1.0:
            min_expected = int(expected * threshold)
            passed = actual >= min_expected
            msg = f"Cardinality {actual} < minimum {min_expected} ({threshold * 100}% of {expected})"
        else:
            passed = actual >= expected
            msg = f"Cardinality {actual} < expected {expected}"

        return {
            "status": "PASS" if passed else "FAIL",
            "expected": float(expected),
            "actual": float(actual),
            "message": None if passed else msg,
        }
    except Exception as e:
        return {
            "status": "ERROR",
            "expected": float(expected) if expected is not None else None,
            "actual": None,
            "message": f"Error: {str(e)}",
        }


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
    "is_legit": _is_legit,
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


def _build_union_sql(rules: List[RuleDef], table_ref: str) -> str:
    """Constructs UNION ALL SQL query for row-level validations using SQLGlot."""
    table_expr = _parse_table_ref(table_ref)
    queries = []

    for rule in rules:
        check = rule.check_type

        # Handle aliases
        if check == "is_primary_key":
            check = "is_unique"
        elif check == "is_composite_key":
            check = "are_unique"
        elif check == "is_yesterday":
            check = "is_t_minus_1"
        elif check == "is_in":
            check = "is_contained_in"
        elif check == "not_in":
            check = "not_contained_in"

        if check in __RULE_DISPATCH_SIMPLE:
            builder = __RULE_DISPATCH_SIMPLE[check]
            needs_table = False
        elif check in __RULE_DISPATCH_WITH_TABLE:
            builder = __RULE_DISPATCH_WITH_TABLE[check]
            needs_table = True
        else:
            warnings.warn(f"❌ Unknown rule: {check}")
            continue

        field_val = rule.field if isinstance(rule.field, str) else rule.field
        ctx = __RuleCtx(
            column=field_val,
            value=rule.value,
            name=check,
        )

        try:
            if needs_table:
                expr_ok = builder(ctx, table_expr)
            else:
                expr_ok = builder(ctx)

            dq_tag = f"{ctx.column}:{check}:{rule.value}"

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
        except Exception as e:
            warnings.warn(f"❌ Error building SQL for {check} on {rule.field}: {e}")
            continue

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


def validate_row_level(
    client: bigquery.Client, table_ref: str, rules: List[RuleDef]
) -> Tuple[bigquery.table.RowIterator, bigquery.table.RowIterator]:
    """
    Validates BigQuery table at row level using specified rules.

    Args:
        client: BigQuery client instance
        table_ref: Fully qualified table reference
        rules: List of row-level validation rules

    Returns:
        Tuple of (aggregated results, raw violations)
    """
    engine = "bigquery"
    rules_valid = []
    rules_ignored = []
    ignored_reasons = {}

    for rule in rules:
        skip_reason = rule.get_skip_reason(target_level="row_level", engine=engine)

        if skip_reason:
            rules_ignored.append(rule)
            ignored_reasons[skip_reason] = ignored_reasons.get(skip_reason, 0) + 1
        else:
            rules_valid.append(rule)

    if rules_ignored:
        warnings.warn(
            f"⚠️  {len(rules_ignored)}/{len(rules)} rules ignored:\n"
            + "\n".join(
                f"  • {reason}: {count} rule(s)"
                for reason, count in ignored_reasons.items()
            )
        )

    if not rules_valid:
        warnings.warn(
            f"No valid rules to execute for level='row_level' and engine='{engine}'."
        )
        # Return empty results
        empty_query = f"SELECT * FROM `{table_ref}` WHERE FALSE"
        empty = client.query(empty_query).result()
        return empty, empty

    union_sql = _build_union_sql(rules_valid, table_ref)

    violations_subquery = sqlglot.parse_one(union_sql, dialect="bigquery")

    table = client.get_table(table_ref)
    cols = [exp.Column(this=f.name) for f in table.schema]

    # Raw violations
    raw_query = (
        exp.Select(expressions=cols + [exp.Column(this="dq_status")])
        .with_("violations", as_=violations_subquery)
        .from_("violations")
    )
    raw_sql = raw_query.sql(dialect="bigquery")

    # Aggregated violations
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


def validate_table_level(
    client: bigquery.Client, table_ref: str, rules: List[RuleDef]
) -> pd.DataFrame:
    """
    Validates BigQuery table at table level using specified rules.

    Args:
        client: BigQuery client instance
        table_ref: Fully qualified table reference
        rules: List of table-level validation rules

    Returns:
        DataFrame with validation results
    """
    engine = "bigquery"
    rules_valid = []
    rules_ignored = []
    ignored_reasons = {}

    for rule in rules:
        skip_reason = rule.get_skip_reason(target_level="table_level", engine=engine)

        if skip_reason:
            rules_ignored.append(rule)
            ignored_reasons[skip_reason] = ignored_reasons.get(skip_reason, 0) + 1
        else:
            rules_valid.append(rule)

    if rules_ignored:
        warnings.warn(
            f"⚠️  {len(rules_ignored)}/{len(rules)} rules ignored:\n"
            + "\n".join(
                f"  • {reason}: {count} rule(s)"
                for reason, count in ignored_reasons.items()
            )
        )

    if not rules_valid:
        warnings.warn(
            f"No valid rules to execute for level='table_level' and engine='{engine}'."
        )
        return pd.DataFrame(
            columns=[
                "id",
                "timestamp",
                "level",
                "category",
                "check_type",
                "field",
                "status",
                "expected",
                "actual",
                "message",
            ]
        )

    execution_time = datetime.utcnow()
    results = []

    for rule in rules_valid:
        check_type = rule.check_type

        fn = globals().get(check_type)
        if fn is None:
            warnings.warn(f"❌ Function not found: {check_type} for field {rule.field}")
            results.append(
                {
                    "id": str(uuid.uuid4()),
                    "timestamp": execution_time,
                    "level": "TABLE",
                    "category": rule.category or "unknown",
                    "check_type": check_type,
                    "field": str(rule.field),
                    "status": "ERROR",
                    "expected": None,
                    "actual": None,
                    "message": f"Function '{check_type}' not implemented",
                }
            )
            continue

        try:
            result = fn(client, table_ref, rule)

            results.append(
                {
                    "id": str(uuid.uuid4()),
                    "timestamp": execution_time,
                    "level": "TABLE",
                    "category": rule.category or "unknown",
                    "check_type": check_type,
                    "field": str(rule.field),
                    "status": result.get("status", "ERROR"),
                    "expected": result.get("expected"),
                    "actual": result.get("actual"),
                    "message": result.get("message"),
                }
            )

        except Exception as e:
            warnings.warn(f"❌ Error executing {check_type} on {rule.field}: {e}")
            results.append(
                {
                    "id": str(uuid.uuid4()),
                    "timestamp": execution_time,
                    "level": "TABLE",
                    "category": rule.category or "unknown",
                    "check_type": check_type,
                    "field": str(rule.field),
                    "status": "ERROR",
                    "expected": None,
                    "actual": None,
                    "message": f"Execution error: {str(e)}",
                }
            )

    if not results:
        return pd.DataFrame(
            columns=[
                "id",
                "timestamp",
                "level",
                "category",
                "check_type",
                "field",
                "status",
                "expected",
                "actual",
                "message",
            ]
        )

    summary_df = pd.DataFrame(results)

    # Sort
    summary_df["_sort"] = summary_df["status"].map({"FAIL": 0, "ERROR": 1, "PASS": 2})
    summary_df = (
        summary_df.sort_values("_sort").drop(columns=["_sort"]).reset_index(drop=True)
    )

    return summary_df


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

    row_rules = [
        r for r in rules if r.level and r.level.upper().replace("_LEVEL", "") == "ROW"
    ]
    table_rules = [
        r for r in rules if r.level and r.level.upper().replace("_LEVEL", "") == "TABLE"
    ]

    no_level = [r for r in rules if not r.level]
    if no_level:
        warnings.warn(
            f"⚠️  {len(no_level)} rule(s) without level defined. "
            f"These will be skipped. Please set 'level' to 'ROW' or 'TABLE'."
        )

    if row_rules:
        final, raw = validate_row_level(client, table_ref, row_rules)
    else:
        empty_query = f"SELECT * FROM `{table_ref}` WHERE FALSE"
        empty = client.query(empty_query).result()
        final, raw = empty, empty

    if table_rules:
        table_summary = validate_table_level(client, table_ref, table_rules)
    else:
        table_summary = pd.DataFrame(
            columns=[
                "id",
                "timestamp",
                "level",
                "category",
                "check_type",
                "field",
                "status",
                "expected",
                "actual",
                "message",
            ]
        )

    return final, raw, table_summary


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
    rules: List[RuleDef],
    total_rows: int,
    df_with_errors: Optional[bigquery.table.RowIterator] = None,
    table_error: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Summarizes validation results from both row-level and table-level checks.

    Args:
        rules: List of all validation rules
        total_rows: Total number of rows in table
        df_with_errors: Row iterator with row-level violations
        table_error: DataFrame with table-level results

    Returns:
        Summary DataFrame with aggregated metrics
    """
    summaries = []

    # ========== ROW-LEVEL SUMMARY ==========
    row_rules = [
        r for r in rules if r.level and r.level.upper().replace("_LEVEL", "") == "ROW"
    ]

    if row_rules and df_with_errors is not None:
        violations_count = {}
        for row in df_with_errors:
            dq_status = row.get("dq_status", "")
            if dq_status and dq_status.strip():
                # Split with limit to handle colons in values
                parts = dq_status.split(":", 2)
                if len(parts) >= 2:
                    field = parts[0]
                    check_type = parts[1]
                    key = (field, check_type)
                    violations_count[key] = violations_count.get(key, 0) + 1

        for rule in row_rules:
            field_str = (
                rule.field if isinstance(rule.field, str) else ",".join(rule.field)
            )

            violations = violations_count.get((field_str, rule.check_type), 0)
            pass_count = total_rows - violations
            pass_rate = pass_count / total_rows if total_rows > 0 else 1.0
            pass_threshold = rule.threshold if rule.threshold else 1.0

            status = "PASS" if pass_rate >= pass_threshold else "FAIL"

            expected_val = None
            if rule.value is not None:
                if isinstance(rule.value, (int, float)):
                    expected_val = float(rule.value)
                elif isinstance(rule.value, str):
                    try:
                        expected_val = float(rule.value)
                    except (ValueError, TypeError):
                        expected_val = None

            summaries.append(
                {
                    "id": str(uuid.uuid4()),
                    "timestamp": datetime.utcnow(),
                    "level": "ROW",
                    "category": rule.category or "unknown",
                    "check_type": rule.check_type,
                    "field": field_str,
                    "rows": total_rows,
                    "violations": violations,
                    "pass_rate": round(pass_rate, 4),
                    "pass_threshold": pass_threshold,
                    "status": status,
                    "expected": expected_val,
                    "actual": None,
                    "message": (
                        None
                        if status == "PASS"
                        else f"{violations} row(s) failed validation"
                    ),
                }
            )

    # ========== TABLE-LEVEL SUMMARY ==========
    if table_error is not None and len(table_error) > 0:
        for _, row_dict in table_error.iterrows():
            expected = row_dict.get("expected")
            actual = row_dict.get("actual")

            if pd.notna(expected) and pd.notna(actual) and expected != 0:
                compliance_rate = round(actual / expected, 4)
            else:
                compliance_rate = None

            summaries.append(
                {
                    "id": row_dict["id"],
                    "timestamp": row_dict["timestamp"],
                    "level": row_dict["level"],
                    "category": row_dict["category"],
                    "check_type": row_dict["check_type"],
                    "field": row_dict["field"],
                    "status": row_dict["status"],
                    "expected": expected,
                    "actual": actual,
                    "pass_rate": compliance_rate,
                    "message": row_dict["message"],
                }
            )

    if not summaries:
        return pd.DataFrame(
            columns=[
                "id",
                "timestamp",
                "level",
                "category",
                "check_type",
                "field",
                "rows",
                "violations",
                "pass_rate",
                "pass_threshold",
                "status",
                "expected",
                "actual",
                "message",
            ]
        )

    summary_df = pd.DataFrame(summaries)

    # Sort
    summary_df["_sort_status"] = summary_df["status"].map(
        {"FAIL": 0, "ERROR": 1, "PASS": 2}
    )
    summary_df["_sort_level"] = summary_df["level"].map({"ROW": 0, "TABLE": 1})

    summary_df = (
        summary_df.sort_values(["_sort_status", "_sort_level", "check_type"])
        .drop(columns=["_sort_status", "_sort_level"])
        .reset_index(drop=True)
    )

    return summary_df


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
