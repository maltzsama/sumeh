#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module provides utilities for generating and validating SQL expressions
and data quality rules using DuckDB. It includes functions for building SQL
expressions, validating dataframes against rules, summarizing rule violations,
and schema validation.

Classes:
    __RuleCtx: A dataclass representing the context required to generate SQL
              expressions for data quality rules.

Functions:
    __escape_single_quotes: Escapes single quotes in a string for SQL compatibility.

    __format_sequence: Formats a sequence (list, tuple, or string) into a SQL-compatible representation for IN/NOT IN clauses.

    _is_complete: Generates a SQL expression to check if a column is not NULL.

    _are_complete: Generates a SQL expression to check if all columns in a list are not NULL.

    _is_unique: Generates a SQL expression to check if a column has unique values.

    _are_unique: Generates a SQL expression to check if a combination of columns has unique values.

    _is_greater_than: Generates a SQL expression to check if a column's value is greater than a given value.

    _is_less_than: Generates a SQL expression to check if a column's value is less than a given value.

    _is_greater_or_equal_than: Generates a SQL expression to check if a column's value is greater than or equal to a given value.

    _is_less_or_equal_than: Generates a SQL expression to check if a column's value is less than or equal to a given value.

    _is_equal_than: Generates a SQL expression to check if a column's value is equal to a given value.

    _is_between: Generates a SQL expression to check if a column's value is between two values.

    _has_pattern: Generates a SQL expression to check if a column's value matches a regular expression pattern.

    _is_contained_in: Generates a SQL expression to check if a column's value is in a given sequence.

    _not_contained_in: Generates a SQL expression to check if a column's value is not in a given sequence.

    _satisfies: Generates a SQL expression based on a custom condition provided as a string.

    _build_union_sql: Builds a SQL query that combines multiple rule-based conditions into a UNION ALL query.

    validate: Validates a DuckDB dataframe against a set of rules and returns the results.

    summarize: Summarizes rule violations and calculates pass rates for each rule.

    validate_schema: Validates the schema of a DuckDB table against an expected schema.
"""

# !/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
import uuid
import warnings
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Callable, Any, Optional, Tuple

import duckdb as dk
from duckdb import DuckDBPyRelation
from sqlglot import exp, parse_one
from sqlglot.expressions import Column, Literal

from sumeh.core.rules.rule_model import RuleDef
from sumeh.core.utils import __compare_schemas


def __escape_single_quotes(txt: str) -> str:
    """Escapes single quotes for SQL compatibility."""
    return txt.replace("'", "''")


def __format_sequence(value: Any) -> List[str]:
    """
    Formats a sequence into a list of values for IN/NOT IN clauses.

    Args:
        value (Any): Input value (string, list, or tuple).

    Returns:
        List[str]: List of formatted values.
    """
    if value is None:
        raise ValueError("value cannot be None for IN/NOT IN")

    vals = re.findall(r"'([^']*)'", str(value)) or [
        v.strip() for v in str(value).strip("[]").split(",")
    ]
    return [str(x).strip() for x in vals if x != ""]


@dataclass(slots=True)
class __RuleCtx:
    """
    Context class used to generate SQL expressions for data quality rules.

    Attributes:
        column (Any): Column(s) to apply the rule.
        value (Any): Value associated with the rule.
        name (str): Name of the rule (check_type).
    """

    column: Any
    value: Any
    name: str


# ========== SQLGLOT-BASED SQL EXPRESSION BUILDERS ==========


def _is_positive(r: __RuleCtx) -> exp.Expression:
    """Generates SQL expression: column < 0 (violation)."""
    return exp.LT(this=Column(this=r.column), expression=Literal.number(0))


def _is_negative(r: __RuleCtx) -> exp.Expression:
    """Generates SQL expression: column >= 0 (violation)."""
    return exp.GTE(this=Column(this=r.column), expression=Literal.number(0))


def _is_complete(r: __RuleCtx) -> exp.Expression:
    """Generates SQL expression: column IS NOT NULL."""
    return exp.Is(this=Column(this=r.column), expression=exp.Null()).not_()


def _are_complete(r: __RuleCtx) -> exp.Expression:
    """Generates SQL expression: all columns IS NOT NULL."""
    conditions = [
        exp.Is(this=Column(this=c), expression=exp.Null()).not_() for c in r.column
    ]
    return exp.And(expressions=conditions)


def _is_unique(r: __RuleCtx) -> exp.Expression:
    """Generates SQL expression to check uniqueness using subquery."""
    # (SELECT COUNT(*) FROM tbl AS d2 WHERE d2.column = tbl.column) = 1
    subquery = (
        exp.Select(expressions=[exp.Count(this=exp.Star())])
        .from_(exp.alias_(exp.Table(this="tbl"), "d2"))
        .where(
            exp.EQ(
                this=Column(this=r.column, table="d2"),
                expression=Column(this=r.column, table="tbl"),
            )
        )
    )

    return exp.EQ(this=exp.Paren(this=subquery), expression=Literal.number(1))


def _are_unique(r: __RuleCtx) -> exp.Expression:
    """Generates SQL expression to check composite key uniqueness."""
    # Build concatenation for outer table
    outer_parts = [Column(this=c, table="tbl") for c in r.column]
    outer_concat = exp.Concat(expressions=outer_parts, separator="|")

    # Build concatenation for inner table
    inner_parts = [Column(this=c, table="d2") for c in r.column]
    inner_concat = exp.Concat(expressions=inner_parts, separator="|")

    # Subquery
    subquery = (
        exp.Select(expressions=[exp.Count(this=exp.Star())])
        .from_(exp.alias_(exp.Table(this="tbl"), "d2"))
        .where(exp.EQ(this=inner_concat, expression=outer_concat))
    )

    return exp.EQ(this=exp.Paren(this=subquery), expression=Literal.number(1))


def _is_greater_than(r: __RuleCtx) -> exp.Expression:
    """Generates SQL expression: column <= value (violation)."""
    return exp.LTE(
        this=Column(this=r.column),
        expression=(
            Literal.number(r.value)
            if isinstance(r.value, (int, float))
            else Literal.string(r.value)
        ),
    )


def _is_greater_or_equal_than(r: __RuleCtx) -> exp.Expression:
    """Generates SQL expression: column < value (violation)."""
    return exp.LT(
        this=Column(this=r.column),
        expression=(
            Literal.number(r.value)
            if isinstance(r.value, (int, float))
            else Literal.string(r.value)
        ),
    )


def _is_less_than(r: __RuleCtx) -> exp.Expression:
    """Generates SQL expression: column >= value (violation)."""
    return exp.GTE(
        this=Column(this=r.column),
        expression=(
            Literal.number(r.value)
            if isinstance(r.value, (int, float))
            else Literal.string(r.value)
        ),
    )


def _is_less_or_equal_than(r: __RuleCtx) -> exp.Expression:
    """Generates SQL expression: column > value (violation)."""
    return exp.GT(
        this=Column(this=r.column),
        expression=(
            Literal.number(r.value)
            if isinstance(r.value, (int, float))
            else Literal.string(r.value)
        ),
    )


def _is_equal(r: __RuleCtx) -> exp.Expression:
    """Generates SQL expression: column = value."""
    return exp.EQ(
        this=Column(this=r.column),
        expression=(
            Literal.number(r.value)
            if isinstance(r.value, (int, float))
            else Literal.string(r.value)
        ),
    )


def _is_equal_than(r: __RuleCtx) -> exp.Expression:
    """Alias for is_equal."""
    return _is_equal(r)


def _is_contained_in(r: __RuleCtx) -> exp.Expression:
    """Generates SQL expression: column IN (values)."""
    vals = __format_sequence(r.value)
    return exp.In(
        this=Column(this=r.column), expressions=[Literal.string(v) for v in vals]
    )


def _is_in(r: __RuleCtx) -> exp.Expression:
    """Alias for is_contained_in."""
    return _is_contained_in(r)


def _not_contained_in(r: __RuleCtx) -> exp.Expression:
    """Generates SQL expression: column NOT IN (values)."""
    vals = __format_sequence(r.value)
    return exp.In(
        this=Column(this=r.column), expressions=[Literal.string(v) for v in vals]
    ).not_()


def _not_in(r: __RuleCtx) -> exp.Expression:
    """Alias for not_contained_in."""
    return _not_contained_in(r)


def _is_between(r: __RuleCtx) -> exp.Expression:
    """Generates SQL expression: column BETWEEN lo AND hi."""
    lo, hi = [v.strip() for v in str(r.value).strip("[]").split(",")]
    return exp.Between(
        this=Column(this=r.column),
        low=(
            Literal.number(lo)
            if lo.replace(".", "").replace("-", "").isdigit()
            else Literal.string(lo)
        ),
        high=(
            Literal.number(hi)
            if hi.replace(".", "").replace("-", "").isdigit()
            else Literal.string(hi)
        ),
    )


def _has_pattern(r: __RuleCtx) -> exp.Expression:
    """Generates SQL expression: REGEXP_MATCHES(column, pattern)."""
    column_expr = Column(this=r.column)

    # For regex operations, we need string types
    # Cast to VARCHAR to ensure compatibility
    string_column = exp.Cast(this=column_expr, to=exp.DataType.build("VARCHAR"))

    return exp.Anonymous(
        this="REGEXP_MATCHES", expressions=[string_column, Literal.string(str(r.value))]
    )


def _is_legit(r: __RuleCtx) -> exp.Expression:
    """Generates SQL expression: column IS NOT NULL AND REGEXP_MATCHES(CAST(column AS VARCHAR), '^\\S+$')."""
    return exp.And(
        expressions=[
            exp.Is(this=Column(this=r.column), expression=exp.Null()).not_(),
            exp.Anonymous(
                this="REGEXP_MATCHES",
                expressions=[
                    exp.Cast(
                        this=Column(this=r.column), to=exp.DataType.build("VARCHAR")
                    ),
                    Literal.string(r"^\S+$"),
                ],
            ),
        ]
    )


def _satisfies(r: __RuleCtx) -> exp.Expression:
    """Returns custom SQL expression parsed from string."""
    return exp.Paren(this=parse_one(r.value, dialect="duckdb"))


# ========== DATE VALIDATION SQL BUILDERS ==========


def _validate_date_format(r: __RuleCtx) -> exp.Expression:
    """Validates date format using regex."""
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
        expressions=[
            exp.Is(this=Column(this=r.column), expression=exp.Null()),
            exp.Anonymous(
                this="REGEXP_MATCHES",
                expressions=[
                    exp.Cast(
                        this=Column(this=r.column), to=exp.DataType.build("VARCHAR")
                    ),
                    Literal.string(f"^{regex}$"),
                ],
            ).not_(),
        ]
    )


def _is_future_date(r: __RuleCtx) -> exp.Expression:
    """Checks if date is in the future."""
    return exp.GT(this=Column(this=r.column), expression=exp.CurrentDate())


def _is_past_date(r: __RuleCtx) -> exp.Expression:
    """Checks if date is in the past."""
    return exp.LT(this=Column(this=r.column), expression=exp.CurrentDate())


def _is_date_after(r: __RuleCtx) -> exp.Expression:
    """Checks if date is before target (violation)."""
    return exp.LT(
        this=Column(this=r.column),
        expression=exp.Cast(
            this=Literal.string(r.value), to=exp.DataType.build("DATE")
        ),
    )


def _is_date_before(r: __RuleCtx) -> exp.Expression:
    """Checks if date is after target (violation)."""
    return exp.GT(
        this=Column(this=r.column),
        expression=exp.Cast(
            this=Literal.string(r.value), to=exp.DataType.build("DATE")
        ),
    )


def _is_date_between(r: __RuleCtx) -> exp.Expression:
    """Checks if date is not between range (violation)."""
    start, end = [d.strip() for d in r.value.strip("[]").split(",")]
    between_expr = exp.Between(
        this=Column(this=r.column),
        low=exp.Cast(this=Literal.string(start), to=exp.DataType.build("DATE")),
        high=exp.Cast(this=Literal.string(end), to=exp.DataType.build("DATE")),
    )
    return exp.Not(this=between_expr)


def _all_date_checks(r: __RuleCtx) -> exp.Expression:
    """Default date check."""
    return _is_past_date(r)


def _is_in_millions(r: __RuleCtx) -> exp.Expression:
    """Checks if value < 1,000,000 (violation)."""
    return exp.LT(this=Column(this=r.column), expression=Literal.number(1000000))


def _is_in_billions(r: __RuleCtx) -> exp.Expression:
    """Checks if value < 1,000,000,000 (violation)."""
    return exp.LT(this=Column(this=r.column), expression=Literal.number(1000000000))


def _is_today(r: __RuleCtx) -> exp.Expression:
    """Checks if date equals today."""
    return exp.EQ(this=Column(this=r.column), expression=exp.CurrentDate())


def _is_yesterday(r: __RuleCtx) -> exp.Expression:
    """Checks if date equals yesterday."""
    return exp.EQ(
        this=Column(this=r.column),
        expression=exp.Sub(this=exp.CurrentDate(), expression=Literal.number(1)),
    )


def _is_t_minus_1(r: __RuleCtx) -> exp.Expression:
    """Checks if date equals yesterday."""
    return exp.EQ(
        this=Column(this=r.column),
        expression=exp.Sub(this=exp.CurrentDate(), expression=Literal.number(1)),
    )


def _is_t_minus_2(r: __RuleCtx) -> exp.Expression:
    """Checks if date equals T-2."""
    return exp.EQ(
        this=Column(this=r.column),
        expression=exp.Sub(this=exp.CurrentDate(), expression=Literal.number(2)),
    )


def _is_t_minus_3(r: __RuleCtx) -> exp.Expression:
    """Checks if date equals T-3."""
    return exp.EQ(
        this=Column(this=r.column),
        expression=exp.Sub(this=exp.CurrentDate(), expression=Literal.number(3)),
    )


def _is_on_weekday(r: __RuleCtx) -> exp.Expression:
    """Checks if date falls on weekday (Mon-Fri)."""
    dow = exp.Extract(this=Literal.string("DOW"), expression=Column(this=r.column))
    return exp.Between(this=dow, low=Literal.number(1), high=Literal.number(5))


def _is_on_weekend(r: __RuleCtx) -> exp.Expression:
    """Checks if date falls on weekend (Sat-Sun)."""
    dow = exp.Extract(this=Literal.string("DOW"), expression=Column(this=r.column))
    return exp.Or(
        expressions=[
            exp.EQ(this=dow, expression=Literal.number(0)),
            exp.EQ(this=dow, expression=Literal.number(6)),
        ]
    )


def _is_on_monday(r: __RuleCtx) -> exp.Expression:
    """Checks if date is Monday."""
    return exp.EQ(
        this=exp.Extract(this=Literal.string("DOW"), expression=Column(this=r.column)),
        expression=Literal.number(1),
    )


def _is_on_tuesday(r: __RuleCtx) -> exp.Expression:
    """Checks if date is Tuesday."""
    return exp.EQ(
        this=exp.Extract(this=Literal.string("DOW"), expression=Column(this=r.column)),
        expression=Literal.number(2),
    )


def _is_on_wednesday(r: __RuleCtx) -> exp.Expression:
    """Checks if date is Wednesday."""
    return exp.EQ(
        this=exp.Extract(this=Literal.string("DOW"), expression=Column(this=r.column)),
        expression=Literal.number(3),
    )


def _is_on_thursday(r: __RuleCtx) -> exp.Expression:
    """Checks if date is Thursday."""
    return exp.EQ(
        this=exp.Extract(this=Literal.string("DOW"), expression=Column(this=r.column)),
        expression=Literal.number(4),
    )


def _is_on_friday(r: __RuleCtx) -> exp.Expression:
    """Checks if date is Friday."""
    return exp.EQ(
        this=exp.Extract(this=Literal.string("DOW"), expression=Column(this=r.column)),
        expression=Literal.number(5),
    )


def _is_on_saturday(r: __RuleCtx) -> exp.Expression:
    """Checks if date is Saturday."""
    return exp.EQ(
        this=exp.Extract(this=Literal.string("DOW"), expression=Column(this=r.column)),
        expression=Literal.number(6),
    )


def _is_on_sunday(r: __RuleCtx) -> exp.Expression:
    """Checks if date is Sunday."""
    return exp.EQ(
        this=exp.Extract(this=Literal.string("DOW"), expression=Column(this=r.column)),
        expression=Literal.number(0),
    )


# ========== RULE DISPATCH TABLE ==========

__RULE_DISPATCH: dict[str, Callable[[__RuleCtx], exp.Expression]] = {
    "is_positive": _is_positive,
    "is_negative": _is_negative,
    "is_complete": _is_complete,
    "are_complete": _are_complete,
    "is_unique": _is_unique,
    "are_unique": _are_unique,
    "is_greater_than": _is_greater_than,
    "is_less_than": _is_less_than,
    "is_greater_or_equal_than": _is_greater_or_equal_than,
    "is_less_or_equal_than": _is_less_or_equal_than,
    "is_equal": _is_equal,
    "is_equal_than": _is_equal_than,
    "is_in_millions": _is_in_millions,
    "is_in_billions": _is_in_billions,
    "is_between": _is_between,
    "has_pattern": _has_pattern,
    "is_legit": _is_legit,
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
    "is_today": _is_today,
    "is_yesterday": _is_yesterday,
    "is_t_minus_2": _is_t_minus_2,
    "is_t_minus_3": _is_t_minus_3,
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


# ========== TABLE-LEVEL VALIDATION FUNCTIONS ==========


def has_std(conn: dk.DuckDBPyConnection, rule: RuleDef) -> dict:
    """Checks if the standard deviation meets expectations."""
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
        result = conn.execute(f"SELECT STDDEV({field}) FROM tbl").fetchone()
        actual = float(result[0]) if result[0] is not None else 0.0
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


def has_mean(conn: dk.DuckDBPyConnection, rule: RuleDef) -> dict:
    """Checks if the mean meets expectations."""
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
        result = conn.execute(f"SELECT AVG({field}) FROM tbl").fetchone()
        actual = float(result[0]) if result[0] is not None else 0.0
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


def has_sum(conn: dk.DuckDBPyConnection, rule: RuleDef) -> dict:
    """Checks if the sum meets expectations."""
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
        result = conn.execute(f"SELECT SUM({field}) FROM tbl").fetchone()
        actual = float(result[0]) if result[0] is not None else 0.0
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


def has_min(conn: dk.DuckDBPyConnection, rule: RuleDef) -> dict:
    """Checks if the minimum value meets expectations."""
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
        result = conn.execute(f"SELECT MIN({field}) FROM tbl").fetchone()
        actual = float(result[0]) if result[0] is not None else 0.0
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


def has_max(conn: dk.DuckDBPyConnection, rule: RuleDef) -> dict:
    """Checks if the maximum value meets expectations."""
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
        result = conn.execute(f"SELECT MAX({field}) FROM tbl").fetchone()
        actual = float(result[0]) if result[0] is not None else 0.0
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


def has_cardinality(conn: dk.DuckDBPyConnection, rule: RuleDef) -> dict:
    """Checks if the cardinality meets expectations."""
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
        result = conn.execute(f"SELECT COUNT(DISTINCT {field}) FROM tbl").fetchone()
        actual = int(result[0]) if result[0] is not None else 0
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


def has_infogain(conn: dk.DuckDBPyConnection, rule: RuleDef) -> dict:
    """Checks if the information gain meets expectations."""
    field = rule.field
    expected = rule.value
    threshold = rule.threshold if rule.threshold else 1.0

    if expected is None:
        return {
            "status": "ERROR",
            "expected": None,
            "actual": None,
            "message": "Expected value not defined",
        }

    try:
        import numpy as np

        sql = f"""
        WITH counts AS (
            SELECT {field}, COUNT(*) as cnt
            FROM tbl
            GROUP BY {field}
        ),
        total AS (SELECT COUNT(*) as total FROM tbl),
        probs AS (
            SELECT 
                cnt::DOUBLE / total as probability
            FROM counts, total
        )
        SELECT -SUM(probability * LOG2(probability)) as entropy,
               (SELECT COUNT(DISTINCT {field}) FROM tbl) as n_unique
        FROM probs
        """
        result = conn.execute(sql).fetchone()
        entropy = float(result[0]) if result[0] is not None else 0.0
        n_unique = int(result[1]) if result[1] is not None else 1

        max_entropy = float(np.log2(n_unique)) if n_unique > 1 else 1.0
        info_gain = entropy / max_entropy if max_entropy > 0 else 0.0
        actual = float(info_gain)
        expected = float(expected)

        min_acceptable = expected * threshold
        passed = actual >= min_acceptable

        return {
            "status": "PASS" if passed else "FAIL",
            "expected": float(expected),
            "actual": float(actual),
            "message": (
                None
                if passed
                else f"Info gain {actual:.4f} < minimum {min_acceptable:.4f} (threshold: {threshold * 100:.0f}%)"
            ),
        }
    except Exception as e:
        return {
            "status": "ERROR",
            "expected": float(expected) if expected is not None else None,
            "actual": None,
            "message": f"Error: {str(e)}",
        }


def has_entropy(conn: dk.DuckDBPyConnection, rule: RuleDef) -> dict:
    """Checks if the entropy meets expectations."""
    field = rule.field
    expected = rule.value
    threshold = rule.threshold if rule.threshold else 1.0

    if expected is None:
        return {
            "status": "ERROR",
            "expected": None,
            "actual": None,
            "message": "Expected value not defined",
        }

    try:
        sql = f"""
        WITH counts AS (
            SELECT {field}, COUNT(*) as cnt
            FROM tbl
            GROUP BY {field}
        ),
        total AS (SELECT COUNT(*) as total FROM tbl),
        probs AS (
            SELECT 
                cnt::DOUBLE / total as probability
            FROM counts, total
        )
        SELECT -SUM(probability * LOG2(probability)) as entropy
        FROM probs
        """
        result = conn.execute(sql).fetchone()
        actual = float(result[0]) if result[0] is not None else 0.0
        expected = float(expected)

        min_acceptable = expected * threshold
        passed = actual >= min_acceptable

        return {
            "status": "PASS" if passed else "FAIL",
            "expected": float(expected),
            "actual": float(actual),
            "message": (
                None
                if passed
                else f"Entropy {actual:.4f} < minimum {min_acceptable:.4f} (threshold: {threshold * 100:.0f}%)"
            ),
        }
    except Exception as e:
        return {
            "status": "ERROR",
            "expected": float(expected) if expected is not None else None,
            "actual": None,
            "message": f"Error: {str(e)}",
        }


# ========== VALIDATION ORCHESTRATION ==========


def _build_union_sql(rules: List[RuleDef]) -> str:
    """
    Builds a SQL query using SQLGlot that combines multiple rule-based conditions.

    Args:
        rules (List[RuleDef]): List of validation rules.

    Returns:
        str: SQL query string combining all rules.
    """
    pieces: list[str] = []

    for rule in rules:
        check = rule.check_type
        builder = __RULE_DISPATCH.get(check)
        if builder is None:
            warnings.warn(f"❌ Unknown rule: {check}")
            continue

        field_val = rule.field if isinstance(rule.field, str) else rule.field
        ctx = __RuleCtx(
            column=field_val,
            value=rule.value,
            name=check,
        )

        # Get SQLGlot expression
        expr_ok = builder(ctx)

        # Build SELECT statement using SQLGlot
        dq_tag = __escape_single_quotes(f"{ctx.column}:{check}:{rule.value}")

        select_stmt = (
            exp.Select(
                expressions=[
                    exp.Star(),
                    exp.alias_(Literal.string(dq_tag), "dq_status"),
                ]
            )
            .from_(exp.Table(this="tbl"))
            .where(exp.Not(this=exp.Paren(this=expr_ok)))
        )

        # Convert to SQL string
        pieces.append(select_stmt.sql(dialect="duckdb"))

    if not pieces:
        # Return empty result
        empty_query = (
            exp.Select(
                expressions=[exp.Star(), exp.alias_(Literal.string(""), "dq_status")]
            )
            .from_(exp.Table(this="tbl"))
            .where(exp.false())
        )
        return empty_query.sql(dialect="duckdb")

    return "\nUNION ALL\n".join(pieces)


def validate_row_level(
    conn: dk.DuckDBPyConnection, df_rel: dk.DuckDBPyRelation, rules: List[RuleDef]
) -> Tuple[DuckDBPyRelation, DuckDBPyRelation]:
    """
    Validates DataFrame at row level using specified rules.

    Args:
        conn (dk.DuckDBPyConnection): DuckDB connection.
        df_rel (dk.DuckDBPyRelation): Input DuckDB relation to validate.
        rules (List[RuleDef]): List of row-level validation rules.

    Returns:
        Tuple[DuckDBPyRelation, DuckDBPyRelation]:
            - Relation with violations and dq_status column
            - Raw violations relation
    """
    engine = "duckdb"
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
        return df_rel.limit(0), df_rel.limit(0)

    # Create view for the relation
    df_rel.create_view("tbl")

    # Handle alias mappings
    mapped_rules = []
    for rule in rules_valid:
        check_type = rule.check_type
        if check_type == "is_primary_key":
            check_type = "is_unique"
        elif check_type == "is_composite_key":
            check_type = "are_unique"
        elif check_type == "is_yesterday":
            check_type = "is_t_minus_1"
        elif check_type == "is_in":
            check_type = "is_contained_in"
        elif check_type == "not_in":
            check_type = "not_contained_in"

        # Create a copy with mapped check_type
        from copy import copy

        mapped_rule = copy(rule)
        mapped_rule.check_type = check_type
        mapped_rules.append(mapped_rule)

    # Build union SQL
    union_sql = _build_union_sql(mapped_rules)

    # Get column names
    cols_df = conn.sql("PRAGMA table_info('tbl')").df()
    colnames = cols_df["name"].tolist()
    cols_sql = ", ".join(colnames)

    # Execute violations query
    raw = conn.sql(union_sql)

    # Aggregate violations by row
    final_sql = f"""
    SELECT
        {cols_sql},
        STRING_AGG(dq_status, ';') AS dq_status
    FROM raw
    GROUP BY {cols_sql}
    """
    final = conn.sql(final_sql)

    return final, raw


def validate_table_level(
    conn: dk.DuckDBPyConnection, df_rel: dk.DuckDBPyRelation, rules: List[RuleDef]
) -> dk.DuckDBPyRelation:
    """
    Validates DataFrame at table level using specified rules.

    Args:
        conn (dk.DuckDBPyConnection): DuckDB connection.
        df_rel (dk.DuckDBPyRelation): Input DuckDB relation to validate.
        rules (List[RuleDef]): List of table-level validation rules.

    Returns:
        DuckDBPyRelation: Summary relation with validation results.
    """
    engine = "duckdb"
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
        return conn.sql(
            """
            SELECT 
                NULL::VARCHAR AS id,
                NULL::TIMESTAMP AS timestamp,
                NULL::VARCHAR AS level,
                NULL::VARCHAR AS category,
                NULL::VARCHAR AS check_type,
                NULL::VARCHAR AS field,
                NULL::VARCHAR AS status,
                NULL::DOUBLE AS expected,
                NULL::DOUBLE AS actual,
                NULL::VARCHAR AS message
            WHERE 1=0
        """
        )

    # Create view
    df_rel.create_view("tbl")

    execution_time = datetime.utcnow()
    results = []

    for rule in rules_valid:
        check_type = rule.check_type

        # Get function from globals
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
            result = fn(conn, rule)

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
        return conn.sql(
            """
            SELECT 
                NULL::VARCHAR AS id,
                NULL::TIMESTAMP AS timestamp,
                NULL::VARCHAR AS level,
                NULL::VARCHAR AS category,
                NULL::VARCHAR AS check_type,
                NULL::VARCHAR AS field,
                NULL::VARCHAR AS status,
                NULL::DOUBLE AS expected,
                NULL::DOUBLE AS actual,
                NULL::VARCHAR AS message
            WHERE 1=0
        """
        )

    # Convert results to DuckDB relation
    import pandas as pd

    summary_df = pd.DataFrame(results)

    # Sort: FAIL first, then ERROR, then PASS
    summary_df["_sort"] = summary_df["status"].map({"FAIL": 0, "ERROR": 1, "PASS": 2})
    summary_df = (
        summary_df.sort_values("_sort").drop(columns=["_sort"]).reset_index(drop=True)
    )

    return conn.from_df(summary_df)


def validate(
    conn: dk.DuckDBPyConnection, df_rel: dk.DuckDBPyRelation, rules: List[RuleDef]
) -> Tuple[DuckDBPyRelation, DuckDBPyRelation, DuckDBPyRelation]:
    """
    Main validation function that orchestrates row-level and table-level validations.

    Args:
        conn (dk.DuckDBPyConnection): DuckDB connection.
        df_rel (dk.DuckDBPyRelation): Input DuckDB relation to validate.
        rules (List[RuleDef]): List of all validation rules.

    Returns:
        Tuple[DuckDBPyRelation, DuckDBPyRelation, DuckDBPyRelation]:
            - Relation with row-level violations and dq_status
            - Raw row-level violations relation
            - Table-level summary relation
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
        df_with_status, row_violations = validate_row_level(conn, df_rel, row_rules)
    else:
        df_with_status = df_rel
        row_violations = df_rel.limit(0)

    if table_rules:
        table_summary = validate_table_level(conn, df_rel, table_rules)
    else:
        table_summary = conn.sql(
            """
            SELECT 
                NULL::VARCHAR AS id,
                NULL::TIMESTAMP AS timestamp,
                NULL::VARCHAR AS level,
                NULL::VARCHAR AS category,
                NULL::VARCHAR AS check_type,
                NULL::VARCHAR AS field,
                NULL::VARCHAR AS status,
                NULL::DOUBLE AS expected,
                NULL::DOUBLE AS actual,
                NULL::VARCHAR AS message
            WHERE 1=0
        """
        )

    return df_with_status, row_violations, table_summary


def summarize(
    conn: dk.DuckDBPyConnection,
    rules: List[RuleDef],
    total_rows: int,
    df_with_errors: Optional[DuckDBPyRelation] = None,
    table_error: Optional[DuckDBPyRelation] = None,
) -> dk.DuckDBPyRelation:
    """
    Summarizes validation results from both row-level and table-level checks.

    Args:
        conn (dk.DuckDBPyConnection): DuckDB connection.
        rules (List[RuleDef]): List of all validation rules.
        total_rows (int): Total number of rows in the input relation.
        df_with_errors (Optional[DuckDBPyRelation]): Relation with row-level violations.
        table_error (Optional[DuckDBPyRelation]): Relation with table-level results.

    Returns:
        DuckDBPyRelation: Summary relation with aggregated validation metrics.
    """
    import pandas as pd

    summaries = []

    # ========== ROW-LEVEL SUMMARY ==========
    row_rules = [
        r for r in rules if r.level and r.level.upper().replace("_LEVEL", "") == "ROW"
    ]

    if row_rules and df_with_errors is not None:
        # Parse violations from dq_status
        df_with_errors.create_view("violations_raw")

        viol_sql = """
        SELECT
            split_part(dq_status, ':', 1) AS check_type,
            split_part(dq_status, ':', 2) AS field,
            COUNT(*) AS violations
        FROM violations_raw
        WHERE dq_status IS NOT NULL AND dq_status <> ''
        GROUP BY check_type, field
        """
        viol_df = conn.sql(viol_sql).df()

        viol_dict = {
            (row["check_type"], row["field"]): row["violations"]
            for _, row in viol_df.iterrows()
        }

        for rule in row_rules:
            field_str = (
                rule.field if isinstance(rule.field, str) else ",".join(rule.field)
            )

            violations = viol_dict.get((rule.check_type, field_str), 0)
            pass_count = total_rows - violations
            pass_rate = pass_count / total_rows if total_rows > 0 else 1.0
            pass_threshold = rule.threshold if rule.threshold else 1.0

            status = "PASS" if pass_rate >= pass_threshold else "FAIL"

            # Convert expected value to float safely
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
    if table_error is not None:
        table_df = table_error.df()
        for _, row_dict in table_df.iterrows():
            expected = row_dict.get("expected")
            actual = row_dict.get("actual")

            # Calculate compliance rate if possible
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
        return conn.sql(
            """
            SELECT 
                NULL::VARCHAR AS id,
                NULL::TIMESTAMP AS timestamp,
                NULL::VARCHAR AS level,
                NULL::VARCHAR AS category,
                NULL::VARCHAR AS check_type,
                NULL::VARCHAR AS field,
                NULL::INTEGER AS rows,
                NULL::INTEGER AS violations,
                NULL::DOUBLE AS pass_rate,
                NULL::DOUBLE AS pass_threshold,
                NULL::VARCHAR AS status,
                NULL::DOUBLE AS expected,
                NULL::DOUBLE AS actual,
                NULL::VARCHAR AS message
            WHERE 1=0
        """
        )

    summary_df = pd.DataFrame(summaries)

    # Sort: FAIL first, ERROR second, PASS last; ROW before TABLE
    summary_df["_sort_status"] = summary_df["status"].map(
        {"FAIL": 0, "ERROR": 1, "PASS": 2}
    )
    summary_df["_sort_level"] = summary_df["level"].map({"ROW": 0, "TABLE": 1})

    summary_df = (
        summary_df.sort_values(["_sort_status", "_sort_level", "check_type"])
        .drop(columns=["_sort_status", "_sort_level"])
        .reset_index(drop=True)
    )

    return conn.from_df(summary_df)


# ========== SCHEMA VALIDATION ==========


def extract_schema(conn: dk.DuckDBPyConnection, table: str) -> List[Dict[str, Any]]:
    """
    Extracts schema from DuckDB table.

    Args:
        conn (dk.DuckDBPyConnection): DuckDB connection.
        table (str): Table name.

    Returns:
        List[Dict[str, Any]]: List of dictionaries containing field information.
    """
    df_info = conn.execute(f"PRAGMA table_info('{table}')").fetchdf()
    return [
        {
            "field": row["name"],
            "data_type": row["type"],
            "nullable": not bool(row["notnull"]),
            "max_length": None,
        }
        for _, row in df_info.iterrows()
    ]


def validate_schema(
    conn: dk.DuckDBPyConnection, expected: List[Dict[str, Any]], table: str
) -> Tuple[bool, List[Dict[str, Any]]]:
    """
    Validates the schema of a DuckDB table against an expected schema.

    Args:
        conn (dk.DuckDBPyConnection): DuckDB connection.
        expected (List[Dict[str, Any]]): Expected schema.
        table (str): Table name.

    Returns:
        Tuple[bool, List[Dict[str, Any]]]:
            - Boolean indicating whether the schema matches
            - List of schema errors/mismatches
    """
    actual = extract_schema(conn, table)
    result, errors = __compare_schemas(actual, expected)
    return result, errors
