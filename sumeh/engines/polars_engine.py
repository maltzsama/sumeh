#!/usr/bin/env python
# -*- coding: utf-8 -*-

import operator
import re
import uuid
import warnings
from datetime import date as _dt
from datetime import datetime, timedelta
from functools import reduce
from typing import List, Dict, Any, Tuple, Optional

import numpy as np
import polars as pl

from sumeh.core.rules.rule_model import RuleDef
from sumeh.core.utils import (
    __convert_value,
    __compare_schemas,
    __transform_date_format_in_pattern,
)


# ========== ROW-LEVEL VALIDATION FUNCTIONS ==========


def is_positive(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the specified field is negative and adds a data quality status column.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be filtered.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(pl.col(rule.field) < 0)
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def is_negative(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the specified field is non-negative and adds a data quality status column.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be filtered.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(pl.col(rule.field) >= 0)
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def is_complete(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the specified field is null and adds a data quality status column.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be checked.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(pl.col(rule.field).is_null())
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def is_unique(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Identifies duplicate rows based on the specified field and adds a data quality status column.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to check for uniqueness.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        pl.DataFrame: DataFrame containing rows where the field is not unique with dq_status column.
    """
    dup_vals = (
        df.group_by(rule.field)
        .agg(pl.len().alias("cnt"))
        .filter(pl.col("cnt") > 1)
        .select(rule.field)
        .to_series()
        .to_list()
    )
    viol = df.filter(pl.col(rule.field).is_in(dup_vals))
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def are_complete(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where any of the specified fields are null and adds a data quality status column.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be checked.
        rule (RuleDef): Rule definition containing fields (list), check_type, and value.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    fields = rule.field if isinstance(rule.field, list) else [rule.field]
    cond = reduce(operator.or_, [pl.col(f).is_null() for f in fields])
    viol = df.filter(cond)
    viol = viol.with_columns(
        pl.lit(f"{fields}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def are_unique(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Identifies duplicate rows based on a combination of specified fields and adds a data quality status column.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to check.
        rule (RuleDef): Rule definition containing fields (list), check_type, and value.

    Returns:
        pl.DataFrame: DataFrame containing rows where the field combination is not unique with dq_status column.
    """
    fields = rule.field if isinstance(rule.field, list) else [rule.field]

    combo = df.with_columns(
        pl.concat_str([pl.col(f).cast(str) for f in fields], separator="|").alias(
            "_combo"
        )
    )
    dupes = (
        combo.group_by("_combo")
        .agg(pl.len().alias("cnt"))
        .filter(pl.col("cnt") > 1)
        .select("_combo")
        .to_series()
        .to_list()
    )
    viol = combo.filter(pl.col("_combo").is_in(dupes)).drop("_combo")
    viol = viol.with_columns(
        pl.lit(f"{fields}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def is_greater_than(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the specified field is less than or equal to the given value.

    Args:
        df (pl.DataFrame): The Polars DataFrame to filter.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(pl.col(rule.field) <= rule.value)
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def is_greater_or_equal_than(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the specified field is less than the given value.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to filter.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(pl.col(rule.field) < rule.value)
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def is_less_than(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the specified field is greater than or equal to the given value.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to filter.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(pl.col(rule.field) >= rule.value)
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def is_less_or_equal_than(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the specified field is greater than the given value.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be filtered.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(pl.col(rule.field) > rule.value)
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def is_equal(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the specified field is not equal to the given value.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be filtered.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(~pl.col(rule.field).eq(rule.value))
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def is_equal_than(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Alias for is_equal.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be filtered.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    return is_equal(df, rule)


def is_contained_in(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the specified field is not in the given list of values.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to filter.
        rule (RuleDef): Rule definition containing field, check_type, and value (list format).

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    vals = re.findall(r"'([^']*)'", str(rule.value)) or [
        v.strip() for v in str(rule.value).strip("[]").split(",")
    ]
    viol = df.filter(~pl.col(rule.field).is_in(vals))
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def is_in(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Alias for is_contained_in.

    Args:
        df (pl.DataFrame): The input DataFrame to evaluate.
        rule (RuleDef): Rule definition.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations.
    """
    return is_contained_in(df, rule)


def not_contained_in(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the specified field is in the given list of values.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to filter.
        rule (RuleDef): Rule definition containing field, check_type, and value (list format).

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    vals = re.findall(r"'([^']*)'", str(rule.value)) or [
        v.strip() for v in str(rule.value).strip("[]").split(",")
    ]
    viol = df.filter(pl.col(rule.field).is_in(vals))
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def not_in(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Alias for not_contained_in.

    Args:
        df (pl.DataFrame): The input DataFrame to be filtered.
        rule (RuleDef): Rule definition.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations.
    """
    return not_contained_in(df, rule)


def is_between(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the specified field is not within the given range.

    Args:
        df (pl.DataFrame): The input Polars DataFrame.
        rule (RuleDef): Rule definition containing field, check_type, and value (range format).

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    lo, hi = [__convert_value(x) for x in str(rule.value).strip("[]").split(",")]
    viol = df.filter(~pl.col(rule.field).is_between(lo, hi))
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def has_pattern(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the specified field does not match the given regex pattern.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be filtered.
        rule (RuleDef): Rule definition containing field, check_type, and value (regex pattern).

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(~pl.col(rule.field).str.contains(rule.value, literal=False))
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def is_legit(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the specified field is null or does not match a non-whitespace pattern.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be validated.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    mask = pl.col(rule.field).is_not_null() & pl.col(rule.field).str.contains(r"^\S+$")
    viol = df.filter(~mask)
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


# ========== TABLE-LEVEL VALIDATION FUNCTIONS ==========


def has_min(df: pl.DataFrame, rule: RuleDef) -> dict:
    """
    Checks if the minimum value of the specified field meets expectations.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be checked.
        rule (RuleDef): Rule definition containing field, value (expected min), and optional threshold.

    Returns:
        dict: Validation result with status, expected, actual, and message.
    """
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
        actual = float(df.select(pl.col(field).min()).to_numpy()[0, 0] or 0.0)
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


def has_max(df: pl.DataFrame, rule: RuleDef) -> dict:
    """
    Checks if the maximum value of the specified field meets expectations.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be checked.
        rule (RuleDef): Rule definition containing field, value (expected max), and optional threshold.

    Returns:
        dict: Validation result with status, expected, actual, and message.
    """
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
        actual = float(df.select(pl.col(field).max()).to_numpy()[0, 0] or 0.0)
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


def has_std(df: pl.DataFrame, rule: RuleDef) -> dict:
    """
    Checks if the standard deviation of the specified field meets expectations.

    Args:
        df (pl.DataFrame): The input Polars DataFrame.
        rule (RuleDef): Rule definition containing field, value (expected std), and optional threshold.

    Returns:
        dict: Validation result with status, expected, actual, and message.
    """
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
        actual = float(df.select(pl.col(field).std()).to_numpy()[0, 0] or 0.0)
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


def has_mean(df: pl.DataFrame, rule: RuleDef) -> dict:
    """
    Checks if the mean of the specified field meets expectations.

    Args:
        df (pl.DataFrame): The input Polars DataFrame.
        rule (RuleDef): Rule definition containing field, value (expected mean), and optional threshold.

    Returns:
        dict: Validation result with status, expected, actual, and message.
    """
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
        actual = float(df.select(pl.col(field).mean()).to_numpy()[0, 0] or 0.0)
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


def has_sum(df: pl.DataFrame, rule: RuleDef) -> dict:
    """
    Checks if the sum of the specified field meets expectations.

    Args:
        df (pl.DataFrame): The input Polars DataFrame.
        rule (RuleDef): Rule definition containing field, value (expected sum), and optional threshold.

    Returns:
        dict: Validation result with status, expected, actual, and message.
    """
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
        actual = float(df.select(pl.col(field).sum()).to_numpy()[0, 0] or 0.0)
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


def has_cardinality(df: pl.DataFrame, rule: RuleDef) -> dict:
    """
    Checks if the cardinality (distinct count) of the specified field meets expectations.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be checked.
        rule (RuleDef): Rule definition containing field, value (expected cardinality), and optional threshold.

    Returns:
        dict: Validation result with status, expected, actual, and message.
    """
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
        actual = int(df.select(pl.col(field).n_unique()).to_numpy()[0, 0] or 0)
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


def has_infogain(df: pl.DataFrame, rule: RuleDef) -> dict:
    """
    Checks if the information gain of the specified field meets expectations.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to evaluate.
        rule (RuleDef): Rule definition containing field, value (expected info gain), and optional threshold.

    Returns:
        dict: Validation result with status, expected, actual, and message.
    """
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
        # Get value counts
        value_counts = df.group_by(field).agg(pl.len().alias("count"))
        total_count = len(df)

        # Calculate probabilities
        value_counts = value_counts.with_columns(
            (pl.col("count") / total_count).alias("probability")
        )

        # Shannon entropy: -Σ(p * log2(p))
        entropy = float(
            value_counts.select(
                (-pl.col("probability") * pl.col("probability").log(2)).sum()
            ).to_numpy()[0, 0]
            or 0.0
        )

        # Max entropy (uniform distribution)
        n_unique = len(value_counts)
        max_entropy = float(np.log2(n_unique)) if n_unique > 1 else 1.0

        # Normalized information gain
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


def has_entropy(df: pl.DataFrame, rule: RuleDef) -> dict:
    """
    Checks if the entropy of the specified field meets expectations.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to evaluate.
        rule (RuleDef): Rule definition containing field, value (expected entropy), and optional threshold.

    Returns:
        dict: Validation result with status, expected, actual, and message.
    """
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
        # Get value counts
        value_counts = df.group_by(field).agg(pl.len().alias("count"))
        total_count = len(df)

        # Calculate probabilities
        value_counts = value_counts.with_columns(
            (pl.col("count") / total_count).alias("probability")
        )

        # Shannon entropy: -Σ(p * log2(p))
        entropy = float(
            value_counts.select(
                (-pl.col("probability") * pl.col("probability").log(2)).sum()
            ).to_numpy()[0, 0]
            or 0.0
        )
        actual = float(entropy)
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


# ========== DATE VALIDATION FUNCTIONS ==========


def validate_date_format(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the specified field has wrong date format based on the format from the rule.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be checked.
        rule (RuleDef): Rule definition containing field, check_type, and value (date format).

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    fmt = rule.value
    regex = __transform_date_format_in_pattern(fmt)
    viol = df.filter(
        ~pl.col(rule.field).str.contains(regex, literal=False)
        | pl.col(rule.field).is_null()
    )
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{fmt}").alias("dq_status")
    )
    return viol


def is_future_date(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the specified field has a date greater than the current date.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be checked.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    today = _dt.today().isoformat()
    viol = df.filter(
        pl.col(rule.field).str.strptime(pl.Date, "%Y-%m-%d")
        > pl.lit(today).cast(pl.Date)
    )
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{today}").alias("dq_status")
    )
    return viol


def is_past_date(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the specified field has a date lower than the current date.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be checked.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    today = _dt.today().isoformat()
    viol = df.filter(
        pl.col(rule.field).str.strptime(pl.Date, "%Y-%m-%d")
        < pl.lit(today).cast(pl.Date)
    )
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{today}").alias("dq_status")
    )
    return viol


def is_date_between(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where date field is not between two dates in format: "[<initial_date>, <final_date>]".

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be checked.
        rule (RuleDef): Rule definition containing field, check_type, and value (date range).

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    start_str, end_str = [s.strip() for s in rule.value.strip("[]").split(",")]
    start_expr = pl.lit(start_str).str.strptime(pl.Date, "%Y-%m-%d")
    end_expr = pl.lit(end_str).str.strptime(pl.Date, "%Y-%m-%d")

    viol = df.filter(
        ~pl.col(rule.field)
        .str.strptime(pl.Date, "%Y-%m-%d")
        .is_between(start_expr, end_expr)
    )
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def is_date_after(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the specified field has a date lower than the date informed in the rule.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be checked.
        rule (RuleDef): Rule definition containing field, check_type, and value (target date).

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(pl.col(rule.field).str.strptime(pl.Date, "%Y-%m-%d") < rule.value)
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def is_date_before(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the specified field has a date greater than the date informed in the rule.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be checked.
        rule (RuleDef): Rule definition containing field, check_type, and value (target date).

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(pl.col(rule.field).str.strptime(pl.Date, "%Y-%m-%d") > rule.value)
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def all_date_checks(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Default date check - filters past dates.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be filtered.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    return is_past_date(df, rule)


def is_in_millions(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the field value is less than 1,000,000.

    Args:
        df (pl.DataFrame): The input DataFrame to filter and modify.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(pl.col(rule.field) < 1_000_000)
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def is_in_billions(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the field value is less than 1,000,000,000.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to filter.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(pl.col(rule.field) < 1_000_000_000)
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def is_today(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the date field does not equal today.

    Args:
        df (pl.DataFrame): The input DataFrame to filter.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    today = _dt.today().isoformat()
    viol = df.filter(
        pl.col(rule.field).str.strptime(pl.Date, "%Y-%m-%d")
        != pl.lit(today).cast(pl.Date)
    )
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def is_yesterday(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Alias for is_t_minus_1. Filters rows where date field does not equal yesterday.

    Args:
        df (pl.DataFrame): The input Polars DataFrame.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    target = (_dt.today() - timedelta(days=1)).isoformat()
    viol = df.filter(
        pl.col(rule.field).str.strptime(pl.Date, "%Y-%m-%d")
        != pl.lit(target).cast(pl.Date)
    )
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def is_t_minus_1(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the date field does not equal one day ago (T-1).

    Args:
        df (pl.DataFrame): The input DataFrame to be filtered.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    target = (_dt.today() - timedelta(days=1)).isoformat()
    viol = df.filter(
        pl.col(rule.field).str.strptime(pl.Date, "%Y-%m-%d")
        != pl.lit(target).cast(pl.Date)
    )
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def is_t_minus_2(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the date field does not equal two days ago (T-2).

    Args:
        df (pl.DataFrame): The input DataFrame to be filtered.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    target = (_dt.today() - timedelta(days=2)).isoformat()
    viol = df.filter(
        pl.col(rule.field).str.strptime(pl.Date, "%Y-%m-%d")
        != pl.lit(target).cast(pl.Date)
    )
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def is_t_minus_3(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the date field does not equal three days ago (T-3).

    Args:
        df (pl.DataFrame): The input DataFrame to be filtered.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    target = (_dt.today() - timedelta(days=3)).isoformat()
    viol = df.filter(
        pl.col(rule.field).str.strptime(pl.Date, "%Y-%m-%d")
        != pl.lit(target).cast(pl.Date)
    )
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def is_on_weekday(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the date field does not fall on a weekday (Mon-Fri).

    Args:
        df (pl.DataFrame): The input Polars DataFrame.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(
        pl.col(rule.field).str.strptime(pl.Date, "%Y-%m-%d").dt.weekday() >= 5
    )
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def is_on_weekend(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the date field does not fall on a weekend (Sat-Sun).

    Args:
        df (pl.DataFrame): The input Polars DataFrame.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(
        pl.col(rule.field).str.strptime(pl.Date, "%Y-%m-%d").dt.weekday() < 5
    )
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def _day_of_week(df: pl.DataFrame, rule: RuleDef, dow: int) -> pl.DataFrame:
    """
    Helper function to filter rows based on day of week.

    Args:
        df (pl.DataFrame): The input DataFrame.
        rule (RuleDef): Rule definition.
        dow (int): Day of week (0=Monday, 6=Sunday).

    Returns:
        pl.DataFrame: Filtered DataFrame with violations.
    """
    viol = df.filter(
        pl.col(rule.field).str.strptime(pl.Date, "%Y-%m-%d").dt.weekday() != dow
    )
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


def is_on_monday(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """Filters rows where date is not Monday."""
    return _day_of_week(df, rule, 0)


def is_on_tuesday(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """Filters rows where date is not Tuesday."""
    return _day_of_week(df, rule, 1)


def is_on_wednesday(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """Filters rows where date is not Wednesday."""
    return _day_of_week(df, rule, 2)


def is_on_thursday(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """Filters rows where date is not Thursday."""
    return _day_of_week(df, rule, 3)


def is_on_friday(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """Filters rows where date is not Friday."""
    return _day_of_week(df, rule, 4)


def is_on_saturday(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """Filters rows where date is not Saturday."""
    return _day_of_week(df, rule, 5)


def is_on_sunday(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """Filters rows where date is not Sunday."""
    return _day_of_week(df, rule, 6)


def satisfies(df: pl.DataFrame, rule: RuleDef) -> pl.DataFrame:
    """
    Filters rows where the specified expression is not satisfied.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be filtered.
        rule (RuleDef): Rule definition with value containing SQL expression.

    Returns:
        pl.DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    ctx = pl.SQLContext(sumeh=df)
    viol = ctx.execute(
        f"""
        SELECT *
        FROM sumeh
        WHERE NOT ({rule.value})
        """,
        eager=True,
    )
    viol = viol.with_columns(
        pl.lit(f"{rule.field}:{rule.check_type}:{rule.value}").alias("dq_status")
    )
    return viol


# ========== VALIDATION ORCHESTRATION ==========


def validate_row_level(
    df: pl.DataFrame, rules: List[RuleDef]
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Validates DataFrame at row level using specified rules.

    Args:
        df (pl.DataFrame): Input Polars DataFrame to validate.
        rules (List[RuleDef]): List of row-level validation rules.

    Returns:
        Tuple[pl.DataFrame, pl.DataFrame]:
            - DataFrame with violations and dq_status column
            - Raw violations DataFrame
    """
    engine = "polars"
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
        return df.head(0), df.head(0)

    # Add _id column
    df = df.with_columns(pl.arange(0, pl.len()).alias("_id"))

    raw_list = []

    for rule in rules_valid:
        # Handle alias mappings
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

        # Get validation function
        fn = globals().get(check_type)
        if fn is None:
            warnings.warn(f"❌ Function not found: {check_type} for field {rule.field}")
            continue

        try:
            viol = fn(df, rule)
            raw_list.append(viol)
        except Exception as e:
            warnings.warn(f"❌ Error executing {check_type} on {rule.field}: {e}")
            continue

    if not raw_list:
        return df.head(0), df.head(0)

    # Concatenate all violations
    raw = pl.concat(raw_list) if raw_list else df.head(0)

    if raw.is_empty():
        return df.head(0), df.head(0)

    # Group by _id to aggregate violations
    summary = (
        raw.group_by("_id", maintain_order=True)
        .agg(pl.col("dq_status"))
        .with_columns(pl.col("dq_status").list.join(";").alias("dq_status"))
    )

    # Join back to original df
    out = df.join(summary, on="_id", how="inner").drop("_id")

    return out, raw


def validate_table_level(df: pl.DataFrame, rules: List[RuleDef]) -> pl.DataFrame:
    """
    Validates DataFrame at table level using specified rules.

    Args:
        df (pl.DataFrame): Input Polars DataFrame to validate.
        rules (List[RuleDef]): List of table-level validation rules.

    Returns:
        pl.DataFrame: Summary DataFrame with validation results.
    """
    engine = "polars"
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
        return pl.DataFrame(
            schema={
                "id": str,
                "timestamp": pl.Datetime,
                "level": str,
                "category": str,
                "check_type": str,
                "field": str,
                "status": str,
                "expected": float,
                "actual": float,
                "message": str,
            }
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
            result = fn(df, rule)

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
        return pl.DataFrame(
            schema={
                "id": str,
                "timestamp": pl.Datetime,
                "level": str,
                "category": str,
                "check_type": str,
                "field": str,
                "status": str,
                "expected": float,
                "actual": float,
                "message": str,
            }
        )

    summary_df = pl.DataFrame(results)

    # Sort: FAIL first, then ERROR, then PASS
    summary_df = summary_df.with_columns(
        pl.when(pl.col("status") == "FAIL")
        .then(0)
        .when(pl.col("status") == "ERROR")
        .then(1)
        .otherwise(2)
        .alias("_sort")
    )
    summary_df = summary_df.sort("_sort").drop("_sort")

    return summary_df


def validate(
    df: pl.DataFrame, rules: List[RuleDef]
) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Main validation function that orchestrates row-level and table-level validations.

    Args:
        df (pl.DataFrame): Input Polars DataFrame to validate.
        rules (List[RuleDef]): List of all validation rules.

    Returns:
        Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
            - DataFrame with row-level violations and dq_status
            - Raw row-level violations DataFrame
            - Table-level summary DataFrame
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
        df_with_status, row_violations = validate_row_level(df, row_rules)
    else:
        df_with_status = df
        row_violations = df.head(0)

    if table_rules:
        table_summary = validate_table_level(df, table_rules)
    else:
        table_summary = pl.DataFrame(
            schema={
                "id": str,
                "timestamp": pl.Datetime,
                "level": str,
                "category": str,
                "check_type": str,
                "field": str,
                "status": str,
                "expected": float,
                "actual": float,
                "message": str,
            }
        )

    return df_with_status, row_violations, table_summary


def summarize(
    rules: List[RuleDef],
    total_rows: int,
    df_with_errors: Optional[pl.DataFrame] = None,
    table_error: Optional[pl.DataFrame] = None,
) -> pl.DataFrame:
    """
    Summarizes validation results from both row-level and table-level checks.

    Args:
        rules (List[RuleDef]): List of all validation rules.
        total_rows (int): Total number of rows in the input DataFrame.
        df_with_errors (Optional[pl.DataFrame]): DataFrame with row-level violations.
        table_error (Optional[pl.DataFrame]): DataFrame with table-level results.

    Returns:
        pl.DataFrame: Summary DataFrame with aggregated validation metrics.
    """
    summaries = []

    # ========== ROW-LEVEL SUMMARY ==========
    row_rules = [
        r for r in rules if r.level and r.level.upper().replace("_LEVEL", "") == "ROW"
    ]

    if row_rules and df_with_errors is not None:
        # Parse violations from dq_status
        if "dq_status" in df_with_errors.columns:
            exploded = (
                df_with_errors.select(
                    pl.col("dq_status").str.split(";").list.explode().alias("dq_status")
                )
                .filter(pl.col("dq_status") != "")
                .with_columns(
                    [
                        pl.col("dq_status")
                        .str.split(":")
                        .list.get(0)
                        .alias("check_type"),
                        pl.col("dq_status").str.split(":").list.get(1).alias("field"),
                    ]
                )
            ).drop("dq_status")

            viol_count = exploded.group_by(["check_type", "field"]).agg(
                pl.len().alias("violations")
            )

            viol_dict = {
                (row["check_type"], row["field"]): row["violations"]
                for row in viol_count.to_dicts()
            }
        else:
            viol_dict = {}

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
    if table_error is not None and not table_error.is_empty():
        for row_dict in table_error.to_dicts():
            expected = row_dict.get("expected")
            actual = row_dict.get("actual")

            # Calculate compliance rate if possible
            if expected is not None and actual is not None and expected != 0:
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
        return pl.DataFrame(
            schema={
                "id": str,
                "timestamp": pl.Datetime,
                "level": str,
                "category": str,
                "check_type": str,
                "field": str,
                "rows": int,
                "violations": int,
                "pass_rate": float,
                "pass_threshold": float,
                "status": str,
                "expected": float,
                "actual": float,
                "message": str,
            }
        )

    summary_df = pl.DataFrame(summaries)

    # Sort: FAIL first, ERROR second, PASS last; ROW before TABLE
    summary_df = summary_df.with_columns(
        [
            pl.when(pl.col("status") == "FAIL")
            .then(0)
            .when(pl.col("status") == "ERROR")
            .then(1)
            .otherwise(2)
            .alias("_sort_status"),
            pl.when(pl.col("level") == "ROW").then(0).otherwise(1).alias("_sort_level"),
        ]
    )

    summary_df = summary_df.sort(["_sort_status", "_sort_level", "check_type"]).drop(
        ["_sort_status", "_sort_level"]
    )

    return summary_df


# ========== SCHEMA VALIDATION ==========


def extract_schema(df: pl.DataFrame) -> List[Dict[str, Any]]:
    """
    Extracts schema from Polars DataFrame.

    Args:
        df (pl.DataFrame): Input Polars DataFrame.

    Returns:
        List[Dict[str, Any]]: List of dictionaries containing field information.
    """
    return [
        {
            "field": name,
            "data_type": str(dtype),
            "nullable": True,
            "max_length": None,
        }
        for name, dtype in df.schema.items()
    ]


def validate_schema(df: pl.DataFrame, expected) -> Tuple[bool, List[Dict[str, Any]]]:
    """
    Validates the schema of a Polars DataFrame against an expected schema.

    Args:
        df (pl.DataFrame): The Polars DataFrame whose schema is to be validated.
        expected (list): The expected schema.

    Returns:
        Tuple[bool, List[Dict[str, Any]]]:
            - Boolean indicating whether the schema matches
            - List of schema errors/mismatches
    """
    actual = extract_schema(df)
    result, errors = __compare_schemas(actual, expected)
    return result, errors
