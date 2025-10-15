#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Dask data quality validation engine for Sumeh.

This module provides validation functions for data quality rules in Dask DataFrames.
It supports row-level and table-level validations including completeness, uniqueness,
pattern matching, date validations, and numeric comparisons.
"""

import operator
import re
import uuid
import warnings
from datetime import datetime, date
from functools import reduce
from typing import List, Dict, Any, Tuple, Optional

import dask.dataframe as dd
import numpy as np
import pandas as pd

from sumeh.core.rules.rule_model import RuleDef
from sumeh.core.utils import (
    __convert_value,
    __compare_schemas,
    __parse_field_list,
)


def is_positive(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters rows where the specified field is negative (violation).

    Args:
        df: Input Dask DataFrame to validate
        rule: Rule definition containing field, check_type, and value

    Returns:
        DataFrame with violations and dq_status column
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    viol = df[df[field] < 0]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_negative(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters rows where the specified field is non-negative (violation).

    Args:
        df: Input Dask DataFrame to validate
        rule: Rule definition containing field, check_type, and value

    Returns:
        DataFrame with violations and dq_status column
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    viol = df[df[field] >= 0]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_in_millions(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Checks if the values in a specified field of a Dask DataFrame are in the millions
    (greater than or equal to 1,000,000) and returns a DataFrame of violations.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to check.
        rule (RuleDef): A dictionary containing the rule parameters. It is expected to
                     include the field name, check type, and value.

    Returns:
        dd.DataFrame: A DataFrame containing rows where the specified field's value
                      is greater than or equal to 1,000,000. An additional column
                      `dq_status` is added to indicate the field, check, and value
                      that triggered the violation.
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    viol = df[df[field] < 1_000_000]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_in_billions(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Identifies rows in a Dask DataFrame where the value in a specified field
    is greater than or equal to one billion and marks them with a data quality status.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (RuleDef): A dictionary containing the rule parameters. It is expected
                     to include the field name, check type, and value.

    Returns:
        dd.DataFrame: A Dask DataFrame containing only the rows where the specified
                      field's value is greater than or equal to one billion. An
                      additional column `dq_status` is added, indicating the field,
                      check type, and value that triggered the rule.
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    viol = df[df[field] < 1_000_000_000]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_complete(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters rows where the specified field is null (violation).

    Args:
        df: Input Dask DataFrame to validate
        rule: Rule definition containing field, check_type, and value

    Returns:
        DataFrame with violations and dq_status column
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    viol = df[df[field].isnull()]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_unique(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Identifies duplicate rows based on the specified field.

    Args:
        df: Input Dask DataFrame to validate
        rule: Rule definition containing field, check_type, and value

    Returns:
        DataFrame with violations and dq_status column
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    counts = df[field].value_counts().compute()
    dup_vals = counts[counts > 1].index.tolist()
    viol = df[df[field].isin(dup_vals)]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def are_complete(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters rows where any of the specified fields are null (violation).

    Args:
        df: Input Dask DataFrame to validate
        rule: Rule definition containing fields list, check_type, and value

    Returns:
        DataFrame with violations and dq_status column
    """
    fields = __parse_field_list(rule.field)
    check = rule.check_type
    value = rule.value
    mask = ~reduce(operator.and_, [df[f].notnull() for f in fields])
    viol = df[mask]
    return viol.assign(dq_status=f"{str(fields)}:{check}:{value}")


def are_unique(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Identifies duplicate rows based on combination of specified fields.

    Args:
        df: Input Dask DataFrame to validate
        rule: Rule definition containing fields list, check_type, and value

    Returns:
        DataFrame with violations and dq_status column
    """
    fields = __parse_field_list(rule.field)
    check = rule.check_type
    value = rule.value
    combo = (
        df[fields]
        .astype(str)
        .apply(lambda row: "|".join(row.values), axis=1, meta=("combo", "object"))
    )
    counts = combo.value_counts().compute()
    dupes = counts[counts > 1].index.tolist()
    viol = df[combo.isin(dupes)]
    return viol.assign(dq_status=f"{str(fields)}:{check}:{value}")


def is_greater_than(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the value in a specified field
    is greater than a given threshold and annotates the result with a data quality status.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (RuleDef): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The column name to check.
            - 'check' (str): The type of check being performed (e.g., 'greater_than').
            - 'value' (numeric): The threshold value to compare against.

    Returns:
        dd.DataFrame: A filtered DataFrame containing rows that violate the rule,
        with an additional column `dq_status` indicating the rule details in the format
        "{field}:{check}:{value}".
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    viol = df[df[field] > value]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_greater_or_equal_than(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where a specified field's value
    is less than a given threshold, and annotates the resulting rows with a
    data quality status.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (RuleDef): A dictionary containing the rule parameters. It should
                     include the following keys:
                     - 'field': The column name in the DataFrame to check.
                     - 'check': The type of check being performed (e.g., 'greater_or_equal').
                     - 'value': The threshold value to compare against.

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows that
                      violate the rule, with an additional column `dq_status`
                      indicating the field, check type, and threshold value.
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    viol = df[df[field] < value]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_less_than(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the value in a specified field
    is greater than or equal to a given threshold, and marks them with a data quality status.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (RuleDef): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The column name to check.
            - 'check' (str): The type of check being performed (e.g., "less_than").
            - 'value' (numeric): The threshold value for the check.

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows that violate the rule,
        with an additional column `dq_status` indicating the rule that was violated in the
        format "{field}:{check}:{value}".
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    viol = df[df[field] >= value]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_less_or_equal_than(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the value in a specified field
    is greater than a given threshold, violating a "less or equal than" rule.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (RuleDef): A dictionary containing the rule parameters. It should include:
            - 'field': The column name in the DataFrame to be checked.
            - 'check': The type of check being performed (e.g., "less_or_equal_than").
            - 'value': The threshold value to compare against.

    Returns:
        dd.DataFrame: A new DataFrame containing only the rows that violate the rule.
        An additional column `dq_status` is added to indicate the rule violation
        in the format "{field}:{check}:{value}".
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    viol = df[df[field] > value]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_equal(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where a specified field does not equal a given value.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (RuleDef): A dictionary containing the rule parameters. It should include:
            - 'field': The column name in the DataFrame to be checked.
            - 'check': The type of check to perform (expected to be 'equal' for this function).
            - 'value': The value to compare against.

    Returns:
        dd.DataFrame: A new DataFrame containing rows that violate the equality rule.
                      An additional column `dq_status` is added, indicating the rule details
                      in the format "{field}:{check}:{value}".
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    viol = df[~df[field].eq(value)]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_equal_than(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the specified field does not equal the given value.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (RuleDef): A dictionary containing the rule parameters. It should include:
            - 'field': The column name in the DataFrame to check.
            - 'check': The type of check being performed (expected to be 'equal' for this function).
            - 'value': The value to compare against.

    Returns:
        dd.DataFrame: A new DataFrame containing rows that violate the equality rule.
                      An additional column `dq_status` is added, indicating the rule details
                      in the format "{field}:{check}:{value}".
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    viol = df[~df[field].eq(value)]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_contained_in(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the values in a specified field
    are not contained within a given list of allowed values.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (RuleDef): A dictionary containing the rule parameters. It should include:
            - 'field': The column name in the DataFrame to check.
            - 'check': The type of check being performed (e.g., "is_contained_in").
            - 'value': A string representation of a list of allowed values (e.g., "[value1, value2]").

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows that violate the rule.
        An additional column `dq_status` is added to indicate the rule violation in the format:
        "{field}:{check}:{value}".
    """
    parsed_value = __parse_field_list(rule.value)

    # Se retornou lista, usa direto; se string, split
    if isinstance(parsed_value, list):
        vals = parsed_value
    elif isinstance(parsed_value, str):
        vals = [parsed_value]
    else:
        vals = []

    viol = df[~df[rule.field].isin(vals)]
    return viol.assign(dq_status=f"{rule.field}:{rule.check_type}:{rule.value}")


def is_in(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Checks if the specified rule is contained within the given Dask DataFrame.

    This function acts as a wrapper for the `is_contained_in` function,
    passing the provided DataFrame and rule to it.

    Args:
        df (dd.DataFrame): The Dask DataFrame to evaluate.
        rule (RuleDef): A dictionary representing the rule to check against the DataFrame.

    Returns:
        dd.DataFrame: A Dask DataFrame resulting from the evaluation of the rule.
    """
    return is_contained_in(df, rule)


def not_contained_in(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the specified field's value is
    contained in a given list, and assigns a data quality status to the resulting rows.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be filtered.
        rule (RuleDef): A dictionary containing the rule parameters. It should include:
            - 'field': The column name in the DataFrame to check.
            - 'check': The type of check being performed (e.g., "not_contained_in").
            - 'value': A string representation of a list of values to check against,
              formatted as "[value1, value2, ...]".

    Returns:
        dd.DataFrame: A new DataFrame containing only the rows where the specified
        field's value is in the provided list, with an additional column `dq_status`
        indicating the rule applied in the format "field:check:value".
    """
    parsed_value = __parse_field_list(rule.value)

    if isinstance(parsed_value, list):
        vals = parsed_value
    elif isinstance(parsed_value, str):
        vals = [parsed_value]
    else:
        vals = []

    viol = df[df[rule.field].isin(vals)]
    return viol.assign(dq_status=f"{rule.field}:{rule.check_type}:{rule.value}")


def not_in(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters a Dask DataFrame by excluding rows where the specified rule is satisfied.

    This function delegates the filtering logic to the `not_contained_in` function.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be filtered.
        rule (RuleDef): A dictionary defining the filtering rule. The structure and
                     interpretation of this rule depend on the implementation of
                     `not_contained_in`.

    Returns:
        dd.DataFrame: A new Dask DataFrame with rows excluded based on the rule.
    """
    return not_contained_in(df, rule)


def is_between(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where a specified field's value
    does not fall within a given range.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (RuleDef): A dictionary containing the rule parameters. It should
            include:
            - 'field': The column name in the DataFrame to check.
            - 'check': The type of check being performed (e.g., "between").
            - 'value': A string representing the range in the format "[lo,hi]".

    Returns:
        dd.DataFrame: A new DataFrame containing only the rows that violate
        the specified range condition. An additional column `dq_status` is
        added to indicate the field, check, and value that caused the violation.
    """
    parsed_value = __parse_field_list(rule.value)

    if isinstance(parsed_value, list) and len(parsed_value) == 2:
        lo = __convert_value(str(parsed_value[0]))
        hi = __convert_value(str(parsed_value[1]))
    elif isinstance(parsed_value, str):
        # Fallback: split manual
        parts = parsed_value.split(",")
        if len(parts) == 2:
            lo = __convert_value(parts[0].strip())
            hi = __convert_value(parts[1].strip())
        else:
            raise ValueError(f"Invalid range format: {rule.value}")
    else:
        raise ValueError(f"Invalid range format: {rule.value}")

    viol = df[~df[rule.field].between(lo, hi)]
    return viol.assign(dq_status=f"{rule.field}:{rule.check_type}:{rule.value}")


def has_pattern(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Identifies rows in a Dask DataFrame that do not match a specified pattern.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (RuleDef): A dictionary containing the rule parameters. It should include:
            - 'field': The column name in the DataFrame to apply the pattern check.
            - 'check': A descriptive label for the type of check being performed.
            - 'value': The regex pattern to match against the specified column.

    Returns:
        dd.DataFrame: A DataFrame containing rows that do not match the specified pattern.
                      An additional column `dq_status` is added, indicating the rule details
                      in the format "{field}:{check}:{value}".
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    viol = df[~df[field].str.match(value, na=False)]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_legit(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Validates a Dask DataFrame against a specified rule and returns rows that violate the rule.

    Args:
        df (dd.DataFrame): The Dask DataFrame to validate.
        rule (RuleDef): A dictionary containing the validation rule. It should include:
            - 'field': The column name in the DataFrame to validate.
            - 'check': The type of validation check (e.g., regex, condition).
            - 'value': The value or pattern to validate against.

    Returns:
        dd.DataFrame: A new DataFrame containing rows that violate the rule, with an additional
        column `dq_status` indicating the field, check, and value of the failed validation.
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    s = df[field].astype("string")
    mask = s.notnull() & s.str.contains(r"^\S+$", na=False)
    viol = df[~mask]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_primary_key(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Determines if the specified rule identifies a primary key in the given Dask DataFrame.

    This function checks whether the combination of columns specified in the rule
    results in unique values across the DataFrame, effectively identifying a primary key.

    Args:
        df (dd.DataFrame): The Dask DataFrame to evaluate.
        rule (RuleDef): A dictionary defining the rule to check for primary key uniqueness.
                     Typically, this includes the column(s) to be evaluated.

    Returns:
        dd.DataFrame: A Dask DataFrame indicating whether the rule satisfies the primary key condition.
    """
    return is_unique(df, rule)


def is_composite_key(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Determines if the given DataFrame satisfies the composite key condition based on the provided rule.

    Args:
        df (dd.DataFrame): A Dask DataFrame to be checked.
        rule (RuleDef): A dictionary defining the composite key rule.

    Returns:
        dd.DataFrame: A Dask DataFrame indicating whether the composite key condition is met.
    """
    return are_unique(df, rule)


def has_max(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Identifies rows in a Dask DataFrame where the value of a specified field exceeds a given maximum value.

    Parameters:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (RuleDef): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the column to check.
            - 'check': A string describing the check (e.g., 'max').
            - 'value': The maximum allowable value for the specified field.

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows that violate the rule.
                      An additional column `dq_status` is added to indicate the rule violation
                      in the format "{field}:{check}:{value}".
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
        actual = float(df[field].max().compute())
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


def has_min(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Checks if the values in a specified field of a Dask DataFrame are greater than
    or equal to a given minimum value. Returns a DataFrame containing rows that
    violate this rule, with an additional column indicating the data quality status.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to validate.
        rule (RuleDef): A dictionary containing the rule parameters. It should include:
            - 'field': The column name to check.
            - 'check': The type of check being performed (e.g., 'min').
            - 'value': The minimum value to compare against.

    Returns:
        dd.DataFrame: A DataFrame containing rows that do not meet the minimum value
        requirement, with an additional column `dq_status` indicating the rule
        violation in the format "field:check:value".
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
        actual = float(df[field].min().compute())
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


def has_std(
    df: dd.DataFrame, rule: RuleDef
) -> dict[str, str | float | None | Any] | dict[str, str | None | Any]:
    """
    Checks if the standard deviation of a specified field in a Dask DataFrame exceeds a given value.

    Parameters:
        df (dd.DataFrame): The input Dask DataFrame to evaluate.
        rule (RuleDef): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The name of the column to calculate the standard deviation for.
            - 'check' (str): A descriptive label for the check being performed.
            - 'value' (float): The threshold value for the standard deviation.

    Returns:
        dd.DataFrame:
            - If the standard deviation of the specified field exceeds the given value,
              returns the original DataFrame with an additional column `dq_status` indicating the rule details.
            - If the standard deviation does not exceed the value, returns an empty DataFrame with the same structure.
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
        actual = float(df[field].std().compute())
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


def has_mean(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Checks if the mean of a specified field in a Dask DataFrame satisfies a given condition.

    Parameters:
        df (dd.DataFrame): The input Dask DataFrame to evaluate.
        rule (RuleDef): A dictionary containing the rule to apply. It should include:
            - 'field' (str): The column name to calculate the mean for.
            - 'check' (str): The type of check to perform (e.g., 'greater_than').
            - 'value' (float): The threshold value to compare the mean against.

    Returns:
        dd.DataFrame: A new Dask DataFrame with an additional column `dq_status` if the mean
        satisfies the condition. If the condition is not met, an empty Dask DataFrame is returned.
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
        actual = float(df[field].mean().compute())
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


def has_sum(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Checks if the sum of a specified field in a Dask DataFrame exceeds a given value
    and returns a modified DataFrame with a status column if the condition is met.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to evaluate.
        rule (RuleDef): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The column name to sum.
            - 'check' (str): A descriptive label for the check (used in the status message).
            - 'value' (float): The threshold value to compare the sum against.

    Returns:
        dd.DataFrame: A new Dask DataFrame. If the sum exceeds the threshold, the DataFrame
        will include a `dq_status` column with a status message. Otherwise, an empty
        DataFrame with the same structure as the input is returned.
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
        actual = float(df[field].sum().compute())
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


def has_cardinality(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Checks if the cardinality (number of unique values) of a specified field in a Dask DataFrame
    exceeds a given threshold and returns a modified DataFrame based on the result.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to evaluate.
        rule (RuleDef): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The column name to check cardinality for.
            - 'check' (str): A descriptive label for the check (used in the output).
            - 'value' (int): The maximum allowed cardinality.

    Returns:
        dd.DataFrame: If the cardinality of the specified field exceeds the given value,
        returns the original DataFrame with an additional column `dq_status` indicating
        the rule violation. Otherwise, returns an empty DataFrame with the same structure
        as the input DataFrame.
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
        actual = int(df[field].nunique().compute() or 0)
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


def has_infogain(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Evaluates whether a given field in a Dask DataFrame satisfies an information gain condition
    based on the specified rule. If the condition is met, the DataFrame is updated with a
    `dq_status` column indicating the rule applied. Otherwise, an empty DataFrame is returned.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to evaluate.
        rule (RuleDef): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The column name to evaluate.
            - 'check' (str): The type of check being performed (used for status annotation).
            - 'value' (float): The threshold value for the information gain.

    Returns:
        dd.DataFrame: The original DataFrame with an added `dq_status` column if the condition
        is met, or an empty DataFrame if the condition is not satisfied.
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
        # Calculate entropy
        value_counts = df[field].value_counts().compute()
        total_count = len(df)

        probabilities = value_counts / total_count
        entropy = float(-np.sum(probabilities * np.log2(probabilities)))

        # Calculate max entropy
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


def has_entropy(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Evaluates the entropy of a specified field in a Dask DataFrame and applies a rule to determine
    if the entropy exceeds a given threshold. If the threshold is exceeded, a new column `dq_status`
    is added to the DataFrame with information about the rule violation. Otherwise, an empty DataFrame
    is returned.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to evaluate.
        rule (RuleDef): A dictionary containing the rule parameters. It should include:
            - `field` (str): The column name to evaluate.
            - `check` (str): The type of check being performed (used for status message).
            - `value` (float): The threshold value for the entropy.

    Returns:
        dd.DataFrame: A DataFrame with the `dq_status` column added if the entropy exceeds the threshold,
        or an empty DataFrame if the threshold is not exceeded.
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
        # Calculate Shannon entropy
        value_counts = df[field].value_counts().compute()
        total_count = len(df)

        probabilities = value_counts / total_count
        entropy = float(-np.sum(probabilities * np.log2(probabilities)))

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


def satisfies(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters a Dask DataFrame based on a rule and returns rows that do not satisfy the rule.

    The function evaluates a rule on the given Dask DataFrame and identifies rows that
    violate the rule. The rule is specified as a dictionary containing a field, a check,
    and a value. The rule's logical expression is converted to Python syntax for evaluation.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be filtered.
        rule (RuleDef): A dictionary specifying the rule to evaluate. It should contain:
            - 'field': The column name in the DataFrame to evaluate.
            - 'check': The type of check or condition to apply.
            - 'value': The value or expression to evaluate against.

    Returns:
        dd.DataFrame: A new Dask DataFrame containing rows that do not satisfy the rule.
        An additional column `dq_status` is added, which contains a string in the format
        "{field}:{check}:{value}" to indicate the rule that was violated.

    Example:
        >>> import dask.dataframe as dd
        >>> data = {'col1': [1, 2, 3], 'col2': [4, 5, 6]}
        >>> df = dd.from_pandas(pd.DataFrame(data), npartitions=1)
        >>> rule = {'field': 'col1', 'check': '>', 'value': '2'}
        >>> result = satisfies(df, rule)
        >>> result.compute()
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    py_expr = value
    py_expr = re.sub(r"(?<![=!<>])=(?!=)", "==", py_expr)
    py_expr = re.sub(r"\bAND\b", "&", py_expr, flags=re.IGNORECASE)
    py_expr = re.sub(r"\bOR\b", "|", py_expr, flags=re.IGNORECASE)

    def _filter_viol(pdf: pd.DataFrame) -> pd.DataFrame:
        mask = pdf.eval(py_expr)
        return pdf.loc[~mask]

    meta = df._meta
    viol = df.map_partitions(_filter_viol, meta=meta)
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def validate_date_format(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Validates the date format of a specified column in a Dask DataFrame.

    This function checks whether the values in a specified column of the
    DataFrame conform to a given date format. Rows with invalid date formats
    are returned with an additional column indicating the validation status.

    Args:
        df (dd.DataFrame): The Dask DataFrame to validate.
        rule (RuleDef): A dictionary containing the validation rule. It should
                     include the following keys:
                     - 'field': The name of the column to validate.
                     - 'check': A string describing the validation check.
                     - 'fmt': The expected date format (e.g., '%Y-%m-%d').

    Returns:
        dd.DataFrame: A DataFrame containing rows where the date format
                      validation failed. An additional column `dq_status`
                      is added, which contains a string describing the
                      validation status in the format "{field}:{check}:{fmt}".
    """
    # field, check, fmt = __extract_params(rule)
    field = rule.field
    check = rule.check_type
    value = rule.value
    col_dt = dd.to_datetime(df[field], format=value, errors="coerce")
    viol = df[col_dt.isna()]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_future_date(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Checks for rows in a Dask DataFrame where the specified date field contains a future date.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to validate.
        rule (RuleDef): A dictionary containing the rule parameters. It is expected to include:
            - field: The name of the column to check.
            - check: A descriptive label for the check (used in the output).
            - _: Additional parameters (ignored in this function).

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the date in the specified
        field is in the future. An additional column `dq_status` is added to indicate the status
        of the validation in the format: "<field>:<check>:<current_date>".

    Notes:
        - The function coerces the specified column to datetime format, and invalid parsing results
          in NaT (Not a Time).
        - Rows with NaT in the specified column are excluded from the output.
        - The current date is determined using the system's local date.
    """
    field = rule.field
    check = rule.check_type
    value = rule.value

    today = pd.Timestamp(date.today())
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt > today]
    return viol.assign(dq_status=f"{field}:{check}:{today.date().isoformat()}")


def is_past_date(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Checks if the values in a specified date column of a Dask DataFrame are in the past.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be checked.
        rule (RuleDef): A dictionary containing the rule parameters. It is expected to include
                     the field name to check, the check type, and additional parameters.

    Returns:
        dd.DataFrame: A Dask DataFrame containing rows where the date in the specified column
                      is in the past. An additional column `dq_status` is added to indicate
                      the field, check type, and the date of the check in the format
                      "field:check:YYYY-MM-DD".
    """
    field = rule.field
    check = rule.check_type
    value = rule.value

    today = pd.Timestamp(date.today())
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt < today]
    return viol.assign(dq_status=f"{field}:{check}:{today.date().isoformat()}")


def is_date_between(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where a date field does not fall within a specified range.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be checked.
        rule (RuleDef): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the column in the DataFrame to check.
            - 'check': A string representing the type of check (used for status annotation).
            - 'val': A string representing the date range in the format "[start_date, end_date]".

    Returns:
        dd.DataFrame: A DataFrame containing rows where the date field does not fall within the specified range.
                      An additional column 'dq_status' is added to indicate the rule violation in the format
                      "{field}:{check}:{val}".
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    start, end = [pd.Timestamp(v.strip()) for v in value.strip("[]").split(",")]
    col_dt = dd.to_datetime(df[field], errors="coerce")
    mask = (col_dt >= start) & (col_dt <= end)
    viol = df[~mask]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_date_after(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where a specified date field is
    earlier than a given reference date.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (RuleDef): A dictionary containing the rule parameters. It should
            include:
            - field (str): The name of the column to check.
            - check (str): A descriptive label for the check (used in the
              output status).
            - date_str (str): The reference date as a string in a format
              compatible with `pd.Timestamp`.

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the
        specified date field is earlier than the reference date. An additional
        column `dq_status` is added, which contains a string describing the
        rule violation in the format `field:check:date_str`.

    Raises:
        ValueError: If the `rule` dictionary does not contain the required keys.
    """

    field = rule.field
    check = rule.check_type
    value = rule.value
    ref = pd.Timestamp(value)
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt < ref]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_date_before(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Checks if the values in a specified date column of a Dask DataFrame are before a given reference date.

    Parameters:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be validated.
        rule (RuleDef): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the column to check.
            - 'check': A descriptive string for the check (e.g., "is_date_before").
            - 'value': The reference date as a string in a format parsable by pandas.Timestamp.

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the date in the specified column
        is after the reference date. An additional column 'dq_status' is added to indicate the validation
        status in the format "{field}:{check}:{value}".
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    ref = pd.Timestamp(value)
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt > ref]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def all_date_checks(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Applies date validation checks on a Dask DataFrame based on the provided rule.

    This function serves as an alias for the `is_past_date` function, which performs
    checks to determine if dates in the DataFrame meet the specified criteria.

    Args:
        df (dd.DataFrame): The Dask DataFrame containing the data to be validated.
        rule (RuleDef): A dictionary specifying the validation rules to be applied.

    Returns:
        dd.DataFrame: A Dask DataFrame with the results of the date validation checks.
    """
    return is_past_date(df, rule)


def is_t_minus_1(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where a specified datetime column
    matches the date of "T-1" (yesterday) and assigns a data quality status.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be checked.
        rule (RuleDef): A dictionary containing the rule parameters. It is expected
            to include the following keys:
            - 'field': The name of the column to check.
            - 'check': A string describing the check being performed.
            - 'value': Additional value or metadata related to the check.

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the
        specified column matches "T-1". An additional column `dq_status` is added
        to indicate the data quality status in the format "{field}:{check}:{value}".
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    target = pd.Timestamp(date.today() - pd.Timedelta(days=1))
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt != target]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_t_minus_2(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where a specified datetime column
    matches the date two days prior to the current date.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be filtered.
        rule (RuleDef): A dictionary containing the rule parameters. It is expected to
            include the following keys:
            - 'field': The name of the column in the DataFrame to check.
            - 'check': A string representing the type of check (used for metadata).
            - 'value': A value associated with the rule (used for metadata).

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the specified
        column matches the target date (two days prior to the current date). An additional
        column `dq_status` is added to indicate the rule applied in the format
        "{field}:{check}:{value}".
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    target = pd.Timestamp(date.today() - pd.Timedelta(days=2))
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt != target]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_t_minus_3(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the specified date field matches
    exactly three days prior to the current date.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be checked.
        rule (RuleDef): A dictionary containing rule parameters. It is expected to include
                     the field name to check, the type of check, and the value (unused in this function).

    Returns:
        dd.DataFrame: A filtered Dask DataFrame containing only the rows where the specified
                      date field matches three days prior to the current date. An additional
                      column `dq_status` is added to indicate the rule applied in the format
                      "{field}:{check}:{value}".
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    target = pd.Timestamp(date.today() - pd.Timedelta(days=3))
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt != target]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_today(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the specified field matches today's date.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be filtered.
        rule (RuleDef): A dictionary containing the rule parameters. It is expected to have
            the following keys:
            - field (str): The name of the column in the DataFrame to check.
            - check (str): A descriptive label for the type of check being performed.
            - value (str): A descriptive label for the expected value.

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the specified
        field matches today's date. An additional column `dq_status` is added to indicate
        the rule applied in the format "{field}:{check}:{value}".
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    target = pd.Timestamp(date.today())
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt != target]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_yesterday(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Determines if the rows in a Dask DataFrame correspond to "yesterday"
    based on a given rule.

    This function acts as a wrapper for the `is_t_minus_1` function,
    applying the same logic to check if the data corresponds to the
    previous day.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to evaluate.
        rule (RuleDef): A dictionary containing the rule or criteria
                     to determine "yesterday".

    Returns:
        dd.DataFrame: A Dask DataFrame with the evaluation results.
    """
    return is_t_minus_1(df, rule)


def is_on_weekday(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to include only rows where the date in the specified field falls on a weekday
    (Monday to Friday) and assigns a data quality status column.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be filtered.
        rule (RuleDef): A dictionary containing the rule parameters. It is expected to have the following keys:
            - field (str): The name of the column in the DataFrame containing date values.
            - check (str): A descriptive string for the check being performed.
            - value (str): A value associated with the rule, used for constructing the `dq_status` column.

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the date in the specified field
        falls on a weekday. An additional column `dq_status` is added to indicate the rule applied.
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    col_dt = dd.to_datetime(df[field], errors="coerce")
    dow = col_dt.dt.weekday
    viol = df[(dow >= 5) & (dow <= 6)]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_on_weekend(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Identifies rows in a Dask DataFrame where the date in a specified column falls on a weekend.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be checked.
        rule (RuleDef): A dictionary containing the rule parameters. It is expected to have
                     the following keys:
                     - 'field': The name of the column in the DataFrame to check.
                     - 'check': A string representing the type of check (used for status annotation).
                     - 'value': A value associated with the rule (used for status annotation).

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the date in the specified
                      column falls on a weekend (Saturday or Sunday). An additional column `dq_status`
                      is added to indicate the rule applied in the format "{field}:{check}:{value}".
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    col_dt = dd.to_datetime(df[field], errors="coerce")
    dow = col_dt.dt.weekday
    viol = df[(dow >= 0) & (dow <= 4)]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_on_monday(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the date in a specified column falls on a Monday.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be checked.
        rule (RuleDef): A dictionary containing the rule parameters. It is expected to include:
            - 'field': The name of the column in the DataFrame to check.
            - 'check': A string representing the type of check (used for status assignment).
            - 'value': A value associated with the rule (used for status assignment).

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the date in the specified
        column falls on a Monday. An additional column `dq_status` is added to indicate the rule
        applied in the format "{field}:{check}:{value}".
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt.dt.weekday != 0]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_on_tuesday(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the specified date field falls on a Tuesday.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be checked.
        rule (RuleDef): A dictionary containing the rule parameters. It is expected to include:
            - 'field': The name of the column in the DataFrame to check.
            - 'check': A string representing the type of check (used for status annotation).
            - 'value': A value associated with the rule (used for status annotation).

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the specified date field
        falls on a Tuesday. An additional column `dq_status` is added to indicate the rule applied
        in the format "{field}:{check}:{value}".
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt.dt.weekday != 1]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_on_wednesday(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the date in a specified column falls on a Wednesday.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be checked.
        rule (RuleDef): A dictionary containing the rule parameters. It is expected to include:
            - `field` (str): The name of the column in the DataFrame to check.
            - `check` (str): A descriptive string for the check being performed.
            - `value` (str): A value associated with the rule (not directly used in the function).

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the date in the specified column
        falls on a Wednesday. An additional column `dq_status` is added to indicate the rule applied in the
        format `{field}:{check}:{value}`.
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt.dt.weekday != 2]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_on_thursday(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the specified date field falls on a Thursday.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be checked.
        rule (RuleDef): A dictionary containing the rule parameters. It is expected to include:
            - field (str): The name of the column in the DataFrame to check.
            - check (str): A descriptive string for the type of check being performed.
            - value (str): A value associated with the rule (not used in the logic but included in the output).

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the specified date field
        falls on a Thursday. An additional column `dq_status` is added to indicate the rule applied
        in the format "{field}:{check}:{value}".
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt.dt.weekday != 3]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_on_friday(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where a specified date field falls on a Friday.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be checked.
        rule (RuleDef): A dictionary containing the rule parameters. It is expected to have
            the following keys:
            - field (str): The name of the column in the DataFrame to check.
            - check (str): A descriptive string for the check being performed.
            - value (str): A value associated with the rule, used for status annotation.

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the specified
        date field falls on a Friday. An additional column `dq_status` is added to the
        DataFrame, containing a string in the format "{field}:{check}:{value}" to indicate
        the rule applied.
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt.dt.weekday != 4]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_on_saturday(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the date in a specified column falls on a Saturday.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be checked.
        rule (RuleDef): A dictionary containing the rule parameters. It is expected to include:
            - 'field': The name of the column in the DataFrame to check.
            - 'check': A string representing the type of check (used for status assignment).
            - 'value': A value associated with the rule (used for status assignment).

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the date in the specified
        column falls on a Saturday. An additional column `dq_status` is added to indicate the rule
        applied in the format "{field}:{check}:{value}".
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt.dt.weekday != 5]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_on_sunday(df: dd.DataFrame, rule: RuleDef) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where a specified date field falls on a Sunday.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be checked.
        rule (RuleDef): A dictionary containing the rule parameters. It is expected to include:
            - field (str): The name of the column in the DataFrame to check.
            - check (str): A descriptive string for the check being performed.
            - value (str): A value associated with the rule, used for status annotation.

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the specified
        date field falls on a Sunday. An additional column `dq_status` is added to indicate
        the rule applied in the format "{field}:{check}:{value}".
    """
    field = rule.field
    check = rule.check_type
    value = rule.value
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt.dt.weekday != 6]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def validate(
    df: dd.DataFrame, rules: list[RuleDef]
) -> tuple[dd.DataFrame, dd.DataFrame]:
    """
    Main validation function that orchestrates row-level and table-level validations.

    Args:
        df: Input Dask DataFrame to validate
        rules: List of all validation rules

    Returns:
        Tuple of (aggregated_violations, raw_violations, table_summary)
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
        empty = dd.from_pandas(
            pd.DataFrame(columns=df.columns.tolist() + ["dq_status"]), npartitions=1
        )
        df_with_status = empty
        row_violations = empty

    if table_rules:
        table_summary = validate_table_level(df, table_rules)
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

    return df_with_status, row_violations, table_summary


def validate_row_level(
    df: dd.DataFrame, rules: List[RuleDef]
) -> Tuple[dd.DataFrame, dd.DataFrame]:
    """
    Validates DataFrame at row level using specified rules.

    Args:
        df: Input Dask DataFrame to validate
        rules: List of row-level validation rules

    Returns:
        Tuple of (aggregated violations, raw violations)
    """
    engine = "dask"
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
        empty = dd.from_pandas(
            pd.DataFrame(columns=df.columns.tolist() + ["dq_status"]), npartitions=1
        )
        return empty, empty

    # Initialize empty DataFrame
    empty = dd.from_pandas(
        pd.DataFrame(columns=df.columns.tolist() + ["dq_status"]), npartitions=1
    )
    raw_df = empty

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
            raw_df = dd.concat([raw_df, viol], interleave_partitions=True)
        except Exception as e:
            warnings.warn(f"❌ Error executing {check_type} on {rule.field}: {e}")
            continue

    # Group and aggregate violations
    group_cols = [c for c in df.columns if c != "dq_status"]

    def _concat_status(series: pd.Series) -> str:
        """Concatenates status values with semicolon separator."""
        return ";".join([s for s in series.astype(str) if s])

    agg_df = (
        raw_df.groupby(group_cols)
        .dq_status.apply(_concat_status, meta=("dq_status", "object"))
        .reset_index()
    )

    return agg_df, raw_df


def validate_table_level(df: dd.DataFrame, rules: List[RuleDef]) -> pd.DataFrame:
    """
    Validates DataFrame at table level using specified rules.

    Args:
        df: Input Dask DataFrame to validate
        rules: List of table-level validation rules

    Returns:
        Summary DataFrame with validation results
    """
    engine = "dask"
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

    # Sort: FAIL first, then ERROR, then PASS
    summary_df["_sort"] = summary_df["status"].map({"FAIL": 0, "ERROR": 1, "PASS": 2})
    summary_df = (
        summary_df.sort_values("_sort").drop(columns=["_sort"]).reset_index(drop=True)
    )

    return summary_df


def summarize(
    rules: List[RuleDef],
    total_rows: int,
    df_with_errors: Optional[dd.DataFrame] = None,
    table_error: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Summarizes validation results from both row-level and table-level checks.

    Args:
        rules: List of all validation rules
        total_rows: Total number of rows in the input DataFrame
        df_with_errors: DataFrame with row-level violations
        table_error: DataFrame with table-level results

    Returns:
        Summary DataFrame with aggregated validation metrics
    """

    summaries = []

    # ========== ROW-LEVEL SUMMARY ==========
    row_rules = [
        r for r in rules if r.level and r.level.upper().replace("_LEVEL", "") == "ROW"
    ]

    if row_rules and df_with_errors is not None:
        # Compute violations
        df_computed = df_with_errors.compute()
        df_computed = df_computed[df_computed["dq_status"].astype(bool)]

        viol_dict = {}

        if len(df_computed) > 0:
            # ✅ FIX: Split apenas nos 2 primeiros ":" (máximo 3 partes)
            # Usar str.split com n=2 e depois verificar
            try:
                # Método 1: Split com limite explícito
                split_data = []
                for status in df_computed["dq_status"]:
                    parts = status.split(":", 2)  # Split em no máximo 3 partes
                    if len(parts) >= 2:
                        field = parts[0]
                        check_type = parts[1]
                        value = parts[2] if len(parts) > 2 else ""
                        split_data.append(
                            {"field": field, "check_type": check_type, "value": value}
                        )

                if split_data:
                    split_df = pd.DataFrame(split_data)

                    viol_count = (
                        split_df.groupby(["field", "check_type"], dropna=False)
                        .size()
                        .reset_index(name="violations")
                    )

                    viol_dict = {
                        (row["field"], row["check_type"]): row["violations"]
                        for _, row in viol_count.iterrows()
                    }
            except Exception as e:
                warnings.warn(f"⚠️  Error parsing dq_status: {e}")
                viol_dict = {}

        for rule in row_rules:
            field_str = (
                rule.field if isinstance(rule.field, str) else ",".join(rule.field)
            )

            violations = viol_dict.get((field_str, rule.check_type), 0)
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
        empty_df = pd.DataFrame(
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
        return dd.from_pandas(empty_df, npartitions=1)

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

    return dd.from_pandas(summary_df, npartitions=1)


def extract_schema(df: dd.DataFrame) -> List[Dict[str, Any]]:
    """
    Extracts schema from Dask DataFrame.

    Args:
        df: Input Dask DataFrame

    Returns:
        List of dictionaries containing field information
    """
    return [
        {
            "field": col,
            "data_type": str(dtype),
            "nullable": True,
            "max_length": None,
        }
        for col, dtype in df.dtypes.items()
    ]


def validate_schema(
    df: dd.DataFrame, expected: List[Dict[str, Any]]
) -> tuple[bool, list[dict[str, Any]]]:
    """
    Validates the schema of a Dask DataFrame against an expected schema.

    Args:
        df: Dask DataFrame to validate
        expected: Expected schema definition

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    actual = extract_schema(df)
    result, errors = __compare_schemas(actual, expected)
    return result, errors
