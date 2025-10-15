#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Pandas engine for data quality validation.

Provides row-level and table-level validation functions for pandas DataFrames,
including completeness, uniqueness, range checks, pattern matching, date validations,
SQL-style custom expressions, and schema validation.

Functions:
    is_positive: Filters rows where the specified field is less than zero.

    is_negative: Filters rows where the specified field is greater than or equal to zero.

    is_in_millions: Retains rows where the field value is at least 1,000,000 and flags them with dq_status.

    is_in_billions: Retains rows where the field value is at least 1,000,000,000 and flags them with dq_status.

    is_t_minus_1: Retains rows where the date field equals yesterday (T-1) and flags them with dq_status.

    is_t_minus_2: Retains rows where the date field equals two days ago (T-2) and flags them with dq_status.

    is_t_minus_3: Retains rows where the date field equals three days ago (T-3) and flags them with dq_status.

    is_today: Retains rows where the date field equals today and flags them with dq_status.

    is_yesterday: Retains rows where the date field equals yesterday and flags them with dq_status.

    is_on_weekday: Retains rows where the date field falls on a weekday (Mon-Fri) and flags them with dq_status.

    is_on_weekend: Retains rows where the date field is on a weekend (Sat-Sun) and flags them with dq_status.

    is_on_monday: Retains rows where the date field is on Monday and flags them with dq_status.

    is_on_tuesday: Retains rows where the date field is on Tuesday and flags them with dq_status.

    is_on_wednesday: Retains rows where the date field is on Wednesday and flags them with dq_status.

    is_on_thursday: Retains rows where the date field is on Thursday and flags them with dq_status.

    is_on_friday: Retains rows where the date field is on Friday and flags them with dq_status.

    is_on_saturday: Retains rows where the date field is on Saturday and flags them with dq_status.

    is_on_sunday: Retains rows where the date field is on Sunday and flags them with dq_status.

    is_complete: Filters rows where the specified field is null.

    is_unique: Filters rows with duplicate values in the specified field.

    are_complete: Filters rows where any of the specified fields are null.

    are_unique: Filters rows with duplicate combinations of the specified fields.

    is_greater_than: Filters rows where the specified field is less than or equal to the given value.

    is_greater_or_equal_than: Filters rows where the specified field is less than the given value.

    is_less_than: Filters rows where the specified field is greater than or equal to the given value.

    is_less_or_equal_than: Filters rows where the specified field is greater than the given value.

    is_equal: Filters rows where the specified field is not equal to the given value.

    is_equal_than: Alias for `is_equal`.

    is_contained_in: Filters rows where the specified field is not in the given list of values.

    not_contained_in: Filters rows where the specified field is in the given list of values.

    is_between: Filters rows where the specified field is not within the given range.

    has_pattern: Filters rows where the specified field does not match the given regex pattern.

    is_legit: Filters rows where the specified field is null or contains whitespace.

    has_max: Filters rows where the specified field exceeds the given maximum value.

    has_min: Filters rows where the specified field is below the given minimum value.

    has_std: Checks if the standard deviation of the specified field exceeds the given value.

    has_mean: Checks if the mean of the specified field exceeds the given value.

    has_sum: Checks if the sum of the specified field exceeds the given value.

    has_cardinality: Checks if the cardinality (number of unique values) of the specified field exceeds the given value.

    has_infogain: Placeholder for information gain validation (currently uses cardinality).

    has_entropy: Placeholder for entropy validation (currently uses cardinality).

    satisfies: Filters rows that do not satisfy the given custom expression.

    validate_date_format: Filters rows where the specified field does not match the expected date format or is null.

    is_future_date: Filters rows where the specified date field is after today’s date.

    is_past_date: Filters rows where the specified date field is before today’s date.

    is_date_between: Filters rows where the specified date field is not within the given [start,end] range.

    is_date_after: Filters rows where the specified date field is before the given date.

    is_date_before: Filters rows where the specified date field is after the given date.

    all_date_checks: Alias for `is_past_date` (checks date against today).

    validate: Validates a DataFrame against a list of rules and returns the original DataFrame with data quality status and a DataFrame of violations.

    __build_rules_df: Converts a list of rules into a Pandas DataFrame for summarization.

    summarize: Summarizes the results of data quality checks, including pass rates and statuses.

    validate_schema: Validates the schema of a DataFrame against an expected schema and returns a boolean result and a list of errors.
"""
import re
import warnings
from datetime import date, timedelta
from typing import List, Dict, Any, Tuple, Optional

import numpy as np
import pandas as pd

from sumeh.core.rules.rule_model import RuleDef
from sumeh.core.utils import (
    __convert_value,
    __compare_schemas,
    __transform_date_format_in_pattern,
)


def is_positive(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Identifies rows in a DataFrame where the specified field contains negative values.

    Args:
        df (pd.DataFrame): The input DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It is expected to include:
            - 'field': The column name in the DataFrame to check.
            - 'check': A descriptive label for the type of check being performed.
            - 'value': A value associated with the rule (not directly used in this function).

    Returns:
        pd.DataFrame: A DataFrame containing only the rows where the specified field has negative values.
                      An additional column 'dq_status' is added to indicate the rule violation in the format
                      "{rule.field}:{rule.check_type}:{rule.value}".
    """
    viol = df[df[rule.field] < 0].copy()
    viol["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return viol


def is_negative(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters a DataFrame to identify rows where a specified field does not satisfy a "negative" condition.

    Args:
        df (pd.DataFrame): The input DataFrame to be checked.
        rule (RuleDef): A dictionary containing the rule parameters. It is expected to include:
            - 'field': The column name in the DataFrame to check.
            - 'check': The type of check being performed (e.g., "negative").
            - 'value': Additional value associated with the rule (not used in this function).

    Returns:
        pd.DataFrame: A new DataFrame containing rows where the specified field is non-negative (>= 0).
                      An additional column 'dq_status' is added to indicate the rule violation in the format
                      "{rule.field}:{rule.check_type}:{rule.value}".
    """
    viol = df[df[rule.field] >= 0].copy()
    viol["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return viol


def is_complete(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Checks for missing values in a specified field of a DataFrame based on a given rule.

    Args:
        df (pd.DataFrame): The input DataFrame to check for completeness.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the field/column to check for missing values.
            - 'check': The type of check being performed (not used in this function).
            - 'value': Additional value associated with the rule (not used in this function).

    Returns:
        pd.DataFrame: A DataFrame containing rows where the specified field has missing values.
                      An additional column 'dq_status' is added to indicate the rule that was violated.
    """
    viol = df[df[rule.field].isna()].copy()
    viol["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return viol


def is_unique(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Checks for duplicate values in a specified field of a DataFrame based on a rule.

    Args:
        df (pd.DataFrame): The input DataFrame to check for duplicates.
        rule (dict): A dictionary containing the rule parameters. It is expected to
                     include the field to check, the type of check, and a value.

    Returns:
        pd.DataFrame: A DataFrame containing the rows with duplicate values in the
                      specified field. An additional column 'dq_status' is added
                      to indicate the field, check type, and value associated with
                      the rule.
    """
    dup = df[rule.field].duplicated(keep=False)
    viol = df[dup].copy()
    viol["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return viol


def are_complete(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Checks for completeness of specified fields in a DataFrame based on a given rule.

    This function identifies rows in the DataFrame where any of the specified fields
    contain missing values (NaN). It returns a DataFrame containing only the rows
    that violate the completeness rule, along with an additional column `dq_status`
    that describes the rule violation.

    Args:
        df (pd.DataFrame): The input DataFrame to check for completeness.
        rule (dict): A dictionary containing the rule parameters. It is expected to
            include the following keys:
            - fields: A list of column names to check for completeness.
            - check: A string describing the type of check (e.g., "completeness").
            - value: A value associated with the rule (e.g., a threshold or description).

    Returns:
        pd.DataFrame: A DataFrame containing rows that violate the completeness rule.
        The returned DataFrame includes all original columns and an additional column
        `dq_status` that describes the rule violation in the format "fields:check:value".
    """

    fields = rule.field if isinstance(rule.field, list) else [rule.field]
    mask = df[fields].isna().any(axis=1)
    viol = df[mask].copy()
    viol["dq_status"] = f"{fields}:{rule.check_type}:{rule.value}"
    return viol


def are_unique(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Checks for duplicate rows in the specified fields of a DataFrame based on a given rule.

    Args:
        df (pd.DataFrame): The input DataFrame to check for uniqueness.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - fields: A list of column names to check for uniqueness.
            - check: A string representing the type of check (e.g., "unique").
            - value: A value associated with the rule (e.g., a description or identifier).

    Returns:
        pd.DataFrame: A DataFrame containing the rows that violate the uniqueness rule.
                      An additional column 'dq_status' is added to indicate the rule
                      that was violated in the format "{fields}:{check}:{value}".
    """
    fields = rule.field if isinstance(rule.field, list) else [rule.field]
    combo = df[fields].astype(str).agg("|".join, axis=1)
    dup = combo.duplicated(keep=False)
    viol = df[dup].copy()
    viol["dq_status"] = f"{fields}:{rule.check_type}:{rule.value}"
    return viol


def is_greater_than(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters a DataFrame to return rows where a specified field's value is greater than a given threshold.

    Args:
        df (pd.DataFrame): The input DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The column name in the DataFrame to be checked.
            - 'check' (str): The type of check being performed (e.g., 'greater_than').
            - 'value' (numeric): The threshold value to compare against.

    Returns:
        pd.DataFrame: A new DataFrame containing rows where the specified field's value is greater than the given threshold.
                      An additional column 'dq_status' is added to indicate the rule applied in the format "field:check:value".
    """

    viol = df[df[rule.field] <= rule.value].copy()
    viol["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return viol


def is_greater_or_equal_than(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters a DataFrame to include only rows where the value in a specified field
    is greater than or equal to a given threshold. Adds a 'dq_status' column to
    indicate the rule applied.

    Args:
        df (pd.DataFrame): The input DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The column name to apply the rule on.
            - 'check' (str): The type of check being performed (e.g., 'greater_or_equal').
            - 'value' (numeric): The threshold value for the comparison.

    Returns:
        pd.DataFrame: A new DataFrame containing only the rows that satisfy the rule,
        with an additional 'dq_status' column describing the rule applied.
    """
    viol = df[df[rule.field] < rule.value].copy()
    viol["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return viol


def is_less_than(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters a DataFrame to return rows where a specified field's value is less than a given threshold.

    Args:
        df (pd.DataFrame): The input DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The column name in the DataFrame to be checked.
            - 'check' (str): A descriptive string for the check (e.g., "less_than").
            - 'value' (numeric): The threshold value to compare against.

    Returns:
        pd.DataFrame: A new DataFrame containing only the rows where the specified field's value
        is less than the given threshold. An additional column 'dq_status' is added to indicate
        the rule applied in the format "field:check:value".
    """

    viol = df[df[rule.field] >= rule.value].copy()
    viol["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return viol


def is_less_or_equal_than(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters rows in a DataFrame where the value in a specified field is less than or equal to a given value.

    Args:
        df (pd.DataFrame): The input DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The column name in the DataFrame to apply the rule on.
            - 'check' (str): A descriptive label for the check being performed.
            - 'value' (numeric): The threshold value to compare against.

    Returns:
        pd.DataFrame: A new DataFrame containing only the rows that satisfy the condition.
                      An additional column 'dq_status' is added to indicate the rule applied
                      in the format "{rule.field}:{rule.check_type}:{rule.value}".
    """

    viol = df[df[rule.field] > rule.value].copy()
    viol["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return viol


def is_equal(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters a DataFrame to identify rows where the value in a specified field
    does not match a given value, and annotates these rows with a data quality status.

    Args:
        df (pd.DataFrame): The input DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The column name in the DataFrame to check.
            - 'check': A string describing the check being performed (e.g., "is_equal").
            - 'value': The value to compare against.

    Returns:
        pd.DataFrame: A DataFrame containing rows that do not satisfy the equality check.
        An additional column 'dq_status' is added to indicate the data quality status
        in the format "{rule.field}:{rule.check_type}:{rule.value}".
    """

    viol = df[df[rule.field] != rule.value].copy()
    viol["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return viol


def is_equal_than(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Compares the values in a DataFrame against a specified rule and returns the result.

    This function acts as a wrapper for the `is_equal` function, passing the given
    DataFrame and rule to it.

    Args:
        df (pd.DataFrame): The DataFrame to be evaluated.
        rule (dict): A dictionary containing the comparison rule.

    Returns:
        pd.DataFrame: A DataFrame indicating the result of the comparison.
    """
    return is_equal(df, rule)


def is_contained_in(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters a DataFrame to identify rows where the values in a specified field
    are not contained within a given set of values.

    Args:
        df (pd.DataFrame): The input DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It is expected
                     to include the following keys:
                     - 'field': The column name in the DataFrame to check.
                     - 'check': A descriptive string for the check being performed.
                     - 'value': A list or string representation of the allowed values.

    Returns:
        pd.DataFrame: A DataFrame containing rows from the input DataFrame that
                      do not meet the rule criteria. An additional column
                      'dq_status' is added to indicate the rule violation in
                      the format "field:check:value".
    """

    vals = re.findall(r"'([^']*)'", str(rule.value)) or [
        v.strip() for v in str(rule.value).strip("[]").split(",")
    ]
    viol = df[~df[rule.field].isin(vals)].copy()
    viol["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return viol


def is_in(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Checks if the values in a DataFrame satisfy a given rule by delegating
    the operation to the `is_contained_in` function.

    Args:
        df (pd.DataFrame): The input DataFrame to be evaluated.
        rule (dict): A dictionary defining the rule to check against the DataFrame.

    Returns:
        pd.DataFrame: A DataFrame indicating whether each element satisfies the rule.
    """
    return is_contained_in(df, rule)


def not_contained_in(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters a DataFrame to return rows where the specified field contains values
    that are not allowed according to the provided rule.

    Args:
        df (pd.DataFrame): The input DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The column name in the DataFrame to check.
            - 'check': The type of check being performed (used for status annotation).
            - 'value': A list or string representation of values that are not allowed.

    Returns:
        pd.DataFrame: A DataFrame containing rows that violate the rule. An additional
        column 'dq_status' is added to indicate the rule violation in the format
        "{rule.field}:{rule.check_type}:{rule.value}".
    """
    vals = re.findall(r"'([^']*)'", str(rule.value)) or [
        v.strip() for v in str(rule.value).strip("[]").split(",")
    ]
    viol = df[df[rule.field].isin(vals)].copy()
    viol["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return viol


def not_in(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters a DataFrame by excluding rows that match the specified rule.

    This function is a wrapper around the `not_contained_in` function,
    which performs the actual filtering logic.

    Args:
        df (pd.DataFrame): The input DataFrame to be filtered.
        rule (dict): A dictionary specifying the filtering criteria.

    Returns:
        pd.DataFrame: A new DataFrame with rows that do not match the rule.
    """
    return not_contained_in(df, rule)


def is_between(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters a DataFrame to identify rows where a specified field's values are not within a given range.

    Args:
        df (pd.DataFrame): The input DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The column name in the DataFrame to check.
            - 'check': A descriptive label for the check being performed.
            - 'value': A string representation of the range in the format '[lo, hi]'.

    Returns:
        pd.DataFrame: A DataFrame containing rows that violate the range condition.
                      An additional column 'dq_status' is added to indicate the rule violation in the format 'field:check:value'.
    """

    lo, hi = [__convert_value(x) for x in str(rule.value).strip("[]").split(",")]
    viol = df[~df[rule.field].between(lo, hi)].copy()
    viol["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return viol


def has_pattern(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Checks if the values in a specified column of a DataFrame match a given pattern.

    Args:
        df (pd.DataFrame): The input DataFrame to check.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The column name in the DataFrame to check.
            - 'check': A descriptive label for the check being performed.
            - 'pattern': The regex pattern to match against the column values.

    Returns:
        pd.DataFrame: A DataFrame containing rows that do not match the pattern.
                      An additional column 'dq_status' is added to indicate the
                      field, check, and pattern that caused the violation.
    """

    viol = df[~df[rule.field].astype(str).str.contains(rule.value, na=False)].copy()
    viol["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return viol


def is_legit(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Validates a DataFrame against a specified rule and identifies rows that violate the rule.

    Args:
        df (pd.DataFrame): The input DataFrame to validate.
        rule (dict): A dictionary containing the validation rule. It is expected to have
                     keys that define the field to check, the type of check, and the value
                     to validate against.

    Returns:
        pd.DataFrame: A DataFrame containing rows that violate the rule. An additional
                      column 'dq_status' is added to indicate the field, check, and value
                      that caused the violation in the format "{rule.field}:{rule.check_type}:{rule.value}".
    """

    mask = df[rule.field].notna() & df[rule.field].astype(str).str.contains(
        r"^\S+$", na=False
    )
    viol = df[~mask].copy()
    viol["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return viol


def has_max(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Identifies rows in a DataFrame where the value in a specified field exceeds a given maximum value.

    Args:
        df (pd.DataFrame): The input DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The column name to check.
            - 'check' (str): The type of check being performed (e.g., 'max').
            - 'value' (numeric): The maximum allowable value for the specified field.

    Returns:
        pd.DataFrame: A DataFrame containing rows that violate the rule, with an additional column
        'dq_status' indicating the rule violation in the format "field:check:value".
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
        actual = float(df[field].max())
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
            "expected": expected,
            "actual": actual,
            "message": None if passed else msg,
        }
    except Exception as e:
        return {
            "status": "ERROR",
            "expected": expected,
            "actual": None,
            "message": f"Error: {str(e)}",
        }


def has_min(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters a DataFrame to identify rows where a specified field's value is less than a given threshold.

    Args:
        df (pd.DataFrame): The input DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The column name in the DataFrame to be checked.
            - 'check': The type of check being performed (e.g., 'min').
            - 'value': The threshold value for the check.

    Returns:
        pd.DataFrame: A new DataFrame containing rows that violate the rule, with an additional
        column 'dq_status' indicating the field, check type, and threshold value.
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
        actual = float(df[field].min())
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
            "expected": expected,
            "actual": actual,
            "message": None if passed else msg,
        }
    except Exception as e:
        return {
            "status": "ERROR",
            "expected": expected,
            "actual": None,
            "message": f"Error: {str(e)}",
        }


def has_std(df: pd.DataFrame, rule: RuleDef) -> dict:
    """
    Validates that the standard deviation of the specified column meets the expected threshold.
    Supports both range-based validation (for thresholds < 1.0) and simple comparison.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be validated.
        rule (RuleDef): A dictionary containing the rule parameters. It should include:
            - field (str): The name of the column to calculate the standard deviation for.
            - value (float): The expected standard deviation value to validate against.
            - threshold (float, optional): Tolerance threshold (default: 1.0).
              If < 1.0, creates an acceptable range around the expected value.

    Returns:
        dict: A dictionary containing validation results with keys:
            - status (str): "PASS", "FAIL", or "ERROR"
            - expected (float): The expected threshold value
            - actual (float): The actual computed standard deviation
            - message (str): Description of failure or error, None if passed
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
        actual = float(df[field].std())
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
            "expected": expected,
            "actual": actual,
            "message": None if passed else msg,
        }
    except Exception as e:
        return {
            "status": "ERROR",
            "expected": expected,
            "actual": None,
            "message": f"Error: {str(e)}",
        }


def has_mean(df: pd.DataFrame, rule: RuleDef) -> dict:
    """
    Validates that the mean (average) value of the specified column meets the expected threshold.
    Applies tolerance logic where threshold < 1.0 represents a percentage of the expected value.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be validated.
        rule (RuleDef): A dictionary containing the rule parameters. It should include:
            - field (str): The name of the column to calculate the mean for.
            - value (float): The expected mean value to validate against.
            - threshold (float, optional): Tolerance threshold (default: 1.0).
              If < 1.0, represents minimum acceptable percentage of expected value.

    Returns:
        dict: A dictionary containing validation results with keys:
            - status (str): "PASS", "FAIL", or "ERROR"
            - expected (float): The expected threshold value
            - actual (float): The actual computed mean
            - message (str): Description of failure or error, None if passed
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
        actual = float(df[field].mean())
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
            "expected": expected,
            "actual": actual,
            "message": None if passed else msg,
        }
    except Exception as e:
        return {
            "status": "ERROR",
            "expected": expected,
            "actual": None,
            "message": f"Error: {str(e)}",
        }


def has_sum(df: pd.DataFrame, rule: RuleDef) -> dict:
    """
    Validates that the sum of values in the specified column meets the expected threshold.
    Applies tolerance logic where threshold < 1.0 represents a percentage of the expected value.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be validated.
        rule (RuleDef): A dictionary containing the rule parameters. It should include:
            - field (str): The name of the column to calculate the sum for.
            - value (float): The expected sum value to validate against.
            - threshold (float, optional): Tolerance threshold (default: 1.0).
              If < 1.0, represents minimum acceptable percentage of expected value.

    Returns:
        dict: A dictionary containing validation results with keys:
            - status (str): "PASS", "FAIL", or "ERROR"
            - expected (float): The expected threshold value
            - actual (float): The actual computed sum
            - message (str): Description of failure or error, None if passed
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
        actual = float(df[field].sum())
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
            "expected": expected,
            "actual": actual,
            "message": None if passed else msg,
        }
    except Exception as e:
        return {
            "status": "ERROR",
            "expected": expected,
            "actual": None,
            "message": f"Error: {str(e)}",
        }


def has_cardinality(df: pd.DataFrame, rule: RuleDef) -> dict:
    """
    Validates that the number of distinct values (cardinality) in the specified column
    meets the expected threshold. Applies tolerance logic for partial matches.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be validated.
        rule (RuleDef): A dictionary containing the rule parameters. It should include:
            - field (str): The name of the column to calculate cardinality for.
            - value (int): The expected number of distinct values to validate against.
            - threshold (float, optional): Tolerance threshold (default: 1.0).
              If < 1.0, represents minimum acceptable percentage of expected value.

    Returns:
        dict: A dictionary containing validation results with keys:
            - status (str): "PASS", "FAIL", or "ERROR"
            - expected (int): The expected threshold value
            - actual (int): The actual computed cardinality
            - message (str): Description of failure or error, None if passed
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
        actual = int(df[field].nunique())
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
            "expected": expected,
            "actual": actual,
            "message": None if passed else msg,
        }
    except Exception as e:
        return {
            "status": "ERROR",
            "expected": expected,
            "actual": None,
            "message": f"Error: {str(e)}",
        }


def has_infogain(df: pd.DataFrame, rule: RuleDef) -> dict:
    """
    Validates the information gain of the specified column.
    Information gain measures how much useful variability the column possesses.
    Calculated as normalized entropy by the maximum possible entropy.

    Value ranges from 0 to 1:
    - 1.0 = perfectly uniform distribution (maximum information)
    - 0.0 = all values are identical (no information)

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be validated.
        rule (RuleDef): A dictionary containing the rule parameters. It should include:
            - field (str): The name of the column to calculate information gain for.
            - value (float): The expected information gain value to validate against.
            - threshold (float, optional): Tolerance threshold (default: 1.0).
              If < 1.0, represents minimum acceptable percentage of expected value.

    Returns:
        dict: A dictionary containing validation results with keys:
            - status (str): "PASS", "FAIL", or "ERROR"
            - expected (float): The expected threshold value
            - actual (float): The actual computed information gain
            - message (str): Description of failure or error, None if passed
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
        # Calculate current entropy
        value_counts = df[field].value_counts()
        probabilities = value_counts / len(df)
        entropy = -np.sum(probabilities * np.log2(probabilities + 1e-10))

        # Calculate maximum possible entropy (uniform distribution)
        n_unique = len(value_counts)
        max_entropy = np.log2(n_unique) if n_unique > 1 else 1.0

        # Normalized information gain (0 to 1)
        info_gain = entropy / max_entropy if max_entropy > 0 else 0.0
        actual = float(info_gain)

        min_acceptable = expected * threshold
        passed = actual >= min_acceptable

        return {
            "status": "PASS" if passed else "FAIL",
            "expected": expected,
            "actual": actual,
            "message": (
                None
                if passed
                else f"Info gain {actual:.4f} < minimum {min_acceptable:.4f} (threshold: {threshold * 100:.0f}%)"
            ),
        }
    except Exception as e:
        return {
            "status": "ERROR",
            "expected": expected,
            "actual": None,
            "message": f"Error: {str(e)}",
        }


def has_entropy(df: pd.DataFrame, rule: RuleDef) -> dict:
    """
    Validates the Shannon entropy of the specified column.
    Entropy measures the randomness/disorder in data distribution:
    - High entropy = data is widely distributed across values
    - Low entropy = data is concentrated in few values

    Formula: H(X) = -Σ(p(x) * log2(p(x)))

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be validated.
        rule (RuleDef): A dictionary containing the rule parameters. It should include:
            - field (str): The name of the column to calculate entropy for.
            - value (float): The expected entropy value to validate against.
            - threshold (float, optional): Tolerance threshold (default: 1.0).
              If < 1.0, represents minimum acceptable percentage of expected value.

    Returns:
        dict: A dictionary containing validation results with keys:
            - status (str): "PASS", "FAIL", or "ERROR"
            - expected (float): The expected threshold value
            - actual (float): The actual computed entropy
            - message (str): Description of failure or error, None if passed
    """
    field = rule.field
    expected = rule.value
    threshold = rule.threshold if rule.threshold else 1.0

    if expected is None:
        return {
            "status": "ERROR",
            "expected": None,
            "actual": None,
            "message": "Expected value not defined for has_entropy",
        }

    try:
        value_counts = df[field].value_counts()
        total = len(df)
        probabilities = value_counts / total
        entropy = float(-np.sum(probabilities * np.log2(probabilities)))
        actual = entropy
        expected = float(expected)

        min_acceptable = expected * threshold
        passed = actual >= min_acceptable

        return {
            "status": "PASS" if passed else "FAIL",
            "expected": expected,
            "actual": actual,
            "message": (
                None
                if passed
                else f"Entropy {actual:.4f} < minimum {min_acceptable:.4f} (threshold: {threshold * 100:.0f}%)"
            ),
        }
    except Exception as e:
        return {
            "status": "ERROR",
            "expected": expected,
            "actual": None,
            "message": f"Error: {str(e)}",
        }


def satisfies(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters a DataFrame based on a rule and returns rows that do not satisfy the rule.

    Args:
        df (pd.DataFrame): The input DataFrame to be evaluated.
        rule (RuleDef): A dictionary containing the rule to be applied.

    Returns:
        pd.DataFrame: A DataFrame containing rows that do not satisfy the rule. An additional
        column `dq_status` is added to indicate the field, check, and expression that failed.
    """
    mask = df.eval(rule.value)
    viol = df[~mask].copy()
    viol["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return viol


def validate_date_format(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Validates the date format of a specified field in a DataFrame against a given format.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to validate.
        rule (dict): A dictionary containing the validation rule. It should include:
            - 'field': The name of the column to validate.
            - 'check': A description or identifier for the validation check.
            - 'fmt': The expected date format to validate against.

    Returns:
        pd.DataFrame: A DataFrame containing rows that violate the date format rule.
                      An additional column 'dq_status' is added to indicate the
                      validation status in the format "{field}:{check}:{fmt}".
    """
    fmt = rule.value
    pattern = __transform_date_format_in_pattern(fmt)
    mask = (
        ~df[rule.field].astype(str).str.match(pattern, na=False) | df[rule.field].isna()
    )
    viol = df[mask].copy()
    viol["dq_status"] = f"{rule.field}:{rule.check_type}:{fmt}"
    return viol


def is_future_date(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Identifies rows in a DataFrame where the date in a specified field is in the future.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be checked.
        rule (dict): A dictionary containing the rule parameters. It is expected to include
                     the field name to check and the check type.

    Returns:
        pd.DataFrame: A DataFrame containing only the rows where the date in the specified
                      field is in the future. An additional column 'dq_status' is added to
                      indicate the field, check type, and the current date in ISO format.
    """

    field = rule.field
    check = rule.check_type
    value = rule.value
    today = pd.Timestamp("today").normalize()
    viol = df[df[field] > today]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_past_date(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Identifies rows in a DataFrame where the date in a specified column is in the past.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be checked.
        rule (dict): A dictionary containing the rule parameters. It is expected to include
                     the field name to check and the check type.

    Returns:
        pd.DataFrame: A DataFrame containing the rows where the date in the specified column
                      is earlier than the current date. An additional column 'dq_status' is
                      added to indicate the field, check type, and the current date.

    Notes:
        - The function uses `pd.to_datetime` to convert the specified column to datetime format.
          Any invalid date entries will be coerced to NaT (Not a Time).
        - Rows with invalid or missing dates are excluded from the result.
    """

    field = rule.field
    check = rule.check_type
    value = rule.value
    today = pd.Timestamp("today").normalize()
    viol = df[df[field] < today]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_date_between(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters rows in a DataFrame where the values in a specified date column
    are not within a given date range.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be checked.
        rule (dict): A dictionary containing the rule parameters. It is expected
                     to include the following:
                     - field: The name of the column to check.
                     - check: A string representing the type of check (used for
                              status annotation).
                     - raw: A string representing the date range in the format
                            '[start_date, end_date]'.

    Returns:
        pd.DataFrame: A DataFrame containing the rows where the date values in
                      the specified column are outside the given range. An
                      additional column 'dq_status' is added to indicate the
                      rule that was violated.
    """

    start_str, end_str = [s.strip() for s in rule.value.strip("[]").split(",")]
    start = pd.to_datetime(start_str)
    end = pd.to_datetime(end_str)
    dates = pd.to_datetime(df[rule.field], errors="coerce")
    mask = ~dates.between(start, end)
    viol = df[mask].copy()
    viol["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return viol


def is_date_after(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters a DataFrame to return rows where a specified date field is earlier than a given target date.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be checked.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - field (str): The name of the column in the DataFrame to check.
            - check (str): A descriptive label for the check being performed.
            - date_str (str): The target date as a string in a format parsable by `pd.to_datetime`.

    Returns:
        pd.DataFrame: A DataFrame containing rows where the date in the specified field is earlier
        than the target date. An additional column `dq_status` is added to indicate the rule that
        was violated in the format "{field}:{check}:{date_str}".
    """

    target = pd.to_datetime(rule.value)
    dates = pd.to_datetime(df[rule.field], errors="coerce")
    viol = df[dates < target].copy()
    viol["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return viol


def is_date_before(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters a DataFrame to identify rows where a date field is after a specified target date.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be checked.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - field (str): The name of the column in the DataFrame containing date values.
            - check (str): A descriptive label for the check being performed.
            - date_str (str): The target date as a string in a format parsable by `pd.to_datetime`.

    Returns:
        pd.DataFrame: A DataFrame containing rows where the date in the specified field is after
        the target date. An additional column `dq_status` is added to indicate the rule that was
        violated in the format "{field}:{check}:{date_str}".
    """

    target = pd.to_datetime(rule.value)
    dates = pd.to_datetime(df[rule.field], errors="coerce")
    viol = df[dates > target].copy()
    viol["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return viol


def all_date_checks(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Applies all date-related validation checks on the given DataFrame based on the specified rule.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be validated.
        rule (dict): A dictionary specifying the validation rules to be applied.

    Returns:
        pd.DataFrame: A DataFrame with the results of the date validation checks.
    """
    return is_past_date(df, rule)


def is_in_millions(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters rows in the DataFrame where the specified field's value is greater than or equal to one million
    and adds a "dq_status" column with a formatted string indicating the rule applied.

    Args:
        df (pd.DataFrame): The input DataFrame to filter.
        rule (dict): A dictionary containing the rule parameters. It is expected to include:
            - field (str): The column name to check.
            - check (str): The type of check being performed (e.g., "greater_than").
            - value (any): The value associated with the rule (not used in this function).

    Returns:
        pd.DataFrame: A new DataFrame containing rows where the specified field's value is >= 1,000,000.
                      Includes an additional "dq_status" column with the rule details.
    """

    out = df[df[rule.field] < 1_000_000].copy()
    out["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return out


def is_in_billions(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters a DataFrame to include only rows where the specified field's value
    is greater than or equal to one billion, and adds a data quality status column.

    Args:
        df (pd.DataFrame): The input DataFrame to filter.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - field (str): The column name to check.
            - check (str): The type of check being performed (used for status annotation).
            - value (any): The value associated with the rule (used for status annotation).

    Returns:
        pd.DataFrame: A new DataFrame containing rows where the specified field's
        value is greater than or equal to one billion. Includes an additional
        column `dq_status` with the format "{rule.field}:{rule.check_type}:{rule.value}".
    """

    out = df[df[rule.field] < 1_000_000_000].copy()
    out["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return out


def is_today(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters a DataFrame to include only rows where the specified date field matches today's date.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to filter.
        rule (dict): A dictionary containing the rule parameters. It is expected to include
                     the field name, a check operation, and a value.

    Returns:
        pd.DataFrame: A new DataFrame containing only the rows where the specified date field
                      matches today's date. An additional column "dq_status" is added to indicate
                      the rule applied in the format "{rule.field}:{rule.check_type}:{rule.value}".
    """

    today = pd.Timestamp(date.today())
    mask = df[rule.field].dt.normalize() != today
    out = df[mask].copy()
    out["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return out


def is_yesterday(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters a DataFrame to include only rows where the specified date field matches yesterday's date.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to filter.
        rule (RuleDef): A rule parameters. It is expected to have
                      to return the field name,
                     check type, and value.

    Returns:
        pd.DataFrame: A filtered DataFrame containing only rows where the specified date field
                      matches yesterday's date. An additional column `dq_status` is added to
                      indicate the data quality status in the format "{rule.field}:{rule.check_type}:{rule.value}".
    """

    target = pd.Timestamp(date.today() - timedelta(days=1))
    mask = df[rule.field].dt.normalize() != target
    out = df[mask].copy()
    out["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return out


def is_t_minus_2(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters a DataFrame to include only rows where the specified date field
    matches the date two days prior to the current date.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to filter.
        rule (dict): A dictionary containing the rule parameters. It is expected
            to include the field name, check type, and value.

    Returns:
        pd.DataFrame: A filtered DataFrame containing only the rows where the
        specified date field matches the target date (two days prior). An
        additional column "dq_status" is added to indicate the rule applied.
    """

    target = pd.Timestamp(date.today() - timedelta(days=2))
    mask = df[rule.field].dt.normalize() != target
    out = df[mask].copy()
    out["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return out


def is_t_minus_3(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters a DataFrame to include only rows where the specified date field
    matches the date three days prior to the current date.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to filter.
        rule (dict): A dictionary containing the rule parameters. The rule
            should include the field to check, the type of check, and the value.

    Returns:
        pd.DataFrame: A filtered DataFrame containing only the rows where the
        specified date field matches the target date (three days prior). An
        additional column "dq_status" is added to indicate the rule applied.
    """

    target = pd.Timestamp(date.today() - timedelta(days=3))
    mask = df[rule.field].dt.normalize() != target
    out = df[mask].copy()
    out["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return out


def is_on_weekday(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters a DataFrame to include only rows where the specified date field falls on a weekday
    (Monday to Friday) and adds a "dq_status" column indicating the rule applied.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be filtered.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - field (str): The name of the date column to check.
            - check (str): A descriptive string for the check being performed.
            - value (str): A value associated with the rule for documentation purposes.

    Returns:
        pd.DataFrame: A filtered DataFrame containing only rows where the specified date field
        falls on a weekday, with an additional "dq_status" column describing the rule applied.
    """

    mask = ~df[rule.field].dt.dayofweek.between(0, 4)
    out = df[mask].copy()
    out["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return out


def is_on_weekend(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters a DataFrame to include only rows where the specified date field falls on a weekend
    (Saturday or Sunday) and adds a "dq_status" column indicating the rule applied.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be filtered.
        rule (dict): A dictionary containing the rule parameters. It is expected to include:
            - field (str): The name of the date column to check.
            - check (str): A descriptive string for the type of check being performed.
            - value (str): A value associated with the rule for documentation purposes.

    Returns:
        pd.DataFrame: A new DataFrame containing only the rows where the specified date field
        falls on a weekend. Includes an additional "dq_status" column with the rule details.
    """

    mask = ~df[rule.field].dt.dayofweek.isin([5, 6])
    out = df[mask].copy()
    out["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return out


def _day_of_week(df: pd.DataFrame, rule: RuleDef, dow: int) -> pd.DataFrame:
    """
    Filters a DataFrame to include only rows where the day of the week of a specified datetime field matches the given day.

    Args:
        df (pd.DataFrame): The input DataFrame containing a datetime field.
        rule (RuleDef): A dictionary containing rule parameters.
        dow (int): The day of the week to filter by (0=Monday, 6=Sunday).

    Returns:
        pd.DataFrame: A filtered DataFrame containing only rows where the day of the week matches `dow`.
                      An additional column, "dq_status", is added to indicate the rule applied.
    """

    mask = df[rule.field].dt.dayofweek != dow
    out = df[mask].copy()
    out["dq_status"] = f"{rule.field}:{rule.check_type}:{rule.value}"
    return out


def is_on_monday(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters the rows of a DataFrame based on whether a specific date column corresponds to a Monday.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be filtered.
        rule (RuleDef): A dictionary containing the filtering rules, including the column to check.

    Returns:
        pd.DataFrame: A filtered DataFrame containing only the rows where the specified date column corresponds to a Monday.
    """
    return _day_of_week(df, rule, 0)


def is_on_tuesday(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters the rows of a DataFrame based on whether a specific date column corresponds to a Tuesday.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be filtered.
        rule (RuleDef): A dictionary containing the filtering rules, including the column to check.

    Returns:
        pd.DataFrame: A filtered DataFrame containing only the rows where the specified date column corresponds to a Tuesday.
    """
    return _day_of_week(df, rule, 1)


def is_on_wednesday(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters the rows of a DataFrame based on whether a date column corresponds to Wednesday.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be filtered.
        rule (dict): A dictionary containing the rule configuration.
                     It is expected to specify the column to evaluate.

    Returns:
        pd.DataFrame: A filtered DataFrame containing only the rows where the specified date column
                      corresponds to Wednesday.
    """
    return _day_of_week(df, rule, 2)


def is_on_thursday(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters the rows of a DataFrame based on whether a date column corresponds to a Thursday.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be filtered.
        rule (dict): A dictionary containing the filtering rules, including the column to check.

    Returns:
        pd.DataFrame: A filtered DataFrame containing only the rows where the specified date column
                      corresponds to a Thursday.
    """
    return _day_of_week(df, rule, 3)


def is_on_friday(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters the rows of a DataFrame based on whether a specific date column corresponds to a Friday.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be filtered.
        rule (dict): A dictionary containing the rules or parameters for filtering.
                     It should specify the column to check for the day of the week.

    Returns:
        pd.DataFrame: A filtered DataFrame containing only the rows where the specified date column corresponds to a Friday.
    """
    return _day_of_week(df, rule, 4)


def is_on_saturday(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Filters a DataFrame to include only rows where the date corresponds to a Saturday.

    Args:
        df (pd.DataFrame): The input DataFrame containing date information.
        rule (dict): A dictionary containing rules or parameters for filtering.

    Returns:
        pd.DataFrame: A filtered DataFrame containing only rows where the date is a Saturday.
    """
    return _day_of_week(df, rule, 5)


def is_on_sunday(df: pd.DataFrame, rule: RuleDef) -> pd.DataFrame:
    """
    Determines whether the dates in a given DataFrame fall on a Sunday.

    Args:
        df (pd.DataFrame): The input DataFrame containing date-related data.
        rule (dict): A dictionary containing rules or parameters for the operation.

    Returns:
        pd.DataFrame: A DataFrame indicating whether each date falls on a Sunday.
    """
    return _day_of_week(df, rule, 6)


def validate_row_level(
    df: pd.DataFrame, rules: List[RuleDef]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validates a pandas DataFrame against a set of rules.

    Args:
        df: Input DataFrame to validate
        rules: List of Rule objects defining validation checks

    Returns:
        Tuple containing:
            - DataFrame with ONLY rows that failed validation + 'dq_status' column
            - DataFrame with violations exploded (one row per violation per rule)

    Notes:
        - Only returns rows that violated at least one rule
        - 'dq_status' column summarizes all validation issues per row
    """
    engine = "pandas"
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
        return pd.DataFrame(), pd.DataFrame()

    df = df.copy().reset_index(drop=True)
    df["_id"] = df.index
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

    raw = pd.concat(raw_list, ignore_index=True) if raw_list else pd.DataFrame()

    if raw.empty or "dq_status" not in raw.columns:
        return pd.DataFrame(), pd.DataFrame()

    summary = raw.groupby("_id")["dq_status"].agg(";".join).reset_index()

    out = df.merge(summary, on="_id", how="inner").drop(columns=["_id"])

    return out, raw


import uuid
from datetime import datetime


def validate_table_level(df: pd.DataFrame, rules: List[RuleDef]) -> pd.DataFrame:
    """
    Validates table-level rules (aggregations and schema).

    Args:
        df (pd.DataFrame): Input DataFrame to validate.
        rules (List[RuleDef]): List of table-level validation rules.

    Returns:
        pd.DataFrame: Summary DataFrame with validation results.
    """

    # Filtrar regras aplicáveis
    engine: str = "pandas"
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

    # Avisar sobre regras ignoradas
    if rules_ignored:
        warnings.warn(
            f"⚠️  {len(rules_ignored)}/{len(rules)} rules ignored:\n"
            + "\n".join(
                f"  • {reason}: {count} rule(s)"
                for reason, count in ignored_reasons.items()
            )
        )

    # Se não há regras válidas, retornar DataFrame vazio
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

    # Timestamp único para todas as validações desta execução
    execution_time = datetime.utcnow()

    # Lista para armazenar resultados
    results = []

    for rule in rules_valid:
        check_type = rule.check_type

        # Get validation function
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
                    "expected": rule.value,
                    "actual": None,
                    "message": f"Function '{check_type}' not implemented",
                }
            )
            continue

        try:
            # Executar validação table-level
            result = fn(df, rule)

            # Padronizar formato
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
                    "expected": rule.value,
                    "actual": None,
                    "message": f"Execution error: {str(e)}",
                }
            )

    # Converter para DataFrame
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

    # Reordenar colunas
    column_order = [
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
    summary_df = summary_df[column_order]

    # Ordenar: FAIL primeiro, depois PASS
    status_order = {"FAIL": 0, "ERROR": 1, "PASS": 2}
    summary_df["_sort"] = summary_df["status"].map(status_order)
    summary_df = (
        summary_df.sort_values("_sort").drop(columns=["_sort"]).reset_index(drop=True)
    )

    return summary_df


def validate(
    df: pd.DataFrame, rules: List[RuleDef]
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Main validation function that orchestrates row-level and table-level validations.

    Args:
        df (pd.DataFrame): Input DataFrame to validate.
        rules (List[RuleDef]): List of all validation rules.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
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
        df_with_status = df.copy()
        row_violations = pd.DataFrame()

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


def summarize(
    rules: List[RuleDef],
    total_rows: int,
    df_with_errors: Optional[pd.DataFrame] = None,
    table_error: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Summarizes quality check results for a given DataFrame based on specified rules.

    Args:
        rules: List of RuleDef objects representing quality check rules
        total_rows: Total number of rows in the original dataset
        df_with_errors: DataFrame with 'dq_status' column from row-level validations
        table_error: DataFrame with table-level validation results

    Returns:
        DataFrame with columns:
            - id: Unique identifier for each rule
            - timestamp: Summary generation timestamp
            - check: Type of check performed
            - level: Validation level (ROW or TABLE from RuleDef)
            - category: Rule category (from RuleDef)
            - column: Column name associated with the rule
            - rule: Rule being checked
            - value: Value associated with the rule
            - rows: Total number of rows in dataset
            - violations: Number of rows that violated the rule
            - pass_rate: Proportion of rows that passed
            - pass_threshold: Threshold for passing
            - status: PASS or FAIL based on pass rate
    """
    # Parse violations from dq_status
    summaries = []

    # ========== ROW-LEVEL SUMMARY ==========
    row_rules = [
        r for r in rules if r.level and r.level.upper().replace("_LEVEL", "") == "ROW"
    ]

    if row_rules and df_with_errors is not None:
        # Parse violations do dq_status
        if "dq_status" in df_with_errors.columns:
            violations_series = df_with_errors["dq_status"].dropna()

            if not violations_series.empty:
                split = violations_series.str.split(";").explode()
                parts = split.str.split(":", expand=True, n=2)

                if len(parts.columns) >= 2:
                    parts.columns = (
                        ["check_type", "field", "details"]
                        if len(parts.columns) == 3
                        else ["check_type", "field"]
                    )
                    viol_count = (
                        parts.groupby(["check_type", "field"])
                        .size()
                        .reset_index(name="violations")
                    )
                else:
                    viol_count = pd.DataFrame(
                        columns=["check_type", "field", "violations"]
                    )
            else:
                viol_count = pd.DataFrame(columns=["check_type", "field", "violations"])
        else:
            viol_count = pd.DataFrame(columns=["check_type", "field", "violations"])

        for rule in row_rules:
            field_str = (
                rule.field if isinstance(rule.field, str) else ",".join(rule.field)
            )

            mask = (viol_count["check_type"] == rule.check_type) & (
                viol_count["field"] == field_str
            )
            violations = (
                int(viol_count.loc[mask, "violations"].sum()) if mask.any() else 0
            )

            pass_count = total_rows - violations
            pass_rate = pass_count / total_rows if total_rows > 0 else 1.0
            pass_threshold = rule.threshold if rule.threshold else 1.0

            status = "PASS" if pass_rate >= pass_threshold else "FAIL"

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
                    "expected": rule.value,
                    "actual": None,
                    "message": (
                        None
                        if status == "PASS"
                        else f"{violations} row(s) failed validation"
                    ),
                }
            )

    # ========== TABLE-LEVEL SUMMARY ==========
    if table_error is not None and not table_error.empty:
        for _, row in table_error.iterrows():
            expected = row.get("expected", None)
            actual = row.get("actual", None)

            # Calcula taxa só se der
            if pd.notna(expected) and pd.notna(actual) and expected != 0:
                compliance_rate = round(actual / expected, 4)
            else:
                compliance_rate = None

            summaries.append(
                {
                    "id": row["id"],
                    "timestamp": row["timestamp"],
                    "level": row["level"],
                    "category": row["category"],
                    "check_type": row["check_type"],
                    "field": row["field"],
                    "status": row["status"],
                    "expected": expected,
                    "actual": actual,
                    "pass_rate": compliance_rate,
                    "message": row["message"],
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

    status_order = {"FAIL": 0, "ERROR": 1, "PASS": 2}
    summary_df["_sort_status"] = summary_df["status"].map(status_order)
    summary_df["_sort_level"] = summary_df["level"].map({"ROW": 0, "TABLE": 1})

    summary_df = (
        summary_df.sort_values(["_sort_status", "_sort_level", "check_type"])
        .drop(columns=["_sort_status", "_sort_level"])
        .reset_index(drop=True)
    )

    return summary_df


def extract_schema(df) -> List[Dict[str, Any]]:
    actual = [
        {
            "field": c,
            "data_type": str(dtype),
            "nullable": True,
            "max_length": None,
        }
        for c, dtype in df.dtypes.items()
    ]
    return actual


def validate_schema(df, expected) -> tuple[bool, list[dict[str, Any]]]:
    """
    Validates the schema of a given DataFrame against an expected schema.

    Args:
        df: The DataFrame whose schema needs to be validated.
        expected: The expected schema, represented as a list of tuples where each tuple
                  contains the column name and its data type.

    Returns:
        Tuple[bool, List[Tuple[str, str]]]: A tuple containing:
            - A boolean indicating whether the schema matches the expected schema.
            - A list of tuples representing the errors, where each tuple contains
              the column name and a description of the mismatch.
    """
    actual = extract_schema(df)
    result, errors = __compare_schemas(actual, expected)
    return result, errors
