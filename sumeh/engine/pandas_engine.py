#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module provides a set of data quality validation functions using the Pandas library.
It includes various checks for data validation, such as completeness, uniqueness, range checks,
pattern matching, date validations, SQL-style custom expressions, and schema validation.

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
import warnings
import re
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from sumeh.services.utils import (
    __convert_value,
    __extract_params,
    __compare_schemas,
    __transform_date_format_in_pattern,
)
from typing import List, Dict, Any, Tuple
import uuid
import numpy as np


def is_positive(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
                      "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    viol = df[df[field] < 0].copy()
    viol["dq_status"] = f"{field}:{check}:{value}"
    return viol


def is_negative(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    """
    Filters a DataFrame to identify rows where a specified field does not satisfy a "negative" condition.

    Args:
        df (pd.DataFrame): The input DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It is expected to include:
            - 'field': The column name in the DataFrame to check.
            - 'check': The type of check being performed (e.g., "negative").
            - 'value': Additional value associated with the rule (not used in this function).

    Returns:
        pd.DataFrame: A new DataFrame containing rows where the specified field is non-negative (>= 0).
                      An additional column 'dq_status' is added to indicate the rule violation in the format
                      "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    viol = df[df[field] >= 0].copy()
    viol["dq_status"] = f"{field}:{check}:{value}"
    return viol


def is_complete(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
    field, check, value = __extract_params(rule)
    viol = df[df[field].isna()].copy()
    viol["dq_status"] = f"{field}:{check}:{value}"
    return viol


def is_unique(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
    field, check, value = __extract_params(rule)
    dup = df[field].duplicated(keep=False)
    viol = df[dup].copy()
    viol["dq_status"] = f"{field}:{check}:{value}"
    return viol


def are_complete(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
    fields, check, value = __extract_params(rule)
    mask = df[fields].isna().any(axis=1)
    viol = df[mask].copy()
    viol["dq_status"] = f"{fields}:{check}:{value}"
    return viol


def are_unique(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
    fields, check, value = __extract_params(rule)
    combo = df[fields].astype(str).agg("|".join, axis=1)
    dup = combo.duplicated(keep=False)
    viol = df[dup].copy()
    viol["dq_status"] = f"{fields}:{check}:{value}"
    return viol


def is_greater_than(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
    field, check, value = __extract_params(rule)
    viol = df[df[field] > value].copy()
    viol["dq_status"] = f"{field}:{check}:{value}"
    return viol


def is_greater_or_equal_than(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
    field, check, value = __extract_params(rule)
    viol = df[df[field] >= value].copy()
    viol["dq_status"] = f"{field}:{check}:{value}"
    return viol


def is_less_than(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
    field, check, value = __extract_params(rule)
    viol = df[df[field] < value].copy()
    viol["dq_status"] = f"{field}:{check}:{value}"
    return viol


def is_less_or_equal_than(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
                      in the format "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    viol = df[df[field] <= value].copy()
    viol["dq_status"] = f"{field}:{check}:{value}"
    return viol


def is_equal(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
        in the format "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    viol = df[df[field] != value].copy()
    viol["dq_status"] = f"{field}:{check}:{value}"
    return viol


def is_equal_than(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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


def is_contained_in(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
    field, check, value = __extract_params(rule)
    vals = re.findall(r"'([^']*)'", str(value)) or [
        v.strip() for v in str(value).strip("[]").split(",")
    ]
    viol = df[~df[field].isin(vals)].copy()
    viol["dq_status"] = f"{field}:{check}:{value}"
    return viol


def is_in(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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


def not_contained_in(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
        "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    vals = re.findall(r"'([^']*)'", str(value)) or [
        v.strip() for v in str(value).strip("[]").split(",")
    ]
    viol = df[df[field].isin(vals)].copy()
    viol["dq_status"] = f"{field}:{check}:{value}"
    return viol


def not_in(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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


def is_between(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
    field, check, value = __extract_params(rule)
    lo, hi = [__convert_value(x) for x in str(value).strip("[]").split(",")]
    viol = df[~df[field].between(lo, hi)].copy()
    viol["dq_status"] = f"{field}:{check}:{value}"
    return viol


def has_pattern(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
    field, check, pattern = __extract_params(rule)
    viol = df[~df[field].astype(str).str.contains(pattern, na=False)].copy()
    viol["dq_status"] = f"{field}:{check}:{pattern}"
    return viol


def is_legit(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
                      that caused the violation in the format "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    mask = df[field].notna() & df[field].astype(str).str.contains(r"^\S+$", na=False)
    viol = df[~mask].copy()
    viol["dq_status"] = f"{field}:{check}:{value}"
    return viol


def has_max(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
    field, check, value = __extract_params(rule)
    viol = df[df[field] > value].copy()
    viol["dq_status"] = f"{field}:{check}:{value}"
    return viol


def has_min(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
    field, check, value = __extract_params(rule)
    viol = df[df[field] < value].copy()
    viol["dq_status"] = f"{field}:{check}:{value}"
    return viol


def has_std(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    """
    Checks if the standard deviation of a specified field in the DataFrame exceeds a given value.

    Parameters:
        df (pd.DataFrame): The input DataFrame to evaluate.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The column name in the DataFrame to calculate the standard deviation for.
            - 'check': A string representing the type of check (not used in the logic but included in the output).
            - 'value': A numeric threshold to compare the standard deviation against.

    Returns:
        pd.DataFrame:
            - If the standard deviation of the specified field exceeds the given value,
              returns a copy of the DataFrame with an additional column 'dq_status' indicating the rule details.
            - If the standard deviation does not exceed the value, returns an empty DataFrame with the same structure as the input.
    """
    field, check, value = __extract_params(rule)
    std_val = df[field].std(skipna=True) or 0.0
    if std_val > value:
        out = df.copy()
        out["dq_status"] = f"{field}:{check}:{value}"
        return out
    return df.iloc[0:0].copy()


def has_mean(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    """
    Checks if the mean of a specified column in a DataFrame satisfies a given condition.

    Parameters:
        df (pd.DataFrame): The input DataFrame to evaluate.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The column name to calculate the mean for.
            - 'check' (str): The condition to check (e.g., 'greater_than').
            - 'value' (float): The threshold value to compare the mean against.

    Returns:
        pd.DataFrame: A copy of the input DataFrame with an additional column 'dq_status'
        if the condition is met. The 'dq_status' column contains a string in the format
        "{field}:{check}:{value}". If the condition is not met, an empty DataFrame is returned.
    """
    field, check, value = __extract_params(rule)
    mean_val = df[field].mean(skipna=True) or 0.0
    if mean_val > value:
        out = df.copy()
        out["dq_status"] = f"{field}:{check}:{value}"
        return out
    return df.iloc[0:0].copy()


def has_sum(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    """
    Checks if the sum of values in a specified column of a DataFrame exceeds a given threshold.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be checked.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The column name to calculate the sum for.
            - 'check' (str): A descriptive label for the check (used in the output).
            - 'value' (float): The threshold value to compare the sum against.

    Returns:
        pd.DataFrame:
            - If the sum of the specified column exceeds the threshold, returns a copy of the input DataFrame
              with an additional column 'dq_status' indicating the rule that was applied.
            - If the sum does not exceed the threshold, returns an empty DataFrame with the same structure as the input.
    """
    field, check, value = __extract_params(rule)
    sum_val = df[field].sum(skipna=True) or 0.0
    if sum_val > value:
        out = df.copy()
        out["dq_status"] = f"{field}:{check}:{value}"
        return out
    return df.iloc[0:0].copy()


def has_cardinality(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    """
    Checks if the cardinality (number of unique values) of a specified field in the DataFrame
    exceeds a given value and returns a modified DataFrame if the condition is met.

    Parameters:
        df (pd.DataFrame): The input DataFrame to check.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The column name in the DataFrame to check.
            - 'check': The type of check being performed (e.g., 'cardinality').
            - 'value': The threshold value for the cardinality.

    Returns:
        pd.DataFrame:
            - If the cardinality of the specified field exceeds the given value,
              a copy of the DataFrame is returned with an additional column 'dq_status'
              indicating the field, check, and value.
            - If the cardinality does not exceed the value, an empty DataFrame is returned.
    """
    field, check, value = __extract_params(rule)
    card = df[field].nunique(dropna=True) or 0
    if card > value:
        out = df.copy()
        out["dq_status"] = f"{field}:{check}:{value}"
        return out
    return df.iloc[0:0].copy()


def has_infogain(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    """
    Checks if the given DataFrame satisfies the information gain criteria
    defined by the provided rule. This function internally delegates the
    operation to the `has_cardinality` function.

    Args:
        df (pd.DataFrame): The input DataFrame to be evaluated.
        rule (dict): A dictionary defining the rule for information gain.

    Returns:
        pd.DataFrame: The resulting DataFrame after applying the rule.
    """
    return has_cardinality(df, rule)


def has_entropy(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    """
    Checks if the given DataFrame satisfies a specific rule related to entropy.

    This function is a wrapper around the `has_cardinality` function, delegating
    the rule-checking logic to it.

    Args:
        df (pd.DataFrame): The input DataFrame to be evaluated.
        rule (dict): A dictionary containing the rule to be applied.

    Returns:
        pd.DataFrame: The resulting DataFrame after applying the rule.
    """
    return has_cardinality(df, rule)


def satisfies(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    """
    Filters a DataFrame based on a rule and returns rows that do not satisfy the rule.

    Args:
        df (pd.DataFrame): The input DataFrame to be evaluated.
        rule (dict): A dictionary containing the rule to be applied. It is expected
            to contain parameters that can be extracted using the `__extract_params` function.

    Returns:
        pd.DataFrame: A DataFrame containing rows that do not satisfy the rule. An additional
        column `dq_status` is added to indicate the field, check, and expression that failed.
    """
    field, check, expr = __extract_params(rule)
    mask = df.eval(expr)
    viol = df[~mask].copy()
    viol["dq_status"] = f"{field}:{check}:{expr}"
    return viol


def validate_date_format(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
    field, check, fmt = __extract_params(rule)
    pattern = __transform_date_format_in_pattern(fmt)
    mask = ~df[field].astype(str).str.match(pattern, na=False) | df[field].isna()
    viol = df[mask].copy()
    viol["dq_status"] = f"{field}:{check}:{fmt}"
    return viol


def is_future_date(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
    field, check, _ = __extract_params(rule)
    today = date.today()
    dates = pd.to_datetime(df[field], errors="coerce")
    viol = df[dates > today].copy()
    viol["dq_status"] = f"{field}:{check}:{today.isoformat()}"
    return viol


def is_past_date(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
    field, check, _ = __extract_params(rule)
    today = date.today()
    dates = pd.to_datetime(df[field], errors="coerce")
    viol = df[dates < today].copy()
    viol["dq_status"] = f"{field}:{check}:{today.isoformat()}"
    return viol


def is_date_between(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
    field, check, raw = __extract_params(rule)
    start_str, end_str = [s.strip() for s in raw.strip("[]").split(",")]
    start = pd.to_datetime(start_str)
    end = pd.to_datetime(end_str)
    dates = pd.to_datetime(df[field], errors="coerce")
    mask = ~dates.between(start, end)
    viol = df[mask].copy()
    viol["dq_status"] = f"{field}:{check}:{raw}"
    return viol


def is_date_after(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
    field, check, date_str = __extract_params(rule)
    target = pd.to_datetime(date_str)
    dates = pd.to_datetime(df[field], errors="coerce")
    viol = df[dates < target].copy()
    viol["dq_status"] = f"{field}:{check}:{date_str}"
    return viol


def is_date_before(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
    field, check, date_str = __extract_params(rule)
    target = pd.to_datetime(date_str)
    dates = pd.to_datetime(df[field], errors="coerce")
    viol = df[dates > target].copy()
    viol["dq_status"] = f"{field}:{check}:{date_str}"
    return viol


def all_date_checks(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    """
    Applies all date-related validation checks on the given DataFrame based on the specified rule.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be validated.
        rule (dict): A dictionary specifying the validation rules to be applied.

    Returns:
        pd.DataFrame: A DataFrame with the results of the date validation checks.
    """
    return is_past_date(df, rule)


def is_in_millions(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
    field, check, value = __extract_params(rule)
    out = df[df[field] < 1_000_000].copy()
    out["dq_status"] = f"{field}:{check}:{value}"
    return out


def is_in_billions(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
        column `dq_status` with the format "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    out = df[df[field] < 1_000_000_000].copy()
    out["dq_status"] = f"{field}:{check}:{value}"
    return out


def is_today(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    """
    Filters a DataFrame to include only rows where the specified date field matches today's date.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to filter.
        rule (dict): A dictionary containing the rule parameters. It is expected to include
                     the field name, a check operation, and a value.

    Returns:
        pd.DataFrame: A new DataFrame containing only the rows where the specified date field
                      matches today's date. An additional column "dq_status" is added to indicate
                      the rule applied in the format "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    today = pd.Timestamp(date.today())
    mask = df[field].dt.normalize() != today
    out = df[mask].copy()
    out["dq_status"] = f"{field}:{check}:{value}"
    return out


def is_yesterday(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    """
    Filters a DataFrame to include only rows where the specified date field matches yesterday's date.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to filter.
        rule (dict): A dictionary containing the rule parameters. It is expected to have
                     keys that allow `__extract_params(rule)` to return the field name,
                     check type, and value.

    Returns:
        pd.DataFrame: A filtered DataFrame containing only rows where the specified date field
                      matches yesterday's date. An additional column `dq_status` is added to
                      indicate the data quality status in the format "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    target = pd.Timestamp(date.today() - timedelta(days=1))
    mask = df[field].dt.normalize() != target
    out = df[mask].copy()
    out["dq_status"] = f"{field}:{check}:{value}"
    return out


def is_t_minus_2(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
    field, check, value = __extract_params(rule)
    target = pd.Timestamp(date.today() - timedelta(days=2))
    mask = df[field].dt.normalize() != target
    out = df[mask].copy()
    out["dq_status"] = f"{field}:{check}:{value}"
    return out


def is_t_minus_3(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
    field, check, value = __extract_params(rule)
    target = pd.Timestamp(date.today() - timedelta(days=3))
    mask = df[field].dt.normalize() != target
    out = df[mask].copy()
    out["dq_status"] = f"{field}:{check}:{value}"
    return out


def is_on_weekday(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
    field, check, value = __extract_params(rule)
    mask = ~df[field].dt.dayofweek.between(0, 4)
    out = df[mask].copy()
    out["dq_status"] = f"{field}:{check}:{value}"
    return out


def is_on_weekend(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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
    field, check, value = __extract_params(rule)
    mask = ~df[field].dt.dayofweek.isin([5, 6])
    out = df[mask].copy()
    out["dq_status"] = f"{field}:{check}:{value}"
    return out


def _day_of_week(df: pd.DataFrame, rule: dict, dow: int) -> pd.DataFrame:
    """
    Filters a DataFrame to include only rows where the day of the week of a specified datetime field matches the given day.

    Args:
        df (pd.DataFrame): The input DataFrame containing a datetime field.
        rule (dict): A dictionary containing rule parameters. The function expects this to be parsed by `__extract_params`.
        dow (int): The day of the week to filter by (0=Monday, 6=Sunday).

    Returns:
        pd.DataFrame: A filtered DataFrame containing only rows where the day of the week matches `dow`.
                      An additional column, "dq_status", is added to indicate the rule applied.
    """
    field, check, value = __extract_params(rule)
    mask = df[field].dt.dayofweek != dow
    out = df[mask].copy()
    out["dq_status"] = f"{field}:{check}:{value}"
    return out


def is_on_monday(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    """
    Filters the rows of a DataFrame based on whether a specific date column corresponds to a Monday.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be filtered.
        rule (dict): A dictionary containing the filtering rules, including the column to check.

    Returns:
        pd.DataFrame: A filtered DataFrame containing only the rows where the specified date column corresponds to a Monday.
    """
    return _day_of_week(df, rule, 0)


def is_on_tuesday(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    """
    Filters the rows of a DataFrame based on whether a specific date column corresponds to a Tuesday.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be filtered.
        rule (dict): A dictionary containing the filtering rules, including the column to check.

    Returns:
        pd.DataFrame: A filtered DataFrame containing only the rows where the specified date column corresponds to a Tuesday.
    """
    return _day_of_week(df, rule, 1)


def is_on_wednesday(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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


def is_on_thursday(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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


def is_on_friday(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
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


def is_on_saturday(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    """
    Filters a DataFrame to include only rows where the date corresponds to a Saturday.

    Args:
        df (pd.DataFrame): The input DataFrame containing date information.
        rule (dict): A dictionary containing rules or parameters for filtering.

    Returns:
        pd.DataFrame: A filtered DataFrame containing only rows where the date is a Saturday.
    """
    return _day_of_week(df, rule, 5)


def is_on_sunday(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    """
    Determines whether the dates in a given DataFrame fall on a Sunday.

    Args:
        df (pd.DataFrame): The input DataFrame containing date-related data.
        rule (dict): A dictionary containing rules or parameters for the operation.

    Returns:
        pd.DataFrame: A DataFrame indicating whether each date falls on a Sunday.
    """
    return _day_of_week(df, rule, 6)


def validate(df: pd.DataFrame, rules: list[dict]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validates a pandas DataFrame against a set of rules and returns the processed DataFrame
    along with a DataFrame containing validation violations.

    Args:
        df (pd.DataFrame): The input DataFrame to validate.
        rules (list[dict]): A list of dictionaries, where each dictionary represents a validation
            rule. Each rule should contain the following keys:
            - 'check_type' (str): The type of validation to perform. This should correspond to a
              function name available in the global scope. Special cases include 'is_primary_key'
              and 'is_composite_key', which map to 'is_unique' and 'are_unique', respectively.
            - 'execute' (bool, optional): Whether to execute the rule. Defaults to True.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: A tuple containing:
            - The processed DataFrame with validation statuses merged.
            - A DataFrame containing rows that violated the validation rules.

    Notes:
        - The input DataFrame is copied and reset to ensure the original data is not modified.
        - An '_id' column is temporarily added to track row indices during validation.
        - If a rule's 'check_type' does not correspond to a known function, a warning is issued.
        - The 'dq_status' column in the violations DataFrame summarizes validation issues for
          each row.
    """
    df = df.copy().reset_index(drop=True)
    df["_id"] = df.index
    raw_list = []
    for rule in rules:
        if not rule.get("execute", True):
            continue
        rt = rule["check_type"]
        fn = globals().get(
            rt
            if rt not in ("is_primary_key", "is_composite_key")
            else ("is_unique" if rt == "is_primary_key" else "are_unique")
        )
        if fn is None:
            warnings.warn(f"Unknown rule: {rt}")
            continue
        viol = fn(df, rule)
        raw_list.append(viol)
    raw = (
        pd.concat(raw_list, ignore_index=True)
        if raw_list
        else pd.DataFrame(columns=df.columns)
    )
    summary = raw.groupby("_id")["dq_status"].agg(";".join).reset_index()
    out = df.merge(summary, on="_id", how="left").drop(columns=["_id"])
    return out, raw


def __build_rules_df(rules: List[dict]) -> pd.DataFrame:
    """
    Builds a pandas DataFrame from a list of rule dictionaries.

    Args:
        rules (List[dict]): A list of dictionaries where each dictionary represents a rule.
            Each rule dictionary may contain the following keys:
                - "field" (str or list): The column(s) the rule applies to.
                - "check_type" (str): The type of check or rule to apply.
                - "value" (optional): The value associated with the rule.
                - "threshold" (optional): A numeric threshold for the rule. Defaults to 1.0 if not provided or invalid.
                - "execute" (optional): A boolean indicating whether the rule should be executed. Defaults to True.

    Returns:
        pd.DataFrame: A DataFrame containing the processed rules with the following columns:
            - "column": The column(s) the rule applies to, as a comma-separated string if multiple.
            - "rule": The type of check or rule.
            - "value": The value associated with the rule, or an empty string if not provided.
            - "pass_threshold": The numeric threshold for the rule.

    Notes:
        - Rules with "execute" set to False are skipped.
        - Duplicate rows based on "column", "rule", and "value" are removed from the resulting DataFrame.
    """
    rows = []
    for r in rules:
        if not r.get("execute", True):
            continue

        col = ",".join(r["field"]) if isinstance(r["field"], list) else r["field"]

        thr_raw = r.get("threshold")
        try:
            thr = float(thr_raw) if thr_raw is not None else 1.0
        except (TypeError, ValueError):
            thr = 1.0

        val = r.get("value")
        rows.append(
            {
                "column": col,
                "rule": r["check_type"],
                "value": val if val is not None else "",
                "pass_threshold": thr,
            }
        )

    df_rules = pd.DataFrame(rows)
    if not df_rules.empty:
        df_rules = df_rules.drop_duplicates(subset=["column", "rule", "value"])
    return df_rules


def summarize(qc_df: pd.DataFrame, rules: list[dict], total_rows: int) -> pd.DataFrame:
    """
    Summarizes quality check results for a given DataFrame based on specified rules.

    Args:
        qc_df (pd.DataFrame): The input DataFrame containing a 'dq_status' column with
            quality check results in the format 'column:rule:value', separated by semicolons.
        rules (list[dict]): A list of dictionaries representing the quality check rules.
            Each dictionary should define the 'column', 'rule', 'value', and 'pass_threshold'.
        total_rows (int): The total number of rows in the original dataset.

    Returns:
        pd.DataFrame: A DataFrame summarizing the quality check results with the following columns:
            - 'id': A unique identifier for each rule.
            - 'timestamp': The timestamp of the summary generation.
            - 'check': The type of check performed (e.g., 'Quality Check').
            - 'level': The severity level of the check (e.g., 'WARNING').
            - 'column': The column name associated with the rule.
            - 'rule': The rule being checked.
            - 'value': The value associated with the rule.
            - 'rows': The total number of rows in the dataset.
            - 'violations': The number of rows that violated the rule.
            - 'pass_rate': The proportion of rows that passed the rule.
            - 'pass_threshold': The threshold for passing the rule.
            - 'status': The status of the rule ('PASS' or 'FAIL') based on the pass rate.

    Notes:
        - The function calculates the number of violations for each rule and merges it with the
          provided rules to compute the pass rate and status.
        - The 'timestamp' column is set to the current time with seconds and microseconds set to zero.
    """
    split = qc_df["dq_status"].str.split(";").explode().dropna()
    parts = split.str.split(":", expand=True)
    parts.columns = ["column", "rule", "value"]
    viol_count = (
        parts.groupby(["column", "rule", "value"]).size().reset_index(name="violations")
    )
    rules_df = __build_rules_df(rules)
    df = rules_df.merge(viol_count, on=["column", "rule", "value"], how="left")
    df["violations"] = df["violations"].fillna(0).astype(int)
    df["rows"] = total_rows
    df["pass_rate"] = (total_rows - df["violations"]) / total_rows
    df["status"] = np.where(df["pass_rate"] >= df["pass_threshold"], "PASS", "FAIL")
    df["timestamp"] = datetime.now().replace(second=0, microsecond=0)
    df["check"] = "Quality Check"
    df["level"] = "WARNING"
    df.insert(0, "id", np.array([uuid.uuid4() for _ in range(len(df))], dtype="object"))
    return df[
        [
            "id",
            "timestamp",
            "check",
            "level",
            "column",
            "rule",
            "value",
            "rows",
            "violations",
            "pass_rate",
            "pass_threshold",
            "status",
        ]
    ]


def __pandas_schema_to_list(df, expected) -> Tuple[bool, List[Tuple[str, str]]]:
    actual = [
        {
            "field": c,
            "data_type": str(dtype).lower(),
            "nullable": True,
            "max_length": None,
        }
        for c, dtype in df.dtypes.items()
    ]
    return __compare_schemas(actual, expected)


def validate_schema(df, expected) -> Tuple[bool, List[Tuple[str, str]]]:
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
    actual = __pandas_schema_to_list(df)
    result, errors = __compare_schemas(actual, expected)
    return result, errors
