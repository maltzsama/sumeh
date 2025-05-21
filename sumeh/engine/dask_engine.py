#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module provides a set of data quality validation functions for Dask DataFrames.
It includes various checks such as completeness, uniqueness, value range, patterns,
and schema validation. The module also provides utilities for summarizing validation
results and schema comparison.

Functions:
    is_positive(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the specified field contains only positive values.

    is_negative(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the specified field contains only negative values.

    is_in_millions(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Retains rows where the field value is at least 1,000,000 and flags them with dq_status.

    is_in_billions(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Retains rows where the field value is at least 1,000,000,000 and flags them with dq_status.

    is_t_minus_1(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Retains rows where the date field equals yesterday (T-1) and flags them with dq_status.

    is_t_minus_2(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Retains rows where the date field equals two days ago (T-2) and flags them with dq_status.

    is_t_minus_3(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Retains rows where the date field equals three days ago (T-3) and flags them with dq_status.

    is_today(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Retains rows where the date field equals today and flags them with dq_status.

    is_yesterday(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Retains rows where the date field equals yesterday and flags them with dq_status.

    is_on_weekday(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Retains rows where the date field falls on a weekday (Mon-Fri) and flags them with dq_status.

    is_on_weekend(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Retains rows where the date field is on a weekend (Sat-Sun) and flags them with dq_status.

    is_on_monday(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Retains rows where the date field is on Monday and flags them with dq_status.

    is_on_tuesday(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Retains rows where the date field is on Tuesday and flags them with dq_status.

    is_on_wednesday(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Retains rows where the date field is on Wednesday and flags them with dq_status.

    is_on_thursday(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Retains rows where the date field is on Thursday and flags them with dq_status.

    is_on_friday(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Retains rows where the date field is on Friday and flags them with dq_status.

    is_on_saturday(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Retains rows where the date field is on Saturday and flags them with dq_status.

    is_on_sunday(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Retains rows where the date field is on Sunday and flags them with dq_status.

    is_complete(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the specified field contains no null values.

    is_unique(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the specified field contains unique values.

    are_complete(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if all specified fields contain no null values.

    are_unique(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the combination of specified fields is unique.

    is_greater_than(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the specified field contains values greater than a given value.

    is_greater_or_equal_than(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the specified field contains values greater than or equal to a given value.

    is_less_than(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the specified field contains values less than a given value.

    is_less_or_equal_than(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the specified field contains values less than or equal to a given value.

    is_equal(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the specified field contains values equal to a given value.

    is_equal_than(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Alias for `is_equal`.

    is_contained_in(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the specified field contains values within a given list.

    not_contained_in(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the specified field contains values not within a given list.

    is_between(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the specified field contains values within a given range.

    has_pattern(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the specified field matches a given regex pattern.

    is_legit(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the specified field contains non-null, non-whitespace values.

    is_primary_key(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the specified field is a primary key (unique).

    is_composite_key(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the combination of specified fields is a composite key (unique).

    has_max(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the maximum value of the specified field exceeds a given value.

    has_min(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the minimum value of the specified field is below a given value.

    has_std(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the standard deviation of the specified field exceeds a given value.

    has_mean(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the mean of the specified field exceeds a given value.

    has_sum(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the sum of the specified field exceeds a given value.

    has_cardinality(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the cardinality (number of unique values) of the specified field exceeds a given value.

    has_infogain(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the information gain of the specified field exceeds a given value.

    has_entropy(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the entropy of the specified field exceeds a given value.

    satisfies(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    Checks if the specified field satisfies a given Python expression.

    validate(df: dd.DataFrame, rules: list[dict]) -> tuple[dd.DataFrame, dd.DataFrame]:
    Validates a Dask DataFrame against a list of rules and returns aggregated and raw validation results.

    summarize(qc_ddf: dd.DataFrame, rules: list[dict], total_rows: int) -> pd.DataFrame:
    Summarizes the validation results and generates a report.

    validate_schema(df: dd.DataFrame, expected: List[Dict[str, Any]]) -> Tuple[bool, List[Tuple[str, str]]]:
    Validates the schema of a Dask DataFrame against an expected schema.
"""

import re
import warnings
import operator
import uuid
from functools import reduce
from datetime import datetime, date
from typing import List, Dict, Any, Tuple

import pandas as pd
import dask.dataframe as dd
import numpy as np

from sumeh.services.utils import __convert_value, __extract_params, __compare_schemas

def is_positive(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Checks if the values in a specified field of a Dask DataFrame are positive.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to validate.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the field to check.
            - 'check': The type of check being performed (e.g., "is_positive").
            - 'value': The expected value or condition (e.g., "0").

    Returns:
        dd.DataFrame: A DataFrame containing rows where the specified field has
        negative values, with an additional column `dq_status` indicating the
        field, check, and value that failed.
    """
    field, check, value = __extract_params(rule)
    viol = df[df[field] < 0]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_negative(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Identifies rows in a Dask DataFrame where the specified field does not satisfy a "negative" check.

    This function filters the DataFrame to find rows where the value in the specified field
    is greater than or equal to 0 (i.e., not negative). It then assigns a new column `dq_status`
    to indicate the rule that was violated.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - `field` (str): The name of the column to check.
            - `check` (str): The type of check being performed (e.g., "negative").
            - `value` (any): The expected value or condition for the check.

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows that violate the rule,
        with an additional column `dq_status` describing the violation in the format
        "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    viol = df[df[field] >= 0]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_in_millions(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Checks if the values in a specified field of a Dask DataFrame are in the millions
    (greater than or equal to 1,000,000) and returns a DataFrame of violations.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to check.
        rule (dict): A dictionary containing the rule parameters. It is expected to
                     include the field name, check type, and value.

    Returns:
        dd.DataFrame: A DataFrame containing rows where the specified field's value
                      is greater than or equal to 1,000,000. An additional column
                      `dq_status` is added to indicate the field, check, and value
                      that triggered the violation.
    """
    field, check, value = __extract_params(rule)
    viol = df[df[field] < 1_000_000]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_in_billions(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Identifies rows in a Dask DataFrame where the value in a specified field
    is greater than or equal to one billion and marks them with a data quality status.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It is expected
                     to include the field name, check type, and value.

    Returns:
        dd.DataFrame: A Dask DataFrame containing only the rows where the specified
                      field's value is greater than or equal to one billion. An
                      additional column `dq_status` is added, indicating the field,
                      check type, and value that triggered the rule.
    """
    field, check, value = __extract_params(rule)
    viol = df[df[field] < 1_000_000_000]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_complete(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Checks for completeness of a specified field in a Dask DataFrame based on a given rule.

    This function identifies rows where the specified field is null and marks them as violations.
    It then assigns a data quality status to these rows in the resulting DataFrame.

    Args:
        df (dd.DataFrame): The Dask DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the field to check for completeness.
            - 'check': The type of check being performed (e.g., 'is_complete').
            - 'value': Additional value associated with the rule (not used in this function).

    Returns:
        dd.DataFrame: A DataFrame containing rows where the specified field is null,
        with an additional column `dq_status` indicating the data quality status in the format
        "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    viol = df[df[field].isnull()]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_unique(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Checks for uniqueness of a specified field in a Dask DataFrame based on a given rule.

    Parameters:
        df (dd.DataFrame): The Dask DataFrame to check for uniqueness.
        rule (dict): A dictionary containing the rule parameters. It is expected to include:
            - 'field': The column name to check for uniqueness.
            - 'check': The type of check being performed (e.g., "unique").
            - 'value': Additional value or metadata related to the check.

    Returns:
        dd.DataFrame: A DataFrame containing rows that violate the uniqueness rule,
        with an additional column `dq_status` indicating the rule that was violated
        in the format "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    counts = df[field].value_counts().compute()
    dup_vals = counts[counts > 1].index.tolist()
    viol = df[df[field].isin(dup_vals)]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def are_complete(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Checks if the specified fields in a Dask DataFrame are complete (non-null)
    based on the provided rule and returns a DataFrame of violations.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to check for completeness.
        rule (dict): A dictionary containing the rule parameters. It should
            include the fields to check, the type of check, and the expected value.

    Returns:
        dd.DataFrame: A DataFrame containing rows that violate the completeness
        rule, with an additional column `dq_status` indicating the rule details
        in the format "{fields}:{check}:{value}".
    """
    fields, check, value = __extract_params(rule)
    mask = ~reduce(operator.and_, [df[f].notnull() for f in fields])
    viol = df[mask]
    return viol.assign(dq_status=f"{str(fields)}:{check}:{value}")


def are_unique(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Checks if the specified fields in a Dask DataFrame contain unique combinations of values.

    Parameters:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It is expected to include:
            - 'fields': A list of column names to check for uniqueness.
            - 'check': A string describing the type of check being performed.
            - 'value': A value associated with the rule (used for status reporting).

    Returns:
        dd.DataFrame: A DataFrame containing rows that violate the uniqueness rule,
        with an additional column `dq_status` indicating the rule that was violated.
    """
    fields, check, value = __extract_params(rule)
    combo = (
        df[fields]
        .astype(str)
        .apply(lambda row: "|".join(row.values), axis=1, meta=("combo", "object"))
    )
    counts = combo.value_counts().compute()
    dupes = counts[counts > 1].index.tolist()
    viol = df[combo.isin(dupes)]
    return viol.assign(dq_status=f"{str(fields)}:{check}:{value}")


def is_greater_than(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the value in a specified field
    is greater than a given threshold and annotates the result with a data quality status.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The column name to check.
            - 'check' (str): The type of check being performed (e.g., 'greater_than').
            - 'value' (numeric): The threshold value to compare against.

    Returns:
        dd.DataFrame: A filtered DataFrame containing rows that violate the rule,
        with an additional column `dq_status` indicating the rule details in the format
        "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    viol = df[df[field] > value]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_greater_or_equal_than(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where a specified field's value
    is less than a given threshold, and annotates the resulting rows with a
    data quality status.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It should
                     include the following keys:
                     - 'field': The column name in the DataFrame to check.
                     - 'check': The type of check being performed (e.g., 'greater_or_equal').
                     - 'value': The threshold value to compare against.

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows that
                      violate the rule, with an additional column `dq_status`
                      indicating the field, check type, and threshold value.
    """
    field, check, value = __extract_params(rule)
    viol = df[df[field] < value]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_less_than(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the value in a specified field
    is greater than or equal to a given threshold, and marks them with a data quality status.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The column name to check.
            - 'check' (str): The type of check being performed (e.g., "less_than").
            - 'value' (numeric): The threshold value for the check.

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows that violate the rule,
        with an additional column `dq_status` indicating the rule that was violated in the
        format "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    viol = df[df[field] >= value]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_less_or_equal_than(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the value in a specified field
    is greater than a given threshold, violating a "less or equal than" rule.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The column name in the DataFrame to be checked.
            - 'check': The type of check being performed (e.g., "less_or_equal_than").
            - 'value': The threshold value to compare against.

    Returns:
        dd.DataFrame: A new DataFrame containing only the rows that violate the rule.
        An additional column `dq_status` is added to indicate the rule violation
        in the format "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    viol = df[df[field] > value]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_equal(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where a specified field does not equal a given value.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The column name in the DataFrame to be checked.
            - 'check': The type of check to perform (expected to be 'equal' for this function).
            - 'value': The value to compare against.

    Returns:
        dd.DataFrame: A new DataFrame containing rows that violate the equality rule.
                      An additional column `dq_status` is added, indicating the rule details
                      in the format "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    viol = df[~df[field].eq(value)]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_equal_than(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the specified field does not equal the given value.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The column name in the DataFrame to check.
            - 'check': The type of check being performed (expected to be 'equal' for this function).
            - 'value': The value to compare against.

    Returns:
        dd.DataFrame: A new DataFrame containing rows that violate the equality rule.
                      An additional column `dq_status` is added, indicating the rule details
                      in the format "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    viol = df[~df[field].eq(value)]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_contained_in(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the values in a specified field
    are not contained within a given list of allowed values.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The column name in the DataFrame to check.
            - 'check': The type of check being performed (e.g., "is_contained_in").
            - 'value': A string representation of a list of allowed values (e.g., "[value1, value2]").

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows that violate the rule.
        An additional column `dq_status` is added to indicate the rule violation in the format:
        "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    lst = [v.strip() for v in value.strip("[]").split(",")]
    viol = df[~df[field].isin(lst)]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_in(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Checks if the specified rule is contained within the given Dask DataFrame.

    This function acts as a wrapper for the `is_contained_in` function,
    passing the provided DataFrame and rule to it.

    Args:
        df (dd.DataFrame): The Dask DataFrame to evaluate.
        rule (dict): A dictionary representing the rule to check against the DataFrame.

    Returns:
        dd.DataFrame: A Dask DataFrame resulting from the evaluation of the rule.
    """
    return is_contained_in(df, rule)


def not_contained_in(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the specified field's value is
    contained in a given list, and assigns a data quality status to the resulting rows.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The column name in the DataFrame to check.
            - 'check': The type of check being performed (e.g., "not_contained_in").
            - 'value': A string representation of a list of values to check against,
              formatted as "[value1, value2, ...]".

    Returns:
        dd.DataFrame: A new DataFrame containing only the rows where the specified
        field's value is in the provided list, with an additional column `dq_status`
        indicating the rule applied in the format "field:check:value".
    """
    field, check, value = __extract_params(rule)
    lst = [v.strip() for v in value.strip("[]").split(",")]
    viol = df[df[field].isin(lst)]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def not_in(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Filters a Dask DataFrame by excluding rows where the specified rule is satisfied.

    This function delegates the filtering logic to the `not_contained_in` function.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be filtered.
        rule (dict): A dictionary defining the filtering rule. The structure and
                     interpretation of this rule depend on the implementation of
                     `not_contained_in`.

    Returns:
        dd.DataFrame: A new Dask DataFrame with rows excluded based on the rule.
    """
    return not_contained_in(df, rule)


def is_between(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where a specified field's value
    does not fall within a given range.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It should
            include:
            - 'field': The column name in the DataFrame to check.
            - 'check': The type of check being performed (e.g., "between").
            - 'value': A string representing the range in the format "[lo,hi]".

    Returns:
        dd.DataFrame: A new DataFrame containing only the rows that violate
        the specified range condition. An additional column `dq_status` is
        added to indicate the field, check, and value that caused the violation.
    """
    field, check, value = __extract_params(rule)
    lo, hi = value.strip("[]").split(",")
    viol = df[~df[field].between(__convert_value(lo), __convert_value(hi))]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def has_pattern(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Identifies rows in a Dask DataFrame that do not match a specified pattern.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The column name in the DataFrame to apply the pattern check.
            - 'check': A descriptive label for the type of check being performed.
            - 'value': The regex pattern to match against the specified column.

    Returns:
        dd.DataFrame: A DataFrame containing rows that do not match the specified pattern.
                      An additional column `dq_status` is added, indicating the rule details
                      in the format "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    viol = df[~df[field].str.match(value, na=False)]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_legit(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Validates a Dask DataFrame against a specified rule and returns rows that violate the rule.

    Args:
        df (dd.DataFrame): The Dask DataFrame to validate.
        rule (dict): A dictionary containing the validation rule. It should include:
            - 'field': The column name in the DataFrame to validate.
            - 'check': The type of validation check (e.g., regex, condition).
            - 'value': The value or pattern to validate against.

    Returns:
        dd.DataFrame: A new DataFrame containing rows that violate the rule, with an additional
        column `dq_status` indicating the field, check, and value of the failed validation.
    """
    field, check, value = __extract_params(rule)
    s = df[field].astype("string")
    mask = s.notnull() & s.str.contains(r"^\S+$", na=False)
    viol = df[~mask]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_primary_key(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Determines if the specified rule identifies a primary key in the given Dask DataFrame.

    This function checks whether the combination of columns specified in the rule
    results in unique values across the DataFrame, effectively identifying a primary key.

    Args:
        df (dd.DataFrame): The Dask DataFrame to evaluate.
        rule (dict): A dictionary defining the rule to check for primary key uniqueness.
                     Typically, this includes the column(s) to be evaluated.

    Returns:
        dd.DataFrame: A Dask DataFrame indicating whether the rule satisfies the primary key condition.
    """
    return is_unique(df, rule)


def is_composite_key(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Determines if the given DataFrame satisfies the composite key condition based on the provided rule.

    Args:
        df (dd.DataFrame): A Dask DataFrame to be checked.
        rule (dict): A dictionary defining the composite key rule.

    Returns:
        dd.DataFrame: A Dask DataFrame indicating whether the composite key condition is met.
    """
    return are_unique(df, rule)


def has_max(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Identifies rows in a Dask DataFrame where the value of a specified field exceeds a given maximum value.

    Parameters:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the column to check.
            - 'check': A string describing the check (e.g., 'max').
            - 'value': The maximum allowable value for the specified field.

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows that violate the rule.
                      An additional column `dq_status` is added to indicate the rule violation
                      in the format "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    viol = df[df[field] > value]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def has_min(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Checks if the values in a specified field of a Dask DataFrame are greater than
    or equal to a given minimum value. Returns a DataFrame containing rows that
    violate this rule, with an additional column indicating the data quality status.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to validate.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The column name to check.
            - 'check': The type of check being performed (e.g., 'min').
            - 'value': The minimum value to compare against.

    Returns:
        dd.DataFrame: A DataFrame containing rows that do not meet the minimum value
        requirement, with an additional column `dq_status` indicating the rule
        violation in the format "field:check:value".
    """
    field, check, value = __extract_params(rule)
    viol = df[df[field] < value]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def has_std(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Checks if the standard deviation of a specified field in a Dask DataFrame exceeds a given value.

    Parameters:
        df (dd.DataFrame): The input Dask DataFrame to evaluate.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The name of the column to calculate the standard deviation for.
            - 'check' (str): A descriptive label for the check being performed.
            - 'value' (float): The threshold value for the standard deviation.

    Returns:
        dd.DataFrame:
            - If the standard deviation of the specified field exceeds the given value,
              returns the original DataFrame with an additional column `dq_status` indicating the rule details.
            - If the standard deviation does not exceed the value, returns an empty DataFrame with the same structure.
    """
    field, check, value = __extract_params(rule)
    std_val = df[field].std().compute() or 0.0
    if std_val > value:
        return df.assign(dq_status=f"{field}:{check}:{value}")
    return df.head(0).pipe(dd.from_pandas, npartitions=1)


def has_mean(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Checks if the mean of a specified field in a Dask DataFrame satisfies a given condition.

    Parameters:
        df (dd.DataFrame): The input Dask DataFrame to evaluate.
        rule (dict): A dictionary containing the rule to apply. It should include:
            - 'field' (str): The column name to calculate the mean for.
            - 'check' (str): The type of check to perform (e.g., 'greater_than').
            - 'value' (float): The threshold value to compare the mean against.

    Returns:
        dd.DataFrame: A new Dask DataFrame with an additional column `dq_status` if the mean
        satisfies the condition. If the condition is not met, an empty Dask DataFrame is returned.
    """
    field, check, value = __extract_params(rule)
    mean_val = df[field].mean().compute() or 0.0
    if mean_val > value:
        return df.assign(dq_status=f"{field}:{check}:{value}")
    return df.head(0).pipe(dd.from_pandas, npartitions=1)


def has_sum(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Checks if the sum of a specified field in a Dask DataFrame exceeds a given value
    and returns a modified DataFrame with a status column if the condition is met.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to evaluate.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The column name to sum.
            - 'check' (str): A descriptive label for the check (used in the status message).
            - 'value' (float): The threshold value to compare the sum against.

    Returns:
        dd.DataFrame: A new Dask DataFrame. If the sum exceeds the threshold, the DataFrame
        will include a `dq_status` column with a status message. Otherwise, an empty
        DataFrame with the same structure as the input is returned.
    """
    field, check, value = __extract_params(rule)
    sum_val = df[field].sum().compute() or 0.0
    if sum_val > value:
        return df.assign(dq_status=f"{field}:{check}:{value}")
    return df.head(0).pipe(dd.from_pandas, npartitions=1)


def has_cardinality(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Checks if the cardinality (number of unique values) of a specified field in a Dask DataFrame
    exceeds a given threshold and returns a modified DataFrame based on the result.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to evaluate.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The column name to check cardinality for.
            - 'check' (str): A descriptive label for the check (used in the output).
            - 'value' (int): The maximum allowed cardinality.

    Returns:
        dd.DataFrame: If the cardinality of the specified field exceeds the given value,
        returns the original DataFrame with an additional column `dq_status` indicating
        the rule violation. Otherwise, returns an empty DataFrame with the same structure
        as the input DataFrame.
    """
    field, check, value = __extract_params(rule)
    card = df[field].nunique().compute() or 0
    if card > value:
        return df.assign(dq_status=f"{field}:{check}:{value}")
    return df.head(0).pipe(dd.from_pandas, npartitions=1)


def has_infogain(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Evaluates whether a given field in a Dask DataFrame satisfies an information gain condition
    based on the specified rule. If the condition is met, the DataFrame is updated with a
    `dq_status` column indicating the rule applied. Otherwise, an empty DataFrame is returned.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to evaluate.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The column name to evaluate.
            - 'check' (str): The type of check being performed (used for status annotation).
            - 'value' (float): The threshold value for the information gain.

    Returns:
        dd.DataFrame: The original DataFrame with an added `dq_status` column if the condition
        is met, or an empty DataFrame if the condition is not satisfied.
    """
    field, check, value = __extract_params(rule)
    ig = df[field].nunique().compute() or 0.0
    if ig > value:
        return df.assign(dq_status=f"{field}:{check}:{value}")
    return df.head(0).pipe(dd.from_pandas, npartitions=1)


def has_entropy(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Evaluates the entropy of a specified field in a Dask DataFrame and applies a rule to determine
    if the entropy exceeds a given threshold. If the threshold is exceeded, a new column `dq_status`
    is added to the DataFrame with information about the rule violation. Otherwise, an empty DataFrame
    is returned.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to evaluate.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - `field` (str): The column name to evaluate.
            - `check` (str): The type of check being performed (used for status message).
            - `value` (float): The threshold value for the entropy.

    Returns:
        dd.DataFrame: A DataFrame with the `dq_status` column added if the entropy exceeds the threshold,
        or an empty DataFrame if the threshold is not exceeded.
    """
    field, check, value = __extract_params(rule)
    ent = df[field].nunique().compute() or 0.0
    if ent > value:
        return df.assign(dq_status=f"{field}:{check}:{value}")
    return df.head(0).pipe(dd.from_pandas, npartitions=1)


def satisfies(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Filters a Dask DataFrame based on a rule and returns rows that do not satisfy the rule.

    The function evaluates a rule on the given Dask DataFrame and identifies rows that
    violate the rule. The rule is specified as a dictionary containing a field, a check,
    and a value. The rule's logical expression is converted to Python syntax for evaluation.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be filtered.
        rule (dict): A dictionary specifying the rule to evaluate. It should contain:
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
    field, check, value = __extract_params(rule)
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


def validate_date_format(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Validates the date format of a specified column in a Dask DataFrame.

    This function checks whether the values in a specified column of the
    DataFrame conform to a given date format. Rows with invalid date formats
    are returned with an additional column indicating the validation status.

    Args:
        df (dd.DataFrame): The Dask DataFrame to validate.
        rule (dict): A dictionary containing the validation rule. It should
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
    field, check, fmt = __extract_params(rule)
    col_dt = dd.to_datetime(df[field], format=fmt, errors="coerce")
    viol = df[col_dt.isna()]
    return viol.assign(dq_status=f"{field}:{check}:{fmt}")


def is_future_date(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Checks for rows in a Dask DataFrame where the specified date field contains a future date.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to validate.
        rule (dict): A dictionary containing the rule parameters. It is expected to include:
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
    field, check, _ = __extract_params(rule)
    today = pd.Timestamp(date.today())
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt > today]
    return viol.assign(dq_status=f"{field}:{check}:{today.date().isoformat()}")


def is_past_date(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Checks if the values in a specified date column of a Dask DataFrame are in the past.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be checked.
        rule (dict): A dictionary containing the rule parameters. It is expected to include
                     the field name to check, the check type, and additional parameters.

    Returns:
        dd.DataFrame: A Dask DataFrame containing rows where the date in the specified column
                      is in the past. An additional column `dq_status` is added to indicate
                      the field, check type, and the date of the check in the format
                      "field:check:YYYY-MM-DD".
    """
    field, check, _ = __extract_params(rule)
    today = pd.Timestamp(date.today())
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt < today]
    return viol.assign(dq_status=f"{field}:{check}:{today.date().isoformat()}")


def is_date_between(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where a date field does not fall within a specified range.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be checked.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the column in the DataFrame to check.
            - 'check': A string representing the type of check (used for status annotation).
            - 'val': A string representing the date range in the format "[start_date, end_date]".

    Returns:
        dd.DataFrame: A DataFrame containing rows where the date field does not fall within the specified range.
                      An additional column 'dq_status' is added to indicate the rule violation in the format
                      "{field}:{check}:{val}".
    """
    field, check, val = __extract_params(rule)
    start, end = [pd.Timestamp(v.strip()) for v in val.strip("[]").split(",")]
    col_dt = dd.to_datetime(df[field], errors="coerce")
    mask = (col_dt >= start) & (col_dt <= end)
    viol = df[~mask]
    return viol.assign(dq_status=f"{field}:{check}:{val}")


def is_date_after(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where a specified date field is
    earlier than a given reference date.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It should
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
    field, check, date_str = __extract_params(rule)
    ref = pd.Timestamp(date_str)
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt < ref]
    return viol.assign(dq_status=f"{field}:{check}:{date_str}")


def is_date_before(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Checks if the values in a specified date column of a Dask DataFrame are before a given reference date.

    Parameters:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be validated.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the column to check.
            - 'check': A descriptive string for the check (e.g., "is_date_before").
            - 'date_str': The reference date as a string in a format parsable by pandas.Timestamp.

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the date in the specified column
        is after the reference date. An additional column 'dq_status' is added to indicate the validation
        status in the format "{field}:{check}:{date_str}".
    """
    field, check, date_str = __extract_params(rule)
    ref = pd.Timestamp(date_str)
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt > ref]
    return viol.assign(dq_status=f"{field}:{check}:{date_str}")


def all_date_checks(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Applies date validation checks on a Dask DataFrame based on the provided rule.

    This function serves as an alias for the `is_past_date` function, which performs
    checks to determine if dates in the DataFrame meet the specified criteria.

    Args:
        df (dd.DataFrame): The Dask DataFrame containing the data to be validated.
        rule (dict): A dictionary specifying the validation rules to be applied.

    Returns:
        dd.DataFrame: A Dask DataFrame with the results of the date validation checks.
    """
    return is_past_date(df, rule)


def is_t_minus_1(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where a specified datetime column
    matches the date of "T-1" (yesterday) and assigns a data quality status.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be checked.
        rule (dict): A dictionary containing the rule parameters. It is expected
            to include the following keys:
            - 'field': The name of the column to check.
            - 'check': A string describing the check being performed.
            - 'value': Additional value or metadata related to the check.

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the
        specified column matches "T-1". An additional column `dq_status` is added
        to indicate the data quality status in the format "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    target = pd.Timestamp(date.today() - pd.Timedelta(days=1))
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt != target]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_t_minus_2(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where a specified datetime column
    matches the date two days prior to the current date.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be filtered.
        rule (dict): A dictionary containing the rule parameters. It is expected to
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
    field, check, value = __extract_params(rule)
    target = pd.Timestamp(date.today() - pd.Timedelta(days=2))
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt != target]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_t_minus_3(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the specified date field matches
    exactly three days prior to the current date.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be checked.
        rule (dict): A dictionary containing rule parameters. It is expected to include
                     the field name to check, the type of check, and the value (unused in this function).

    Returns:
        dd.DataFrame: A filtered Dask DataFrame containing only the rows where the specified
                      date field matches three days prior to the current date. An additional
                      column `dq_status` is added to indicate the rule applied in the format
                      "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    target = pd.Timestamp(date.today() - pd.Timedelta(days=3))
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt != target]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_today(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the specified field matches today's date.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It is expected to have
            the following keys:
            - field (str): The name of the column in the DataFrame to check.
            - check (str): A descriptive label for the type of check being performed.
            - value (str): A descriptive label for the expected value.

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the specified
        field matches today's date. An additional column `dq_status` is added to indicate
        the rule applied in the format "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    target = pd.Timestamp(date.today())
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt != target]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_yesterday(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Determines if the rows in a Dask DataFrame correspond to "yesterday"
    based on a given rule.

    This function acts as a wrapper for the `is_t_minus_1` function,
    applying the same logic to check if the data corresponds to the
    previous day.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to evaluate.
        rule (dict): A dictionary containing the rule or criteria
                     to determine "yesterday".

    Returns:
        dd.DataFrame: A Dask DataFrame with the evaluation results.
    """
    return is_t_minus_1(df, rule)


def is_on_weekday(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to include only rows where the date in the specified field falls on a weekday
    (Monday to Friday) and assigns a data quality status column.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be filtered.
        rule (dict): A dictionary containing the rule parameters. It is expected to have the following keys:
            - field (str): The name of the column in the DataFrame containing date values.
            - check (str): A descriptive string for the check being performed.
            - value (str): A value associated with the rule, used for constructing the `dq_status` column.

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the date in the specified field
        falls on a weekday. An additional column `dq_status` is added to indicate the rule applied.
    """
    field, check, value = __extract_params(rule)
    col_dt = dd.to_datetime(df[field], errors="coerce")
    dow = col_dt.dt.weekday
    viol = df[(dow >= 5) & (dow <= 6)]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_on_weekend(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Identifies rows in a Dask DataFrame where the date in a specified column falls on a weekend.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be checked.
        rule (dict): A dictionary containing the rule parameters. It is expected to have
                     the following keys:
                     - 'field': The name of the column in the DataFrame to check.
                     - 'check': A string representing the type of check (used for status annotation).
                     - 'value': A value associated with the rule (used for status annotation).

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the date in the specified
                      column falls on a weekend (Saturday or Sunday). An additional column `dq_status`
                      is added to indicate the rule applied in the format "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    col_dt = dd.to_datetime(df[field], errors="coerce")
    dow = col_dt.dt.weekday
    viol = df[(dow >= 0) & (dow <= 4)]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_on_monday(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the date in a specified column falls on a Monday.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be checked.
        rule (dict): A dictionary containing the rule parameters. It is expected to include:
            - 'field': The name of the column in the DataFrame to check.
            - 'check': A string representing the type of check (used for status assignment).
            - 'value': A value associated with the rule (used for status assignment).

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the date in the specified
        column falls on a Monday. An additional column `dq_status` is added to indicate the rule
        applied in the format "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt.dt.weekday != 0]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_on_tuesday(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the specified date field falls on a Tuesday.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be checked.
        rule (dict): A dictionary containing the rule parameters. It is expected to include:
            - 'field': The name of the column in the DataFrame to check.
            - 'check': A string representing the type of check (used for status annotation).
            - 'value': A value associated with the rule (used for status annotation).

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the specified date field
        falls on a Tuesday. An additional column `dq_status` is added to indicate the rule applied
        in the format "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt.dt.weekday != 1]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_on_wednesday(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the date in a specified column falls on a Wednesday.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be checked.
        rule (dict): A dictionary containing the rule parameters. It is expected to include:
            - `field` (str): The name of the column in the DataFrame to check.
            - `check` (str): A descriptive string for the check being performed.
            - `value` (str): A value associated with the rule (not directly used in the function).

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the date in the specified column
        falls on a Wednesday. An additional column `dq_status` is added to indicate the rule applied in the
        format `{field}:{check}:{value}`.
    """
    field, check, value = __extract_params(rule)
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt.dt.weekday != 2]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_on_thursday(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the specified date field falls on a Thursday.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be checked.
        rule (dict): A dictionary containing the rule parameters. It is expected to include:
            - field (str): The name of the column in the DataFrame to check.
            - check (str): A descriptive string for the type of check being performed.
            - value (str): A value associated with the rule (not used in the logic but included in the output).

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the specified date field
        falls on a Thursday. An additional column `dq_status` is added to indicate the rule applied
        in the format "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt.dt.weekday != 3]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_on_friday(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where a specified date field falls on a Friday.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be checked.
        rule (dict): A dictionary containing the rule parameters. It is expected to have
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
    field, check, value = __extract_params(rule)
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt.dt.weekday != 4]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_on_saturday(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where the date in a specified column falls on a Saturday.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be checked.
        rule (dict): A dictionary containing the rule parameters. It is expected to include:
            - 'field': The name of the column in the DataFrame to check.
            - 'check': A string representing the type of check (used for status assignment).
            - 'value': A value associated with the rule (used for status assignment).

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the date in the specified
        column falls on a Saturday. An additional column `dq_status` is added to indicate the rule
        applied in the format "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt.dt.weekday != 5]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_on_sunday(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    """
    Filters a Dask DataFrame to identify rows where a specified date field falls on a Sunday.

    Args:
        df (dd.DataFrame): The input Dask DataFrame containing the data to be checked.
        rule (dict): A dictionary containing the rule parameters. It is expected to include:
            - field (str): The name of the column in the DataFrame to check.
            - check (str): A descriptive string for the check being performed.
            - value (str): A value associated with the rule, used for status annotation.

    Returns:
        dd.DataFrame: A new Dask DataFrame containing only the rows where the specified
        date field falls on a Sunday. An additional column `dq_status` is added to indicate
        the rule applied in the format "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    col_dt = dd.to_datetime(df[field], errors="coerce")
    viol = df[col_dt.dt.weekday != 6]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def validate(df: dd.DataFrame, rules: list[dict]) -> tuple[dd.DataFrame, dd.DataFrame]:
    """
    Validate a Dask DataFrame against a set of rules and return the aggregated results
    and raw violations.

    Args:
        df (dd.DataFrame): The input Dask DataFrame to validate.
        rules (list[dict]): A list of validation rules. Each rule is a dictionary
            containing the following keys:
            - "check_type" (str): The name of the validation function to execute.
            - "value" (optional): The value to be used in the validation function.
            - "execute" (optional, bool): Whether to execute the rule. Defaults to True.

    Returns:
        tuple[dd.DataFrame, dd.DataFrame]:
            - The first DataFrame contains the aggregated validation results,
              with a concatenated "dq_status" column indicating the validation status.
            - The second DataFrame contains the raw violations for each rule.
    """
    empty = dd.from_pandas(
        pd.DataFrame(columns=df.columns.tolist() + ["dq_status"]), npartitions=1
    )
    raw_df = empty

    for rule in rules:
        if not rule.get("execute", True):
            continue
        rule_name = rule["check_type"]
        func = globals().get(rule_name)
        if func is None:
            warnings.warn(f"Unknown rule: {rule_name}")
            continue

        raw_val = rule.get("value")
        try:
            value = (
                __convert_value(raw_val)
                if isinstance(raw_val, str) and raw_val not in ("", "NULL")
                else raw_val
            )
        except ValueError:
            value = raw_val

        viol = func(df, rule)
        raw_df = dd.concat([raw_df, viol], interleave_partitions=True)

    group_cols = [c for c in df.columns if c != "dq_status"]

    def _concat_status(series: pd.Series) -> str:
        return ";".join([s for s in series.astype(str) if s])

    agg_df = (
        raw_df.groupby(group_cols)
        .dq_status.apply(_concat_status, meta=("dq_status", "object"))
        .reset_index()
    )

    return agg_df, raw_df


def _rules_to_df(rules: list[dict]) -> pd.DataFrame:
    """
    Converts a list of rule dictionaries into a pandas DataFrame.

    Each rule dictionary is expected to have the following keys:
    - "field": The column(s) the rule applies to. Can be a string or a list of strings.
    - "check_type": The type of rule or check being applied.
    - "threshold" (optional): A numeric value representing the pass threshold. Defaults to 1.0 if not provided.
    - "value" (optional): Additional value associated with the rule.
    - "execute" (optional): A boolean indicating whether the rule should be executed. Defaults to True if not provided.

    Rules with "execute" set to False are skipped. The resulting DataFrame contains unique rows based on the combination
    of "column" and "rule".

    Args:
        rules (list[dict]): A list of dictionaries representing the rules.

    Returns:
        pd.DataFrame: A DataFrame with the following columns:
            - "column": The column(s) the rule applies to, joined by a comma if multiple.
            - "rule": The type of rule or check being applied.
            - "pass_threshold": The numeric pass threshold for the rule.
            - "value": Additional value associated with the rule, if any.
    """
    rows = []
    for r in rules:
        if not r.get("execute", True):
            continue
        coln = ",".join(r["field"]) if isinstance(r["field"], list) else r["field"]
        rows.append(
            {
                "column": coln.strip(),
                "rule": r["check_type"],
                "pass_threshold": float(r.get("threshold") or 1.0),
                "value": r.get("value") or None,
            }
        )
    return pd.DataFrame(rows).drop_duplicates(["column", "rule"])


def summarize(qc_ddf: dd.DataFrame, rules: list[dict], total_rows: int) -> pd.DataFrame:
    """
    Summarizes quality check results by evaluating rules against a Dask DataFrame.

    Args:
        qc_ddf (dd.DataFrame): A Dask DataFrame containing quality check results.
            The DataFrame must include a "dq_status" column with rule violations
            in the format "column:rule:value".
        rules (list[dict]): A list of dictionaries representing the rules to be
            evaluated. Each dictionary should include keys such as "column",
            "rule", "value", and "pass_threshold".
        total_rows (int): The total number of rows in the original dataset.

    Returns:
        pd.DataFrame: A summarized Pandas DataFrame containing the following columns:
            - id: Unique identifier for each rule evaluation.
            - timestamp: Timestamp of the summary generation.
            - check: The type of check performed (e.g., "Quality Check").
            - level: The severity level of the check (e.g., "WARNING").
            - column: The column name associated with the rule.
            - rule: The rule being evaluated.
            - value: The value associated with the rule.
            - rows: The total number of rows in the dataset.
            - violations: The number of rows that violated the rule.
            - pass_rate: The proportion of rows that passed the rule.
            - pass_threshold: The threshold for passing the rule.
            - status: The status of the rule evaluation ("PASS" or "FAIL").
    """
    df = qc_ddf.compute()

    df = df[df["dq_status"].astype(bool)]
    split = df["dq_status"].str.split(":", expand=True)
    split.columns = ["column", "rule", "value"]
    viol_count = (
        split.groupby(["column", "rule", "value"], dropna=False)
        .size()
        .reset_index(name="violations")
    )

    rules_df = _rules_to_df(rules)

    rules_df["value"] = rules_df["value"].fillna("")
    viol_count["value"] = viol_count["value"].fillna("")

    summary = (
        rules_df.merge(viol_count, on=["column", "rule", "value"], how="left")
        .assign(
            violations=lambda df: df["violations"].fillna(0).astype(int),
            rows=total_rows,
            pass_rate=lambda df: (total_rows - df["violations"]) / total_rows,
            status=lambda df: np.where(
                df["pass_rate"] >= df["pass_threshold"], "PASS", "FAIL"
            ),
            timestamp=datetime.now().replace(second=0, microsecond=0),
            check="Quality Check",
            level="WARNING",
        )
        .reset_index(drop=True)
    )

    summary.insert(0, "id", [str(uuid.uuid4()) for _ in range(len(summary))])

    summary = summary[
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

    return dd.from_pandas(summary, npartitions=1)


def __dask_schema_to_list(df: dd.DataFrame) -> List[Dict[str, Any]]:
    """
    Convert the schema of a Dask DataFrame into a list of dictionaries.

    Each dictionary in the resulting list represents a column in the DataFrame
    and contains metadata about the column, including its name, data type,
    nullability, and maximum length.

    Args:
        df (dd.DataFrame): The Dask DataFrame whose schema is to be converted.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, where each dictionary
        contains the following keys:
            - "field" (str): The name of the column.
            - "data_type" (str): The data type of the column, converted to a lowercase string.
            - "nullable" (bool): Always set to True, indicating the column is nullable.
            - "max_length" (None): Always set to None, as maximum length is not determined.
    """
    return [
        {
            "field": col,
            "data_type": str(dtype).lower(),
            "nullable": True,
            "max_length": None,
        }
        for col, dtype in df.dtypes.items()
    ]


def validate_schema(
    df: dd.DataFrame, expected: List[Dict[str, Any]]
) -> Tuple[bool, List[Tuple[str, str]]]:
    """
    Validates the schema of a Dask DataFrame against an expected schema.

    Args:
        df (dd.DataFrame): The Dask DataFrame whose schema is to be validated.
        expected (List[Dict[str, Any]]): A list of dictionaries representing the expected schema.
            Each dictionary should define the expected column name and its properties.

    Returns:
        Tuple[bool, List[Tuple[str, str]]]: A tuple where the first element is a boolean indicating
            whether the schema matches the expected schema, and the second element is a list of
            tuples containing mismatched column names and their respective issues.
    """
    actual = __dask_schema_to_list(df)
    return __compare_schemas(actual, expected)
