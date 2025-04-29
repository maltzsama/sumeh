#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module provides a set of data quality validation functions using the Polars library. 
It includes various checks for data validation, such as completeness, uniqueness, range checks, 
pattern matching, and schema validation.

Functions:
    is_positive(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Filters rows where the specified field is less than zero.

    is_negative(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Filters rows where the specified field is greater than or equal to zero.

    is_complete(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Filters rows where the specified field is null.

    is_unique(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Filters rows with duplicate values in the specified field.

    are_complete(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Filters rows where any of the specified fields are null.

    are_unique(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Filters rows with duplicate combinations of the specified fields.

    is_greater_than(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Filters rows where the specified field is less than or equal to the given value.

    is_greater_or_equal_than(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Filters rows where the specified field is less than the given value.

    is_less_than(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Filters rows where the specified field is greater than or equal to the given value.

    is_less_or_equal_than(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Filters rows where the specified field is greater than the given value.

    is_equal(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Filters rows where the specified field is not equal to the given value.

    is_equal_than(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Alias for `is_equal`.

    is_contained_in(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Filters rows where the specified field is not in the given list of values.

    not_contained_in(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Filters rows where the specified field is in the given list of values.

    is_between(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Filters rows where the specified field is not within the given range.

    has_pattern(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Filters rows where the specified field does not match the given regex pattern.

    is_legit(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Filters rows where the specified field is null or contains whitespace.

    has_max(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Filters rows where the specified field exceeds the given maximum value.

    has_min(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Filters rows where the specified field is below the given minimum value.

    has_std(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Checks if the standard deviation of the specified field exceeds the given value.

    has_mean(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Checks if the mean of the specified field exceeds the given value.

    has_sum(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Checks if the sum of the specified field exceeds the given value.

    has_cardinality(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Checks if the cardinality (number of unique values) of the specified field exceeds the given value.

    has_infogain(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Placeholder for information gain validation (currently uses cardinality).

    has_entropy(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Placeholder for entropy validation (currently uses cardinality).

    satisfies(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    Filters rows that do not satisfy the given SQL condition.

    validate(df: pl.DataFrame, rules: list[dict]) -> Tuple[pl.DataFrame, pl.DataFrame]:
    Validates a DataFrame against a list of rules and returns the original DataFrame with 
    data quality status and a DataFrame of violations.

    __build_rules_df(rules: list[dict]) -> pl.DataFrame:
    Converts a list of rules into a Polars DataFrame for summarization.

    summarize(qc_df: pl.DataFrame, rules: list[dict], total_rows: int) -> pl.DataFrame:
    Summarizes the results of data quality checks, including pass rates and statuses.

    __polars_schema_to_list(df: pl.DataFrame) -> List[Dict[str, Any]]:
    Converts a Polars DataFrame schema into a list of dictionaries.

    validate_schema(df, expected) -> Tuple[bool, List[Tuple[str, str]]]:
    Validates the schema of a DataFrame against an expected schema and returns a boolean 
    result and a list of errors.
"""

import warnings
from functools import reduce
import polars as pl
from sumeh.services.utils import __convert_value, __extract_params, __compare_schemas
import operator
from datetime import datetime
from typing import List, Dict, Any, Tuple


def is_positive(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Filters a Polars DataFrame to identify rows where the specified field 
    contains negative values and appends a new column indicating the data 
    quality status.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It is 
            expected to include the following keys:
            - 'field': The name of the column to check.
            - 'check': The type of check being performed (e.g., "is_positive").
            - 'value': The reference value for the check.

    Returns:
        pl.DataFrame: A new Polars DataFrame containing only the rows where 
        the specified field has negative values, with an additional column 
        named "dq_status" that describes the rule applied.
    """
    field, check, value = __extract_params(rule)
    return df.filter(pl.col(field) < 0).with_columns(
        [pl.lit(f"{field}:{check}:{value}").alias("dq_status")]
    )


def is_negative(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Filters a Polars DataFrame to exclude rows where the specified field is negative
    and adds a new column indicating the data quality status.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the column to check.
            - 'check': The type of check being performed (e.g., "is_negative").
            - 'value': The value associated with the rule (not used in this function).

    Returns:
        pl.DataFrame: A new DataFrame with rows where the specified field is non-negative
        and an additional column named "dq_status" containing the rule details.
    """
    field, check, value = __extract_params(rule)
    return df.filter(pl.col(field) >= 0).with_columns(
        [pl.lit(f"{field}:{check}:{value}").alias("dq_status")]
    )


def is_complete(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Filters a Polars DataFrame to include only rows where the specified field is not null
    and appends a new column indicating the data quality status.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be filtered and modified.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The name of the column to check for non-null values.
            - 'check' (str): A descriptive string for the type of check being performed.
            - 'value' (str): A value associated with the rule for status annotation.

    Returns:
        pl.DataFrame: A new Polars DataFrame with rows filtered based on the rule and
        an additional column named "dq_status" containing the data quality status.
    """
    field, check, value = __extract_params(rule)
    return df.filter(pl.col(field).is_not_null()).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def is_unique(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Checks for duplicate values in a specified field of a Polars DataFrame and 
    returns a filtered DataFrame containing only the rows with duplicate values. 
    Additionally, it adds a new column 'dq_status' with a formatted string 
    indicating the field, check type, and value.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to check for duplicates.
        rule (dict): A dictionary containing the rule parameters. It is expected 
                     to have keys that allow extraction of the field to check, 
                     the type of check, and a value.

    Returns:
        pl.DataFrame: A filtered DataFrame containing rows with duplicate values 
                      in the specified field, along with an additional column 
                      'dq_status' describing the rule applied.
    """
    field, check, value = __extract_params(rule)
    dup_vals = (
        df.group_by(field)
        .agg(pl.len().alias("cnt"))
        .filter(pl.col("cnt") > 1)
        .select(field)
        .to_series()
        .to_list()
    )
    return df.filter(pl.col(field).is_in(dup_vals)).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def are_complete(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Filters a Polars DataFrame to identify rows where specified fields contain null values
    and tags them with a data quality status.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'fields': A list of column names to check for null values.
            - 'check': A string representing the type of check (e.g., "is_null").
            - 'value': A value associated with the check (not used in this function).

    Returns:
        pl.DataFrame: A filtered DataFrame containing only rows where at least one of the
        specified fields is null, with an additional column "dq_status" indicating the
        data quality status.
    """
    fields, check, value = __extract_params(rule)
    cond = reduce(operator.or_, [pl.col(f).is_null() for f in fields])

    tag = f"{fields}:{check}:{value}"
    return df.filter(cond).with_columns(pl.lit(tag).alias("dq_status"))


def are_unique(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Checks for duplicate combinations of specified fields in a Polars DataFrame 
    and returns a DataFrame containing the rows with duplicates along with a 
    data quality status column.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to check for duplicates.
        rule (dict): A dictionary containing the rule parameters. It is expected 
                     to include the following keys:
                     - 'fields': A list of column names to check for uniqueness.
                     - 'check': A string representing the type of check (e.g., "unique").
                     - 'value': A value associated with the check (e.g., "True").

    Returns:
        pl.DataFrame: A DataFrame containing rows with duplicate combinations of 
                      the specified fields. An additional column, "dq_status", 
                      is added to indicate the data quality status in the format 
                      "{fields}:{check}:{value}".
    """
    fields, check, value = __extract_params(rule)
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
    return (
        combo.filter(pl.col("_combo").is_in(dupes))
        .drop("_combo")
        .with_columns(pl.lit(f"{fields}:{check}:{value}").alias("dq_status"))
    )


def is_greater_than(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Filters a Polars DataFrame to include only rows where the specified field's value 
    is less than or equal to a given value, and adds a new column indicating the 
    data quality status.

    Args:
        df (pl.DataFrame): The Polars DataFrame to filter.
        rule (dict): A dictionary containing the filtering rule. It should include:
            - 'field': The name of the column to apply the filter on.
            - 'check': A string describing the check (e.g., "greater_than").
            - 'value': The value to compare against.

    Returns:
        pl.DataFrame: A new DataFrame with rows filtered based on the rule and an 
        additional column named "dq_status" indicating the applied rule.
    """
    field, check, value = __extract_params(rule)
    return df.filter(pl.col(field) <= value).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def is_greater_or_equal_than(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Filters a Polars DataFrame to include only rows where the specified field 
    is greater than or equal to a given value, and adds a new column indicating 
    the data quality status.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be filtered.
        rule (dict): A dictionary containing the filtering rule. It should 
            include the following keys:
            - 'field': The name of the column to be checked.
            - 'check': The type of check being performed (e.g., "greater_or_equal").
            - 'value': The threshold value for the comparison.

    Returns:
        pl.DataFrame: A new Polars DataFrame with rows filtered based on the 
        specified rule and an additional column named "dq_status" indicating 
        the data quality status in the format "field:check:value".
    """
    field, check, value = __extract_params(rule)
    return df.filter(pl.col(field) < value).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def is_less_than(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Filters a Polars DataFrame to include only rows where the specified field
    is greater than or equal to a given value. Adds a new column indicating
    the data quality status.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to filter.
        rule (dict): A dictionary containing the filtering rule. It should
            include the following keys:
            - 'field': The name of the column to apply the filter on.
            - 'check': A string representing the type of check (not used in logic).
            - 'value': The threshold value for the filter.

    Returns:
        pl.DataFrame: A new Polars DataFrame with rows filtered based on the
        condition and an additional column named "dq_status" containing the
        rule description in the format "field:check:value".
    """
    field, check, value = __extract_params(rule)
    return df.filter(pl.col(field) >= value).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def is_less_or_equal_than(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Filters a Polars DataFrame to include only rows where the specified field's value 
    is greater than the given value, and adds a new column indicating the rule applied.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the column to apply the filter on.
            - 'check': The type of check being performed (e.g., 'less_or_equal_than').
            - 'value': The value to compare against.

    Returns:
        pl.DataFrame: A new DataFrame with rows filtered based on the rule and an 
        additional column named "dq_status" indicating the rule applied.
    """
    field, check, value = __extract_params(rule)
    return df.filter(pl.col(field) > value).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def is_equal(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Filters rows in a Polars DataFrame that do not match a specified equality condition
    and adds a column indicating the data quality status.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The column name to apply the equality check on.
            - 'check': The type of check (expected to be 'eq' for equality).
            - 'value': The value to compare against.

    Returns:
        pl.DataFrame: A new DataFrame with rows filtered based on the rule and an
        additional column named "dq_status" indicating the rule applied.
    """
    field, check, value = __extract_params(rule)
    return df.filter(~pl.col(field).eq(value)).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def is_equal_than(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Filters rows in a Polars DataFrame where the specified field is not equal to a given value
    and adds a new column indicating the data quality status.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the column to check.
            - 'check': The type of check (expected to be 'equal' for this function).
            - 'value': The value to compare against.

    Returns:
        pl.DataFrame: A new Polars DataFrame with rows filtered based on the rule and an
        additional column named "dq_status" indicating the applied rule.
    """
    field, check, value = __extract_params(rule)
    return df.filter(~pl.col(field).eq(value)).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def is_contained_in(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Filters a Polars DataFrame to exclude rows where the specified field's value is 
    contained in a given list of values, and adds a new column indicating the rule applied.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The column name to check.
            - 'check': The type of check being performed (e.g., "is_contained_in").
            - 'value': A string representation of a list of values to check against, 
              e.g., "[value1, value2, value3]".

    Returns:
        pl.DataFrame: A new DataFrame with rows filtered based on the rule and an 
        additional column "dq_status" indicating the rule applied.
    """
    field, check, value = __extract_params(rule)
    lst = [v.strip() for v in value.strip("[]").split(",")]
    return df.filter(~pl.col(field).is_in(lst)).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def not_contained_in(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Filters a Polars DataFrame to include only rows where the specified field's value 
    is in a given list, and adds a new column indicating the data quality status.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to filter.
        rule (dict): A dictionary containing the filtering rule. It should include:
            - 'field': The column name to apply the filter on.
            - 'check': A string representing the type of check (not used in logic).
            - 'value': A string representation of a list of values (e.g., "[value1, value2]").

    Returns:
        pl.DataFrame: A new Polars DataFrame with rows filtered based on the rule and 
        an additional column "dq_status" indicating the applied rule.
    """
    field, check, value = __extract_params(rule)
    lst = [v.strip() for v in value.strip("[]").split(",")]
    return df.filter(pl.col(field).is_in(lst)).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def is_between(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Filters a Polars DataFrame to exclude rows where the specified field's value 
    falls within a given range, and adds a column indicating the data quality status.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to filter.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the column to check.
            - 'check': The type of check being performed (e.g., "is_between").
            - 'value': A string representing the range in the format "[lo,hi]".

    Returns:
        pl.DataFrame: A new Polars DataFrame with rows outside the specified range 
        and an additional column named "dq_status" indicating the rule applied.

    Raises:
        ValueError: If the 'value' parameter is not in the expected format "[lo,hi]".
    """
    field, check, value = __extract_params(rule)
    lo, hi = value.strip("[]").split(",")
    lo, hi = __convert_value(lo), __convert_value(hi)
    return df.filter(~pl.col(field).is_between(lo, hi)).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def has_pattern(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Filters a Polars DataFrame based on a pattern-matching rule and adds a data quality status column.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The column name in the DataFrame to apply the pattern check.
            - 'check': A descriptive label for the check being performed.
            - 'pattern': The regex pattern to match against the column values.

    Returns:
        pl.DataFrame: A new DataFrame with rows not matching the pattern removed and an additional
        column named "dq_status" indicating the rule applied in the format "field:check:pattern".
    """
    field, check, pattern = __extract_params(rule)
    return df.filter(~pl.col(field).str.contains(pattern, literal=False)).with_columns(
        pl.lit(f"{field}:{check}:{pattern}").alias("dq_status")
    )


def is_legit(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Filters a Polars DataFrame based on a validation rule and appends a data quality status column.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to validate.
        rule (dict): A dictionary containing the validation rule. It should include:
            - 'field': The name of the column to validate.
            - 'check': The type of validation check (e.g., regex, condition).
            - 'value': The value or pattern to validate against.

    Returns:
        pl.DataFrame: A new DataFrame containing rows that failed the validation,
        with an additional column 'dq_status' indicating the validation rule applied.
    """
    field, check, value = __extract_params(rule)
    mask = pl.col(field).is_not_null() & pl.col(field).str.contains(r"^\S+$")
    return df.filter(~mask).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def has_max(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Filters a Polars DataFrame to include only rows where the value in a specified 
    column exceeds a given threshold, and adds a new column indicating the rule applied.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The column name to apply the filter on.
            - 'check' (str): The type of check being performed (e.g., "max").
            - 'value' (numeric): The threshold value to compare against.

    Returns:
        pl.DataFrame: A new DataFrame containing only the rows that satisfy the condition, 
        with an additional column named "dq_status" that describes the applied rule.
    """
    field, check, value = __extract_params(rule)
    return df.filter(pl.col(field) > value).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def has_min(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Filters a Polars DataFrame to include only rows where the value of a specified 
    column is less than a given threshold and adds a new column indicating the 
    data quality status.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the column to apply the filter on.
            - 'check': A string representing the type of check (e.g., 'min').
            - 'value': The threshold value for the filter.

    Returns:
        pl.DataFrame: A new Polars DataFrame containing only the rows that satisfy 
        the condition, with an additional column named "dq_status" indicating the 
        applied rule in the format "field:check:value".
    """
    field, check, value = __extract_params(rule)
    return df.filter(pl.col(field) < value).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def has_std(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Evaluates whether the standard deviation of a specified column in a Polars DataFrame
    exceeds a given threshold and returns a modified DataFrame accordingly.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to evaluate.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The name of the column to calculate the standard deviation for.
            - 'check' (str): A descriptive label for the check being performed.
            - 'value' (float): The threshold value for the standard deviation.

    Returns:
        pl.DataFrame: A modified DataFrame. If the standard deviation of the specified column
        exceeds the threshold, the DataFrame will include a new column `dq_status` with a
        descriptive string. Otherwise, an empty DataFrame with the `dq_status` column is returned.
    """
    field, check, value = __extract_params(rule)
    std_val = df.select(pl.col(field).std()).to_numpy()[0] or 0.0
    if std_val > value:
        return df.with_columns(pl.lit(f"{field}:{check}:{value}").alias("dq_status"))
    return df.head(0).with_columns(pl.lit("dq_status").alias("dq_status")).head(0)


def has_mean(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Checks if the mean value of a specified column in a Polars DataFrame satisfies a given condition.

    Args:
        df (pl.DataFrame): The input Polars DataFrame.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The name of the column to calculate the mean for.
            - 'check' (str): The condition to check (e.g., 'greater than').
            - 'value' (float): The threshold value to compare the mean against.

    Returns:
        pl.DataFrame: 
            - If the mean value of the specified column is greater than the threshold value, 
              returns the original DataFrame with an additional column "dq_status" containing 
              a string in the format "{field}:{check}:{value}".
            - If the condition is not met, returns an empty DataFrame with the same schema as the input.
    """
    field, check, value = __extract_params(rule)
    mean_val = df.select(pl.col(field).mean()).to_numpy()[0] or 0.0
    if mean_val > value:
        return df.with_columns(pl.lit(f"{field}:{check}:{value}").alias("dq_status"))
    return df.head(0)


def has_sum(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Checks if the sum of a specified column in a Polars DataFrame exceeds a given value.

    Args:
        df (pl.DataFrame): The input Polars DataFrame.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the column to sum.
            - 'check': A string representing the check type (not used in this function).
            - 'value': The threshold value to compare the sum against.

    Returns:
        pl.DataFrame: If the sum of the specified column exceeds the given value, 
        returns the original DataFrame with an additional column `dq_status` containing 
        a string in the format "{field}:{check}:{value}". Otherwise, returns an empty DataFrame.
    """
    field, check, value = __extract_params(rule)
    sum_val = df.select(pl.col(field).sum()).to_numpy()[0] or 0.0
    if sum_val > value:
        return df.with_columns(pl.lit(f"{field}:{check}:{value}").alias("dq_status"))
    return df.head(0)


def has_cardinality(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Checks if the cardinality (number of unique values) of a specified field in the given DataFrame
    satisfies a condition defined in the rule. If the cardinality exceeds the specified value, 
    a new column "dq_status" is added to the DataFrame with a string indicating the rule violation. 
    Otherwise, an empty DataFrame is returned.

    Args:
        df (pl.DataFrame): The input DataFrame to evaluate.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - "field" (str): The column name to check.
            - "check" (str): The type of check (e.g., "greater_than").
            - "value" (int): The threshold value for the cardinality.

    Returns:
        pl.DataFrame: The original DataFrame with an added "dq_status" column if the rule is violated,
                      or an empty DataFrame if the rule is not violated.
    """
    field, check, value = __extract_params(rule)
    card = df.select(pl.col(field).n_unique()).to_numpy()[0] or 0
    if card > value:
        return df.with_columns(pl.lit(f"{field}:{check}:{value}").alias("dq_status"))
    return df.head(0)


def has_infogain(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Evaluates whether a given DataFrame satisfies an information gain condition 
    based on a specified rule. If the condition is met, a new column indicating 
    the rule is added; otherwise, an empty DataFrame is returned.

    Args:
        df (pl.DataFrame): The input DataFrame to evaluate.
        rule (dict): A dictionary containing the rule parameters. It should 
            include the following keys:
            - 'field': The column name to evaluate.
            - 'check': The type of check to perform (not used directly in this function).
            - 'value': The threshold value for the information gain.

    Returns:
        pl.DataFrame: The original DataFrame with an additional column named 
        "dq_status" if the condition is met, or an empty DataFrame if the 
        condition is not met.
    """
    field, check, value = __extract_params(rule)
    ig = df.select(pl.col(field).n_unique()).to_numpy()[0] or 0.0
    if ig > value:
        return df.with_columns(pl.lit(f"{field}:{check}:{value}").alias("dq_status"))
    return df.head(0)


def has_entropy(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Evaluates the entropy of a specified field in a Polars DataFrame based on a given rule.

    Parameters:
        df (pl.DataFrame): The input Polars DataFrame to evaluate.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The column name in the DataFrame to evaluate.
            - 'check' (str): The type of check to perform (not used directly in this function).
            - 'value' (float): The threshold value for entropy comparison.

    Returns:
        pl.DataFrame: 
            - If the entropy of the specified field exceeds the given threshold (`value`), 
              returns the original DataFrame with an additional column `dq_status` indicating 
              the rule that was applied.
            - If the entropy does not exceed the threshold, returns an empty DataFrame with 
              the same schema as the input DataFrame.

    Notes:
        - The entropy is calculated as the number of unique values in the specified field.
        - The `dq_status` column contains a string in the format "{field}:{check}:{value}".
    """
    field, check, value = __extract_params(rule)
    ent = df.select(pl.col(field).n_unique()).to_numpy()[0] or 0.0
    if ent > value:
        return df.with_columns(pl.lit(f"{field}:{check}:{value}").alias("dq_status"))
    return df.head(0)


def satisfies(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    """
    Evaluates a given rule against a Polars DataFrame and returns rows that do not satisfy the rule.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to be evaluated.
        rule (dict): A dictionary containing the rule to be applied. The rule should include
                     the following keys:
                     - 'field': The column name in the DataFrame to be checked.
                     - 'check': The type of check or condition to be applied.
                     - 'value': The value or expression to validate against.

    Returns:
        pl.DataFrame: A DataFrame containing rows that do not satisfy the rule, with an additional
                      column `dq_status` indicating the rule that was violated in the format
                      "field:check:value".

    Example:
        rule = {"field": "age", "check": ">", "value": "18"}
        result = satisfies(df, rule)
    """
    field, check, value = __extract_params(rule)
    ctx = pl.SQLContext(sumeh=df)
    viol = ctx.execute(
        f"""
        SELECT *
        FROM sumeh
        WHERE NOT ({value})
        """,
        eager=True,
    )
    return viol.with_columns(pl.lit(f"{field}:{check}:{value}").alias("dq_status"))


def validate(df: pl.DataFrame, rules: list[dict]) -> Tuple[pl.DataFrame,pl.DataFrame]:
    """
    Validates a Polars DataFrame against a set of rules and returns the updated DataFrame 
    with validation statuses and a DataFrame containing the validation violations.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to validate.
        rules (list[dict]): A list of dictionaries representing validation rules. Each rule 
            should contain the following keys:
            - "check_type" (str): The type of validation to perform (e.g., "is_primary_key", 
                "is_composite_key", "has_pattern", etc.).
            - "value" (optional): The value to validate against, depending on the rule type.
            - "execute" (bool, optional): Whether to execute the rule. Defaults to True.

    Returns:
        Tuple[pl.DataFrame, pl.DataFrame]: A tuple containing:
            - The original DataFrame with an additional "dq_status" column indicating the 
                validation status for each row.
            - A DataFrame containing rows that violated the validation rules, including 
                details of the violations.

    Notes:
        - The function dynamically resolves validation functions based on the "check_type" 
            specified in the rules.
        - If a rule's "check_type" is unknown, a warning is issued, and the rule is skipped.
        - The "__id" column is temporarily added to the DataFrame for internal processing 
            and is removed in the final output.
    """
    df = df.with_columns(pl.arange(0, pl.len()).alias("__id"))
    df_with_dq = df.with_columns(pl.lit("").alias("dq_status"))
    result = df_with_dq.head(0)
    for rule in rules:
        if not rule.get("execute", True):
            continue
        rule_name = rule["check_type"]
        if rule_name == "is_primary_key":
            rule_name = "is_unique"
        elif rule_name == "is_composite_key":
            rule_name = "are_unique"

        func = globals().get(rule_name)
        if func is None:
            warnings.warn(f"Unknown rule: {rule_name}")
            continue

        raw_value = rule.get("value")
        if rule_name in ("has_pattern", "satisfies"):
            value = raw_value
        else:
            try:
                value = (
                    __convert_value(raw_value)
                    if isinstance(raw_value, str) and raw_value not in ("", "NULL")
                    else raw_value
                )
            except ValueError:
                value = raw_value

        viol = func(df_with_dq, rule)
        result = pl.concat([result, viol]) if not result.is_empty() else viol

    summary = (
        result.group_by("__id", maintain_order=True)
        .agg("dq_status")
        .with_columns(pl.col("dq_status").list.join(";").alias("dq_status"))
    )
    out = df.join(summary, on="__id", how="left").drop("__id")

    return out, result


def __build_rules_df(rules: list[dict]) -> pl.DataFrame:
    """
    Builds a Polars DataFrame from a list of rule dictionaries.

    This function processes a list of rule dictionaries, filters out rules
    that are not marked for execution, and constructs a DataFrame with the
    relevant rule information. It ensures uniqueness of rows based on 
    specific columns and casts the data to appropriate types.

    Args:
        rules (list[dict]): A list of dictionaries, where each dictionary 
            represents a rule. Each rule dictionary may contain the following keys:
            - "field" (str or list): The column(s) the rule applies to.
            - "check_type" (str): The type of rule or check.
            - "threshold" (float, optional): The pass threshold for the rule. Defaults to 1.0.
            - "value" (any, optional): Additional value associated with the rule.
            - "execute" (bool, optional): Whether the rule should be executed. Defaults to True.

    Returns:
        pl.DataFrame: A Polars DataFrame containing the processed rules with the following columns:
            - "column" (str): The column(s) the rule applies to, joined by commas if multiple.
            - "rule" (str): The type of rule or check.
            - "pass_threshold" (float): The pass threshold for the rule.
            - "value" (str): The value associated with the rule, or an empty string if not provided.
    """
    rules_df = (
        pl.DataFrame(
            [
                {
                    "column": (
                        ",".join(r["field"])
                        if isinstance(r["field"], list)
                        else r["field"]
                    ),
                    "rule": r["check_type"],
                    "pass_threshold": float(r.get("threshold") or 1.0),
                    "value": r.get("value"),
                }
                for r in rules
                if r.get("execute", True)
            ]
        )
        .unique(subset=["column", "rule", "value"])
        .with_columns(
            [
                pl.col("column").cast(str),
                pl.col("rule").cast(str),
                pl.col("value").cast(str),
            ]
        )
    ).with_columns(pl.col("value").fill_null("").alias("value"))

    return rules_df


def summarize(qc_df: pl.DataFrame, rules: list[dict], total_rows: int) -> pl.DataFrame:
    """
    Summarizes quality check results by processing a DataFrame containing 
    data quality statuses and comparing them against defined rules.

    Args:
        qc_df (pl.DataFrame): A Polars DataFrame containing a column `dq_status` 
            with semicolon-separated strings representing data quality statuses 
            in the format "column:rule:value".
        rules (list[dict]): A list of dictionaries where each dictionary defines 
            a rule with keys such as "column", "rule", "value", and "pass_threshold".
        total_rows (int): The total number of rows in the original dataset, used 
            to calculate the pass rate.

    Returns:
        pl.DataFrame: A summarized DataFrame containing the following columns:
            - id: A unique identifier for each rule.
            - timestamp: The timestamp when the summary was generated.
            - check: A label indicating the type of check (e.g., "Quality Check").
            - level: The severity level of the check (e.g., "WARNING").
            - column: The column name associated with the rule.
            - rule: The rule being evaluated.
            - value: The specific value associated with the rule.
            - rows: The total number of rows in the dataset.
            - violations: The number of rows that violated the rule.
            - pass_rate: The proportion of rows that passed the rule.
            - pass_threshold: The threshold for passing the rule.
            - status: The status of the rule evaluation ("PASS" or "FAIL").
    """
    exploded = (
        qc_df.select(
            pl.col("dq_status").str.split(";").list.explode().alias("dq_status")
        )
        .filter(pl.col("dq_status") != "")
        .with_columns(
            [
                pl.col("dq_status").str.split(":").list.get(0).alias("column"),
                pl.col("dq_status").str.split(":").list.get(1).alias("rule"),
                pl.col("dq_status").str.split(":").list.get(2).alias("value"),
            ]
        )
    ).drop("dq_status")
    viol_count = exploded.group_by(["column", "rule", "value"]).agg(
        pl.len().alias("violations")
    )

    rules_df = __build_rules_df(rules)

    viol_count2 = viol_count.with_columns(pl.col("value").fill_null("").alias("value"))

    step1 = rules_df.join(
        viol_count2,
        on=["column", "rule", "value"],
        how="left",
    )

    step2 = step1.with_columns([pl.col("violations").fill_null(0).alias("violations")])

    step3 = step2.with_columns(
        [
            ((pl.lit(total_rows) - pl.col("violations")) / pl.lit(total_rows)).alias(
                "pass_rate"
            )
        ]
    )

    now = datetime.now().replace(second=0, microsecond=0)
    step4 = step3.with_columns(
        [
            pl.lit(total_rows).alias("rows"),
            pl.when(pl.col("pass_rate") >= pl.col("pass_threshold"))
            .then(pl.lit("PASS"))
            .otherwise(pl.lit("FAIL"))
            .alias("status"),
            pl.lit(now).alias("timestamp"),
            pl.lit("Quality Check").alias("check"),
            pl.lit("WARNING").alias("level"),
        ]
    )

    summary = step4.with_columns(pl.arange(1, pl.len() + 1).alias("id")).select(
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
    )

    return summary


def __polars_schema_to_list(df: pl.DataFrame) -> List[Dict[str, Any]]:
    """
    Converts the schema of a Polars DataFrame into a list of dictionaries, 
    where each dictionary represents a field in the schema.

    Args:
        df (pl.DataFrame): The Polars DataFrame whose schema is to be converted.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each containing the following keys:
            - "field" (str): The name of the field.
            - "data_type" (str): The data type of the field, converted to lowercase.
            - "nullable" (bool): Always set to True, as Polars does not expose nullability in the schema.
            - "max_length" (None): Always set to None, as max length is not applicable.
    """
    return [
        {
            "field": name,
            "data_type": str(dtype).lower(),
            "nullable": True,  # Polars não expõe nullability no schema
            "max_length": None,
        }
        for name, dtype in df.schema.items()
    ]


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
    actual = __polars_schema_to_list(df)
    result, errors = __compare_schemas(actual, expected)
    return result, errors
