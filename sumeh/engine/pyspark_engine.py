#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module provides a set of functions for performing data quality checks on PySpark DataFrames.
It includes various validation rules, schema validation, and summarization utilities.

Functions:
    is_positive: Filters rows where the specified field is negative and adds a data quality status column.

    is_negative: Filters rows where the specified field is non-negative and adds a data quality status column.

    is_in_millions: Retains rows where the field value is at least 1,000,000 and flags them with dq_status.

    is_positive: Filters rows where the specified field is negative and adds a data quality status column.

    is_negative: Filters rows where the specified field is non-negative and adds a data quality status column.

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

    is_complete: Filters rows where the specified field is null and adds a data quality status column.

    is_unique: Identifies duplicate rows based on the specified field and adds a data quality status column.

    are_complete: Filters rows where any of the specified fields are null and adds a data quality status column.

    are_unique: Identifies duplicate rows based on a combination of specified fields and adds a data quality status column.

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

    is_legit: Filters rows where the specified field is null or does not match a non-whitespace pattern.

    is_primary_key(df: DataFrame, rule: dict):
    Checks if the specified field is unique (alias for `is_unique`).

    is_composite_key(df: DataFrame, rule: dict):
    Checks if the combination of specified fields is unique (alias for `are_unique`).

    has_max: Filters rows where the specified field exceeds the given maximum value.

    has_min: Filters rows where the specified field is below the given minimum value.

    has_std: Checks if the standard deviation of the specified field exceeds the given value.

    has_mean: Checks if the mean of the specified field exceeds the given value.

    has_sum: Checks if the sum of the specified field exceeds the given value.

    has_cardinality: Checks if the cardinality (distinct count) of the specified field exceeds the given value.

    has_infogain: Checks if the information gain (distinct count) of the specified field exceeds the given value.

    has_entropy: Checks if the entropy (distinct count) of the specified field exceeds the given value.

    all_date_checks: Filters rows where the specified date field is earlier than the current date.

    satisfies: Filters rows where the specified field matches the given regex pattern.

    validate: Applies a list of validation rules to the DataFrame and returns the results.

    summarize: Summarizes the results of data quality checks, including pass rates and violations.

    validate_schema: Validates the schema of the DataFrame against the expected schema.

    __rules_to_df: Converts a list of rules into a DataFrame for further processing.

    __pyspark_schema_to_list: Converts the schema of a DataFrame into a list of dictionaries for comparison.
"""

import warnings
from pyspark.sql import DataFrame, Window, Row

from pyspark.sql.functions import (
    lit,
    col,
    concat,
    collect_list,
    concat_ws,
    count,
    coalesce,
    stddev,
    avg,
    sum,
    countDistinct,
    current_date,
    monotonically_increasing_id,
    current_timestamp,
    when,
    trim,
    split,
    expr,
    date_sub,
    dayofweek,
    broadcast,
)


from typing import List, Dict, Any, Tuple
import operator
from functools import reduce

from sumeh.services.utils import (
    __convert_value,
    __extract_params,
    __compare_schemas,
    __transform_date_format_in_pattern,
)


def is_positive(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters a DataFrame to identify rows where the specified field does not satisfy a positive check
    and adds a "dq_status" column with details of the rule applied.

    Args:
        df (DataFrame): The input PySpark DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - "field" (str): The name of the column to check.
            - "check" (str): The type of check being performed (e.g., "positive").
            - "value" (any): The value associated with the rule (not directly used in this function).

    Returns:
        DataFrame: A new DataFrame filtered to include only rows where the specified field is less than 0,
        with an additional "dq_status" column describing the rule applied.
    """
    field, check, value = __extract_params(rule)
    return df.filter(col(field) < 0).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_negative(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters rows in the given DataFrame where the specified field is non-negative
    and adds a new column "dq_status" containing a formatted string with rule details.

    Args:
        df (DataFrame): The input PySpark DataFrame to be filtered and modified.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The name of the column to check.
            - 'check' (str): A descriptive string for the check being performed.
            - 'value' (any): The value associated with the rule.

    Returns:
        DataFrame: A new DataFrame with rows filtered based on the rule and an
        additional "dq_status" column describing the rule applied.
    """
    field, check, value = __extract_params(rule)
    return df.filter(col(field) >= 0).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_in_millions(df, rule: dict):
    """
    Filters a DataFrame to include only rows where the specified field's value
    is greater than or equal to 1,000,000 and adds a "dq_status" column with
    a formatted string indicating the rule applied.

    Args:
        df (pyspark.sql.DataFrame): The input DataFrame to filter and modify.
        rule (dict): A dictionary containing the rule parameters. It should
                     include the field to check, the check type, and the value.

    Returns:
        pyspark.sql.DataFrame: A new DataFrame with rows filtered based on the
        rule and an additional "dq_status" column describing the rule applied.
    """
    field, check, value = __extract_params(rule)
    return df.filter(col(field) < lit(1_000_000)).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_in_billions(df, rule: dict):
    """
    Filters a PySpark DataFrame to include only rows where the specified field's value
    is greater than or equal to one billion, and adds a "dq_status" column with a
    formatted string indicating the field, check, and value.

    Args:
        df (pyspark.sql.DataFrame): The input PySpark DataFrame to filter.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the column to check.
            - 'check': The type of check being performed (e.g., "greater_than").
            - 'value': The threshold value for the check.

    Returns:
        pyspark.sql.DataFrame: A new DataFrame filtered by the rule and with an
        additional "dq_status" column.
    """
    field, check, value = __extract_params(rule)
    return df.filter(col(field) < lit(1_000_000_000)).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_complete(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters a DataFrame to identify rows where a specified field is null and adds a
    "dq_status" column indicating the data quality rule applied.

    Args:
        df (DataFrame): The input PySpark DataFrame to be checked.
        rule (dict): A dictionary containing the data quality rule. It should include:
            - "field" (str): The name of the field to check for null values.
            - "check" (str): A description of the check being performed.
            - "value" (str): Additional information about the rule.

    Returns:
        DataFrame: A new DataFrame filtered to include only rows where the specified
        field is null, with an additional "dq_status" column describing the rule.
    """
    field, check, value = __extract_params(rule)
    return df.filter(col(field).isNull()).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def validate_date_format(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters a DataFrame to identify rows where a specified field has wrong date format based in the format from the rule
    and adds a "dq_status" column indicating the data quality rule applied.

    YYYY = full year, ex: 2012;
    YY = only second part of the year, ex: 12;
    MM = Month number (1-12);
    DD = Day (1-31);

    Args:
        df (DataFrame): The input PySpark DataFrame to be checked.
        rule (dict): A dictionary containing the data quality rule. It should include:
            - "field" (str): The name of the field to check for null values.
            - "check" (str): A description of the check being performed.
            - "value" (str): Additional information about the rule.

    Returns:
        DataFrame: A new DataFrame filtered to include only rows where the specified
        field is null, with an additional "dq_status" column describing the rule.
    """

    field, check, date_format = __extract_params(rule)

    date_regex = __transform_date_format_in_pattern(date_format)

    return df.filter(~col(field).rlike(date_regex) | col(field).isNull()).withColumn(
        "dq_status",
        concat(lit(field), lit(":"), lit(check), lit(":"), lit(date_format)),
    )


def is_future_date(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters a DataFrame to identify rows where a specified field has a date greater than the current date and
    adds a "dq_status" column indicating the data quality rule applied.

    Args:
        df (DataFrame): The input PySpark DataFrame to be checked.
        rule (dict): A dictionary containing the data quality rule. It should include:
            - "field" (str): The name of the field to check for null values.
            - "check" (str): A description of the check being performed.
            - "value" (str): Additional information about the rule.

    Returns:
        DataFrame: A new DataFrame filtered to include only rows where the specified
        field is null, with an additional "dq_status" column describing the rule.
    """

    field, check, value = __extract_params(rule)
    return df.filter(col(field) > current_date()).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_past_date(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters a DataFrame to identify rows where a specified field has a date lower than the current date and
    adds a "dq_status" column indicating the data quality rule applied.

    Args:
        df (DataFrame): The input PySpark DataFrame to be checked.
        rule (dict): A dictionary containing the data quality rule. It should include:
            - "field" (str): The name of the field to check for null values.
            - "check" (str): A description of the check being performed.
            - "value" (str): Additional information about the rule.

    Returns:
        DataFrame: A new DataFrame filtered to include only rows where the specified
        field is null, with an additional "dq_status" column describing the rule.
    """

    field, check, value = __extract_params(rule)
    return df.filter(col(field) < current_date()).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_date_between(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters a DataFrame to identify rows where a specified field has a date between two dates passed in the rule using
    the format: "[<initial_date>, <final_date>]" and adds a "dq_status" column indicating the data quality rule applied.

    Args:
        df (DataFrame): The input PySpark DataFrame to be checked.
        rule (dict): A dictionary containing the data quality rule. It should include:
            - "field" (str): The name of the field to check for null values.
            - "check" (str): A description of the check being performed.
            - "value" (str): Additional information about the rule.

    Returns:
        DataFrame: A new DataFrame filtered to include only rows where the specified
        field is null, with an additional "dq_status" column describing the rule.
    """

    field, check, value = __extract_params(rule)
    start_date, end_date = value.strip("[]").split(",")
    return df.filter(~col(field).between(start_date, end_date)).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_date_after(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters a DataFrame to identify rows where a specified field has a date lower than the date informed in the rule
    and adds a "dq_status" column indicating the data quality rule applied.

    Args:
        df (DataFrame): The input PySpark DataFrame to be checked.
        rule (dict): A dictionary containing the data quality rule. It should include:
            - "field" (str): The name of the field to check for null values.
            - "check" (str): A description of the check being performed.
            - "value" (str): Additional information about the rule.

    Returns:
        DataFrame: A new DataFrame filtered to include only rows where the specified
        field is null, with an additional "dq_status" column describing the rule.
    """

    field, check, value = __extract_params(rule)
    return df.filter(col(field) < value).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_date_before(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters a DataFrame to identify rows where a specified field has a date greater than the date informed in the rule
    and adds a "dq_status" column indicating the data quality rule applied.

    Args:
        df (DataFrame): The input PySpark DataFrame to be checked.
        rule (dict): A dictionary containing the data quality rule. It should include:
            - "field" (str): The name of the field to check for null values.
            - "check" (str): A description of the check being performed.
            - "value" (str): Additional information about the rule.

    Returns:
        DataFrame: A new DataFrame filtered to include only rows where the specified
        field is null, with an additional "dq_status" column describing the rule.
    """

    field, check, value = __extract_params(rule)
    return df.filter(col(field) > value).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_unique(df: DataFrame, rule: dict) -> DataFrame:
    """
    Checks for uniqueness of a specified field in a PySpark DataFrame based on the given rule.

    This function identifies rows where the specified field is not unique within the DataFrame.
    It adds a new column `dq_status` to the resulting DataFrame, which contains information
    about the field, the check type, and the value from the rule.

    Args:
        df (DataFrame): The input PySpark DataFrame to check for uniqueness.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - `field` (str): The name of the field to check for uniqueness.
            - `check` (str): The type of check being performed (e.g., "unique").
            - `value` (str): Additional value or metadata related to the check.

    Returns:
        DataFrame: A new DataFrame containing rows where the specified field is not unique.
        The resulting DataFrame includes a `dq_status` column with details about the rule violation.

    Example:
        rule = {"field": "column_name", "check": "unique", "value": "some_value"}
        result_df = is_unique(input_df, rule)
    """
    field, check, value = __extract_params(rule)
    window = Window.partitionBy(col(field))
    df_with_count = df.withColumn("count", count(col(field)).over(window))
    res = (
        df_with_count.filter(col("count") > 1)
        .withColumn(
            "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
        )
        .drop("count")
    )
    return res


def are_complete(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters rows in a DataFrame that do not meet the completeness rule and adds a data quality status column.

    Args:
        df (DataFrame): The input PySpark DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - "fields" (list): A list of column names to check for completeness (non-null values).
            - "check" (str): A descriptive label for the type of check being performed.
            - "value" (str): A descriptive value associated with the check.

    Returns:
        DataFrame: A new DataFrame containing only the rows that fail the completeness check,
        with an additional column "dq_status" describing the failed rule.
    """
    fields, check, value = __extract_params(rule)
    predicate = reduce(operator.and_, [col(field).isNotNull() for field in fields])
    return df.filter(~predicate).withColumn(
        "dq_status",
        concat(lit(str(fields)), lit(":"), lit(check), lit(":"), lit(value)),
    )


def are_unique(df: DataFrame, rule: dict) -> DataFrame:
    """
    Checks for uniqueness of specified fields in a PySpark DataFrame based on the provided rule.

    Args:
        df (DataFrame): The input PySpark DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'fields': A list of column names to check for uniqueness.
            - 'check': A string representing the type of check (e.g., "unique").
            - 'value': A value associated with the rule for logging or identification.

    Returns:
        DataFrame: A DataFrame containing rows that violate the uniqueness rule.
        The resulting DataFrame includes an additional column `dq_status` that
        describes the rule violation in the format: "[fields]:[check]:[value]".

    Notes:
        - The function concatenates the specified fields into a single column
          and checks for duplicate values within that column.
        - Rows that do not meet the uniqueness criteria are returned, while
          rows that satisfy the criteria are excluded from the result.
    """
    fields, check, value = __extract_params(rule)
    combined_col = concat_ws("|", *[coalesce(col(f), lit("")) for f in fields])
    window = Window.partitionBy(combined_col)
    result = (
        df.withColumn("_count", count("*").over(window))
        .filter(col("_count") > 1)
        .drop("_count")
        .withColumn(
            "dq_status",
            concat(lit(str(fields)), lit(":"), lit(check), lit(":"), lit(value)),
        )
    )
    return result


def is_greater_than(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters rows in a DataFrame where the value of a specified field is less than
    or equal to a given threshold and adds a new column indicating the rule applied.

    Args:
        df (DataFrame): The input PySpark DataFrame to filter.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The name of the column to apply the rule on.
            - 'check' (str): A descriptive string for the rule (e.g., "greater_than").
            - 'value' (int or float): The threshold value for the comparison.

    Returns:
        DataFrame: A new DataFrame with rows filtered based on the rule and an
        additional column "dq_status" describing the rule applied.
    """
    field, check, value = __extract_params(rule)
    return df.filter(col(field) <= value).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_greater_or_equal_than(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters rows in a DataFrame where the value of a specified field is less than a given value
    and adds a new column "dq_status" with a formatted string indicating the rule applied.

    Args:
        df (DataFrame): The input PySpark DataFrame to filter.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - "field" (str): The name of the column to check.
            - "check" (str): A descriptive string for the check (e.g., "greater_or_equal").
            - "value" (numeric): The threshold value for the comparison.

    Returns:
        DataFrame: A new DataFrame with rows filtered based on the rule and an additional
        "dq_status" column describing the rule applied.
    """
    field, check, value = __extract_params(rule)
    return df.filter(col(field) < value).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_less_than(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters rows in a PySpark DataFrame where the specified field is greater than
    or equal to a given value and adds a new column indicating the rule applied.

    Args:
        df (DataFrame): The input PySpark DataFrame to filter.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The name of the column to apply the filter on.
            - 'check' (str): A descriptive string for the rule (e.g., "less_than").
            - 'value' (int, float, or str): The value to compare the column against.

    Returns:
        DataFrame: A new DataFrame with rows filtered based on the rule and an
        additional column "dq_status" describing the rule applied.
    """
    field, check, value = __extract_params(rule)
    return df.filter(col(field) >= value).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_less_or_equal_than(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters rows in a PySpark DataFrame where the value of a specified field is greater than a given value
    and adds a new column "dq_status" with a formatted string indicating the rule applied.

    Args:
        df (DataFrame): The input PySpark DataFrame to filter.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - "field" (str): The name of the column to evaluate.
            - "check" (str): A descriptive string for the check being performed.
            - "value" (numeric): The threshold value to compare against.

    Returns:
        DataFrame: A new PySpark DataFrame with rows filtered based on the rule and an additional
        "dq_status" column describing the rule applied.
    """
    field, check, value = __extract_params(rule)
    return df.filter(col(field) > value).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_equal(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters a PySpark DataFrame based on a rule that checks for equality between a specified field
    and a given value. Rows that do not satisfy the equality condition are retained, and a new
    column "dq_status" is added to indicate the rule applied.

    Args:
        df (DataFrame): The input PySpark DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - "field" (str): The name of the column to check.
            - "check" (str): The type of check (e.g., "equal"). This is used for logging purposes.
            - "value" (Any): The value to compare against.

    Returns:
        DataFrame: A new DataFrame with rows that do not satisfy the equality condition and an
        additional "dq_status" column describing the rule applied.
    """
    field, check, value = __extract_params(rule)
    return df.filter(~col(field).eqNullSafe(value)).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_equal_than(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters rows in a PySpark DataFrame that do not satisfy an equality condition
    specified in the rule dictionary and adds a "dq_status" column with details
    about the rule applied.

    Args:
        df (DataFrame): The input PySpark DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - "field" (str): The name of the column to check.
            - "check" (str): The type of check being performed (e.g., "equal").
            - "value" (Any): The value to compare against.

    Returns:
        DataFrame: A new DataFrame with rows filtered based on the rule and an
        additional "dq_status" column describing the rule applied.
    """
    field, check, value = __extract_params(rule)
    return df.filter(~col(field).eqNullSafe(value)).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_contained_in(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters rows in a PySpark DataFrame based on whether a specified column's value
    is not contained in a given list of values. Adds a new column 'dq_status' to
    indicate the rule applied.

    Args:
        df (DataFrame): The input PySpark DataFrame to filter.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the column to check.
            - 'check': The type of check being performed (e.g., "is_contained_in").
            - 'value': A string representation of a list of values (e.g., "[value1,value2]").

    Returns:
        DataFrame: A new PySpark DataFrame with rows filtered based on the rule
        and an additional column 'dq_status' describing the rule applied.

    Example:
        rule = {"field": "column_name", "check": "is_contained_in", "value": "[value1,value2]"}
        result_df = is_contained_in(input_df, rule)
    """
    field, check, value = __extract_params(rule)
    positive_list = value.strip("[]").split(",")
    return df.filter(~col(field).isin(positive_list)).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_in(df: DataFrame, rule: dict) -> DataFrame:
    """
    Checks if the values in the specified column of a DataFrame are contained within a given set of values.

    Args:
        df (DataFrame): The input DataFrame to evaluate.
        rule (dict): A dictionary containing the rule for the check. It should specify the column name
                     and the set of values to check against.

    Returns:
        DataFrame: A DataFrame with the applied rule, typically filtered or modified based on the check.
    """
    return is_contained_in(df, rule)


def not_contained_in(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters rows in a DataFrame where the specified field's value is in a given list
    and adds a column indicating the data quality status.

    Args:
        df (DataFrame): The input PySpark DataFrame to filter.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the column to check.
            - 'check': A string representing the type of check (e.g., "not_contained_in").
            - 'value': A string representation of a list (e.g., "[value1,value2,...]")
              containing the values to check against.

    Returns:
        DataFrame: A new DataFrame with rows filtered based on the rule and an
        additional column "dq_status" indicating the data quality status in the
        format "field:check:value".
    """
    field, check, value = __extract_params(rule)
    negative_list = value.strip("[]").split(",")
    return df.filter(col(field).isin(negative_list)).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def not_in(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters rows in a DataFrame where the specified rule is not contained.

    This function delegates the operation to the `not_contained_in` function.

    Args:
        df (DataFrame): The input DataFrame to be filtered.
        rule (dict): A dictionary specifying the rule to apply for filtering.

    Returns:
        DataFrame: A new DataFrame with rows that do not match the specified rule.
    """
    return not_contained_in(df, rule)


def is_between(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters rows in a PySpark DataFrame where the value of a specified field is not within a given range.
    Adds a new column 'dq_status' to indicate the rule that was applied.

    Args:
        df (DataFrame): The input PySpark DataFrame.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the column to check.
            - 'check': A string representing the type of check (e.g., "between").
            - 'value': A string representing the range in the format "[min_value,max_value]".

    Returns:
        DataFrame: A new DataFrame with rows filtered based on the rule and an additional
        'dq_status' column indicating the applied rule.
    """
    field, check, value = __extract_params(rule)
    min_value, max_value = value.strip("[]").split(",")
    return df.filter(~col(field).between(min_value, max_value)).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def has_pattern(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters a PySpark DataFrame based on a pattern match and adds a data quality status column.

    Args:
        df (DataFrame): The input PySpark DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the column to apply the pattern check.
            - 'check': A descriptive label for the type of check being performed.
            - 'value': The regex pattern to match against the column values.

    Returns:
        DataFrame: A new DataFrame with rows that do not match the pattern filtered out.
                   Additionally, a "dq_status" column is added, containing a string
                   representation of the rule applied in the format "field:check:value".
    """
    field, check, value = __extract_params(rule)
    return df.filter(~col(field).rlike(value)).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_legit(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters a PySpark DataFrame to identify rows that do not meet a specified rule
    and appends a column indicating the data quality status.

    Args:
        df (DataFrame): The input PySpark DataFrame to be validated.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the column to validate.
            - 'check': The type of check being performed (e.g., "is_legit").
            - 'value': The expected value or condition for the validation.

    Returns:
        DataFrame: A new DataFrame containing only the rows that fail the validation
        rule, with an additional column "dq_status" describing the validation status
        in the format "field:check:value".
    """
    field, check, value = __extract_params(rule)
    pattern_legit = "\S*"
    return df.filter(
        ~(col(field).isNotNull() & col(field).rlike(pattern_legit))
    ).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_primary_key(df: DataFrame, rule: dict):
    """
    Determines if a given DataFrame column or set of columns satisfies the primary key constraint.

    A primary key constraint requires that the specified column(s) in the DataFrame have unique values.

    Args:
        df (DataFrame): The PySpark DataFrame to be checked.
        rule (dict): A dictionary containing the rules or specifications for identifying the primary key.
                     Typically, this includes the column(s) to be checked for uniqueness.

    Returns:
        bool: True if the specified column(s) in the DataFrame satisfy the primary key constraint, False otherwise.
    """
    return is_unique(df, rule)


def is_composite_key(df: DataFrame, rule: dict):
    """
    Determines if the given DataFrame satisfies the composite key condition based on the provided rule.

    A composite key is a combination of two or more columns in a DataFrame that uniquely identify a row.

    Args:
        df (DataFrame): The PySpark DataFrame to be evaluated.
        rule (dict): A dictionary containing the rules or criteria to determine the composite key.

    Returns:
        bool: True if the DataFrame satisfies the composite key condition, False otherwise.
    """
    return are_unique(df, rule)


def has_max(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters a PySpark DataFrame to include only rows where the value of a specified field
    is greater than a given threshold. Adds a new column 'dq_status' to indicate the rule applied.

    Args:
        df (DataFrame): The input PySpark DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The name of the column to apply the rule on.
            - 'check' (str): The type of check being performed (e.g., 'max').
            - 'value' (numeric): The threshold value to compare against.

    Returns:
        DataFrame: A new DataFrame filtered based on the rule, with an additional column 'dq_status'
        describing the rule applied in the format "field:check:value".
    """
    field, check, value = __extract_params(rule)
    return df.filter(col(field) > value).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def has_min(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters rows in a DataFrame where the value of a specified field is less than a given threshold
    and adds a new column indicating the data quality status.

    Args:
        df (DataFrame): The input PySpark DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The name of the column to check.
            - 'check' (str): The type of check being performed (e.g., "min").
            - 'value' (numeric): The threshold value for the check.

    Returns:
        DataFrame: A new DataFrame with rows filtered based on the rule and an additional
        "dq_status" column containing a string representation of the rule applied.
    """
    field, check, value = __extract_params(rule)
    return df.filter(col(field) < value).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def has_std(df: DataFrame, rule: dict) -> DataFrame:
    """
    Checks if the standard deviation of a specified field in a DataFrame exceeds a given value.

    Args:
        df (DataFrame): The input PySpark DataFrame.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The name of the column to calculate the standard deviation for.
            - 'check' (str): A descriptive label for the check being performed.
            - 'value' (float): The threshold value for the standard deviation.

    Returns:
        DataFrame: If the standard deviation of the specified field exceeds the given value,
        returns the original DataFrame with an additional column "dq_status" indicating the
        field, check, and value. Otherwise, returns an empty DataFrame.
    """
    field, check, value = __extract_params(rule)
    std_val = df.select(stddev(col(field))).first()[0]
    std_val = std_val or 0.0
    if std_val > value:
        return df.withColumn(
            "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
        )
    else:
        return df.limit(0)


def has_mean(df: DataFrame, rule: dict) -> DataFrame:
    """
    Evaluates whether the mean value of a specified column in a DataFrame satisfies a given rule.

    Args:
        df (DataFrame): The input PySpark DataFrame to evaluate.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The name of the column to calculate the mean for.
            - 'check' (str): The type of check being performed (e.g., 'greater_than').
            - 'value' (float): The threshold value to compare the mean against.

    Returns:
        DataFrame: If the mean value of the specified column exceeds the threshold,
        returns the original DataFrame with an additional column `dq_status` indicating
        the rule violation. If the mean value satisfies the rule, returns an empty DataFrame.
    """
    field, check, value = __extract_params(rule)
    mean_val = (df.select(avg(col(field))).first()[0]) or 0.0
    if mean_val > value:  # regra falhou
        return df.withColumn(
            "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
        )
    else:  # passou
        return df.limit(0)


def has_sum(df: DataFrame, rule: dict) -> DataFrame:
    """
    Checks if the sum of values in a specified column of a DataFrame exceeds a given threshold.

    Args:
        df (DataFrame): The input PySpark DataFrame to evaluate.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The name of the column to sum.
            - 'check' (str): A descriptive label for the check being performed.
            - 'value' (float): The threshold value to compare the sum against.

    Returns:
        DataFrame: If the sum of the specified column exceeds the threshold, returns the original
        DataFrame with an additional column `dq_status` indicating the rule details. If the sum
        does not exceed the threshold, returns an empty DataFrame.
    """
    field, check, value = __extract_params(rule)
    sum_val = (df.select(sum(col(field))).first()[0]) or 0.0
    if sum_val > value:
        return df.withColumn(
            "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
        )
    return df.limit(0)


def has_cardinality(df: DataFrame, rule: dict) -> DataFrame:
    """
    Checks the cardinality of a specified field in a DataFrame against a given rule.

    This function evaluates whether the distinct count of values in a specified column
    (field) of the DataFrame exceeds a given threshold (value) as defined in the rule.
    If the cardinality exceeds the threshold, a new column `dq_status` is added to the
    DataFrame with information about the rule violation. Otherwise, an empty DataFrame
    is returned.

    Args:
        df (DataFrame): The input PySpark DataFrame to be checked.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the column to check.
            - 'check': The type of check being performed (e.g., "cardinality").
            - 'value': The threshold value for the cardinality.

    Returns:
        DataFrame: A DataFrame with the `dq_status` column added if the cardinality
        exceeds the threshold, or an empty DataFrame if the condition is not met.
    """
    field, check, value = __extract_params(rule)
    card_val = df.select(countDistinct(col(field))).first()[0] or 0
    if card_val > value:
        return df.withColumn(
            "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
        )
    return df.limit(0)


def has_infogain(df: DataFrame, rule: dict) -> DataFrame:
    """
    Evaluates whether a given DataFrame satisfies an information gain condition
    based on the provided rule. If the condition is met, it appends a column
    indicating the status; otherwise, it returns an empty DataFrame.

    Args:
        df (DataFrame): The input PySpark DataFrame to evaluate.
        rule (dict): A dictionary containing the rule parameters. It should
                     include the following keys:
                     - 'field': The column name to evaluate.
                     - 'check': The condition type (not used directly in the logic).
                     - 'value': The threshold value for information gain.

    Returns:
        DataFrame: A DataFrame with an additional "dq_status" column if the
                   information gain condition is met, or an empty DataFrame
                   if the condition is not satisfied.
    """
    field, check, value = __extract_params(rule)
    info_gain = df.select(countDistinct(col(field))).first()[0] or 0.0
    if info_gain > value:
        return df.withColumn(
            "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
        )
    return df.limit(0)


def has_entropy(df: DataFrame, rule: dict) -> DataFrame:
    """
    Evaluates the entropy of a specified field in a DataFrame and applies a rule to determine
    whether the DataFrame should be processed further or filtered out.

    Parameters:
        df (DataFrame): The input PySpark DataFrame to evaluate.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field' (str): The name of the column to evaluate.
            - 'check' (str): The type of check being performed (e.g., "entropy").
            - 'value' (float): The threshold value for the entropy check.

    Returns:
        DataFrame: If the entropy of the specified field exceeds the given value, returns the
        original DataFrame with an additional column "dq_status" indicating the rule applied.
        Otherwise, returns an empty DataFrame with the same schema as the input.
    """
    field, check, value = __extract_params(rule)
    entropy_val = df.select(countDistinct(col(field))).first()[0] or 0.0
    if entropy_val > value:
        return df.withColumn(
            "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
        )
    return df.limit(0)


def all_date_checks(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters the input DataFrame based on a date-related rule and adds a data quality status column.

    Args:
        df (DataFrame): The input PySpark DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the column to apply the rule on.
            - 'check': The type of check to perform (e.g., comparison operator).
            - 'value': The value to be used in the check.

    Returns:
        DataFrame: A new DataFrame filtered based on the rule, with an additional column
        "dq_status" indicating the data quality status in the format "field:check:value".
    """
    field, check, value = __extract_params(rule)
    return df.filter((col(field) < current_date())).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_t_minus_1(df, rule: dict):
    """
    Filters the input DataFrame to include only rows where the specified field matches the date
    corresponding to "T-1" (yesterday). Adds a new column "dq_status" to indicate the rule applied.

    Args:
        df (pyspark.sql.DataFrame): The input DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the column to be checked.
            - 'check': The type of check being performed (not used in filtering but included in "dq_status").
            - 'value': The value associated with the check (not used in filtering but included in "dq_status").

    Returns:
        pyspark.sql.DataFrame: A new DataFrame filtered by the rule and with an additional "dq_status" column.
    """
    field, check, value = __extract_params(rule)
    target = date_sub(current_date(), 1)
    return df.filter(col(field) != target).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_t_minus_2(df, rule: dict):
    """
    Filters the input DataFrame to include only rows where the specified field matches the date
    that is two days prior to the current date. Adds a new column 'dq_status' to indicate the
    data quality status.

    Args:
        df (pyspark.sql.DataFrame): The input DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the column to be checked.
            - 'check': A string representing the type of check (not used in filtering).
            - 'value': A value associated with the check (not used in filtering).

    Returns:
        pyspark.sql.DataFrame: A new DataFrame filtered by the rule and with an additional
        'dq_status' column indicating the field, check, and value.
    """
    field, check, value = __extract_params(rule)
    target = date_sub(current_date(), 2)
    return df.filter(col(field) != target).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_t_minus_3(df, rule: dict):
    """
    Filters the input DataFrame to include only rows where the specified field matches
    the date that is three days prior to the current date. Adds a new column 'dq_status'
    to indicate the data quality status.

    Args:
        df (pyspark.sql.DataFrame): The input DataFrame to be filtered.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the column to be checked.
            - 'check': A string representing the type of check (not used in filtering).
            - 'value': A value associated with the rule (not used in filtering).

    Returns:
        pyspark.sql.DataFrame: A new DataFrame filtered by the rule and with an
        additional 'dq_status' column.
    """
    field, check, value = __extract_params(rule)
    target = date_sub(current_date(), 3)
    return df.filter(col(field) != target).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_today(df, rule: dict):
    """
    Filters a DataFrame to include only rows where the specified field matches the current date.

    Args:
        df (pyspark.sql.DataFrame): The input DataFrame to filter.
        rule (dict): A dictionary containing the rule parameters. It is expected to have
                     the following keys:
                     - 'field': The name of the column to check.
                     - 'check': A string representing the type of check (not used in this function).
                     - 'value': A value associated with the rule (not used in this function).

    Returns:
        pyspark.sql.DataFrame: A new DataFrame filtered by the current date and with an additional
                               column "dq_status" indicating the rule applied in the format
                               "field:check:value".
    """
    field, check, value = __extract_params(rule)
    today = current_date()
    return df.filter(col(field) != today).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_yesterday(df, rule: dict):
    """
    Filters a PySpark DataFrame to include only rows where the specified field matches yesterday's date.
    Adds a new column 'dq_status' to indicate the data quality status.

    Args:
        df (pyspark.sql.DataFrame): The input PySpark DataFrame.
        rule (dict): A dictionary containing the rule parameters. It should include:
            - 'field': The name of the column to check.
            - 'check': The type of check being performed (used for status message).
            - 'value': Additional value information (used for status message).

    Returns:
        pyspark.sql.DataFrame: A filtered DataFrame with an additional 'dq_status' column.
    """
    field, check, value = __extract_params(rule)
    yesterday = date_sub(current_date(), 1)
    return df.filter(col(field) != yesterday).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_on_weekday(df, rule: dict):
    """
    Filters a PySpark DataFrame to include only rows where the specified date field
    falls on a weekday (Monday to Friday). Adds a new column 'dq_status' to indicate
    the rule applied.

    Args:
        df (pyspark.sql.DataFrame): The input PySpark DataFrame.
        rule (dict): A dictionary containing the rule parameters. It is expected to
            include the following keys:
            - 'field': The name of the column to check.
            - 'check': A string representing the type of check (used for logging).
            - 'value': A value associated with the rule (used for logging).

    Returns:
        pyspark.sql.DataFrame: A new DataFrame filtered to include only rows where
        the specified date field is a weekday, with an additional 'dq_status' column
        describing the rule applied.
    """
    field, check, value = __extract_params(rule)
    return df.filter(
        (dayofweek(col(field)) == 1) | (dayofweek(col(field)) == 7)
    ).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_on_weekend(df, rule: dict):
    """
    Filters a PySpark DataFrame to include only rows where the specified date field
    falls on a weekend (Saturday or Sunday). Additionally, adds a new column
    'dq_status' to indicate the rule applied.

    Args:
        df (pyspark.sql.DataFrame): The input PySpark DataFrame.
        rule (dict): A dictionary containing the rule parameters. It is expected
                     to have the following keys:
                     - 'field': The name of the date column to check.
                     - 'check': A string representing the type of check (not used in logic).
                     - 'value': A string representing the value to include in the 'dq_status' column.

    Returns:
        pyspark.sql.DataFrame: A new DataFrame filtered to include only rows where
        the specified date field is on a weekend, with an additional 'dq_status' column.
    """
    field, check, value = __extract_params(rule)
    return df.filter(
        (dayofweek(col(field)) != 1) | (dayofweek(col(field)) != 7)
    ).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_on_monday(df, rule: dict):
    """
    Filters a PySpark DataFrame to include only rows where the specified date field falls on a Monday.

    Args:
        df (pyspark.sql.DataFrame): The input PySpark DataFrame.
        rule (dict): A dictionary containing rule parameters. It is expected to include:
            - 'field': The name of the column to check.
            - 'check': A string representing the type of check (not used in this function).
            - 'value': A value associated with the rule (not used in this function).

    Returns:
        pyspark.sql.DataFrame: A new DataFrame filtered to include only rows where the specified
        date field corresponds to a Monday. Additionally, a new column "dq_status" is added,
        containing a concatenated string of the field, check, and value.
    """
    field, check, value = __extract_params(rule)
    return df.filter(dayofweek(col(field)) != 2).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_on_tuesday(df, rule: dict):
    """
    Filters a PySpark DataFrame to include only rows where the day of the week
    for a specified date column is Tuesday. Adds a new column 'dq_status' to
    indicate the validation status.

    Args:
        df (pyspark.sql.DataFrame): The input PySpark DataFrame.
        rule (dict): A dictionary containing the rule parameters. It is expected
            to include:
            - 'field': The name of the column to check.
            - 'check': A string describing the check being performed.
            - 'value': A value associated with the check.

    Returns:
        pyspark.sql.DataFrame: A new DataFrame filtered to include only rows
        where the specified column corresponds to Tuesday, with an additional
        'dq_status' column describing the validation status.
    """
    field, check, value = __extract_params(rule)
    return df.filter(dayofweek(col(field)) != 3).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_on_wednesday(df, rule: dict):
    """
    Filters a PySpark DataFrame to include only rows where the specified date field falls on a Wednesday.

    Args:
        df (pyspark.sql.DataFrame): The input PySpark DataFrame.
        rule (dict): A dictionary containing the rule parameters. It is expected to have the following keys:
            - 'field': The name of the column in the DataFrame to check.
            - 'check': A string representing the type of check (not used in the logic but included for status reporting).
            - 'value': A value associated with the rule (not used in the logic but included for status reporting).

    Returns:
        pyspark.sql.DataFrame: A new DataFrame filtered to include only rows where the specified field corresponds to a Wednesday.
        Additionally, a new column 'dq_status' is added, which contains a string in the format "field:check:value".
    """
    field, check, value = __extract_params(rule)
    return df.filter(dayofweek(col(field)) != 4).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_on_thursday(df, rule: dict):
    """
    Filters a PySpark DataFrame to include only rows where the specified date column falls on a Thursday.

    Args:
        df (DataFrame): The PySpark DataFrame to filter.
        rule (dict): A dictionary containing the rule parameters. It is expected to include:
            - 'field': The name of the column to check.
            - 'check': A string representing the type of check (not used in the filtering logic).
            - 'value': A value associated with the rule (not used in the filtering logic).

    Returns:
        DataFrame: A new PySpark DataFrame filtered to include only rows where the specified column's day of the week is Thursday.
                   Additionally, a new column "dq_status" is added, containing a concatenated string of the field, check, and value.
    """
    field, check, value = __extract_params(rule)
    return df.filter(dayofweek(col(field)) != 5).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_on_friday(df, rule: dict):
    """
    Filters a PySpark DataFrame to include only rows where the specified date field falls on a Friday.

    Args:
        df (pyspark.sql.DataFrame): The input DataFrame to filter.
        rule (dict): A dictionary containing the rule parameters. It is expected to have the following keys:
            - 'field': The name of the column in the DataFrame to check.
            - 'check': A string representing the type of check (not used in this function but included for consistency).
            - 'value': A value associated with the rule (not used in this function but included for consistency).

    Returns:
        pyspark.sql.DataFrame: A new DataFrame filtered to include only rows where the specified date field
        corresponds to a Friday. Additionally, a new column `dq_status` is added, which contains a string
        representation of the rule applied in the format "field:check:value".
    """
    field, check, value = __extract_params(rule)
    return df.filter(dayofweek(col(field)) != 6).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_on_saturday(df, rule: dict):
    """
    Filters a PySpark DataFrame to include only rows where the specified date field falls on a Saturday.

    Args:
        df (pyspark.sql.DataFrame): The input DataFrame to filter.
        rule (dict): A dictionary containing rule parameters. The function expects the rule to include:
            - 'field': The name of the column to check.
            - 'check': A string representing the check being performed (not used in logic, but included in the output column).
            - 'value': A value to include in the output column (not used in logic, but included in the output column).

    Returns:
        pyspark.sql.DataFrame: A new DataFrame filtered to include only rows where the specified field falls on a Saturday.
        Additionally, a new column "dq_status" is added, containing a string in the format "field:check:value".
    """
    field, check, value = __extract_params(rule)
    return df.filter(dayofweek(col(field)) != 7).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_on_sunday(df, rule: dict):
    """
    Filters a PySpark DataFrame to include only rows where the specified date field falls on a Sunday.

    Args:
        df (pyspark.sql.DataFrame): The input DataFrame to filter.
        rule (dict): A dictionary containing the rule parameters. It is expected to include:
            - field (str): The name of the column to check.
            - check (str): A descriptive string for the check being performed.
            - value (str): A value to include in the "dq_status" column for context.

    Returns:
        pyspark.sql.DataFrame: A new DataFrame filtered to include only rows where the specified
        date field corresponds to a Sunday. Additionally, a "dq_status" column is added to the
        DataFrame, containing a string in the format "field:check:value".
    """
    field, check, value = __extract_params(rule)
    return df.filter(dayofweek(col(field)) != 1).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def satisfies(df: DataFrame, rule: dict) -> DataFrame:
    """
    Filters a PySpark DataFrame based on a rule and adds a data quality status column.

    Args:
        df (DataFrame): The input PySpark DataFrame to be filtered.
        rule (dict): A dictionary containing the filtering rule. It should include:
            - 'field': The name of the column to apply the filter on.
            - 'check': The type of check to perform (currently unused in this implementation).
            - 'value': The expression in the pattern of pyspark.sql.functions.expr.

    Returns:
        DataFrame: A new DataFrame filtered based on the rule, with an additional column
        "dq_status" that describes the rule applied in the format "field:check:value".
    """
    field, check, value = __extract_params(rule)
    expression = expr(value)
    return df.filter(~expression).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def validate(df: DataFrame, rules: list[dict]) -> Tuple[DataFrame, DataFrame]:
    """
    Validates a DataFrame against a set of rules and returns the validation results.

    This function applies a series of validation rules to the input DataFrame. Each rule
    is expected to be a dictionary containing the parameters required for validation.
    The function generates two DataFrames as output:
    1. A summarized result DataFrame with aggregated validation statuses.
    2. A raw result DataFrame containing detailed validation results.

    Args:
        df (DataFrame): The input PySpark DataFrame to validate.
        rules (list[dict]): A list of dictionaries, where each dictionary defines a validation rule.
            Each rule should include the following keys:
            - `field` (str): The column name to validate.
            - `rule_name` (str): The name of the validation function to apply.
            - `value` (any): The value or parameter required by the validation function.

    Returns:
        Tuple[DataFrame, DataFrame]: A tuple containing:
            - result (DataFrame): A DataFrame with aggregated validation statuses.
            - raw_result (DataFrame): A DataFrame with detailed validation results.

    Raises:
        KeyError: If a rule references a validation function that does not exist in the global scope.

    Warnings:
        If a rule references an unknown validation function, a warning is issued.

    Notes:
        - The `dq_status` column is used to store validation statuses.
        - The function assumes that the validation functions are defined in the global scope
          and are accessible by their names.
        - The `concat_ws` function is used to concatenate multiple validation statuses
          into a single string for each record in the summarized result.

    Example:
        >>> from pyspark.sql import SparkSession
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
        >>> rules = [{"field": "id", "rule_name": "validate_positive", "value": None}]
        >>> result, raw_result = validate(df, rules)
    """
    df = df.withColumn("dq_status", lit(""))
    raw_result = df.limit(0)
    for rule in rules:
        field, rule_name, value = __extract_params(rule)
        try:
            rule_func = globals()[rule_name]
            raw_result = raw_result.unionByName(rule_func(df, rule))
        except KeyError:
            warnings.warn(f"Unknown rule name: {rule_name}, {field}")
    group_columns = [c for c in df.columns if c != "dq_status"]
    result = raw_result.groupBy(*group_columns).agg(
        concat_ws(";", collect_list("dq_status")).alias("dq_status")
    )
    return result, raw_result


def __rules_to_df(rules: List[Dict]) -> DataFrame:
    """
    Converts a list of rule dictionaries into a PySpark DataFrame.

    Args:
        rules (List[Dict]): A list of dictionaries where each dictionary represents a rule.
            Each rule dictionary should contain the following keys:
                - "field" (str or list): The name of the field or a list of field names.
                - "check_type" (str): The type of rule or check to be applied.
                - "threshold" (float, optional): The threshold value for the rule. Defaults to 1.0 if not provided.
                - "value" (str, optional): The value associated with the rule. Defaults to "N/A" if not provided.
                - "execute" (bool, optional): A flag indicating whether the rule should be executed. Defaults to True.

    Returns:
        DataFrame: A PySpark DataFrame containing the following columns:
            - "column" (str): The name of the field.
            - "rule" (str): The type of rule or check.
            - "pass_threshold" (float): The threshold value for the rule.
            - "value" (str): The value associated with the rule.

    Notes:
        - Rows with "execute" set to False are skipped.
        - Duplicate rows based on the "column" and "rule" columns are removed.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    rows = []
    for r in rules:
        if not r.get("execute", True):
            continue
        col_name = str(r["field"]) if isinstance(r["field"], list) else r["field"]
        rows.append(
            Row(
                column=col_name.strip(),
                rule=r["check_type"],
                pass_threshold=float(r.get("threshold") or 1.0),
                value=r.get("value", "N/A") or "N/A",
            )
        )
    return spark.createDataFrame(rows).dropDuplicates(["column", "rule"])


def summarize(df: DataFrame, rules: List[Dict], total_rows) -> DataFrame:
    """
    Summarizes data quality results based on provided rules and total rows.

    This function processes a DataFrame containing data quality statuses, applies
    rules to calculate violations, and generates a summary DataFrame with metrics
    such as pass rate, status, and other relevant information.

    Args:
        df (DataFrame): The input DataFrame containing a column `dq_status` with
            data quality statuses in the format "column:rule:value".
        rules (List[Dict]): A list of dictionaries representing the data quality
            rules. Each dictionary should define the `column`, `rule`, and optional
            `value` and `pass_threshold`.
        total_rows (int): The total number of rows in the input DataFrame.

    Returns:
        DataFrame: A summary DataFrame containing the following columns:
            - id: A unique identifier for each row.
            - timestamp: The timestamp when the summary was generated.
            - check: The type of check performed (e.g., "Quality Check").
            - level: The severity level of the check (e.g., "WARNING").
            - column: The column name associated with the rule.
            - rule: The rule applied to the column.
            - value: The value associated with the rule.
            - rows: The total number of rows in the input DataFrame.
            - violations: The number of rows that violated the rule.
            - pass_rate: The percentage of rows that passed the rule.
            - pass_threshold: The threshold for passing the rule.
            - status: The overall status of the rule (e.g., "PASS" or "FAIL").
    """
    now_ts = current_timestamp()

    viol_df = (
        df.filter(trim(col("dq_status")) != lit(""))
        .withColumn("dq_status", split(trim(col("dq_status")), ":"))
        .withColumn("column", col("dq_status")[0])
        .withColumn("rule", col("dq_status")[1])
        .withColumn("value", col("dq_status")[2])
        .groupBy("column", "rule", "value")
        .agg(count("*").alias("violations"))
        .withColumn(
            "value",
            coalesce(
                when(col("value") == "", None).otherwise(col("value")), lit("N/A")
            ),
        )
    )

    rules_df = __rules_to_df(rules).withColumn(
        "value", coalesce(col("value"), lit("N/A"))
    )

    base = (
        broadcast(rules_df)
        .join(viol_df, ["column", "rule", "value"], how="left")
        .withColumn("violations", coalesce(col("violations"), lit(0)))
    )

    summary = (
        base.withColumn("rows", lit(total_rows))
        .withColumn(
            "pass_rate", (lit(total_rows) - col("violations")) / lit(total_rows)
        )
        .withColumn(
            "status",
            when(col("pass_rate") >= col("pass_threshold"), "PASS").otherwise("FAIL"),
        )
        .withColumn("timestamp", now_ts)
        .withColumn("check", lit("Quality Check"))
        .withColumn("level", lit("WARNING"))
    )

    summary = summary.withColumn("id", expr("uuid()"))
    summary = summary.select(
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
    )

    return summary


def __pyspark_schema_to_list(df: DataFrame) -> List[Dict[str, Any]]:
    """
    Convert the schema of a PySpark DataFrame into a list of dictionaries.

    Each dictionary in the output list represents a field in the DataFrame schema
    and contains the following keys:
        - "field": The name of the field.
        - "data_type": The data type of the field as a lowercase string.
        - "nullable": A boolean indicating whether the field allows null values.
        - "max_length": Always set to None (reserved for future use).

    Args:
        df (DataFrame): The PySpark DataFrame whose schema is to be converted.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries representing the schema of the DataFrame.
    """
    out: List[Dict[str, Any]] = []
    for f in df.schema.fields:
        out.append(
            {
                "field": f.name,
                "data_type": f.dataType.simpleString().lower(),
                "nullable": f.nullable,
                "max_length": None,
            }
        )
    return out


def validate_schema(df: DataFrame, expected) -> Tuple[bool, List[Tuple[str, str]]]:
    """
    Validates the schema of a PySpark DataFrame against an expected schema.

    Args:
        df (DataFrame): The PySpark DataFrame whose schema is to be validated.
        expected (list): The expected schema represented as a list of tuples,
                         where each tuple contains the column name and its data type
                         and a boolean, if the column is nullable or not.

    Returns:
        Tuple[bool, List[Tuple[str, str]]]: A tuple containing:
            - A boolean indicating whether the schema matches the expected schema.
            - A list of tuples representing the mismatched columns, where each tuple
              contains the column name and the reason for the mismatch.
    """
    actual = __pyspark_schema_to_list(df)
    result, errors = __compare_schemas(actual, expected)
    return result, errors
