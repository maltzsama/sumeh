#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import uuid
import warnings
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional

import numpy as np
from pyspark.sql import DataFrame, SparkSession, Row, Window
from pyspark.sql.functions import (
    lit,
    col,
    collect_list,
    concat_ws,
    count,
    coalesce,
    stddev,
    avg,
    sum as spark_sum,
    countDistinct,
    current_date,
    trim,
    split,
    expr,
    date_sub,
    dayofweek,
    log2,
)

from sumeh.core.rules.rule_model import RuleDef
from sumeh.core.utils import (
    __convert_value,
    __compare_schemas,
    __transform_date_format_in_pattern,
)


# ========== ROW-LEVEL VALIDATION FUNCTIONS ==========


def is_positive(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the specified field is negative and adds a data quality status column.

    Args:
        df (DataFrame): The input PySpark DataFrame to be filtered.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(col(rule.field) < 0)
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def is_negative(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the specified field is non-negative and adds a data quality status column.

    Args:
        df (DataFrame): The input PySpark DataFrame to be filtered.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(col(rule.field) >= 0)
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def is_complete(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the specified field is null and adds a data quality status column.

    Args:
        df (DataFrame): The input PySpark DataFrame to be checked.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(col(rule.field).isNull())
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def is_unique(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Identifies duplicate rows based on the specified field and adds a data quality status column.

    Args:
        df (DataFrame): The input PySpark DataFrame to check for uniqueness.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        DataFrame: DataFrame containing rows where the field is not unique with dq_status column.
    """
    window = Window.partitionBy(col(rule.field))
    df_with_count = df.withColumn("_count", count(col(rule.field)).over(window))
    viol = df_with_count.filter(col("_count") > 1).drop("_count")
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def are_complete(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where any of the specified fields are null and adds a data quality status column.

    Args:
        df (DataFrame): The input PySpark DataFrame to be checked.
        rule (RuleDef): Rule definition containing fields (list), check_type, and value.

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    fields = rule.field if isinstance(rule.field, list) else [rule.field]

    # Create condition: at least one field is null
    null_conditions = [col(field).isNull() for field in fields]
    combined_condition = null_conditions[0]
    for cond in null_conditions[1:]:
        combined_condition = combined_condition | cond

    viol = df.filter(combined_condition)
    viol = viol.withColumn("dq_status", lit(f"{fields}:{rule.check_type}:{rule.value}"))
    return viol


def are_unique(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Identifies duplicate rows based on a combination of specified fields and adds a data quality status column.

    Args:
        df (DataFrame): The input PySpark DataFrame to check.
        rule (RuleDef): Rule definition containing fields (list), check_type, and value.

    Returns:
        DataFrame: DataFrame containing rows where the field combination is not unique with dq_status column.
    """
    fields = rule.field if isinstance(rule.field, list) else [rule.field]

    combined_col = concat_ws("|", *[coalesce(col(f), lit("")) for f in fields])
    window = Window.partitionBy(combined_col)

    viol = (
        df.withColumn("_count", count("*").over(window))
        .filter(col("_count") > 1)
        .drop("_count")
    )
    viol = viol.withColumn("dq_status", lit(f"{fields}:{rule.check_type}:{rule.value}"))
    return viol


def is_greater_than(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the specified field is less than or equal to the given value.

    Args:
        df (DataFrame): The input PySpark DataFrame to filter.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(col(rule.field) <= rule.value)
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def is_greater_or_equal_than(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the specified field is less than the given value.

    Args:
        df (DataFrame): The input PySpark DataFrame to filter.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(col(rule.field) < rule.value)
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def is_less_than(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the specified field is greater than or equal to the given value.

    Args:
        df (DataFrame): The input PySpark DataFrame to filter.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(col(rule.field) >= rule.value)
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def is_less_or_equal_than(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the specified field is greater than the given value.

    Args:
        df (DataFrame): The input PySpark DataFrame to filter.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(col(rule.field) > rule.value)
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def is_equal(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the specified field is not equal to the given value.

    Args:
        df (DataFrame): The input PySpark DataFrame to be filtered.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(~col(rule.field).eqNullSafe(rule.value))
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def is_equal_than(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Alias for is_equal.

    Args:
        df (DataFrame): The input PySpark DataFrame to be filtered.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    return is_equal(df, rule)


def is_contained_in(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the specified field is not in the given list of values.

    Args:
        df (DataFrame): The input PySpark DataFrame to filter.
        rule (RuleDef): Rule definition containing field, check_type, and value (list format).

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    vals = re.findall(r"'([^']*)'", str(rule.value)) or [
        v.strip() for v in str(rule.value).strip("[]").split(",")
    ]
    viol = df.filter(~col(rule.field).isin(vals))
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def is_in(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Alias for is_contained_in.

    Args:
        df (DataFrame): The input DataFrame to evaluate.
        rule (RuleDef): Rule definition.

    Returns:
        DataFrame: Filtered DataFrame with violations.
    """
    return is_contained_in(df, rule)


def not_contained_in(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the specified field is in the given list of values.

    Args:
        df (DataFrame): The input PySpark DataFrame to filter.
        rule (RuleDef): Rule definition containing field, check_type, and value (list format).

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    vals = re.findall(r"'([^']*)'", str(rule.value)) or [
        v.strip() for v in str(rule.value).strip("[]").split(",")
    ]
    viol = df.filter(col(rule.field).isin(vals))
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def not_in(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Alias for not_contained_in.

    Args:
        df (DataFrame): The input DataFrame to be filtered.
        rule (RuleDef): Rule definition.

    Returns:
        DataFrame: Filtered DataFrame with violations.
    """
    return not_contained_in(df, rule)


def is_between(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the specified field is not within the given range.

    Args:
        df (DataFrame): The input PySpark DataFrame.
        rule (RuleDef): Rule definition containing field, check_type, and value (range format).

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    lo, hi = [__convert_value(x) for x in str(rule.value).strip("[]").split(",")]
    viol = df.filter(~col(rule.field).between(lo, hi))
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def has_pattern(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the specified field does not match the given regex pattern.

    Args:
        df (DataFrame): The input PySpark DataFrame to be filtered.
        rule (RuleDef): Rule definition containing field, check_type, and value (regex pattern).

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(~col(rule.field).rlike(rule.value))
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def is_legit(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the specified field is null or does not match a non-whitespace pattern.

    Args:
        df (DataFrame): The input PySpark DataFrame to be validated.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    pattern_legit = r"^\S+$"
    viol = df.filter(
        ~(col(rule.field).isNotNull() & col(rule.field).rlike(pattern_legit))
    )
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def has_max(df: DataFrame, rule: RuleDef) -> dict:
    """
    Checks if the maximum value of the specified field meets expectations.

    Args:
        df (DataFrame): The input PySpark DataFrame to be checked.
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
        from pyspark.sql.functions import max as spark_max

        actual_val = df.select(spark_max(col(field))).first()[0]
        actual = float(actual_val) if actual_val is not None else 0.0
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


def has_min(df: DataFrame, rule: RuleDef) -> dict:
    """
    Checks if the minimum value of the specified field meets expectations.

    Args:
        df (DataFrame): The input PySpark DataFrame to be checked.
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
        from pyspark.sql.functions import min as spark_min

        actual_val = df.select(spark_min(col(field))).first()[0]
        actual = float(actual_val) if actual_val is not None else 0.0
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


# ========== TABLE-LEVEL VALIDATION FUNCTIONS ==========


def has_std(df: DataFrame, rule: RuleDef) -> dict:
    """
    Checks if the standard deviation of the specified field meets expectations.

    Args:
        df (DataFrame): The input PySpark DataFrame.
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
        actual_val = df.select(stddev(col(field))).first()[0]
        actual = float(actual_val) if actual_val is not None else 0.0
        expected = float(expected)  # Garantir que é float

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
            "expected": float(expected),  # Sempre float
            "actual": float(actual),  # Sempre float
            "message": None if passed else msg,
        }
    except Exception as e:
        return {
            "status": "ERROR",
            "expected": float(expected) if expected is not None else None,
            "actual": None,
            "message": f"Error: {str(e)}",
        }


def has_mean(df: DataFrame, rule: RuleDef) -> dict:
    """
    Checks if the mean of the specified field meets expectations.

    Args:
        df (DataFrame): The input PySpark DataFrame.
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
        actual_val = df.select(avg(col(field))).first()[0]
        actual = float(actual_val) if actual_val is not None else 0.0
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


def has_sum(df: DataFrame, rule: RuleDef) -> dict:
    """
    Checks if the sum of the specified field meets expectations.

    Args:
        df (DataFrame): The input PySpark DataFrame.
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
        actual_val = df.select(spark_sum(col(field))).first()[0]
        actual = float(actual_val) if actual_val is not None else 0.0
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


def has_cardinality(df: DataFrame, rule: RuleDef) -> dict:
    """
    Checks if the cardinality (distinct count) of the specified field meets expectations.

    Args:
        df (DataFrame): The input PySpark DataFrame to be checked.
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
        actual = int(df.select(countDistinct(col(field))).first()[0] or 0)
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
            "expected": float(expected),  # Converter para float para consistência
            "actual": float(actual),  # Converter para float para consistência
            "message": None if passed else msg,
        }
    except Exception as e:
        return {
            "status": "ERROR",
            "expected": float(expected) if expected is not None else None,
            "actual": None,
            "message": f"Error: {str(e)}",
        }


def has_infogain(df: DataFrame, rule: RuleDef) -> dict:
    """
    Checks if the information gain of the specified field meets expectations.

    Args:
        df (DataFrame): The input PySpark DataFrame to evaluate.
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
        value_counts_df = df.groupBy(field).count()
        total_count = df.count()

        # Calculate entropy
        value_counts_df = value_counts_df.withColumn(
            "probability", col("count") / lit(float(total_count))
        )

        # Shannon entropy: -Σ(p * log2(p))
        entropy_df = value_counts_df.withColumn(
            "entropy_term", -col("probability") * log2(col("probability"))
        )
        entropy = float(entropy_df.select(spark_sum("entropy_term")).first()[0] or 0.0)

        # Max entropy (uniform distribution)
        n_unique = value_counts_df.count()
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


def has_entropy(df: DataFrame, rule: RuleDef) -> dict:
    """
    Checks if the entropy of the specified field meets expectations.

    Args:
        df (DataFrame): The input PySpark DataFrame to evaluate.
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
        value_counts_df = df.groupBy(field).count()
        total_count = df.count()

        # Calculate probabilities
        value_counts_df = value_counts_df.withColumn(
            "probability", col("count") / lit(float(total_count))
        )

        # Shannon entropy: -Σ(p * log2(p))
        entropy_df = value_counts_df.withColumn(
            "entropy_term", -col("probability") * log2(col("probability"))
        )
        entropy = float(entropy_df.select(spark_sum("entropy_term")).first()[0] or 0.0)
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


def validate_date_format(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the specified field has wrong date format based on the format from the rule.

    Args:
        df (DataFrame): The input PySpark DataFrame to be checked.
        rule (RuleDef): Rule definition containing field, check_type, and value (date format).

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    fmt = rule.value
    pattern = __transform_date_format_in_pattern(fmt)
    viol = df.filter(~col(rule.field).rlike(pattern) | col(rule.field).isNull())
    viol = viol.withColumn("dq_status", lit(f"{rule.field}:{rule.check_type}:{fmt}"))
    return viol


def is_future_date(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the specified field has a date greater than the current date.

    Args:
        df (DataFrame): The input PySpark DataFrame to be checked.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(col(rule.field) > current_date())
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def is_past_date(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the specified field has a date lower than the current date.

    Args:
        df (DataFrame): The input PySpark DataFrame to be checked.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(col(rule.field) < current_date())
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def is_date_between(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where date field is not between two dates in format: "[<initial_date>, <final_date>]".

    Args:
        df (DataFrame): The input PySpark DataFrame to be checked.
        rule (RuleDef): Rule definition containing field, check_type, and value (date range).

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    start_date, end_date = [s.strip() for s in rule.value.strip("[]").split(",")]
    viol = df.filter(~col(rule.field).between(start_date, end_date))
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def is_date_after(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the specified field has a date lower than the date informed in the rule.

    Args:
        df (DataFrame): The input PySpark DataFrame to be checked.
        rule (RuleDef): Rule definition containing field, check_type, and value (target date).

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(col(rule.field) < rule.value)
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def is_date_before(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the specified field has a date greater than the date informed in the rule.

    Args:
        df (DataFrame): The input PySpark DataFrame to be checked.
        rule (RuleDef): Rule definition containing field, check_type, and value (target date).

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(col(rule.field) > rule.value)
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def all_date_checks(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Default date check - filters past dates.

    Args:
        df (DataFrame): The input PySpark DataFrame to be filtered.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    return is_past_date(df, rule)


def is_in_millions(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the field value is less than 1,000,000.

    Args:
        df (DataFrame): The input DataFrame to filter and modify.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(col(rule.field) < lit(1_000_000))
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def is_in_billions(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the field value is less than 1,000,000,000.

    Args:
        df (DataFrame): The input PySpark DataFrame to filter.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(col(rule.field) < lit(1_000_000_000))
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def is_today(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the date field does not equal today.

    Args:
        df (DataFrame): The input DataFrame to filter.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(col(rule.field) != current_date())
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def is_yesterday(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Alias for is_t_minus_1. Filters rows where date field does not equal yesterday.

    Args:
        df (DataFrame): The input PySpark DataFrame.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    target = date_sub(current_date(), 1)
    viol = df.filter(col(rule.field) != target)
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def is_t_minus_1(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Alias for is_t_minus_1. Filters rows where date field does not equal yesterday.

    Args:
        df (DataFrame): The input PySpark DataFrame.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    target = date_sub(current_date(), 1)
    viol = df.filter(col(rule.field) != target)
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def is_t_minus_2(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the date field does not equal two days ago (T-2).

    Args:
        df (DataFrame): The input DataFrame to be filtered.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    target = date_sub(current_date(), 2)
    viol = df.filter(col(rule.field) != target)
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def is_t_minus_3(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the date field does not equal three days ago (T-3).

    Args:
        df (DataFrame): The input DataFrame to be filtered.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    target = date_sub(current_date(), 3)
    viol = df.filter(col(rule.field) != target)
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def is_on_weekday(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the date field does not fall on a weekday (Mon-Fri).

    Args:
        df (DataFrame): The input PySpark DataFrame.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    # dayofweek: 1 = Sunday, 2 = Monday, ..., 7 = Saturday
    viol = df.filter(
        (dayofweek(col(rule.field)) == 1) | (dayofweek(col(rule.field)) == 7)
    )
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def is_on_weekend(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the date field does not fall on a weekend (Sat-Sun).

    Args:
        df (DataFrame): The input PySpark DataFrame.
        rule (RuleDef): Rule definition containing field, check_type, and value.

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    viol = df.filter(
        (dayofweek(col(rule.field)) != 1) & (dayofweek(col(rule.field)) != 7)
    )
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def _day_of_week(df: DataFrame, rule: RuleDef, dow: int) -> DataFrame:
    """
    Helper function to filter rows based on day of week.

    Args:
        df (DataFrame): The input DataFrame.
        rule (RuleDef): Rule definition.
        dow (int): Day of week (1=Sunday, 2=Monday, ..., 7=Saturday in PySpark).

    Returns:
        DataFrame: Filtered DataFrame with violations.
    """
    viol = df.filter(dayofweek(col(rule.field)) != dow)
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


def is_on_monday(df: DataFrame, rule: RuleDef) -> DataFrame:
    """Filters rows where date is not Monday."""
    return _day_of_week(df, rule, 2)  # 2 = Monday in PySpark


def is_on_tuesday(df: DataFrame, rule: RuleDef) -> DataFrame:
    """Filters rows where date is not Tuesday."""
    return _day_of_week(df, rule, 3)


def is_on_wednesday(df: DataFrame, rule: RuleDef) -> DataFrame:
    """Filters rows where date is not Wednesday."""
    return _day_of_week(df, rule, 4)


def is_on_thursday(df: DataFrame, rule: RuleDef) -> DataFrame:
    """Filters rows where date is not Thursday."""
    return _day_of_week(df, rule, 5)


def is_on_friday(df: DataFrame, rule: RuleDef) -> DataFrame:
    """Filters rows where date is not Friday."""
    return _day_of_week(df, rule, 6)


def is_on_saturday(df: DataFrame, rule: RuleDef) -> DataFrame:
    """Filters rows where date is not Saturday."""
    return _day_of_week(df, rule, 7)


def is_on_sunday(df: DataFrame, rule: RuleDef) -> DataFrame:
    """Filters rows where date is not Sunday."""
    return _day_of_week(df, rule, 1)


def satisfies(df: DataFrame, rule: RuleDef) -> DataFrame:
    """
    Filters rows where the specified expression is not satisfied.

    Args:
        df (DataFrame): The input PySpark DataFrame to be filtered.
        rule (RuleDef): Rule definition with value containing PySpark SQL expression.

    Returns:
        DataFrame: Filtered DataFrame with violations and dq_status column.
    """
    expression = expr(rule.value)
    viol = df.filter(~expression)
    viol = viol.withColumn(
        "dq_status", lit(f"{rule.field}:{rule.check_type}:{rule.value}")
    )
    return viol


# ========== VALIDATION ORCHESTRATION ==========


def validate_row_level(
    spark: SparkSession, df: DataFrame, rules: List[RuleDef]
) -> Tuple[DataFrame, DataFrame]:
    """
    Validates DataFrame at row level using specified rules.

    Args:
        spark (SparkSession): Active SparkSession instance.
        df (DataFrame): Input PySpark DataFrame to validate.
        rules (List[RuleDef]): List of row-level validation rules.

    Returns:
        Tuple[DataFrame, DataFrame]:
            - DataFrame with violations and dq_status column
            - Raw violations DataFrame
    """
    engine = "pyspark"
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
        return df.limit(0), df.limit(0)

    # Add _id column
    from pyspark.sql.functions import monotonically_increasing_id

    df = df.withColumn("_id", monotonically_increasing_id())

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
        return df.limit(0), df.limit(0)

    # Union all violations
    raw = raw_list[0]
    for viol_df in raw_list[1:]:
        raw = raw.unionByName(viol_df)

    if raw.count() == 0:
        return df.limit(0), df.limit(0)

    # Group by _id to aggregate violations
    summary = raw.groupBy("_id").agg(
        concat_ws(";", collect_list("dq_status")).alias("dq_status")
    )

    # Join back to original df
    out = df.join(summary, on="_id", how="inner").drop("_id")

    return out, raw


def validate_table_level(
    spark: SparkSession, df: DataFrame, rules: List[RuleDef]
) -> DataFrame:
    """
    Validates DataFrame at table level using specified rules.

    Args:
        spark (SparkSession): Active SparkSession instance.
        df (DataFrame): Input PySpark DataFrame to validate.
        rules (List[RuleDef]): List of table-level validation rules.

    Returns:
        DataFrame: Summary DataFrame with validation results.
    """
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        DoubleType,
        TimestampType,
    )

    engine = "pyspark"
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

    # Define schema
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("level", StringType(), True),
            StructField("category", StringType(), True),
            StructField("check_type", StringType(), True),
            StructField("field", StringType(), True),
            StructField("status", StringType(), True),
            StructField("expected", DoubleType(), True),
            StructField("actual", DoubleType(), True),
            StructField("message", StringType(), True),
        ]
    )

    if not rules_valid:
        warnings.warn(
            f"No valid rules to execute for level='table_level' and engine='{engine}'."
        )
        return spark.createDataFrame([], schema)

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

            # Garantir que expected e actual são float ou None
            expected_val = result.get("expected")
            actual_val = result.get("actual")

            results.append(
                {
                    "id": str(uuid.uuid4()),
                    "timestamp": execution_time,
                    "level": "TABLE",
                    "category": rule.category or "unknown",
                    "check_type": check_type,
                    "field": str(rule.field),
                    "status": result.get("status", "ERROR"),
                    "expected": (
                        float(expected_val) if expected_val is not None else None
                    ),
                    "actual": float(actual_val) if actual_val is not None else None,
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
        return spark.createDataFrame([], schema)

    # Criar DataFrame com schema explícito
    summary_df = spark.createDataFrame([Row(**r) for r in results], schema)

    # Sort: FAIL first, then ERROR, then PASS
    from pyspark.sql.functions import when as spark_when

    summary_df = summary_df.withColumn(
        "_sort",
        spark_when(col("status") == "FAIL", 0)
        .when(col("status") == "ERROR", 1)
        .otherwise(2),
    )
    summary_df = summary_df.orderBy("_sort").drop("_sort")

    return summary_df


def validate(
    spark: SparkSession, df: DataFrame, rules: List[RuleDef]
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Main validation function that orchestrates row-level and table-level validations.

    Args:
        spark (SparkSession): Active SparkSession instance.
        df (DataFrame): Input PySpark DataFrame to validate.
        rules (List[RuleDef]): List of all validation rules.

    Returns:
        Tuple[DataFrame, DataFrame, DataFrame]:
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
        df_with_status, row_violations = validate_row_level(spark, df, row_rules)
    else:
        df_with_status = df
        row_violations = df.limit(0)

    if table_rules:
        table_summary = validate_table_level(spark, df, table_rules)
    else:
        from pyspark.sql.types import (
            StructType,
            StructField,
            StringType,
            DoubleType,
            TimestampType,
        )

        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("level", StringType(), True),
                StructField("category", StringType(), True),
                StructField("check_type", StringType(), True),
                StructField("field", StringType(), True),
                StructField("status", StringType(), True),
                StructField("expected", DoubleType(), True),
                StructField("actual", DoubleType(), True),
                StructField("message", StringType(), True),
            ]
        )
        table_summary = spark.createDataFrame([], schema)

    return df_with_status, row_violations, table_summary


def summarize(
    spark: SparkSession,
    rules: List[RuleDef],
    total_rows: int,
    df_with_errors: Optional[DataFrame] = None,
    table_error: Optional[DataFrame] = None,
) -> DataFrame:
    """
    Summarizes validation results from both row-level and table-level checks.

    Args:
        spark (SparkSession): Active SparkSession instance.
        rules (List[RuleDef]): List of all validation rules.
        total_rows (int): Total number of rows in the input DataFrame.
        df_with_errors (Optional[DataFrame]): DataFrame with row-level violations.
        table_error (Optional[DataFrame]): DataFrame with table-level results.

    Returns:
        DataFrame: Summary DataFrame with aggregated validation metrics.
    """
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        DoubleType,
        IntegerType,
        TimestampType,
    )

    summaries = []

    # ========== ROW-LEVEL SUMMARY ==========
    row_rules = [
        r for r in rules if r.level and r.level.upper().replace("_LEVEL", "") == "ROW"
    ]

    if row_rules and df_with_errors is not None:
        # Parse violations from dq_status
        if "dq_status" in df_with_errors.columns:
            viol_df = (
                df_with_errors.filter(trim(col("dq_status")) != "")
                .withColumn("dq_parts", split(col("dq_status"), ";"))
                .select(expr("explode(dq_parts) as dq_item"))
                .withColumn("parts", split(col("dq_item"), ":"))
                .withColumn("check_type", col("parts")[0])
                .withColumn("field", col("parts")[1])
                .groupBy("check_type", "field")
                .agg(count("*").alias("violations"))
            )

            viol_dict = {
                (row["check_type"], row["field"]): row["violations"]
                for row in viol_df.collect()
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
                # For lists or other types, leave as None

            # IMPORTANTE: ordem dos campos deve corresponder ao schema
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
        for row in table_error.collect():
            expected = row["expected"]
            actual = row["actual"]

            # Calculate compliance rate if possible
            if expected is not None and actual is not None and expected != 0:
                compliance_rate = round(actual / expected, 4)
            else:
                compliance_rate = None

            # IMPORTANTE: ordem dos campos deve corresponder ao schema
            summaries.append(
                {
                    "id": row["id"],
                    "timestamp": row["timestamp"],
                    "level": row["level"],
                    "category": row["category"],
                    "check_type": row["check_type"],
                    "field": row["field"],
                    "rows": None,  # table-level não tem rows
                    "violations": None,  # table-level não tem violations
                    "pass_rate": compliance_rate,
                    "pass_threshold": None,  # table-level não tem pass_threshold
                    "status": row["status"],
                    "expected": expected,
                    "actual": actual,
                    "message": row["message"],
                }
            )

    # Define schema (ordem importa!)
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("level", StringType(), True),
            StructField("category", StringType(), True),
            StructField("check_type", StringType(), True),
            StructField("field", StringType(), True),
            StructField("rows", IntegerType(), True),
            StructField("violations", IntegerType(), True),
            StructField("pass_rate", DoubleType(), True),
            StructField("pass_threshold", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("expected", DoubleType(), True),
            StructField("actual", DoubleType(), True),
            StructField("message", StringType(), True),
        ]
    )

    if not summaries:
        return spark.createDataFrame([], schema)

    # Criar Row objects garantindo ordem correta
    rows_data = []
    for s in summaries:
        rows_data.append(
            Row(
                id=s["id"],
                timestamp=s["timestamp"],
                level=s["level"],
                category=s["category"],
                check_type=s["check_type"],
                field=s["field"],
                rows=s["rows"],
                violations=s["violations"],
                pass_rate=s["pass_rate"],
                pass_threshold=s["pass_threshold"],
                status=s["status"],
                expected=s["expected"],
                actual=s["actual"],
                message=s["message"],
            )
        )

    summary_df = spark.createDataFrame(rows_data, schema)

    # Sort: FAIL first, ERROR second, PASS last; ROW before TABLE
    from pyspark.sql.functions import when as spark_when

    summary_df = summary_df.withColumn(
        "_sort_status",
        spark_when(col("status") == "FAIL", 0)
        .when(col("status") == "ERROR", 1)
        .otherwise(2),
    )
    summary_df = summary_df.withColumn(
        "_sort_level", spark_when(col("level") == "ROW", 0).otherwise(1)
    )

    summary_df = summary_df.orderBy("_sort_status", "_sort_level", "check_type").drop(
        "_sort_status", "_sort_level"
    )

    return summary_df


# ========== SCHEMA VALIDATION ==========


def extract_schema(df: DataFrame) -> List[Dict[str, Any]]:
    """
    Extracts schema from PySpark DataFrame.

    Args:
        df (DataFrame): Input PySpark DataFrame.

    Returns:
        List[Dict[str, Any]]: List of dictionaries containing field information.
    """
    schema = []
    for field in df.schema.fields:
        dtype_str = str(field.dataType)
        schema.append(
            {
                "field": field.name,
                "data_type": dtype_str,
                "nullable": field.nullable,
                "max_length": None,
            }
        )
    return schema


def validate_schema(df: DataFrame, expected) -> Tuple[bool, List[Dict[str, Any]]]:
    """
    Validates the schema of a PySpark DataFrame against an expected schema.

    Args:
        df (DataFrame): The PySpark DataFrame whose schema is to be validated.
        expected (list): The expected schema.

    Returns:
        Tuple[bool, List[Dict[str, Any]]]:
            - Boolean indicating whether the schema matches
            - List of schema errors/mismatches
    """
    actual = extract_schema(df)
    result, errors = __compare_schemas(actual, expected)
    return result, errors
