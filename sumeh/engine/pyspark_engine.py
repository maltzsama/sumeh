#!/usr/bin/env python
# -*- coding: utf-8 -*-

import warnings
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    lit,
    col,
    concat,
    when,
    count,
    collect_list,
    concat_ws,
    count,
    coalesce,
)
import operator
from functools import reduce
from pyspark.sql import Window

def is_positive (df: DataFrame, field: str, _) -> DataFrame:
    return (df.filter(col(field) < 0)
            .withColumn('dq_status',concat(lit(field), lit(':is_positive'))))

def is_negative(df: DataFrame, field: str, _) -> DataFrame:
    return (df.filter(col(field) >= 0)
            .withColumn('dq_status',concat(lit(field), lit(':is_negative'))))

def is_complete(df: DataFrame, field: str, _) -> DataFrame:
    return df.filter(col(field).isNull()).withColumn(
        "dq_status", concat(lit(field), lit(":is_complete"))
    )

def is_unique(df: DataFrame, field: str, _) -> DataFrame:
    window = Window.partitionBy(col(field))
    df_with_count = df.withColumn("count", count(col(field)).over(window))
    res = (
        df_with_count.filter(col("count") > 1)
        .withColumn("dq_status", concat(lit(field), lit(":is_unique")))
        .drop("count")
    )
    return res

def are_complete(df: DataFrame, fields: list[str], _) -> DataFrame:
    predicate = reduce(operator.and_, [col(field).isNotNull() for field in fields])
    return df.filter(~predicate).withColumn(
        "dq_status", concat(lit(str(fields)), lit(":are_complete"))
    )


def are_unique(df: DataFrame, fields: list[str], _) -> DataFrame:
    combined_col = concat_ws("|", *[coalesce(col(f), lit("")) for f in fields])
    window = Window.partitionBy(combined_col)
    result = (
        df.withColumn("_count", count("*").over(window))
        .filter(col("_count") > 1)
        .drop("_count")
        .withColumn(
            "dq_status",
            concat(lit(str(fields)), lit("are_unique")),
        )
    )
    return result

def is_greater_than(df: DataFrame, field: str, value: str) -> DataFrame:
    return (df.filter(col(field) > value)
            .withColumn('dq_status',concat(lit(field), lit(':is_greater_than'))))

def is_greater_or_equal_than(df: DataFrame, field: str, value: str) -> DataFrame:
    return (df.filter(col(field) < value)
            .withColumn('dq_status',concat(lit(field), lit(':is_greater_or_equal_than'))))

def is_less_than(df: DataFrame, field: str, value: str) -> DataFrame:
    return (df.filter(col(field) >= value)
            .withColumn('dq_status',concat(lit(field), lit(':is_less_than'))))

def is_less_or_equal_than(df: DataFrame, field: str, value: str) -> DataFrame:
    return (df.filter(col(field) > value)
            .withColumn('dq_status', concat(lit(field), lit(':is_less_or_equal_than'))))

def is_equal(df: DataFrame, field: str, value: str) -> DataFrame:
    return (df.filter(~col(field).eqNullSafe(value))
            .withColumn('dq_status', concat(lit(field), lit(':is_equal'))))

def is_contained_in(df: DataFrame, field: str, positive_list_str: str) -> DataFrame:
    positive_list = positive_list_str.replace('[', '').replace(']', '').split(',')
    return (df.filter(~col(field).isin(positive_list))
            .withColumn('dq_status', concat(lit(field), lit(':is_contained_in'))))

def not_contained_in(df: DataFrame, field: str, negative_list_str: str) -> DataFrame:
    negative_list = negative_list_str.replace('[', '').replace(']', '').split(',')
    return (df.filter(col(field).isin(negative_list))
            .withColumn('dq_status', concat(lit(field), lit(':not_contained_in'))))

def is_between(df: DataFrame, field: str, limits_list: str) -> DataFrame:
    min_value, max_value = limits_list.replace('[', '').replace(']', '').split(',')
    return (df.filter(~col(field).between(min_value, max_value))
            .withColumn('dq_status', concat(lit(field), lit(':is_between'))))

def has_pattern(df: DataFrame, field: str, pattern: str) -> DataFrame:
    return (df.filter(~col(field).rlike(pattern))
            .withColumn('dq_status', concat(lit(field), lit(':has_pattern'))))

def is_legit(df: DataFrame, field: str, _) -> DataFrame:
    pattern_legit = '\S*'
    return (df.filter(~(col(field).isNotNull() & col(field).rlike(pattern_legit)))
            .withColumn('dq_status', concat(lit(field), lit(':is_legit'))))

def is_primary_key(df: DataFrame, field: str, _):
    return is_unique(df, field, _)

def is_composite_key(df: DataFrame, fields: list[str], _):
    return are_unique(df, fields, _)

def quality_checker(df: DataFrame, rules: list[dict]) -> DataFrame:
    df = df.withColumn("dq_status", lit(""))
    result = df.limit(0)
    for rule in rules:
        rule_name = rule["check_type"]
        field = rule["field"]
        value = rule["value"]

        try:
            rule_func = globals()[rule_name]
            result = result.union(rule_func(df, field, value))
        except KeyError:
            warnings.warn(f"Unknown rule name: {rule_name}, {field}")

    group_columns = [col for col in df.columns if col != "dq_status"]
    summary_df = result.groupBy(*group_columns).agg(
        concat_ws(";", collect_list("dq_status")).alias("dq_status")
    )
    return summary_df