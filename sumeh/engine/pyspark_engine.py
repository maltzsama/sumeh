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


def is_complete(df: DataFrame, field: str) -> DataFrame:
    return df.filter(col(field).isNull()).withColumn(
        "dq_status", concat(lit(field), lit(":is_complete"))
    )


def is_unique(df: DataFrame, field: str) -> DataFrame:
    window = Window.partitionBy(col(field))
    df_with_count = df.withColumn("count", count(col(field)).over(window))
    res = (
        df_with_count.filter(col("count") > 1)
        .withColumn("dq_status", concat(lit(field), lit(":is_unique")))
        .drop("count")
    )
    return res


def are_complete(df: DataFrame, fields: list[str]) -> DataFrame:
    predicate = reduce(operator.and_, [col(field).isNotNull() for field in fields])
    return df.filter(~predicate).withColumn(
        "dq_status", concat(lit(str(fields)), lit(":are_complete"))
    )


def are_unique(df: DataFrame, fields: list[str]) -> DataFrame:
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


def is_greater_than(df: DataFrame, field: str, value: float) -> DataFrame:
    return df.filter(col(field) <= value).withColumn(
        "dq_status", concat(lit(field), lit(":is_greater_than"))
    )


def is_positive(df: DataFrame, field: str) -> DataFrame:
    return df.filter(col(field) <= 0).withColumn(
        "dq_status", concat(lit(field), lit(":is_positive"))
    )


def is_negative(df: DataFrame, field: str) -> DataFrame:
    return df.filter(col(field) >= 0).withColumn(
        "dq_status", concat(lit(field), lit(":is_negative"))
    )


def quality_checker(df: DataFrame, rules: list[dict]) -> DataFrame:
    df = df.withColumn("dq_status", lit(""))
    result = df.limit(0)
    for rule in rules:
        rule_name = rule["check_type"]
        field = rule["field"]
        match rule_name:
            case "is_complete":
                result = result.union(
                    is_complete(df=df, field=field)
                )
            case "is_unique" | "is_primary_key":
                result = result.union(
                    is_unique(df=df, field=field)
                )
            case "are_complete":
                result = result.union(
                    are_complete(df=df, fields=field)
                )
            case "are_unique":
                result = result.union(
                    are_unique(df=df, fields=field)
                )
            case "is_greater_than":
                result = result.union(
                    is_greater_than(df=df, field=field, value=rule["value"])
                )
            case "is_positive":
                result = result.union(is_positive(df=df, field=field))
            case "is_negative":
                result = result.union(is_negative(df=df, field=field))
            case _:
                warnings.warn(f"Unknown rule name: {rule_name}, {field}")
    group_columns = [col for col in df.columns if col != "dq_status"]
    result = result.groupBy(*group_columns).agg(
        concat_ws(";", collect_list("dq_status")).alias("dq_status")
    )
    return result
