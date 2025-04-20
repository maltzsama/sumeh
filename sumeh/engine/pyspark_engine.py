#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
    explode,
    when,
    trim,
    split,
)
from typing import List, Dict
import operator
from functools import reduce

from sumeh.services.utils import __convert_value


def __extract_params(rule: dict) -> tuple:
    rule_name = rule["check_type"]
    field = rule["field"]
    raw_value = rule.get("value")
    if isinstance(raw_value, str) and raw_value not in (None, "", "NULL"):
        try:
            value = __convert_value(raw_value)
        except ValueError:
            value = raw_value
    else:
        value = raw_value
    value = value if value not in (None, "", "NULL") else ""
    return field, rule_name, value


def is_positive(df: DataFrame, rule: dict) -> DataFrame:
    field, check, value = __extract_params(rule)
    return df.filter(col(field) < 0).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_negative(df: DataFrame, rule: dict) -> DataFrame:
    field, check, value = __extract_params(rule)
    return df.filter(col(field) >= 0).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_complete(df: DataFrame, rule: dict) -> DataFrame:
    field, check, value = __extract_params(rule)
    return df.filter(col(field).isNull()).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_unique(df: DataFrame, rule: dict) -> DataFrame:
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
    fields, check, value = __extract_params(rule)
    predicate = reduce(operator.and_, [col(field).isNotNull() for field in fields])
    return df.filter(~predicate).withColumn(
        "dq_status", concat(lit(str(fields)), lit(":"), lit(check), lit(":"), lit(value))
    )


def are_unique(df: DataFrame, rule: dict) -> DataFrame:
    fields, check, value = __extract_params(rule)
    combined_col = concat_ws("|", *[coalesce(col(f), lit("")) for f in fields])
    window = Window.partitionBy(combined_col)
    result = (
        df.withColumn("_count", count("*").over(window))
        .filter(col("_count") > 1)
        .drop("_count")
        .withColumn(
            "dq_status", concat(lit(str(fields)), lit(":"), lit(check), lit(":"), lit(value))
        )
    )
    return result


def is_greater_than(df: DataFrame, rule: dict) -> DataFrame:
    field, check, value = __extract_params(rule)
    return df.filter(col(field) > value).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_greater_or_equal_than(df: DataFrame, rule: dict) -> DataFrame:
    field, check, value = __extract_params(rule)
    return df.filter(col(field) < value).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_less_than(df: DataFrame, rule: dict) -> DataFrame:
    field, check, value = __extract_params(rule)
    return df.filter(col(field) >= value).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_less_or_equal_than(df: DataFrame, rule: dict) -> DataFrame:
    field, check, value = __extract_params(rule)
    return df.filter(col(field) > value).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_equal(df: DataFrame, rule: dict) -> DataFrame:
    field, check, value = __extract_params(rule)
    return df.filter(~col(field).eqNullSafe(value)).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_equal_than(df: DataFrame, rule: dict) -> DataFrame:
    field, check, value = __extract_params(rule)
    return df.filter(~col(field).eqNullSafe(value)).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_contained_in(df: DataFrame, rule: dict) -> DataFrame:
    field, check, value = __extract_params(rule)
    positive_list = value.strip("[]").split(",")
    return df.filter(~col(field).isin(positive_list)).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def not_contained_in(df: DataFrame, rule: dict) -> DataFrame:
    field, check, value = __extract_params(rule)
    negative_list = value.strip("[]").split(",")
    return df.filter(col(field).isin(negative_list)).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_between(df: DataFrame, rule: dict) -> DataFrame:
    field, check, value = __extract_params(rule)
    min_value, max_value = value.strip("[]").split(",")
    return df.filter(~col(field).between(min_value, max_value)).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def has_pattern(df: DataFrame, rule: dict) -> DataFrame:
    field, check, value = __extract_params(rule)
    return df.filter(~col(field).rlike(value)).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_legit(df: DataFrame, rule: dict) -> DataFrame:
    field, check, value = __extract_params(rule)
    pattern_legit = "\S*"
    return df.filter(
        ~(col(field).isNotNull() & col(field).rlike(pattern_legit))
    ).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def is_primary_key(df: DataFrame, rule: dict):

    return is_unique(df, rule)


def is_composite_key(df: DataFrame, rule: dict):
    return are_unique(df, rule)


def has_max(df: DataFrame, rule: dict) -> DataFrame:
    field, check, value = __extract_params(rule)
    return df.filter(col(field) > value).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def has_min(df: DataFrame, rule: dict) -> DataFrame:
    field, check, value = __extract_params(rule)
    return df.filter(col(field) < value).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def has_std(df: DataFrame, rule: dict) -> DataFrame:
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
    field, check, value = __extract_params(rule)
    mean_val = (df.select(avg(col(field))).first()[0]) or 0.0
    if mean_val > value:  # regra falhou
        return df.withColumn(
            "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
        )
    else:  # passou
        return df.limit(0)


def has_sum(df: DataFrame, rule: dict) -> DataFrame:
    field, check, value = __extract_params(rule)
    sum_val = (df.select(sum(col(field))).first()[0]) or 0.0
    if sum_val > value:
        return df.withColumn(
            "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
        )
    return df.limit(0)


def has_cardinality(df: DataFrame, rule: dict) -> DataFrame:
    field, check, value = __extract_params(rule)
    card_val = df.select(countDistinct(col(field))).first()[0] or 0
    if card_val > value:
        return df.withColumn(
            "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
        )
    return df.limit(0)


def has_infogain(df: DataFrame, rule: dict) -> DataFrame:
    field, check, value = __extract_params(rule)
    info_gain = df.select(countDistinct(col(field))).first()[0] or 0.0
    if info_gain > value:
        return df.withColumn(
            "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
        )
    return df.limit(0)


def has_entropy(df: DataFrame, rule: dict) -> DataFrame:
    field, check, value = __extract_params(rule)
    entropy_val = df.select(countDistinct(col(field))).first()[0] or 0.0
    if entropy_val > value:
        return df.withColumn(
            "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
        )
    return df.limit(0)


def all_date_checks(df: DataFrame, rule: dict) -> DataFrame:
    field, check, value = __extract_params(rule)
    return df.filter((col(field) < current_date())).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def satisfies(df: DataFrame, rule: dict) -> DataFrame:
    field, check, value = __extract_params(rule)
    return df.filter(col(field).rlike(expression)).withColumn(
        "dq_status", concat(lit(field), lit(":"), lit(check), lit(":"), lit(value))
    )


def validate(df: DataFrame, rules: list[dict]) -> DataFrame:
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


def _rules_to_df(rules: List[Dict]) -> DataFrame:
    """gera DF com column, rule, pass_threshold e value vindos do config"""
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
    now_ts = current_timestamp()

    viol_df = (
        df.filter(trim(col("dq_status")) == lit(""))
        .withColumn("dq_status", split(trim(col("dq_status")), ":"))
        .withColumn("column", col("dq_status")[0])
        .withColumn("rule", col("dq_status")[1])
        .groupBy("column", "rule")
        .agg(count("*").alias("violations"))
    )

    rules_df = _rules_to_df(rules)

    base = rules_df.join(viol_df, ["column", "rule"], how="left").withColumn(
        "violations", coalesce(col("violations"), lit(0))
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

    summary = summary.withColumn("id", monotonically_increasing_id() + 1)
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
