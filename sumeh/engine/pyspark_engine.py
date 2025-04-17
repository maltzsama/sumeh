#!/usr/bin/env python
# -*- coding: utf-8 -*-

import warnings
from pyspark.sql import DataFrame
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
import operator
from functools import reduce
from pyspark.sql import Window
from sumeh.services.utils import __convert_value


def is_positive(df: DataFrame, field: str, _) -> DataFrame:
    return df.filter(col(field) < 0).withColumn(
        "dq_status", concat(lit(field), lit(":is_positive"))
    )


def is_negative(df: DataFrame, field: str, _) -> DataFrame:
    return df.filter(col(field) >= 0).withColumn(
        "dq_status", concat(lit(field), lit(":is_negative"))
    )


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
    return df.filter(col(field) > value).withColumn(
        "dq_status", concat(lit(field), lit(":is_greater_than"))
    )


def is_greater_or_equal_than(df: DataFrame, field: str, value: str) -> DataFrame:
    return df.filter(col(field) < value).withColumn(
        "dq_status", concat(lit(field), lit(":is_greater_or_equal_than"))
    )


def is_less_than(df: DataFrame, field: str, value: str) -> DataFrame:
    return df.filter(col(field) >= value).withColumn(
        "dq_status", concat(lit(field), lit(":is_less_than"))
    )


def is_less_or_equal_than(df: DataFrame, field: str, value: str) -> DataFrame:
    return df.filter(col(field) > value).withColumn(
        "dq_status", concat(lit(field), lit(":is_less_or_equal_than"))
    )


def is_equal(df: DataFrame, field: str, value: str) -> DataFrame:
    return df.filter(~col(field).eqNullSafe(value)).withColumn(
        "dq_status", concat(lit(field), lit(":is_equal"))
    )


def is_equal_than(df: DataFrame, field: str, value: float) -> DataFrame:
    return df.filter(~col(field).eqNullSafe(value)).withColumn(
        "dq_status", concat(lit(field), lit(":is_equal_than"))
    )


def is_contained_in(df: DataFrame, field: str, positive_list_str: str) -> DataFrame:
    positive_list = positive_list_str.replace("[", "").replace("]", "").split(",")
    return df.filter(~col(field).isin(positive_list)).withColumn(
        "dq_status", concat(lit(field), lit(":is_contained_in"))
    )


def not_contained_in(df: DataFrame, field: str, negative_list_str: str) -> DataFrame:
    negative_list = negative_list_str.replace("[", "").replace("]", "").split(",")
    return df.filter(col(field).isin(negative_list)).withColumn(
        "dq_status", concat(lit(field), lit(":not_contained_in"))
    )


def is_between(df: DataFrame, field: str, limits_list: str) -> DataFrame:
    min_value, max_value = limits_list.replace("[", "").replace("]", "").split(",")
    return df.filter(~col(field).between(min_value, max_value)).withColumn(
        "dq_status", concat(lit(field), lit(":is_between"))
    )


def has_pattern(df: DataFrame, field: str, pattern: str) -> DataFrame:
    return df.filter(~col(field).rlike(pattern)).withColumn(
        "dq_status", concat(lit(field), lit(":has_pattern"))
    )


def is_legit(df: DataFrame, field: str, _) -> DataFrame:
    pattern_legit = "\S*"
    return df.filter(
        ~(col(field).isNotNull() & col(field).rlike(pattern_legit))
    ).withColumn("dq_status", concat(lit(field), lit(":is_legit")))


def is_primary_key(df: DataFrame, field: str, _):
    return is_unique(df, field, _)


def is_composite_key(df: DataFrame, fields: list[str], _):
    return are_unique(df, fields, _)


def has_max(df: DataFrame, field: str, value: float) -> DataFrame:
    return df.filter(col(field) > value).withColumn(
        "dq_status", concat(lit(field), lit(":has_max"))
    )


def has_min(df: DataFrame, field: str, value: float) -> DataFrame:
    return df.filter(col(field) < value).withColumn(
        "dq_status", concat(lit(field), lit(":has_min"))
    )


def has_std(df: DataFrame, field: str, value: float) -> DataFrame:
    std_val = df.select(stddev(col(field))).first()[0]
    std_val = std_val or 0.0
    if std_val > value:
        return df.withColumn(
            "dq_status",
            concat(lit(field), lit(f":has_std({std_val})"))
        )
    else:
        return df.limit(0)


def has_mean(df: DataFrame, field: str, value: float) -> DataFrame:
    mean_val = (df.select(avg(col(field))).first()[0]) or 0.0
    if mean_val > value:   # regra falhou
        return df.withColumn(
            "dq_status",
            concat(lit(field), lit(f":has_mean({mean_val})"))
        )
    else:                  # passou
        return df.limit(0)


def has_sum(df: DataFrame, field: str, value: float) -> DataFrame:
    sum_val = (df.select(sum(col(field))).first()[0]) or 0.0
    if sum_val > value:
        return df.withColumn(
            "dq_status",
            concat(lit(field), lit(f":has_sum({sum_val})"))
        )
    return df.limit(0)


def has_cardinality(df: DataFrame, field: str, value: int) -> DataFrame:
    card_val = df.select(countDistinct(col(field))).first()[0] or 0
    if card_val > value:
        return df.withColumn(
            "dq_status",
            concat(lit(field), lit(f":has_cardinality({card_val})"))
        )
    return df.limit(0)


def has_infogain(df: DataFrame, field: str, value: float) -> DataFrame:
    info_gain = df.select(countDistinct(col(field))).first()[0] or 0.0
    if info_gain > value:
        return df.withColumn(
            "dq_status",
            concat(lit(field), lit(f":has_infogain({info_gain})"))
        )
    return df.limit(0)


def has_entropy(df: DataFrame, field: str, value: float) -> DataFrame:
    entropy_val = df.select(countDistinct(col(field))).first()[0] or 0.0
    if entropy_val > value:
        return df.withColumn(
            "dq_status",
            concat(lit(field), lit(f":has_entropy({entropy_val})"))
        )
    return df.limit(0)


def all_date_checks(df: DataFrame, field: str) -> DataFrame:
    return df.filter((col(field) < current_date())).withColumn(
        "dq_status", concat(lit(field), lit(":all_date_checks"))
    )


def satisfies(df: DataFrame, field: str, expression: str) -> DataFrame:
    return df.filter(col(field).rlike(expression)).withColumn(
        "dq_status", concat(lit(field), lit(":satisfies"))
    )


def quality_checker(df: DataFrame, rules: list[dict]) -> DataFrame:
    df = df.withColumn("dq_status", lit(""))
    result = df.limit(0)
    for rule in rules:
        rule_name = rule["check_type"]
        field = rule["field"]
        raw_value = rule.get("value")
        if isinstance(raw_value, str) and raw_value not in (None, "", "NULL"):
            try:
                value = __convert_value(raw_value)
            except ValueError:
                value = raw_value
        else:
            value = raw_value  # None ou lista já formatada
        try:
            rule_func = globals()[rule_name]
            result = result.union(rule_func(df, field, value))
        except KeyError:
            warnings.warn(f"Unknown rule name: {rule_name}, {field}")
    group_columns = [c for c in df.columns if c != "dq_status"]
    summary_df = result.groupBy(*group_columns).agg(
        concat_ws(";", collect_list("dq_status")).alias("dq_status")
    )
    return summary_df


def quality_summary(qc_df: DataFrame) -> DataFrame:
    total_rows  = qc_df.count()
    now_ts      = current_timestamp()
    exploded = (
        qc_df
        .select(
            explode(
                split(col("dq_status"), ";")
            ).alias("status")
        )
        .filter(trim("status") != "")
        .select(
            trim(split("status", ":")[0]).alias("column"),
            trim(split("status", ":")[1]).alias("rule")
        )
    )
    viol_df = (
        exploded.groupBy("column", "rule")
        .agg(count("*").alias("violations"))
    )
    summary = (
        viol_df
        .withColumn("rows",           lit(total_rows))
        .withColumn("pass_rate",      (lit(total_rows) - col("violations")) / lit(total_rows))
        .withColumn("pass_threshold", lit(1.0))
        .withColumn("status",
                    when(col("pass_rate") >= col("pass_threshold"), "PASS")
                    .otherwise("FAIL"))
        .withColumn("timestamp",  now_ts)
        .withColumn("check",      lit("Quality Check"))
        .withColumn("level",      lit("WARNING"))
        .withColumn("value",      lit("N/A"))
    )
    summary = summary.withColumn(
        "id",
        monotonically_increasing_id() + 1     # começa em 1
    )
    summary = summary.select(
        "id", "timestamp", "check", "level",
        "column", "rule", "value",
        "rows", "violations", "pass_rate",
        "pass_threshold", "status"
    )
    return summary