#!/usr/bin/env python
# -*- coding: utf-8 -*-

import warnings
from functools import reduce
import polars as pl
from sumeh.services.utils import __convert_value, __extract_params
import operator
from datetime import datetime


def is_positive(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    field, check, value = __extract_params(rule)
    return df.filter(pl.col(field) < 0).with_columns(
        [pl.lit(f"{field}:{check}:{value}").alias("dq_status")]
    )


def is_negative(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    field, check, value = __extract_params(rule)
    return df.filter(pl.col(field) >= 0).with_columns(
        [pl.lit(f"{field}:{check}:{value}").alias("dq_status")]
    )


def is_complete(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    field, check, value = __extract_params(rule)
    return df.filter(pl.col(field).is_not_null()).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def is_unique(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    field, check, value = __extract_params(rule)
    dup_vals = (
        df.group_by(field)
        .agg(pl.count().alias("cnt"))
        .filter(pl.col("cnt") > 1)
        .select(field)
        .to_series()
        .to_list()
    )
    return df.filter(pl.col(field).is_in(dup_vals)).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def are_complete(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    fields, check, value = __extract_params(rule)
    cond = reduce(operator.or_, [pl.col(f).is_null() for f in fields])

    tag = f"{fields}:{check}:{value}"
    return df.filter(cond).with_columns(pl.lit(tag).alias("dq_status"))


def are_unique(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    fields, check, value = __extract_params(rule)
    combo = df.with_columns(
        pl.concat_str([pl.col(f).cast(str) for f in fields], separator="|").alias(
            "_combo"
        )
    )
    dupes = (
        combo.group_by("_combo")
        .agg(pl.count().alias("cnt"))
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
    field, check, value = __extract_params(rule)
    return df.filter(pl.col(field) <= value).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def is_greater_or_equal_than(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    field, check, value = __extract_params(rule)
    return df.filter(pl.col(field) < value).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def is_less_than(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    field, check, value = __extract_params(rule)
    return df.filter(pl.col(field) >= value).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def is_less_or_equal_than(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    field, check, value = __extract_params(rule)
    return df.filter(pl.col(field) > value).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def is_equal(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    field, check, value = __extract_params(rule)
    return df.filter(~pl.col(field).eq(value)).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def is_equal_than(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    field, check, value = __extract_params(rule)
    return df.filter(~pl.col(field).eq(value)).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def is_contained_in(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    field, check, value = __extract_params(rule)
    lst = [v.strip() for v in value.strip("[]").split(",")]
    return df.filter(~pl.col(field).is_in(lst)).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def not_contained_in(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    field, check, value = __extract_params(rule)
    lst = [v.strip() for v in value.strip("[]").split(",")]
    return df.filter(pl.col(field).is_in(lst)).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def is_between(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    field, check, value = __extract_params(rule)
    lo, hi = value.strip("[]").split(",")
    lo, hi = __convert_value(lo), __convert_value(hi)
    return df.filter(~pl.col(field).is_between(lo, hi)).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def has_pattern(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    field, check, pattern = __extract_params(rule)
    return df.filter(~pl.col(field).str.contains(pattern, literal=False)).with_columns(
        pl.lit(f"{field}:{check}:{pattern}").alias("dq_status")
    )


def is_legit(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    field, check, value = __extract_params(rule)
    mask = pl.col(field).is_not_null() & pl.col(field).str.contains(r"^\S+$")
    return df.filter(~mask).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def has_max(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    field, check, value = __extract_params(rule)
    return df.filter(pl.col(field) > value).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def has_min(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    field, check, value = __extract_params(rule)
    return df.filter(pl.col(field) < value).with_columns(
        pl.lit(f"{field}:{check}:{value}").alias("dq_status")
    )


def has_std(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    field, check, value = __extract_params(rule)
    std_val = df.select(pl.col(field).std()).to_numpy()[0] or 0.0
    if std_val > value:
        return df.with_columns(pl.lit(f"{field}:{check}:{value}").alias("dq_status"))
    return df.head(0).with_columns(pl.lit("dq_status").alias("dq_status")).head(0)


def has_mean(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    field, check, value = __extract_params(rule)
    mean_val = df.select(pl.col(field).mean()).to_numpy()[0] or 0.0
    if mean_val > value:
        return df.with_columns(pl.lit(f"{field}:{check}:{value}").alias("dq_status"))
    return df.head(0)


def has_sum(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    field, check, value = __extract_params(rule)
    sum_val = df.select(pl.col(field).sum()).to_numpy()[0] or 0.0
    if sum_val > value:
        return df.with_columns(pl.lit(f"{field}:{check}:{value}").alias("dq_status"))
    return df.head(0)


def has_cardinality(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    field, check, value = __extract_params(rule)
    card = df.select(pl.col(field).n_unique()).to_numpy()[0] or 0
    if card > value:
        return df.with_columns(pl.lit(f"{field}:{check}:{value}").alias("dq_status"))
    return df.head(0)


def has_infogain(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    field, check, value = __extract_params(rule)
    ig = df.select(pl.col(field).n_unique()).to_numpy()[0] or 0.0
    if ig > value:
        return df.with_columns(pl.lit(f"{field}:{check}:{value}").alias("dq_status"))
    return df.head(0)


def has_entropy(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    field, check, value = __extract_params(rule)
    ent = df.select(pl.col(field).n_unique()).to_numpy()[0] or 0.0
    if ent > value:
        return df.with_columns(pl.lit(f"{field}:{check}:{value}").alias("dq_status"))
    return df.head(0)


def satisfies(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
    field, check, value = __extract_params(rule)
    ctx = pl.SQLContext(sumeh=df)
    viol = ctx.execute(
        f"""
        SELECT *
        FROM sumeh
        WHERE NOT ({expression})
        """,
        eager=True,
    )
    return viol.with_columns(pl.lit(f"{field}:{check}:{value}").alias("dq_status"))


def validate(df: pl.DataFrame, rules: list[dict]) -> pl.DataFrame:
    df = df.with_columns(pl.arange(0, pl.count()).alias("__id"))
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

    return out, summary


def summarize(qc_df: pl.DataFrame, rules: list[dict], total_rows: int) -> pl.DataFrame:
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
        pl.count().alias("violations_count")
    )

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
    )
    summary = (
        rules_df.join(viol_count, on=["column", "rule", "value"], how="left")
        .with_columns(
            [
                pl.col("violations_count").fill_null(0).alias("violations"),
            ]
        )
        .with_columns(
            [
                (
                    (pl.lit(total_rows) - pl.col("violations")) / pl.lit(total_rows)
                ).alias("pass_rate")
            ]
        )
        .with_columns(
            [
                pl.lit(total_rows).alias("rows"),
                pl.when(pl.col("pass_rate") >= pl.col("pass_threshold"))
                .then(pl.lit("PASS"))
                .otherwise(pl.lit("FAIL"))
                .alias("status"),
                pl.lit(datetime.now().replace(second=0, microsecond=0)).alias(
                    "timestamp"
                ),
                pl.lit("Quality Check").alias("check"),
                pl.lit("WARNING").alias("level"),
            ]
        )
        .with_columns(pl.arange(1, pl.count() + 1).alias("id"))
        .select(
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
    )
    return summary
