#!/usr/bin/env python
# -*- coding: utf-8 -*-

import warnings
from functools import reduce
import numpy as np
import polars as pl
from sumeh.services.utils import __convert_value
import operator
from datetime import datetime


def is_positive(df: pl.DataFrame, field: str, _) -> pl.DataFrame:
    return df.filter(pl.col(field) < 0).with_columns(
        [pl.lit(f"{field}:is_positive").alias("dq_status")]
    )


def is_negative(df: pl.DataFrame, field: str, _) -> pl.DataFrame:
    return df.filter(pl.col(field) >= 0).with_columns(
        [pl.lit(f"{field}:is_negative").alias("dq_status")]
    )


def is_complete(df: pl.DataFrame, field: str, _) -> pl.DataFrame:
    return df.filter(pl.col(field).is_null()).with_columns(
        pl.lit(f"{field}:is_complete").alias("dq_status")
    )


def is_unique(df: pl.DataFrame, field: str, _) -> pl.DataFrame:
    dup_vals = (
        df.group_by(field)
        .agg(pl.count().alias("cnt"))
        .filter(pl.col("cnt") > 1)
        .select(field)
        .to_series()
        .to_list()
    )
    return df.filter(pl.col(field).is_in(dup_vals)).with_columns(
        pl.lit(f"{field}:is_unique").alias("dq_status")
    )


def are_complete(df: pl.DataFrame, fields: list[str], _) -> pl.DataFrame:
    cond = reduce(operator.or_, [pl.col(f).is_null() for f in fields])
    return df.filter(cond).with_columns(
        pl.lit(f"{fields}:are_complete").alias("dq_status")
    )


def are_unique(df: pl.DataFrame, fields: list[str], _) -> pl.DataFrame:
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
        .with_columns(pl.lit(f"{fields}:are_unique").alias("dq_status"))
    )


def is_greater_than(df: pl.DataFrame, field: str, value) -> pl.DataFrame:
    return df.filter(pl.col(field) > value).with_columns(
        pl.lit(f"{field}:is_greater_than").alias("dq_status")
    )


def is_greater_or_equal_than(df: pl.DataFrame, field: str, value) -> pl.DataFrame:
    return df.filter(pl.col(field) < value).with_columns(
        pl.lit(f"{field}:is_greater_or_equal_than").alias("dq_status")
    )


def is_less_than(df: pl.DataFrame, field: str, value) -> pl.DataFrame:
    return df.filter(pl.col(field) >= value).with_columns(
        pl.lit(f"{field}:is_less_than").alias("dq_status")
    )


def is_less_or_equal_than(df: pl.DataFrame, field: str, value) -> pl.DataFrame:
    return df.filter(pl.col(field) > value).with_columns(
        pl.lit(f"{field}:is_less_or_equal_than").alias("dq_status")
    )


def is_equal(df: pl.DataFrame, field: str, value) -> pl.DataFrame:
    return df.filter(~pl.col(field).eq(value)).with_columns(
        pl.lit(f"{field}:is_equal").alias("dq_status")
    )


def is_equal_than(df: pl.DataFrame, field: str, value) -> pl.DataFrame:
    return df.filter(~pl.col(field).eq(value)).with_columns(
        pl.lit(f"{field}:is_equal_than").alias("dq_status")
    )


def is_contained_in(
    df: pl.DataFrame, field: str, positive_list_str: str
) -> pl.DataFrame:
    lst = [v.strip() for v in positive_list_str.strip("[]").split(",")]
    return df.filter(~pl.col(field).is_in(lst)).with_columns(
        pl.lit(f"{field}:is_contained_in").alias("dq_status")
    )


def not_contained_in(
    df: pl.DataFrame, field: str, negative_list_str: str
) -> pl.DataFrame:
    lst = [v.strip() for v in negative_list_str.strip("[]").split(",")]
    return df.filter(pl.col(field).is_in(lst)).with_columns(
        pl.lit(f"{field}:not_contained_in").alias("dq_status")
    )


def is_between(df: pl.DataFrame, field: str, limits_list: str) -> pl.DataFrame:
    lo, hi = limits_list.strip("[]").split(",")
    lo, hi = __convert_value(lo), __convert_value(hi)
    return df.filter(~pl.col(field).is_between(lo, hi)).with_columns(
        pl.lit(f"{field}:is_between").alias("dq_status")
    )


def has_pattern(df: pl.DataFrame, field: str, pattern: str) -> pl.DataFrame:
    return df.filter(~pl.col(field).str.contains(pattern, literal=False)).with_columns(
        pl.lit(f"{field}:has_pattern").alias("dq_status")
    )


def is_legit(df: pl.DataFrame, field: str, _) -> pl.DataFrame:
    mask = pl.col(field).is_not_null() & pl.col(field).str.contains(r"^\S+$")
    return df.filter(~mask).with_columns(pl.lit(f"{field}:is_legit").alias("dq_status"))


def has_max(df: pl.DataFrame, field: str, value: float) -> pl.DataFrame:
    return df.filter(pl.col(field) > value).with_columns(
        pl.lit(f"{field}:has_max").alias("dq_status")
    )


def has_min(df: pl.DataFrame, field: str, value: float) -> pl.DataFrame:
    return df.filter(pl.col(field) < value).with_columns(
        pl.lit(f"{field}:has_min").alias("dq_status")
    )


def has_std(df: pl.DataFrame, field: str, value: float) -> pl.DataFrame:
    std_val = df.select(pl.col(field).std()).to_numpy()[0] or 0.0
    if std_val > value:
        return df.with_columns(pl.lit(f"{field}:has_std({std_val})").alias("dq_status"))
    return df.head(0).with_columns(pl.lit("dq_status").alias("dq_status")).head(0)


def has_mean(df: pl.DataFrame, field: str, value: float) -> pl.DataFrame:
    mean_val = df.select(pl.col(field).mean()).to_numpy()[0] or 0.0
    if mean_val > value:
        return df.with_columns(
            pl.lit(f"{field}:has_mean({mean_val})").alias("dq_status")
        )
    return df.head(0)


def has_sum(df: pl.DataFrame, field: str, value: float) -> pl.DataFrame:
    sum_val = df.select(pl.col(field).sum()).to_numpy()[0] or 0.0
    if sum_val > value:
        return df.with_columns(pl.lit(f"{field}:has_sum({sum_val})").alias("dq_status"))
    return df.head(0)


def has_cardinality(df: pl.DataFrame, field: str, value: int) -> pl.DataFrame:
    card = df.select(pl.col(field).n_unique()).to_numpy()[0] or 0
    if card > value:
        return df.with_columns(
            pl.lit(f"{field}:has_cardinality({card})").alias("dq_status")
        )
    return df.head(0)


def has_infogain(df: pl.DataFrame, field: str, value: float) -> pl.DataFrame:
    ig = df.select(pl.col(field).n_unique()).to_numpy()[0] or 0.0
    if ig > value:
        return df.with_columns(pl.lit(f"{field}:has_infogain({ig})").alias("dq_status"))
    return df.head(0)


def has_entropy(df: pl.DataFrame, field: str, value: float) -> pl.DataFrame:
    ent = df.select(pl.col(field).n_unique()).to_numpy()[0] or 0.0
    if ent > value:
        return df.with_columns(pl.lit(f"{field}:has_entropy({ent})").alias("dq_status"))
    return df.head(0)


def satisfies(df: pl.DataFrame, field: str, expression: str) -> pl.DataFrame:
    ctx = pl.SQLContext(cuallee=df)
    viol = ctx.execute(
        f"""
        SELECT *
        FROM cuallee
        WHERE NOT ({expression})
        """,
        eager=True,
    )
    return viol.with_columns(pl.lit(f"{field}:satisfies").alias("dq_status"))


def validate(df: pl.DataFrame, rules: list[dict]) -> pl.DataFrame:
    """
    Executa validações de qualidade de dados e retorna DataFrame com violações agregadas.
    Versão otimizada que:
    - Minimiza operações de concatenação
    - Reduz uso de memória
    - Mantém compatibilidade com a versão PySpark
    """

    # 1. Preparação inicial (única operação de cópia)
    df_with_id = df.with_columns(
        pl.arange(0, pl.count()).alias("__id"), pl.lit("").alias("dq_status")
    )

    # 2. Lista para acumular DataFrames de violação
    violations = []

    # 3. Processamento das regras (otimizado)
    for rule in rules:
        if not rule.get("execute", True):
            continue

        rule_name = rule["check_type"]
        field = rule["field"]

        # Mapeamento de regras sinônimas
        rule_name = {
            "is_primary_key": "is_unique",
            "is_composite_key": "are_unique",
        }.get(rule_name, rule_name)

        if (rule_func := globals().get(rule_name)) is None:
            warnings.warn(f"Unknown rule: {rule_name}", RuntimeWarning)
            continue

        # Processamento de valor otimizado
        raw_value = rule.get("value")
        value = (
            __convert_value(raw_value)
            if isinstance(raw_value, str) and raw_value not in ("", "NULL")
            else raw_value
        )

        # Aplica a regra e coleta violações
        try:
            viol = rule_func(df_with_id, field, value)
            if not viol.is_empty():
                violations.append(viol.select(["__id", "dq_status"]))
        except Exception as e:
            warnings.warn(f"Error in rule {rule_name}: {str(e)}", RuntimeWarning)

    # 4. Agregação otimizada
    if not violations:
        return df.with_columns(pl.lit("").alias("dq_status"))

    # Concatenação única e agregação
    all_violations = (
        pl.concat(violations)
        .group_by("__id", maintain_order=True)
        .agg(pl.col("dq_status").str.concat(";"))
    )

    # 5. Junção final otimizada
    return (
        df.join(all_violations, on="__id", how="left")
        .with_columns(pl.coalesce(pl.col("dq_status"), pl.lit("")))
        .drop("__id")
    )


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

        raw = rule.get("value")
        try:
            value = (
                __convert_value(raw)
                if isinstance(raw, str) and raw not in ("", "NULL")
                else raw
            )
        except ValueError as e:
            warnings.warn(
                f"Value conversion failed for rule {rule_name}: {str(e)}",
                RuntimeWarning,
            )
            value = raw

        viol = func(df_with_dq, rule["field"], value)
        result = pl.concat([result, viol]) if not result.is_empty() else viol

    summary = (
        result.group_by("__id", maintain_order=True)
        .agg("dq_status")
        .with_columns(pl.col("dq_status").list.join(";").alias("dq_status"))
    )
    out = df.join(summary, on="__id", how="left").drop("__id")

    return out


def summarize(qc_df: pl.DataFrame, rules: list[dict], total_rows: int) -> pl.DataFrame:
    exploded = (
        qc_df.select(
            pl.col("dq_status").str.split(";").list.explode().alias("violation")
        )
        .filter(pl.col("violation") != "")
        .with_columns(
            [
                pl.col("violation").str.split(":").list.get(0).alias("column"),
                pl.col("violation").str.split(":").list.get(1).alias("rule"),
            ]
        )
    )
    viol_count = exploded.group_by(["column", "rule"]).agg(
        pl.count().alias("violations")
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
        .unique(subset=["column", "rule"])
        .with_columns(
            [
                pl.col("column").cast(str),
                pl.col("rule").cast(str),
            ]
        )
    )
    summary = (
        rules_df.join(viol_count, on=["column", "rule"], how="left")
        .with_columns(pl.col("violations").fill_null(0))
        .with_columns(
            ((pl.lit(total_rows) - pl.col("violations")) / pl.lit(total_rows)).alias(
                "pass_rate"
            )
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
