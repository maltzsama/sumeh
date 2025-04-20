#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import warnings
import operator
from functools import reduce
import pandas as pd
import dask.dataframe as dd
import numpy as np
from sumeh.services.utils import __convert_value, __extract_params



def is_positive(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    field, check, value = __extract_params(rule)
    viol = df[df[field] < 0]
    return viol.assign(dq_status=field + ":is_positive")


def is_negative(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    field, check, value = __extract_params(rule)
    viol = df[df[field] >= 0]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_complete(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    field, check, value = __extract_params(rule)
    viol = df[df[field].isnull()]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_unique(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    field, check, value = __extract_params(rule)
    counts = df[field].value_counts().compute()
    dup_vals = counts[counts > 1].index.tolist()
    viol = df[df[field].isin(dup_vals)]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


# TODO: Check
def are_complete(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    fields, check, value = __extract_params(rule)
    mask = ~reduce(operator.and_, [df[f].notnull() for f in fields])
    viol = df[mask]
    return viol.assign(dq_status=f"{str(fields)}:{check}:{value}")


# TODO: Check
def are_unique(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
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
    field, check, value = __extract_params(rule)
    viol = df[df[field] > value]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_greater_or_equal_than(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    field, check, value = __extract_params(rule)
    viol = df[df[field] < value]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_less_than(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    field, check, value = __extract_params(rule)
    viol = df[df[field] >= value]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_less_or_equal_than(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    field, check, value = __extract_params(rule)
    viol = df[df[field] > value]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_equal(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    field, check, value = __extract_params(rule)
    viol = df[~df[field].eq(value)]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_equal_than(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    field, check, value = __extract_params(rule)
    viol = df[~df[field].eq(value)]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_contained_in(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    field, check, value = __extract_params(rule)
    lst = [v.strip() for v in value.strip("[]").split(",")]
    viol = df[~df[field].isin(lst)]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def not_contained_in(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    field, check, value = __extract_params(rule)
    lst = [v.strip() for v in value.strip("[]").split(",")]
    viol = df[df[field].isin(lst)]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_between(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    field, check, value = __extract_params(rule)
    lo, hi = value.strip("[]").split(",")
    viol = df[~df[field].between(__convert_value(lo), __convert_value(hi))]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def has_pattern(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    field, check, value = __extract_params(rule)
    viol = df[~df[field].str.match(value, na=False)]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_legit(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    field, check, value = __extract_params(rule)
    s = df[field].astype("string")
    mask = s.notnull() & s.str.contains(r"^\S+$", na=False)
    viol = df[~mask]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def is_primary_key(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    return is_unique(df, rule)


def is_composite_key(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    return are_unique(df, rule)


def has_max(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    field, check, value = __extract_params(rule)
    viol = df[df[field] > value]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def has_min(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    field, check, value = __extract_params(rule)
    viol = df[df[field] < value]
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def has_std(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    field, check, value = __extract_params(rule)
    std_val = df[field].std().compute() or 0.0
    if std_val > value:
        return df.assign(dq_status=f"{field}:{check}:{value}")
    return df.head(0).pipe(dd.from_pandas, npartitions=1)


def has_mean(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    field, check, value = __extract_params(rule)
    mean_val = df[field].mean().compute() or 0.0
    if mean_val > value:
        return df.assign(dq_status=f"{field}:{check}:{value}")
    return df.head(0).pipe(dd.from_pandas, npartitions=1)


def has_sum(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    field, check, value = __extract_params(rule)
    sum_val = df[field].sum().compute() or 0.0
    if sum_val > value:
        return df.assign(dq_status=f"{field}:{check}:{value}")
    return df.head(0).pipe(dd.from_pandas, npartitions=1)


def has_cardinality(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    field, check, value = __extract_params(rule)
    card = df[field].nunique().compute() or 0
    if card > value:
        return df.assign(dq_status=f"{field}:{check}:{value}")
    return df.head(0).pipe(dd.from_pandas, npartitions=1)


def has_infogain(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    field, check, value = __extract_params(rule)
    ig = df[field].nunique().compute() or 0.0
    if ig > value:
        return df.assign(dq_status=f"{field}:{check}:{value}")
    return df.head(0).pipe(dd.from_pandas, npartitions=1)


def has_entropy(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    field, check, value = __extract_params(rule)
    ent = df[field].nunique().compute() or 0.0
    if ent > value:
        return df.assign(dq_status=f"{field}:{check}:{value}")
    return df.head(0).pipe(dd.from_pandas, npartitions=1)


def satisfies(df: dd.DataFrame, rule: dict) -> dd.DataFrame:
    field, check, value = __extract_params(rule)
    py_expr = sql_expr
    py_expr = re.sub(r"(?<![=!<>])=(?!=)", "==", py_expr)
    py_expr = re.sub(r"\bAND\b", "&", py_expr, flags=re.IGNORECASE)
    py_expr = re.sub(r"\bOR\b", "|", py_expr, flags=re.IGNORECASE)

    def _filter_viol(pdf: pd.DataFrame) -> pd.DataFrame:
        mask = pdf.eval(py_expr)
        return pdf.loc[~mask]

    meta = df._meta
    viol = df.map_partitions(_filter_viol, meta=meta)
    return viol.assign(dq_status=f"{field}:{check}:{value}")


def validate(df: dd.DataFrame, rules: list[dict]) -> tuple[dd.DataFrame, dd.DataFrame]:
    empty = dd.from_pandas(
        pd.DataFrame(columns=df.columns.tolist() + ["dq_status"]), 
        npartitions=1
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
        raw_df
        .groupby(group_cols)
        .dq_status
        .apply(_concat_status, meta=('dq_status', 'object'))
        .reset_index()
    )

    return agg_df, raw_df


def _rules_to_df(rules: list[dict]) -> pd.DataFrame:
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
    df = qc_ddf.compute()
    expl = (
        df["dq_status"]
        .str.split(";", expand=True)
        .stack()
        .str.strip()
        .loc[lambda s: s != ""]
        .reset_index(drop=True)
    )
    cols = expl.str.split(":", expand=True)
    viol = pd.DataFrame(
        {
            "column": cols[0],
            "rule": cols[1],
        }
    )
    viol_count = viol.value_counts().reset_index(name="violations")

    rules_df = _rules_to_df(rules)
    summary = rules_df.merge(viol_count, how="left", on=["column", "rule"])
    summary["violations"] = summary["violations"].fillna(0).astype(int)

    summary["rows"] = total_rows
    summary["pass_rate"] = (total_rows - summary["violations"]) / total_rows
    summary["status"] = np.where(
        summary["pass_rate"] >= summary["pass_threshold"], "PASS", "FAIL"
    )
    summary["timestamp"] = pd.Timestamp.now()
    summary["check"] = "Quality Check"
    summary["level"] = "WARNING"
    summary["id"] = np.arange(1, len(summary) + 1)
    return summary[
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
