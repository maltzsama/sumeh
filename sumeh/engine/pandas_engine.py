#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module provides a set of data quality validation functions using the Pandas library. 
It includes various checks for data validation, such as completeness, uniqueness, range checks, 
pattern matching, date validations, SQL-style custom expressions, and schema validation.

Functions:
    is_positive(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Filters rows where the specified field is less than zero.

    is_negative(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Filters rows where the specified field is greater than or equal to zero.

    is_complete(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Filters rows where the specified field is null.

    is_unique(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Filters rows with duplicate values in the specified field.

    are_complete(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Filters rows where any of the specified fields are null.

    are_unique(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Filters rows with duplicate combinations of the specified fields.

    is_greater_than(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Filters rows where the specified field is less than or equal to the given value.

    is_greater_or_equal_than(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Filters rows where the specified field is less than the given value.

    is_less_than(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Filters rows where the specified field is greater than or equal to the given value.

    is_less_or_equal_than(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Filters rows where the specified field is greater than the given value.

    is_equal(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Filters rows where the specified field is not equal to the given value.

    is_equal_than(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Alias for `is_equal`.

    is_contained_in(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Filters rows where the specified field is not in the given list of values.

    not_contained_in(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Filters rows where the specified field is in the given list of values.

    is_between(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Filters rows where the specified field is not within the given range.

    has_pattern(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Filters rows where the specified field does not match the given regex pattern.

    is_legit(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Filters rows where the specified field is null or contains whitespace.

    has_max(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Filters rows where the specified field exceeds the given maximum value.

    has_min(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Filters rows where the specified field is below the given minimum value.

    has_std(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Checks if the standard deviation of the specified field exceeds the given value.

    has_mean(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Checks if the mean of the specified field exceeds the given value.

    has_sum(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Checks if the sum of the specified field exceeds the given value.

    has_cardinality(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Checks if the cardinality (number of unique values) of the specified field exceeds the given value.

    has_infogain(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Placeholder for information gain validation (currently uses cardinality).

    has_entropy(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Placeholder for entropy validation (currently uses cardinality).

    satisfies(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Filters rows that do not satisfy the given custom expression.

    validate_date_format(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Filters rows where the specified field does not match the expected date format or is null.

    is_future_date(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Filters rows where the specified date field is after today’s date.

    is_past_date(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Filters rows where the specified date field is before today’s date.

    is_date_between(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Filters rows where the specified date field is not within the given [start,end] range.

    is_date_after(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Filters rows where the specified date field is before the given date.

    is_date_before(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Filters rows where the specified date field is after the given date.

    all_date_checks(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    Alias for `is_past_date` (checks date against today).

    validate(df: pd.DataFrame, rules: list[dict]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    Validates a DataFrame against a list of rules and returns the original DataFrame with 
    data quality status and a DataFrame of violations.

    __build_rules_df(rules: list[dict]) -> pd.DataFrame:
    Converts a list of rules into a Pandas DataFrame for summarization.

    summarize(qc_df: pd.DataFrame, rules: list[dict], total_rows: int) -> pd.DataFrame:
    Summarizes the results of data quality checks, including pass rates and statuses.

    validate_schema(df, expected) -> Tuple[bool, List[Tuple[str, str]]]:
    Validates the schema of a DataFrame against an expected schema and returns a boolean 
    result and a list of errors.
"""
import warnings
import re
import pandas as pd
import numpy as np
from datetime import datetime, date
from sumeh.services.utils import __convert_value, __extract_params, __compare_schemas, __transform_date_format_in_pattern
from typing import List, Dict, Any, Tuple


def is_positive(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, value = __extract_params(rule)
    viol = df[df[field] < 0].copy()
    viol['dq_status'] = f"{field}:{check}:{value}"
    return viol


def is_negative(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, value = __extract_params(rule)
    viol = df[df[field] >= 0].copy()
    viol['dq_status'] = f"{field}:{check}:{value}"
    return viol


def is_complete(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, value = __extract_params(rule)
    viol = df[df[field].isna()].copy()
    viol['dq_status'] = f"{field}:{check}:{value}"
    return viol


def is_unique(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, value = __extract_params(rule)
    dup = df[field].duplicated(keep=False)
    viol = df[dup].copy()
    viol['dq_status'] = f"{field}:{check}:{value}"
    return viol


def are_complete(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    fields, check, value = __extract_params(rule)
    mask = df[fields].isna().any(axis=1)
    viol = df[mask].copy()
    viol['dq_status'] = f"{fields}:{check}:{value}"
    return viol


def are_unique(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    fields, check, value = __extract_params(rule)
    combo = df[fields].astype(str).agg('|'.join, axis=1)
    dup = combo.duplicated(keep=False)
    viol = df[dup].copy()
    viol['dq_status'] = f"{fields}:{check}:{value}"
    return viol


def is_greater_than(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, value = __extract_params(rule)
    viol = df[df[field] > value].copy()
    viol['dq_status'] = f"{field}:{check}:{value}"
    return viol


def is_greater_or_equal_than(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, value = __extract_params(rule)
    viol = df[df[field] >= value].copy()
    viol['dq_status'] = f"{field}:{check}:{value}"
    return viol


def is_less_than(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, value = __extract_params(rule)
    viol = df[df[field] < value].copy()
    viol['dq_status'] = f"{field}:{check}:{value}"
    return viol


def is_less_or_equal_than(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, value = __extract_params(rule)
    viol = df[df[field] <= value].copy()
    viol['dq_status'] = f"{field}:{check}:{value}"
    return viol


def is_equal(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, value = __extract_params(rule)
    viol = df[df[field] != value].copy()
    viol['dq_status'] = f"{field}:{check}:{value}"
    return viol


def is_equal_than(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    return is_equal(df, rule)


def is_contained_in(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, value = __extract_params(rule)
    vals = re.findall(r"'([^']*)'", str(value)) or [v.strip() for v in str(value).strip('[]').split(',')]
    viol = df[~df[field].isin(vals)].copy()
    viol['dq_status'] = f"{field}:{check}:{value}"
    return viol

def is_in(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    return is_contained_in(df, rule)

def not_contained_in(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, value = __extract_params(rule)
    vals = re.findall(r"'([^']*)'", str(value)) or [v.strip() for v in str(value).strip('[]').split(',')]
    viol = df[df[field].isin(vals)].copy()
    viol['dq_status'] = f"{field}:{check}:{value}"
    return viol

def not_in(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    return not_contained_in(df, rule)


def is_between(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, value = __extract_params(rule)
    lo, hi = [__convert_value(x) for x in str(value).strip('[]').split(',')]
    viol = df[~df[field].between(lo, hi)].copy()
    viol['dq_status'] = f"{field}:{check}:{value}"
    return viol


def has_pattern(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, pattern = __extract_params(rule)
    viol = df[~df[field].astype(str).str.contains(pattern, na=False)].copy()
    viol['dq_status'] = f"{field}:{check}:{pattern}"
    return viol


def is_legit(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, value = __extract_params(rule)
    mask = df[field].notna() & df[field].astype(str).str.contains(r"^\S+$", na=False)
    viol = df[~mask].copy()
    viol['dq_status'] = f"{field}:{check}:{value}"
    return viol


def has_max(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, value = __extract_params(rule)
    viol = df[df[field] > value].copy()
    viol['dq_status'] = f"{field}:{check}:{value}"
    return viol


def has_min(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, value = __extract_params(rule)
    viol = df[df[field] < value].copy()
    viol['dq_status'] = f"{field}:{check}:{value}"
    return viol


def has_std(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, value = __extract_params(rule)
    std_val = df[field].std(skipna=True) or 0.0
    if std_val > value:
        out = df.copy()
        out['dq_status'] = f"{field}:{check}:{value}"
        return out
    return df.iloc[0:0].copy()


def has_mean(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, value = __extract_params(rule)
    mean_val = df[field].mean(skipna=True) or 0.0
    if mean_val > value:
        out = df.copy()
        out['dq_status'] = f"{field}:{check}:{value}"
        return out
    return df.iloc[0:0].copy()


def has_sum(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, value = __extract_params(rule)
    sum_val = df[field].sum(skipna=True) or 0.0
    if sum_val > value:
        out = df.copy()
        out['dq_status'] = f"{field}:{check}:{value}"
        return out
    return df.iloc[0:0].copy()


def has_cardinality(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, value = __extract_params(rule)
    card = df[field].nunique(dropna=True) or 0
    if card > value:
        out = df.copy()
        out['dq_status'] = f"{field}:{check}:{value}"
        return out
    return df.iloc[0:0].copy()


def has_infogain(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    return has_cardinality(df, rule)


def has_entropy(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    return has_cardinality(df, rule)


def satisfies(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, expr = __extract_params(rule)
    mask = df.eval(expr)
    viol = df[~mask].copy()
    viol['dq_status'] = f"{field}:{check}:{expr}"
    return viol


def validate_date_format(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, fmt = __extract_params(rule)
    pattern = __transform_date_format_in_pattern(fmt)
    mask = ~df[field].astype(str).str.match(pattern, na=False) | df[field].isna()
    viol = df[mask].copy()
    viol['dq_status'] = f"{field}:{check}:{fmt}"
    return viol


def is_future_date(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, _ = __extract_params(rule)
    today = date.today()
    dates = pd.to_datetime(df[field], errors='coerce')
    viol = df[dates > today].copy()
    viol['dq_status'] = f"{field}:{check}:{today.isoformat()}"
    return viol


def is_past_date(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, _ = __extract_params(rule)
    today = date.today()
    dates = pd.to_datetime(df[field], errors='coerce')
    viol = df[dates < today].copy()
    viol['dq_status'] = f"{field}:{check}:{today.isoformat()}"
    return viol


def is_date_between(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, raw = __extract_params(rule)
    start_str, end_str = [s.strip() for s in raw.strip('[]').split(',')]
    start = pd.to_datetime(start_str)
    end = pd.to_datetime(end_str)
    dates = pd.to_datetime(df[field], errors='coerce')
    mask = ~dates.between(start, end)
    viol = df[mask].copy()
    viol['dq_status'] = f"{field}:{check}:{raw}"
    return viol


def is_date_after(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, date_str = __extract_params(rule)
    target = pd.to_datetime(date_str)
    dates = pd.to_datetime(df[field], errors='coerce')
    viol = df[dates < target].copy()
    viol['dq_status'] = f"{field}:{check}:{date_str}"
    return viol


def is_date_before(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    field, check, date_str = __extract_params(rule)
    target = pd.to_datetime(date_str)
    dates = pd.to_datetime(df[field], errors='coerce')
    viol = df[dates > target].copy()
    viol['dq_status'] = f"{field}:{check}:{date_str}"
    return viol


def all_date_checks(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    return is_past_date(df, rule)


def validate(df: pd.DataFrame, rules: list[dict]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy().reset_index(drop=True)
    df['_id'] = df.index
    raw_list = []
    for rule in rules:
        if not rule.get('execute', True):
            continue
        rt = rule['check_type']
        fn = globals().get(rt if rt not in ('is_primary_key','is_composite_key') else ('is_unique' if rt=='is_primary_key' else 'are_unique'))
        if fn is None:
            warnings.warn(f"Unknown rule: {rt}")
            continue
        viol = fn(df, rule)
        raw_list.append(viol)
    raw = pd.concat(raw_list, ignore_index=True) if raw_list else pd.DataFrame(columns=df.columns)
    summary = raw.groupby('_id')['dq_status'].agg(';'.join).reset_index()
    out = df.merge(summary, on='_id', how='left').drop(columns=['_id'])
    return out, raw


def __build_rules_df(rules: List[dict]) -> pd.DataFrame:
    rows = []
    for r in rules:
        if not r.get("execute", True):
            continue

        col = (
            ",".join(r["field"])
            if isinstance(r["field"], list)
            else r["field"]
        )

        thr_raw = r.get("threshold")
        try:
            thr = float(thr_raw) if thr_raw is not None else 1.0
        except (TypeError, ValueError):
            thr = 1.0

        val = r.get("value")
        rows.append({
            "column": col,
            "rule": r["check_type"],
            "value": val if val is not None else "",
            "pass_threshold": thr,
        })

    df_rules = pd.DataFrame(rows)
    if not df_rules.empty:
        df_rules = df_rules.drop_duplicates(subset=["column", "rule", "value"])
    return df_rules



def summarize(qc_df: pd.DataFrame, rules: list[dict], total_rows: int) -> pd.DataFrame:
    split = qc_df['dq_status'].str.split(';').explode().dropna()
    parts = split.str.split(':',expand=True)
    parts.columns=['column','rule','value']
    viol_count = parts.groupby(['column','rule','value']).size().reset_index(name='violations')
    rules_df = __build_rules_df(rules)
    df = rules_df.merge(viol_count, on=['column','rule','value'], how='left')
    df['violations']=df['violations'].fillna(0).astype(int)
    df['rows']=total_rows
    df['pass_rate']=(total_rows-df['violations'])/total_rows
    df['status']=np.where(df['pass_rate']>=df['pass_threshold'],'PASS','FAIL')
    df['timestamp']=datetime.now().replace(second=0,microsecond=0)
    df['check']='Quality Check'
    df['level']='WARNING'
    df.insert(0,'id',range(1,len(df)+1))
    return df[['id','timestamp','check','level','column','rule','value','rows','violations','pass_rate','pass_threshold','status']]


def __polars_schema_to_list(df, expected) -> Tuple[bool, List[Tuple[str,str]]]:
    actual = [{'field':c,'data_type':str(dtype).lower(),'nullable':True,'max_length':None} for c,dtype in df.dtypes.items()]
    return __compare_schemas(actual, expected)
