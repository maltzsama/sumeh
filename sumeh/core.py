#!/usr/bin/env python
# -*- coding: utf-8 -*-
from cuallee import Check, CheckLevel
import warnings
from importlib import import_module
import re
from typing import List, Dict, Any

from .services.utils import __convert_value
from sumeh.services.config import (
    get_config_from_s3,
    get_config_from_csv,
    get_config_from_mysql,
    get_config_from_postgresql,
    get_config_from_bigquery,
    get_config_from_glue_data_catalog,
)

_CONFIG_DISPATCH = {
    "mysql": get_config_from_mysql,
    "postgresql": get_config_from_postgresql,
}


def get_config(source: str, **kwargs) -> List[Dict[str, Any]]:
    match source:
        case s if s.startswith("bigquery://"):
            _, path = s.split("://", 1)
            project, dataset, table = path.split(".")
            return get_config_from_bigquery(
                project_id=project,
                dataset_id=dataset,
                table_id=table,
                **kwargs,
            )

        case s if s.startswith("s3://"):
            return get_config_from_s3(s, **kwargs)

        case s if re.search(r"\.csv$", s, re.IGNORECASE):
            return get_config_from_csv(s, **kwargs)

        case "mysql" | "postgresql" as driver:
            loader = _CONFIG_DISPATCH[driver]
            return loader(**kwargs)

        case "glue":
            return get_config_from_glue_data_catalog(**kwargs)

        case _:
            raise ValueError(f"Unknow source: {source}")


def _detect_engine(df):
    mod = df.__class__.__module__
    match mod:
        case m if m.startswith("pyspark"):
            return "pyspark_engine"
        case m if m.startswith("dask"):
            return "dask_engine"
        case m if m.startswith("polars"):
            return "polars_engine"
        case m if m.startswith("pandas"):
            return "pandas_engine"
        case m if m.startswith("duckdb"):
            return "duckdb_engine"
        case _:
            raise TypeError(f"Unsupported DataFrame type: {type(df)}")


def validate(df, rules, **context):
    engine_name = _detect_engine(df)
    engine = import_module(f"sumeh.engine.{engine_name}")

    match engine_name:
        case "duckdb_engine":
            return engine.validate(df, rules, context.get("conn"))
        case _:
            return engine.validate(df, rules)


def summarize(df, rules: list[dict], **context):
    engine_name = _detect_engine(df)
    engine = import_module(f"sumeh.engine.{engine_name}")
    match engine_name:
        case "duckdb_engine":
            print("DuckDB engine")
            return engine.summarize(
                df_rel=df,
                rules=rules,
                conn=context.get("conn"),
                total_rows=context.get("total_rows"),
            )
        case _:
            print("default engine")
            return engine.summarize(df, rules, total_rows=context.get("total_rows"))


# TODO: refactore to get better performance
def report(df, rules: list[dict], name: str = "Quality Check"):
    """
    Performs a quality check on the given DataFrame based on the provided rules.

    The function iterates over a list of rules and applies different checks to the
    specified fields of the DataFrame. The checks include validation of completeness,
    uniqueness, specific values, patterns, and other conditions. Each rule corresponds
    to a particular type of validation, such as 'is_complete', 'is_greater_than',
    'has_mean', etc. After applying the checks, the function returns the result of
    the validation.

    Parameters:
    - df (DataFrame): The DataFrame to be validated.
    - rules (list of dict): A list of rules defining the checks to be performed.
        Each rule is a dictionary with the following keys:
        - "check_type": The type of check to apply.
        - "field": The column of the DataFrame to check.
        - "value" (optional): The value used for comparison in some checks (e.g., for 'is_greater_than').
        - "threshold" (optional): A percentage threshold to be applied in some checks.
    - name (str): The name of the quality check (default is "Quality Check").

    Returns:
    - quality_check (CheckResult): The result of the quality validation.

    Warnings:
    - If an unknown rule name is encountered, a warning is generated.
    """

    check = Check(CheckLevel.WARNING, name)

    for rule in rules:
        rule_name = rule["check_type"]
        field = rule["field"]
        threshold = rule.get("threshold", 1.0)
        threshold = 1.0 if threshold is None else threshold
        print(field)

        match rule_name:

            case "is_complete":
                check = check.is_complete(field, pct=threshold)

            case "is_unique":
                check = check.is_unique(field, pct=threshold)

            case "is_primary_key":
                check = check.is_primary_key(field, pct=threshold)

            case "are_complete":
                check = check.are_complete(field, pct=threshold)

            case "are_unique":
                check = check.are_complete(field, pct=threshold)

            case "is_composite_key":
                check = check.are_complete(field, pct=threshold)

            case "is_greater_than":
                value = __convert_value(rule["value"])
                check = check.is_greater_than(field, value, pct=threshold)

            case "is_positive":
                check = check.is_positive(field, pct=threshold)

            case "is_negative":
                check = check.is_negative(field, pct=threshold)

            case "is_greater_or_equal_than":
                value = __convert_value(rule["value"])
                check = check.is_greater_or_equal_than(field, value, pct=threshold)

            case "is_less_than":
                value = __convert_value(rule["value"])
                check = check.is_less_than(field, value, pct=threshold)

            case "is_less_or_equal_than":
                value = __convert_value(rule["value"])
                check = check.is_less_or_equal_than(field, value, pct=threshold)

            case "is_equal_than":
                value = __convert_value(rule["value"])
                check = check.is_equal_than(field, value, pct=threshold)

            case "is_contained_in" | "is_in":
                values = rule["value"]
                values = values.replace("[", "").replace("]", "").split(",")
                values = tuple([value.strip() for value in values])
                check = check.is_contained_in(field, values, pct=threshold)

            case "not_contained_in" | "not_in":
                values = rule["value"]
                values = values.replace("[", "").replace("]", "").split(",")
                values = tuple([value.strip() for value in values])
                check = check.is_contained_in(field, values, pct=threshold)

            case "is_between":
                values = rule["value"]
                values = values.replace("[", "").replace("]", "").split(",")
                values = tuple(__convert_value(value) for value in values)
                check = check.is_between(field, values, pct=threshold)

            case "has_pattern":
                pattern = rule["value"]
                check = check.has_pattern(field, pattern, pct=threshold)

            case "is_legit":
                check = check.is_legit(field, pct=threshold)

            case "has_min":
                value = __convert_value(rule["value"])
                check = check.has_min(field, value)

            case "has_max":
                value = __convert_value(rule["value"])
                check = check.has_max(field, value)

            case "has_std":
                value = __convert_value(rule["value"])
                check = check.has_std(field, value)

            case "has_mean":
                value = __convert_value(rule["value"])
                check = check.has_mean(field, value)

            case "has_sum":
                value = __convert_value(rule["value"])
                check = check.has_sum(field, value)

            case "has_cardinality":
                value = __convert_value(rule["value"])
                check = check.has_cardinality(field, value)

            case "has_infogain":
                check = check.has_infogain(field, pct=threshold)

            case "has_entropy":
                value = __convert_value(rule["value"])
                check = check.has_entropy(field, value)

            case "is_in_millions":
                check = check.is_in_millions(field, pct=threshold)

            case "is_in_billions":
                check = check.is_in_millions(field, pct=threshold)

            case "is_t_minus_1":
                check = check.is_t_minus_1(field, pct=threshold)

            case "is_t_minus_2":
                check = check.is_t_minus_2(field, pct=threshold)

            case "is_t_minus_3":
                check = check.is_t_minus_3(field, pct=threshold)

            case "is_today":
                check = check.is_today(field, pct=threshold)

            case "is_yesterday":
                check = check.is_yesterday(field, pct=threshold)

            case "is_on_weekday":
                check = check.is_on_weekday(field, pct=threshold)

            case "is_on_weekend":
                check = check.is_on_weekend(field, pct=threshold)

            case "is_on_monday":
                check = check.is_on_monday(field, pct=threshold)

            case "is_on_tuesday":
                check = check.is_on_tuesday(field, pct=threshold)

            case "is_on_wednesday":
                check = check.is_on_wednesday(field, pct=threshold)

            case "is_on_thursday":
                check = check.is_on_thursday(field, pct=threshold)

            case "is_on_friday":
                check = check.is_on_friday(field, pct=threshold)

            case "is_on_saturday":
                check = check.is_on_saturday(field, pct=threshold)

            case "is_on_sunday":
                check = check.is_on_sunday(field, pct=threshold)

            case "satisfies":
                predicate = rule["value"]
                check = check.satisfies(field, predicate, pct=threshold)

            case _:
                warnings.warn(f"Unknown rule name: {rule_name}, {field}")

    quality_check = check.validate(df)
    return quality_check


import re
from typing import List, Dict, Any


def get_config(source: str, **kwargs) -> List[Dict[str, Any]]:
    # detecta URI de BigQuery: bigquery://project.dataset.table
    if source.startswith("bigquery://"):
        # parseia bigquery://project.dataset.table
        _, path = source.split("://", 1)
        project, dataset, table = path.split(".")
        return get_config_from_bigquery(
            project_id=project,
            dataset_id=dataset,
            table_id=table,
            **kwargs,
        )

    if source.startswith("s3://"):
        return get_config_from_s3(source, **kwargs)

    if re.search(r"\.csv$", source, re.IGNORECASE):
        return get_config_from_csv(source, **kwargs)

    driver = source.lower()
    if driver in ("mysql", "postgresql"):
        loader = _CONFIG_DISPATCH[driver]
        return loader(**kwargs)

    if driver == "glue":
        return get_config_from_glue_data_catalog(**kwargs)

    raise ValueError(f"Fonte de configuração desconhecida: {source}")
