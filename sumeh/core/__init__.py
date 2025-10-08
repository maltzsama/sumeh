#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module provides a set of functions and utilities for data validation, schema
retrieval, and summarization. It supports multiple data sources and engines,
including BigQuery, S3, CSV files, MySQL, PostgreSQL, AWS Glue, DuckDB, and Databricks.

Functions:
    get_rules_config(source: str, **kwargs) -> List[Dict[str, Any]]:
        Retrieves configuration rules based on the specified source.

    get_schema_config(source: str, **kwargs) -> List[Dict[str, Any]]:
        Retrieves the schema configuration based on the provided data source.

    validate(df, rules, **context):

    summarize(df, rules: list[dict], **context):

    report(df, rules: list[dict], name: str = "Quality Check"):

Imports:
    cuallee: Provides the `Check` and `CheckLevel` classes for data validation.
    warnings: Used to issue warnings for unknown rule names.
    importlib: Dynamically imports modules based on engine detection.
    typing: Provides type hints for function arguments and return values.
    re: Used for regular expression matching in source string parsing.
    sumeh.core: Contains functions for retrieving configurations and schemas
      from various data sources.
    sumeh.core.utils: Provides utility functions for value conversion and URI parsing.

    The module uses Python's structural pattern matching (`match-case`) to handle
    different data source types and validation rules.
    The `report` function supports a wide range of validation checks, including
    completeness, uniqueness, value comparisons, patterns, and date-related checks.
    The `validate` and `summarize` functions dynamically detect the appropriate engine
    based on the input DataFrame type and delegate the processing to the corresponding
    engine module.
"""

from cuallee import Check, CheckLevel
import warnings
from importlib import import_module
from typing import List, Dict, Any
import re
from .utils import __convert_value, __detect_engine
from sumeh.core.config import (
    get_config_from_s3,
    get_config_from_csv,
    get_config_from_mysql,
    get_config_from_postgresql,
    get_config_from_bigquery,
    get_config_from_glue_data_catalog,
    get_config_from_duckdb,
    get_config_from_databricks,
    get_schema_from_duckdb,
    get_schema_from_bigquery,
    get_schema_from_s3,
    get_schema_from_csv,
    get_schema_from_mysql,
    get_schema_from_postgresql,
    get_schema_from_databricks,
    get_schema_from_glue,
)


def get_rules_config(source: str, **kwargs) -> List[Dict[str, Any]]:
    """
    Retrieve configuration rules based on the specified source.

    Dispatches to the appropriate loader according to the format of `source`,
    returning a list of parsed rule dictionaries.

    Supported sources:
      - `bigquery://<project>.<dataset>.<table>`
      - `s3://<bucket>/<path>`
      - `<file>.csv`
      - `"mysql"` or `"postgresql"` (requires host/user/etc. in kwargs)
      - `"glue"` (AWS Glue Data Catalog)
      - `duckdb://<db_path>.<table>`
      - `databricks://<catalog>.<schema>.<table>`

    Args:
        source (str):
            Identifier of the rules configuration location. Determines which
            handler is invoked.
        **kwargs:
            Loader-specific parameters (e.g. `host`, `user`, `password`,
            `connection`, `query`, `delimiter`).

    Returns:
        List[Dict[str, Any]]:
            A list of dictionaries, each representing a validation rule with keys
            like `"field"`, `"check_type"`, `"value"`, `"threshold"`, and `"execute"`.

    Raises:
        ValueError:
            If `source` does not match any supported format.
    """
    match source:
        case "bigquery":
            required_params = ["project_id", "dataset_id", "table_id"]
            for param in required_params:
                if param not in kwargs:
                    raise ValueError(f"BigQuery source requires '{param}' in kwargs")

            return get_config_from_bigquery(**kwargs)

        case s if s.startswith("s3://"):
            return get_config_from_s3(s, **kwargs)

        case s if re.search(r"\.csv$", s, re.IGNORECASE):
            delimiter = kwargs.pop("delimiter", None)
            return get_config_from_csv(s, delimiter=delimiter, **kwargs)

        case "mysql":
            if "conn" not in kwargs:
                required_params = ["host", "user", "password", "database"]
                for param in required_params:
                    if param not in kwargs:
                        raise ValueError(
                            f"MySQL schema requires 'conn' OR all of {required_params} in kwargs"
                        )

            return get_config_from_mysql(**kwargs)

        case "postgresql":
            if "conn" not in kwargs:
                required_params = [
                    "host",
                    "user",
                    "password",
                    "database",
                    "schema",
                    "table",
                ]
                for param in required_params:
                    if param not in kwargs:
                        raise ValueError(
                            f"PostgreSQL source requires '{param}' in kwargs"
                        )

            return get_config_from_postgresql(**kwargs)

        case "glue":
            required_params = ["glue_context", "database_name", "table_name"]
            for param in required_params:
                if param not in kwargs:
                    raise ValueError(f"Glue source requires '{param}' in kwargs")
            return get_config_from_glue_data_catalog(**kwargs)

        case s if s.startswith("duckdb://"):
            _, path = s.split("://", 1)
            _, table = path.rsplit(".", 1)
            conn = kwargs.pop("conn", None)
            return get_config_from_duckdb(
                conn=conn,
                table=table,
            )

        case "databricks":
            required_params = ["spark", "catalog", "schema", "table"]
            for param in required_params:
                if param not in kwargs:
                    raise ValueError(f"Databricks source requires '{param}' in kwargs")

            return get_config_from_databricks(**kwargs)

        case _:
            raise ValueError(f"Unknown source: {source}")


def get_schema_config(source: str, **kwargs) -> List[Dict[str, Any]]:
    """
    Retrieve the schema configuration based on the provided data source.

    This function reads from a schema_registry table/file to get the expected
    schema for a given table. Supports various data sources such as BigQuery,
    S3, CSV files, MySQL, PostgreSQL, AWS Glue, DuckDB, and Databricks.

    Args:
        source (str):
            A string representing the data source. Supported formats:
            - `bigquery`: BigQuery source
            - `s3://<bucket>/<path>`: S3 CSV file
            - `<file>.csv`: Local CSV file
            - `mysql`: MySQL database
            - `postgresql`: PostgreSQL database
            - `glue`: AWS Glue Data Catalog
            - `duckdb`: DuckDB database
            - `databricks`: Databricks Unity Catalog

        **kwargs: Source-specific parameters. Common ones:
            - table (str): Table name to look up (REQUIRED for all sources)
            - environment (str): Environment filter (default: 'prod')
            - query (str): Additional WHERE filters (optional)

            For BigQuery: project_id, dataset_id, table_id
            For MySQL/PostgreSQL: host, user, password, database OR conn
            For Glue: glue_context, database_name, table_name
            For DuckDB: conn, table
            For Databricks: spark, catalog, schema, table
            For CSV/S3: file_path/s3_path, table

    Returns:
        List[Dict[str, Any]]: Schema configuration from schema_registry

    Raises:
        ValueError: If source format is invalid or required params are missing

    Examples:
        >>> get_schema_config("bigquery", project_id="proj", dataset_id="ds", table_id="users")
        >>> get_schema_config("mysql", conn=my_conn, table="users")
        >>> get_schema_config("s3://bucket/registry.csv", table="users", environment="prod")
    """
    match source:
        case "bigquery":
            required_params = ["project_id", "dataset_id", "table_id"]
            for param in required_params:
                if param not in kwargs:
                    raise ValueError(f"BigQuery schema requires '{param}' in kwargs")
            return get_schema_from_bigquery(**kwargs)

        case s if s.startswith("s3://"):
            if "table" not in kwargs:
                raise ValueError("S3 schema requires 'table' in kwargs")
            return get_schema_from_s3(s, **kwargs)

        case s if re.search(r"\.csv$", s, re.IGNORECASE):
            if "table" not in kwargs:
                raise ValueError("CSV schema requires 'table' in kwargs")
            return get_schema_from_csv(s, **kwargs)

        case "mysql":

            if "conn" not in kwargs:
                required_params = ["host", "user", "password", "database"]
                for param in required_params:
                    if param not in kwargs:
                        raise ValueError(
                            f"MySQL schema requires 'conn' OR all of {required_params} in kwargs"
                        )
            if "table" not in kwargs:
                raise ValueError("MySQL schema requires 'table' in kwargs")
            return get_schema_from_mysql(**kwargs)

        case "postgresql":

            if "conn" not in kwargs:
                required_params = ["host", "user", "password", "database"]
                for param in required_params:
                    if param not in kwargs:
                        raise ValueError(
                            f"PostgreSQL schema requires 'conn' OR all of {required_params} in kwargs"
                        )
            if "table" not in kwargs:
                raise ValueError("PostgreSQL schema requires 'table' in kwargs")
            return get_schema_from_postgresql(**kwargs)

        case "glue":
            required_params = ["glue_context", "database_name", "table_name"]
            for param in required_params:
                if param not in kwargs:
                    raise ValueError(f"Glue schema requires '{param}' in kwargs")
            return get_schema_from_glue(**kwargs)

        case "duckdb":
            required_params = ["conn", "table"]
            for param in required_params:
                if param not in kwargs:
                    raise ValueError(f"DuckDB schema requires '{param}' in kwargs")
            return get_schema_from_duckdb(**kwargs)

        case "databricks":
            required_params = ["spark", "catalog", "schema", "table"]
            for param in required_params:
                if param not in kwargs:
                    raise ValueError(f"Databricks schema requires '{param}' in kwargs")
            return get_schema_from_databricks(**kwargs)

        case _:
            raise ValueError(f"Unknown source: {source}")


def validate(df, rules, **context):
    """
    Validates a DataFrame against a set of rules using the appropriate engine.

    This function dynamically detects the engine to use based on the input
    DataFrame and delegates the validation process to the corresponding engine's
    implementation.

    Args:
        df (DataFrame): The input DataFrame to be validated.
        rules (list or dict): The validation rules to be applied to the DataFrame.
        **context: Additional context parameters that may be required by the engine.
            - conn (optional): A database connection object, required for certain engines
              like "duckdb_engine".

    Returns:
        bool or dict: The result of the validation process. The return type and structure
        depend on the specific engine's implementation.

    Raises:
        ImportError: If the required engine module cannot be imported.
        AttributeError: If the detected engine does not have a `validate` method.

    Notes:
        - The engine is dynamically determined based on the DataFrame type or other
          characteristics.
        - For "duckdb_engine", a database connection object should be provided in the
          context under the key "conn".
    """
    engine_name = __detect_engine(df)
    engine = import_module(f"sumeh.engines.{engine_name}")

    match engine_name:
        case "duckdb_engine":
            return engine.validate(df, rules, context.get("conn"))
        case _:
            return engine.validate(df, rules)


def summarize(df, rules: list[dict], **context):
    """
    Summarizes a DataFrame based on the provided rules and context.

    This function dynamically detects the appropriate engine to use for summarization
    based on the type of the input DataFrame. It delegates the summarization process
    to the corresponding engine module.

    Args:
        df: The input DataFrame to be summarized. The type of the DataFrame determines
            the engine used for summarization.
        rules (list[dict]): A list of dictionaries defining the summarization rules.
            Each dictionary specifies the operations or transformations to be applied.
        **context: Additional context parameters required by specific engines. Common
            parameters include:
            - conn: A database connection object (used by certain engines like DuckDB).
            - total_rows: The total number of rows in the DataFrame (optional).

    Returns:
        The summarized DataFrame as processed by the appropriate engine.

    Raises:
        TypeError: If the type of the input DataFrame is unsupported.

    Notes:
        - The function uses the `__detect_engine` method to determine the engine name
          based on the input DataFrame.
        - Supported engines are dynamically imported from the `sumeh.engines` package.
        - The "duckdb_engine" case requires a database connection (`conn`) to be passed
          in the context.

    Example:
        summarized_df = summarize(df, rules=[{"operation": "sum", "column": "sales"}], conn=my_conn)
    """
    engine_name = __detect_engine(df)
    engine = import_module(f"sumeh.engines.{engine_name}")
    match engine_name:
        case "duckdb_engine":
            return engine.summarize(
                df_rel=df,
                rules=rules,
                conn=context.get("conn"),
                total_rows=context.get("total_rows"),
            )
        case _:
            return engine.summarize(df, rules, total_rows=context.get("total_rows"))


# TODO: refactor to get better performance or remove
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
