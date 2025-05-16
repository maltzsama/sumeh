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

    __detect_engine(df) -> str:

    validate_schema(df_or_conn: Any, expected: List[Dict[str, Any]], engine: str, **engine_kwargs) -> Tuple[bool, List[Tuple[str, str]]]:

    validate(df, rules, **context):

    summarize(df, rules: list[dict], **context):

    report(df, rules: list[dict], name: str = "Quality Check"):

Constants:
    _CONFIG_DISPATCH: A dictionary mapping data source types (e.g., "mysql", "postgresql")
      to their respective configuration retrieval functions.

Imports:
    cuallee: Provides the `Check` and `CheckLevel` classes for data validation.
    warnings: Used to issue warnings for unknown rule names.
    importlib: Dynamically imports modules based on engine detection.
    typing: Provides type hints for function arguments and return values.
    re: Used for regular expression matching in source string parsing.
    sumeh.services.config: Contains functions for retrieving configurations and schemas
      from various data sources.
    sumeh.services.utils: Provides utility functions for value conversion and URI parsing.

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
from typing import List, Dict, Any, Tuple
import re
from .services.utils import __convert_value, __parse_databricks_uri
from sumeh.services.config import (
    get_config_from_s3,
    get_config_from_csv,
    get_config_from_mysql,
    get_config_from_postgresql,
    get_config_from_bigquery,
    get_config_from_glue_data_catalog,
    get_config_from_duckdb,
    get_config_from_databricks,
)

_CONFIG_DISPATCH = {
    "mysql": get_config_from_mysql,
    "postgresql": get_config_from_postgresql,
}


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
            `connection`, `query`).

    Returns:
        List[Dict[str, Any]]:
            A list of dictionaries, each representing a validation rule with keys
            like `"field"`, `"check_type"`, `"value"`, `"threshold"`, and `"execute"`.

    Raises:
        ValueError:
            If `source` does not match any supported format.
    """
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

        case s if s.startswith("duckdb://"):
            _, path = s.split("://", 1)
            db_path, table = path.rsplit(".", 1)
            conn = kwargs.pop("conn", None)
            return get_config_from_duckdb(
                conn=conn,
                table=table,
            )

        case s if s.startswith("databricks://"):
            parts = __parse_databricks_uri(s)
            return get_config_from_databricks(
                catalog=parts["catalog"],
                schema=parts["schema"],
                table=parts["table"],
                **kwargs,
            )

        case _:
            raise ValueError(f"Unknow source: {source}")


def get_schema_config(source: str, **kwargs) -> List[Dict[str, Any]]:
    """
    Retrieve the schema configuration based on the provided data source.

    This function determines the appropriate method to extract schema information
    based on the format or type of the `source` string. It supports various data
    sources such as BigQuery, S3, CSV files, MySQL, PostgreSQL, AWS Glue, DuckDB,
    and Databricks.

    Args:
        source (str):
            A string representing the data source. The format of the
            string determines the method used to retrieve the schema. Supported
            formats include: `bigquery://<project>.<dataset>.<table>`, `s3://<bucket>/<path>`,
            `<file>.csv`, `mysql`, `postgresql`, `glue`, `duckdb://<db_path>.<table>`,
            `databricks://<catalog>.<schema>.<table>`
        **kwargs: Additional keyword arguments required by specific schema
            retrieval methods. For example:
            For DuckDB: `conn` (a database connection object).
            For other sources: Additional parameters specific to the source.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries representing the schema
        configuration. Each dictionary contains details about a column in the
        schema.

    Raises:
        ValueError: If the `source` string does not match any supported format.

    Examples:
        >>> get_schema_config("bigquery://my_project.my_dataset.my_table")
        >>> get_schema_config("s3://my_bucket/my_file.csv")
        >>> get_schema_config("mysql", host="localhost", user="root", password="password")
    """
    match source:
        case s if s.startswith("bigquery://"):
            _, path = s.split("://", 1)
            project, dataset, table = path.split(".")
            return get_schema_from_bigquery(
                project_id=project,
                dataset_id=dataset,
                table_id=table,
                **kwargs,
            )

        case s if s.startswith("s3://"):
            return get_schema_from_s3(s, **kwargs)

        case s if re.search(r"\.csv$", s, re.IGNORECASE):
            return get_schema_from_csv(s, **kwargs)

        case "mysql":
            return get_schema_from_mysql(**kwargs)

        case "postgresql":
            return get_schema_from_postgresql(**kwargs)

        case "glue":
            return get_schema_from_glue(**kwargs)

        case s if s.startswith("duckdb://"):
            conn = kwargs.pop("conn")
            _, path = s.split("://", 1)
            db_path, table = path.rsplit(".", 1)
            return get_schema_from_duckdb(conn=conn, table=table)

        case s if s.startswith("databricks://"):
            parts = __parse_databricks_uri(s)
            return get_schema_from_databricks(
                catalog=parts["catalog"],
                schema=parts["schema"],
                table=parts["table"],
                **kwargs,
            )

        case _:
            raise ValueError(f"Unknow source: {source}")


def __detect_engine(df):
    """
    Detects the engine type of the given DataFrame based on its module.

    Args:
        df: The DataFrame object whose engine type is to be detected.

    Returns:
        str: A string representing the detected engine type. Possible values are:
            - "pyspark_engine" for PySpark DataFrames
            - "dask_engine" for Dask DataFrames
            - "polars_engine" for Polars DataFrames
            - "pandas_engine" for Pandas DataFrames
            - "duckdb_engine" for DuckDB or BigQuery DataFrames

    Raises:
        TypeError: If the DataFrame type is unsupported.
    """
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
        case m if m.startswith("bigquery"):
            return "duckdb_engine"
        case _:
            raise TypeError(f"Unsupported DataFrame type: {type(df)}")


def validate_schema(
    df_or_conn: Any, expected: List[Dict[str, Any]], engine: str, **engine_kwargs
) -> Tuple[bool, List[Tuple[str, str]]]:
    """
    Validates the schema of a given data source or connection against an expected schema.

    Args:
        df_or_conn (Any): The data source or connection to validate. This can be a DataFrame,
                          database connection, or other supported data structure.
        expected (List[Dict[str, Any]]): A list of dictionaries defining the expected schema.
                                         Each dictionary should describe a column or field,
                                         including its name, type, and other attributes.
        engine (str): The name of the engine to use for validation. This determines the
                      specific validation logic to apply based on the data source type.
        **engine_kwargs: Additional keyword arguments to pass to the engine's validation logic.

    Returns:
        Tuple[bool, List[Tuple[str, str]]]: A tuple where the first element is a boolean indicating
                                            whether the schema is valid, and the second element is
                                            a list of tuples containing error messages for any
                                            validation failures. Each tuple consists of the field
                                            name and the corresponding error message.
    """
    engine_name = __detect_engine(df_or_conn)
    engine = import_module(f"sumeh.engine.{engine_name}")
    return engine.validate_schema(df_or_conn, expected=expected, **engine_kwargs)


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
    engine = import_module(f"sumeh.engine.{engine_name}")

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
        - Supported engines are dynamically imported from the `sumeh.engine` package.
        - The "duckdb_engine" case requires a database connection (`conn`) to be passed
          in the context.

    Example:
        >>> summarized_df = summarize(df, rules=[{"operation": "sum", "column": "sales"}], conn=my_conn)
    """
    engine_name = __detect_engine(df)
    engine = import_module(f"sumeh.engine.{engine_name}")
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

    raise TypeError(f"Unsupported DataFrame type: {type(df)}")


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
