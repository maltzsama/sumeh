#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Dict, Any, Tuple, Optional
import ast


def __convert_value(value):
    """
    Converts the provided value to the appropriate type (date, float, or int).

    Depending on the format of the input value, it will be converted to a datetime object,
    a floating-point number (float), or an integer (int).

    Args:
        value (str): The value to be converted, represented as a string.

    Returns:
        Union[datetime, float, int]: The converted value, which can be a datetime object, float, or int.

    Raises:
        ValueError: If the value does not match an expected format.
    """
    from datetime import datetime

    value = value.strip()
    try:
        if "-" in value:
            return datetime.strptime(value, "%Y-%m-%d")
        else:
            return datetime.strptime(value, "%d/%m/%Y")
    except ValueError:
        if "." in value:
            return float(value)
        return int(value)


def __try_ast_parse(field_str):
    try:
        parsed = ast.literal_eval(field_str)
        if isinstance(parsed, (list, tuple)):
            return list(parsed)
    except:
        pass
    return None


def __parse_field_list(field_input):

    if not isinstance(field_input, str):
        return field_input

    field_str = field_input.strip()

    if (field_str.startswith('"') and field_str.endswith('"')) or (
        field_str.startswith("'") and field_str.endswith("'")
    ):
        field_str = field_str[1:-1].strip()

    if field_str.startswith("[") and field_str.endswith("]"):
        inner = field_str[1:-1].strip()

        if not inner:
            return ""

        if "," not in inner:
            return inner.strip(" \"'")

        strategies = [
            lambda: __try_ast_parse(field_str),
            lambda: [item.strip(" \"'") for item in inner.split(",") if item.strip()],
        ]

        for strategy in strategies:
            try:
                result = strategy()
                if result and isinstance(result, list) and len(result) > 1:
                    return result
                elif result and isinstance(result, list) and len(result) == 1:
                    return result[0]
            except:
                continue

        return inner.strip(" \"'")

    elif "," in field_str:
        items = [item.strip(" \"'") for item in field_str.split(",") if item.strip()]
        if len(items) > 1:
            return items
        elif len(items) == 1:
            return items[0]
        else:
            return ""

    else:
        return field_str.strip(" \"'")


def __extract_params(rule: dict) -> tuple:
    rule_name = rule["check_type"]
    field = rule["field"]
    raw_value = rule.get("value")

    if isinstance(field, str):
        parsed_field = __parse_field_list(field)

        if isinstance(parsed_field, list) and len(parsed_field) == 1:
            field = parsed_field[0]
        else:
            field = parsed_field

    if isinstance(raw_value, str) and raw_value not in (None, "", "NULL"):
        try:
            value = __convert_value(raw_value)
        except ValueError:
            value = raw_value
    else:
        value = raw_value
    value = value if value not in (None, "", "NULL") else ""

    return field, rule_name, value


SchemaDef = Dict[str, Any]


def __compare_schemas(
    actual: List[SchemaDef],
    expected: List[SchemaDef],
) -> Tuple[bool, List[Dict[str, Any]]]:
    """
    Compare two lists of schema definitions and identify discrepancies.

    Args:
        actual (List[SchemaDef]): The list of actual schema definitions.
        expected (List[SchemaDef]): The list of expected schema definitions.

    Returns:
        Tuple[bool, List[Tuple[str, str]]]: A tuple where the first element is a boolean indicating
        whether the schemas match (True if they match, False otherwise), and the second element
        is a list of tuples describing the discrepancies. Each tuple contains:
            - The field name (str).
            - A description of the discrepancy (str), such as "missing", "type mismatch",
              "nullable but expected non-nullable", or "extra column".

    Notes:
        - A field is considered "missing" if it exists in the expected schema but not in the actual schema.
        - A "type mismatch" occurs if the data type of field in the actual schema does not match
          the expected data type.
        - A field is considered "nullable but expected non-nullable" if it is nullable in the actual
          schema but not nullable in the expected schema.
        - An "extra column" is a field that exists in the actual schema but not in the expected schema.
    """

    exp_map = {c["field"]: c for c in expected}
    act_map = {c["field"]: c for c in actual}
    errors: List[Dict[str, Any]] = []

    for fld, exp in exp_map.items():
        if fld not in act_map:
            errors.append(
                {
                    "field": fld,
                    "error_type": "missing_field",
                    "expected": exp,
                    "actual": None,
                }
            )
            continue

        act = act_map[fld]

        # Type mismatch
        if act["data_type"] != exp["data_type"]:
            errors.append(
                {
                    "field": fld,
                    "error_type": "type_mismatch",
                    "expected_type": exp["data_type"],
                    "actual_type": act["data_type"],
                    "expected": exp,
                    "actual": act,
                }
            )

        # Nullable mismatch
        if act["nullable"] and not exp["nullable"]:
            errors.append(
                {
                    "field": fld,
                    "error_type": "nullable_mismatch",
                    "expected_nullable": exp["nullable"],
                    "actual_nullable": act["nullable"],
                    "expected": exp,
                    "actual": act,
                }
            )

    # Extra fields
    extras = set(act_map) - set(exp_map)
    for fld in extras:
        errors.append(
            {
                "field": fld,
                "error_type": "extra_field",
                "expected": None,
                "actual": act_map[fld],
            }
        )

    return len(errors) == 0, errors


def __parse_databricks_uri(uri: str) -> Dict[str, Optional[str]]:
    """
    Parses a Databricks URI into its catalog, schema, and table components.

    The URI is expected to follow the format `protocol://catalog.schema.table` or
    `protocol://schema.table`. If the catalog is not provided, it will be set to `None`.
    If the schema is not provided, the current database from the active Spark session
    will be used.

    Args:
        uri (str): The Databricks URI to parse.

    Returns:
        Dict[str, Optional[str]]: A dictionary containing the parsed components:
            - "catalog" (Optional[str]): The catalog name, or `None` if not provided.
            - "schema" (Optional[str]): The schema name, or the current database if not provided.
            - "table" (Optional[str]): The table name.
    """
    _, path = uri.split("://", 1)
    parts = path.split(".")
    if len(parts) == 3:
        catalog, schema, table = parts
    elif len(parts) == 2:
        catalog, schema, table = None, parts[0], parts[1]
    else:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        catalog = None
        schema = spark.catalog.currentDatabase()
        table = parts[0]
    return {"catalog": catalog, "schema": schema, "table": table}


def __transform_date_format_in_pattern(date_format):
    date_patterns = {
        "DD": "(0[1-9]|[12][0-9]|3[01])",
        "MM": "(0[1-9]|1[012])",
        "YYYY": "(19|20)\\d\\d",
        "YY": "\\d\\d",
        " ": "\\s",
        ".": "\\.",
    }

    date_pattern = date_format
    for single_format, pattern in date_patterns.items():
        date_pattern = date_pattern.replace(single_format, pattern)

    return date_pattern


def __detect_engine(df, **context):
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
    if "client" in context and "table_ref" in context:
        return "bigquery_engine"

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
        case m if "bigquery" in m:
            return "bigquery_engine"
        case _:
            raise TypeError(f"Unsupported DataFrame type: {type(df)}")


def ___where_statment(query) -> str:
    # monta a base da query
    pass
