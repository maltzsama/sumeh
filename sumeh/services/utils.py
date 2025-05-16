#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Dict, Any, Tuple, Optional
from typing import Optional


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


SchemaDef = Dict[str, Any]


def __compare_schemas(
    actual: List[SchemaDef],
    expected: List[SchemaDef],
) -> Tuple[bool, List[Tuple[str, str]]]:
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
        - A "type mismatch" occurs if the data type of a field in the actual schema does not match
          the expected data type.
        - A field is considered "nullable but expected non-nullable" if it is nullable in the actual
          schema but not nullable in the expected schema.
        - An "extra column" is a field that exists in the actual schema but not in the expected schema.
    """

    exp_map = {c["field"]: c for c in expected}
    act_map = {c["field"]: c for c in actual}

    erros: List[Tuple[str, str]] = []

    for fld, exp in exp_map.items():
        if fld not in act_map:
            erros.append((fld, "missing"))
            continue
        act = act_map[fld]
        if act["data_type"] != exp["data_type"]:
            erros.append(
                (
                    fld,
                    f"type mismatch (got {act['data_type']}, expected {exp['data_type']})",
                )
            )

        if act["nullable"] and not exp["nullable"]:
            erros.append((fld, "nullable but expected non-nullable"))

        if exp.get("max_length") is not None:
            pass

    # 2. campos extras (se quiser)
    extras = set(act_map) - set(exp_map)
    for fld in extras:
        erros.append((fld, "extra column"))

    return len(erros) == 0, erros


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
