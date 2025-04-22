#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Dict, Any, Tuple


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

    exp_map = {c["field"]: c for c in expected}
    act_map = {c["field"]: c for c in actual}

    erros: List[Tuple[str, str]] = []

    # 1. faltantes e mismatches
    for fld, exp in exp_map.items():
        if fld not in act_map:
            erros.append((fld, "missing"))
            continue
        act = act_map[fld]
        # tipo
        if act["data_type"] != exp["data_type"]:
            erros.append(
                (
                    fld,
                    f"type mismatch (got {act['data_type']}, expected {exp['data_type']})",
                )
            )
        # nullability
        if act["nullable"] and not exp["nullable"]:
            erros.append((fld, "nullable but expected non-nullable"))
        # max_length
        if exp.get("max_length") is not None:
            # *aqui você precisaria inspecionar o df…*
            pass

    # 2. campos extras (se quiser)
    extras = set(act_map) - set(exp_map)
    for fld in extras:
        erros.append((fld, "extra column"))

    return (len(erros) == 0, erros)


def __parse_databricks_uri(uri: str) -> Dict[str, Optional[str]]:
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
