#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module provides utility functions for working with Google BigQuery schemas and validating them.

Functions:
    __bigquery_schema_to_list(table: bigquery.Table) -> List[Dict[str, Any]]:

    validate_schema(
        Validates the schema of a BigQuery table against an expected schema.

Dependencies:
    - google.cloud.bigquery: Provides the BigQuery client and table schema functionality.
    - typing: Used for type annotations.
    - sumeh.services.utils.__compare_schemas: A utility function for comparing schemas.
"""

from google.cloud import bigquery
from typing import List, Dict, Any, Tuple
from sumeh.services.utils import __compare_schemas


def __bigquery_schema_to_list(table: bigquery.Table) -> List[Dict[str, Any]]:
    """
    Converts a BigQuery table schema into a list of dictionaries representing the schema fields.

    Args:
        table (bigquery.Table): The BigQuery table whose schema is to be converted.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries where each dictionary represents a field in the schema.
            Each dictionary contains the following keys:
                - "field": The name of the field.
                - "data_type": The data type of the field, converted to lowercase.
                - "nullable": A boolean indicating whether the field is nullable.
                - "max_length": Always set to None (reserved for future use).
    """
    return [
        {
            "field": fld.name,
            "data_type": fld.field_type.lower(),
            "nullable": fld.is_nullable,
            "max_length": None,
        }
        for fld in table.schema
    ]


def validate_schema(
    client: bigquery.Client, expected: List[Dict[str, Any]], table_ref: str
) -> Tuple[bool, List[Tuple[str, str]]]:
    """
    Validates the schema of a BigQuery table against an expected schema.

    Args:
        client (bigquery.Client): The BigQuery client used to interact with the BigQuery service.
        expected (List[Dict[str, Any]]): The expected schema as a list of dictionaries, where each dictionary
            represents a field with its attributes (e.g., name, type, mode).
        table_ref (str): The reference to the BigQuery table (e.g., "project.dataset.table").

    Returns:
        Tuple[bool, List[Tuple[str, str]]]: A tuple where the first element is a boolean indicating whether
            the actual schema matches the expected schema, and the second element is a list of tuples
            describing the differences (if any) between the schemas. Each tuple contains a description
            of the difference and the corresponding field name.
    """

    table = client.get_table(table_ref)
    actual = __bigquery_schema_to_list(table)
    return __compare_schemas(actual, expected)
