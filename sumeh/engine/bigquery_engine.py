# #!/usr/bin/env python
# # -*- coding: utf-8 -*-
from google.cloud import bigquery
from typing import List, Dict, Any, Tuple
from sumeh.services.utils import __compare_schemas


def __bigquery_schema_to_list(table: bigquery.Table) -> List[Dict[str, Any]]:
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

    table = client.get_table(table_ref)
    actual = __bigquery_schema_to_list(table)
    return __compare_schemas(actual, expected)
