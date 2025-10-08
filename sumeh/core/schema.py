#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Schema extraction, validation, and type compatibility utilities.
"""

from typing import List, Dict, Any, Tuple
from importlib import import_module
from .utils import __detect_engine


def extract_schema(df, table_name: str = None) -> List[Dict[str, Any]]:
    """
    Extract schema from any DataFrame and return in schema_registry format.

    Uses __detect_engine() to determine the DataFrame type, then delegates
    to the appropriate extraction function.

    Args:
        df: DataFrame from any supported engine
        table_name: Table name to DuckDB

    Returns:
        List of dicts ready to insert into schema_registry:
        [
            {
                'field': 'id',
                'data_type': 'int64',  # RAW type from engine
                'nullable': True,
                'max_length': None,
            },
            ...
        ]

    Raises:
        TypeError: If DataFrame type is unsupported

    Examples:
        >>> import pandas as pd
        >>> df = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob']})
        >>> schema = extract_schema(df)
        >>> len(schema)
        2
        >>> schema[0]['data_type']
        'int64'
    """
    # Import here to avoid circular dependency
    from .utils import __detect_engine
    from importlib import import_module

    # Detect engine using existing utility
    engine_name = __detect_engine(df)
    engine = import_module(f"sumeh.engines.{engine_name}")

    match engine_name:
        case "duckdb_engine":
            if table_name is None:
                raise ValueError("table_name is required for DuckDB engine")
            return engine.extract_schema(df, table_name)
        case _:
            return engine.extract_schema(df)


def validate_schema(
    df_or_conn: Any, expected: List[Dict[str, Any]], **engine_kwargs
) -> Tuple[bool, List[Tuple[str, str]]]:
    """
    Validate DataFrame schema against expected schema.

    Delegates to engine-specific validation implementations.
    Type compatibility checking happens via types_are_compatible().

    Args:
        df_or_conn: DataFrame or connection to validate
        expected: List of dicts from schema_registry with expected schema
        **engine_kwargs: Additional args for engine validation

    Returns:
        Tuple[bool, List[Tuple[str, str]]]:
            - is_valid: True if schema matches
            - errors: List of (field, error_message) tuples

    Examples:
        >>> expected = [
        ...     {'field': 'id', 'data_type': 'int64', 'nullable': False},
        ...     {'field': 'name', 'data_type': 'object', 'nullable': True}
        ... ]
        >>> is_valid, errors = validate_schema(df, expected)
    """
    # Detect engine
    engine_name = __detect_engine(df_or_conn)
    engine = import_module(f"sumeh.engines.{engine_name}")

    # Delegate to engine-specific validation
    return engine.validate_schema(df_or_conn, expected=expected, **engine_kwargs)
