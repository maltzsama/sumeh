#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Schema extraction, validation, and type compatibility utilities.
"""

from typing import Union, List, Dict, Any, Optional, Tuple
from datetime import datetime
from importlib import import_module
from .utils import __detect_engine

# Optional imports for different engines
try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import polars as pl
except ImportError:
    pl = None

try:
    from pyspark.sql import DataFrame as SparkDataFrame
except ImportError:
    SparkDataFrame = None

try:
    import dask.dataframe as dd
except ImportError:
    dd = None

try:
    import duckdb
except ImportError:
    duckdb = None


TYPE_COMPATIBILITY_MAP = {
    # Pandas variants (nullable vs non-nullable)
    ("int64", "int64"): True,
    ("int64", "Int64"): True,
    ("int32", "Int32"): True,
    ("int16", "Int16"): True,
    ("int8", "Int8"): True,
    ("float64", "Float64"): True,
    ("float32", "Float32"): True,
    ("bool", "boolean"): True,
    ("object", "string"): True,
    ("object", "String"): True,
    # Cross-engine: Integers
    ("int64", "integer"): True,  # DuckDB
    ("int64", "bigint"): True,  # SQL
    ("int64", "integertype()"): True,  # PySpark
    ("int32", "int"): True,
    ("int32", "integer"): True,
    # Cross-engine: Floats
    ("float64", "double"): True,  # DuckDB/SQL
    ("float64", "float"): True,
    ("float64", "doubletype()"): True,  # PySpark
    ("float32", "float"): True,
    ("float32", "real"): True,
    # Cross-engine: Strings
    ("object", "varchar"): True,  # DuckDB/SQL
    ("object", "text"): True,
    ("object", "string"): True,
    ("object", "stringtype()"): True,  # PySpark
    ("object", "utf8"): True,  # Polars
    ("string", "varchar"): True,
    ("string", "text"): True,
    # Cross-engine: Booleans
    ("bool", "boolean"): True,
    ("bool", "booleantype()"): True,  # PySpark
    # Cross-engine: Dates/Times
    ("datetime64[ns]", "datetime"): True,
    ("datetime64[ns]", "timestamp"): True,
    ("datetime64[ns]", "timestamptype()"): True,  # PySpark
    ("datetime64", "datetime"): True,
    ("datetime64", "timestamp"): True,
    # Polars specific
    ("utf8", "string"): True,
    ("utf8", "varchar"): True,
    ("utf8", "object"): True,
}


def types_are_compatible(type1: str, type2: str) -> bool:
    """
    Check if two data types are compatible across different engines.

    Performs case-insensitive comparison and checks bidirectional mapping.

    Args:
        type1: First type (e.g., 'int64', 'Int64', 'INTEGER')
        type2: Second type to compare

    Returns:
        bool: True if types are compatible, False otherwise

    Examples:
        >>> types_are_compatible('int64', 'Int64')
        True
        >>> types_are_compatible('INT64', 'integer')
        True
        >>> types_are_compatible('object', 'VARCHAR')
        True
        >>> types_are_compatible('int64', 'float64')
        False
    """
    # Normalize: strip + lowercase
    t1 = type1.strip().lower()
    t2 = type2.strip().lower()

    if t1 == t2:
        return True

    # Check bidirectional mapping
    return TYPE_COMPATIBILITY_MAP.get((t1, t2), False) or TYPE_COMPATIBILITY_MAP.get(
        (t2, t1), False
    )


# ============================================================================
# SCHEMA EXTRACTION
# ============================================================================


def extract_schema_data(
    df: Union["pd.DataFrame", "pl.DataFrame", "SparkDataFrame", "dd.DataFrame", Any],
    table_name: str,
    comment: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Extract schema from any DataFrame and return in schema_registry format.

    Uses __detect_engine() to determine the DataFrame type, then delegates
    to the appropriate extraction function.

    Args:
        df: DataFrame from any supported engine
        table_name: Name of the table for schema_registry
        comment: Optional comment for the table

    Returns:
        List of dicts ready to insert into schema_registry:
        [
            {
                'table_name': 'users',
                'field': 'id',
                'data_type': 'int64',  # RAW type from engine
                'nullable': True,
                'max_length': None,
                'comment': None,
                'created_at': datetime(...),
                'updated_at': datetime(...)
            },
            ...
        ]

    Raises:
        TypeError: If DataFrame type is unsupported

    Examples:
        >>> import pandas as pd
        >>> df = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob']})
        >>> schema = extract_schema_data(df, 'users')
        >>> len(schema)
        2
        >>> schema[0]['data_type']
        'int64'
    """
    # Import here to avoid circular dependency
    from .utils import __detect_engine

    # Detect engine using existing utility
    engine_name = __detect_engine(df)

    # Map engine name to extraction function
    match engine_name:
        case "pandas_engine":
            return _extract_pandas_schema(df, table_name, comment)
        case "polars_engine":
            return _extract_polars_schema(df, table_name, comment)
        case "pyspark_engine":
            return _extract_pyspark_schema(df, table_name, comment)
        case "dask_engine":
            return _extract_dask_schema(df, table_name, comment)
        case "duckdb_engine":
            return _extract_duckdb_schema(df, table_name, comment)
        case _:
            # Shouldn't happen since __detect_engine already raises TypeError
            raise TypeError(f"Unsupported engine: {engine_name}")


def _extract_pandas_schema(
    df: "pd.DataFrame", table_name: str, comment: Optional[str]
) -> List[Dict[str, Any]]:
    """Extract schema from Pandas DataFrame."""
    schema = []
    now = datetime.now()

    for col in df.columns:
        dtype_str = str(df[col].dtype)

        # Check nullability
        nullable = bool(df[col].isnull().any())

        # Calculate max_length for string columns
        max_length = None
        if dtype_str.lower() in ("object", "string"):
            try:
                lengths = df[col].astype(str).str.len()
                max_length = int(lengths.max()) if len(lengths) > 0 else None
            except:
                pass

        schema.append(
            {
                "table_name": table_name,
                "field": col,
                "data_type": dtype_str,  # RAW: 'int64', 'Int64', 'object', etc
                "nullable": nullable,
                "max_length": max_length,
                "comment": comment,
                "created_at": now,
                "updated_at": now,
            }
        )

    return schema


def _extract_polars_schema(
    df: "pl.DataFrame", table_name: str, comment: Optional[str]
) -> List[Dict[str, Any]]:
    """Extract schema from Polars DataFrame."""
    schema = []
    now = datetime.now()

    for col_name, dtype in df.schema.items():
        dtype_str = str(dtype)

        # Check nullability
        nullable = df[col_name].null_count() > 0

        # Max length for strings
        max_length = None
        if "Utf8" in dtype_str or "String" in dtype_str:
            try:
                max_length = df[col_name].str.len_chars().max()
            except:
                pass

        schema.append(
            {
                "table_name": table_name,
                "field": col_name,
                "data_type": dtype_str,  # RAW: 'Int64', 'Utf8', etc
                "nullable": nullable,
                "max_length": max_length,
                "comment": comment,
                "created_at": now,
                "updated_at": now,
            }
        )

    return schema


def _extract_pyspark_schema(
    df: "SparkDataFrame", table_name: str, comment: Optional[str]
) -> List[Dict[str, Any]]:
    """Extract schema from PySpark DataFrame."""
    schema = []
    now = datetime.now()

    for field in df.schema.fields:
        dtype_str = str(field.dataType)

        schema.append(
            {
                "table_name": table_name,
                "field": field.name,
                "data_type": dtype_str,  # RAW: 'IntegerType()', 'StringType()', etc
                "nullable": field.nullable,
                "max_length": None,
                "comment": comment,
                "created_at": now,
                "updated_at": now,
            }
        )

    return schema


def _extract_dask_schema(
    df: "dd.DataFrame", table_name: str, comment: Optional[str]
) -> List[Dict[str, Any]]:
    """Extract schema from Dask DataFrame."""
    schema = []
    now = datetime.now()

    for col in df.columns:
        dtype_str = str(df[col].dtype)

        # Dask is lazy - can't compute nullability here
        nullable = True  # Assume True by default

        schema.append(
            {
                "table_name": table_name,
                "field": col,
                "data_type": dtype_str,
                "nullable": nullable,
                "max_length": None,
                "comment": comment,
                "created_at": now,
                "updated_at": now,
            }
        )

    return schema


def _extract_duckdb_schema(
    df: "duckdb.DuckDBPyRelation", table_name: str, comment: Optional[str]
) -> List[Dict[str, Any]]:
    """Extract schema from DuckDB Relation."""
    schema = []
    now = datetime.now()

    # DuckDB: use .types and .columns
    for col_name, dtype in zip(df.columns, df.types):
        dtype_str = str(dtype)

        schema.append(
            {
                "table_name": table_name,
                "field": col_name,
                "data_type": dtype_str,  # RAW: 'INTEGER', 'VARCHAR', etc
                "nullable": True,  # DuckDB doesn't expose this easily via Relation
                "max_length": None,
                "comment": comment,
                "created_at": now,
                "updated_at": now,
            }
        )

    return schema


def validate_schema(
    df_or_conn: Any, expected: List[Dict[str, Any]], engine: str = None, **engine_kwargs
) -> Tuple[bool, List[Tuple[str, str]]]:
    """
    Validate DataFrame schema against expected schema.

    Delegates to engine-specific validation implementations.
    Type compatibility checking happens via types_are_compatible().

    Args:
        df_or_conn: DataFrame or connection to validate
        expected: List of dicts from schema_registry with expected schema
        engine: Engine name (auto-detected if None)
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
