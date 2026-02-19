"""
Sumeh Schema Gatekeeper.
O(1) Structural validation before O(N) Data Quality processing.
"""

import json
import re
from typing import Any, Dict, List, Union

from sumeh.core.schema.models import SchemaDef, ColumnDef, SchemaReport
from sumeh.core.utils import require_capability

# ============================================================================
# Type Mapping - DataFrame engines only (Pandas, Polars, PySpark, Dask, Ray)
# ============================================================================

TYPE_MAP = {
    # Integers
    "int": "integer",
    "int8": "integer",
    "int16": "integer",
    "int32": "integer",
    "int64": "integer",
    "integer": "integer",
    "long": "integer",
    "bigint": "integer",
    "tinyint": "integer",
    "smallint": "integer",
    # Unsigned integers (Polars)
    "uint8": "integer",
    "uint16": "integer",
    "uint32": "integer",
    "uint64": "integer",
    # Floats
    "float": "float",
    "float16": "float",
    "float32": "float",
    "float64": "float",
    "double": "float",
    "decimal": "float",
    # Strings
    "str": "string",
    "string": "string",
    "object": "string",
    "utf8": "string",
    "utf": "string",
    "categorical": "string",
    # Boolean
    "bool": "boolean",
    "boolean": "boolean",
    # DateTime
    "datetime": "datetime",
    "datetime64": "datetime",
    "timestamp": "datetime",
    "date": "datetime",
    "time": "datetime",
    # Complex types
    "list": "array",
    "array": "array",
    "struct": "complex",
    "dict": "complex",
    "map": "complex",
}


def validate(df_target: Any, expected_schema: SchemaDef) -> SchemaReport:
    """
    Validate DataFrame schema against expected schema definition.

    Args:
        df_target: DataFrame to validate (PySpark, Pandas, Polars, Dask)
        expected_schema: Schema definition with expected structure

    Returns:
        SchemaReport with validation results
    """

    require_capability(df_target, "schema_validation", "Schema validation")

    actual_cols = _extract_actual_schema(df_target)

    report = _validate_recursive(expected_schema.columns, actual_cols)

    if expected_schema.strict_columns:
        expected_keys = {c.name for c in expected_schema.columns}
        report.extra_cols = [k for k in actual_cols.keys() if k not in expected_keys]
        if report.extra_cols:
            report.passed = False

    return report


def _validate_recursive(
    expected_cols: List[ColumnDef], actual_cols: Dict[str, dict], parent_path: str = ""
) -> SchemaReport:
    """
    Recursive validation for nested structures (Structs, Arrays).

    Args:
        expected_cols: Expected column definitions
        actual_cols: Actual extracted schema
        parent_path: Path prefix for nested fields

    Returns:
        SchemaReport with validation results
    """
    missing_cols = []
    type_errors = {}
    metadata_errors = {}

    for col_def in expected_cols:
        full_path = f"{parent_path}.{col_def.name}" if parent_path else col_def.name
        actual = actual_cols.get(col_def.name)

        # Missing column check
        if not actual:
            if not col_def.is_optional:
                missing_cols.append(full_path)
            continue

        # Type validation
        canon_expected = _to_canonical_type(col_def.expected_type)
        canon_actual = _to_canonical_type(actual["raw_type"])

        if canon_expected != "unknown" and canon_expected != canon_actual:
            type_errors[full_path] = (
                f"Expected {canon_expected}, got {canon_actual} ({actual['raw_type']})"
            )
            continue

        # Array element type validation
        if col_def.element_type and actual.get("element_type"):
            expected_elem = _to_canonical_type(col_def.element_type)
            actual_elem = _to_canonical_type(actual["element_type"])
            if expected_elem != actual_elem:
                type_errors[full_path] = (
                    f"Array element type mismatch: expected {expected_elem}, "
                    f"got {actual_elem}"
                )

        # Metadata validation
        actual_comment = actual.get("comment", "").strip()
        if col_def.require_comment and not actual_comment:
            metadata_errors[full_path] = "Missing required column comment."
        if col_def.expected_comment and col_def.expected_comment != actual_comment:
            metadata_errors[full_path] = (
                f"Comment mismatch. Expected: '{col_def.expected_comment}'"
            )

        # Nullability validation
        actual_nullable = actual.get("nullable", True)
        if col_def.nullable is False and actual_nullable is True:
            type_errors[full_path] = (
                "Schema violation: Contract requires NOT NULL, "
                "but source column allows nulls."
            )

        # Recursive validation for nested structures
        if col_def.fields and actual.get("nested_fields"):
            child_report = _validate_recursive(
                expected_cols=col_def.fields,
                actual_cols=actual["nested_fields"],
                parent_path=full_path,
            )
            missing_cols.extend(child_report.missing_cols)
            type_errors.update(child_report.type_errors)
            metadata_errors.update(child_report.metadata_errors)

    passed = not (missing_cols or type_errors or metadata_errors)
    return SchemaReport(
        passed=passed,
        missing_cols=missing_cols,
        type_errors=type_errors,
        metadata_errors=metadata_errors,
    )


def _to_canonical_type(raw_type: str) -> str:
    """
    Convert DataFrame dtype to canonical type.

    Supports: Pandas, Polars, PySpark, Dask, Ray dtypes.

    Args:
        raw_type: Raw dtype string from DataFrame

    Returns:
        Canonical type name (integer, float, string, etc)
    """
    raw = str(raw_type).lower()

    # Remove numbers, brackets, parentheses, and Pandas [ns] suffix
    clean = re.sub(r"[\d\[\]()<>,\s]|ns$", "", raw)

    # Remove 'type' suffix if present
    if clean.endswith("type"):
        clean = clean[:-4]

    return TYPE_MAP.get(clean, "unknown")


def _extract_actual_schema(obj: Any) -> Dict[str, dict]:
    """
    Extract schema from DataFrame object.

    Supports: PySpark, Pandas, Polars, Dask DataFrames.

    Args:
        obj: DataFrame object or dict (for testing)

    Returns:
        Dictionary mapping column names to schema info

    Raises:
        TypeError: If object type is not supported
    """
    extracted = {}

    # Dict (for testing/mocking)
    if isinstance(obj, dict):
        return {
            str(k): {"raw_type": str(v), "comment": "", "nullable": True}
            for k, v in obj.items()
        }

    # PySpark DataFrame
    if hasattr(obj, "schema") and hasattr(obj.schema, "fields"):
        for field in obj.schema.fields:
            field_data = {
                "raw_type": field.dataType.simpleString(),
                "comment": (
                    field.metadata.get("comment", "")
                    if hasattr(field, "metadata")
                    else ""
                ),
                "nullable": field.nullable,
            }

            # Extract array element type
            if hasattr(field.dataType, "elementType"):
                field_data["element_type"] = field.dataType.elementType.simpleString()

            # Extract nested struct fields
            dtype = field.dataType
            if hasattr(dtype, "fields"):
                field_data["nested_fields"] = _extract_actual_schema(dtype)
            elif hasattr(dtype, "elementType") and hasattr(dtype.elementType, "fields"):
                field_data["nested_fields"] = _extract_actual_schema(dtype.elementType)

            extracted[field.name] = field_data

        return extracted

    # Pandas / Polars / Dask DataFrame
    if hasattr(obj, "dtypes") and hasattr(obj, "columns"):
        for col in obj.columns:
            dtype_str = str(obj[col].dtype)

            field_data = {
                "raw_type": dtype_str,
                "comment": "",
                "nullable": True,  # Pandas doesn't enforce NOT NULL at schema level
            }

            # Extract array element type (Polars: List[Int64])
            if dtype_str.startswith("List"):
                match = re.search(r"List\[(\w+)\]", dtype_str)
                if match:
                    field_data["element_type"] = match.group(1).lower()

            extracted[str(col)] = field_data

        return extracted

    # Unsupported type
    supported_types = (
        "PySpark DataFrame, Pandas DataFrame, Polars DataFrame, Dask DataFrame, or dict"
    )
    raise TypeError(
        f"Cannot extract schema from {type(obj).__name__}. "
        f"Supported types: {supported_types}"
    )


def extract_schema(obj: Any, as_json: bool = False) -> Union[Dict[str, Any], str]:
    """
    Extract schema from DataFrame and convert to SchemaDef format.

    Args:
        obj: DataFrame object
        as_json: If True, return JSON string instead of dict

    Returns:
        Schema definition as dict or JSON string

    Raises:
        TypeError: If object type is not supported
    """
    extracted = {}

    # Dict (for testing)
    if isinstance(obj, dict):
        extracted = {
            str(k): {"type": _to_canonical_type(str(v))} for k, v in obj.items()
        }
        return json.dumps(extracted, indent=2) if as_json else extracted

    # PySpark DataFrame
    if hasattr(obj, "schema") and hasattr(obj.schema, "fields"):
        for field in obj.schema.fields:
            dtype = field.dataType

            field_data = {
                "type": _to_canonical_type(dtype.simpleString()),
                "nullable": field.nullable,
            }

            # Extract comment
            comment = (
                field.metadata.get("comment", "") if hasattr(field, "metadata") else ""
            )
            if comment:
                field_data["expected_comment"] = comment
                field_data["require_comment"] = True

            # Extract array element type
            if hasattr(dtype, "elementType"):
                field_data["element_type"] = _to_canonical_type(
                    dtype.elementType.simpleString()
                )

            # Extract nested fields
            if hasattr(dtype, "fields"):
                field_data["fields"] = extract_schema(dtype)
            elif hasattr(dtype, "elementType") and hasattr(dtype.elementType, "fields"):
                field_data["fields"] = extract_schema(dtype.elementType)
                field_data["type"] = "array"

            extracted[field.name] = field_data

        return json.dumps(extracted, indent=2) if as_json else extracted

    # Pandas / Polars / Dask
    if hasattr(obj, "dtypes") and hasattr(obj, "columns"):
        for col in obj.columns:
            dtype_str = str(obj[col].dtype)

            field_data = {"type": _to_canonical_type(dtype_str)}

            # Extract array element type (Polars)
            if dtype_str.startswith("List"):
                match = re.search(r"List\[(\w+)\]", dtype_str)
                if match:
                    field_data["element_type"] = _to_canonical_type(match.group(1))

            extracted[str(col)] = field_data

        return json.dumps(extracted, indent=2) if as_json else extracted

    # Unsupported type
    supported_types = (
        "PySpark DataFrame, Pandas DataFrame, Polars DataFrame, Dask DataFrame, or dict"
    )
    raise TypeError(
        f"Cannot extract schema from {type(obj).__name__}. "
        f"Supported types: {supported_types}"
    )
