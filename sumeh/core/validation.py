"""Validation functions - wrapper around existing core functionality."""

from typing import Any, Tuple, List, Dict


def validate(df: Any, rules: List[Dict], **context) -> Tuple[Any, Any]:
    """
    Validate DataFrame against rules.

    This is a wrapper that will call the main validation logic.
    For now, it imports from the root sumeh module.

    Args:
        df: DataFrame to validate
        rules: List of validation rules
        **context: Additional context (conn for DuckDB, etc.)

    Returns:
        Tuple of (invalid_raw, invalid_agg)
    """
    # Import from main module to avoid circular imports
    import sumeh

    return sumeh.validate(df, rules, **context)


def validate_schema(
    df_or_conn: Any, expected: List[Dict[str, Any]], engine: str, **engine_kwargs
) -> Tuple[bool, List[Tuple[str, str]]]:
    """
    Validate schema of DataFrame or connection.

    Args:
        df_or_conn: DataFrame or connection object
        expected: Expected schema definition
        engine: Engine name
        **engine_kwargs: Engine-specific parameters

    Returns:
        Tuple of (is_valid, errors)
    """
    import sumeh

    return sumeh.validate_schema(df_or_conn, expected, engine, **engine_kwargs)
