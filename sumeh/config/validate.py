"""
Schema validators for rules configuration.

Validates that DataFrames/dicts have correct structure before conversion.
"""

from typing import Set, Any


def validate_rules_schema(df: Any) -> None:
    """
    Validate that DataFrame has required columns for rules.

    Only validates presence of required columns.
    Extra columns are silently ignored (common for metadata/audit fields).

    Args:
        df: DataFrame (pandas, polars, spark, etc)

    Raises:
        ValueError: If required columns are missing
        TypeError: If input is not a DataFrame

    Example:
        >>> # Minimal DataFrame
        >>> df = pd.DataFrame([
        ...     {'field': 'email', 'check_type': 'is_complete'}
        ... ])
        >>> validate_rules_schema(df)  # OK
        >>>
        >>> # DataFrame with extra columns (common in real world)
        >>> df = pd.DataFrame([
        ...     {
        ...         'catalog_name': 'prod',
        ...         'table_name': 'users',
        ...         'field': 'email',
        ...         'check_type': 'is_complete',
        ...         'created_at': '2024-01-01',
        ...         'created_by': 'john',
        ...         'active': True
        ...     }
        ... ])
        >>> validate_rules_schema(df)  # OK - extra columns ignored
    """
    # Get columns based on DataFrame type
    if hasattr(df, "columns"):
        columns = set(df.columns)
    else:
        raise TypeError(
            f"Expected DataFrame, got {type(df)}. "
            f"Supported: pandas, polars, pyspark DataFrames"
        )

    # Required columns
    required = {"field", "check_type"}

    # Check required columns
    missing = required - columns
    if missing:
        raise ValueError(
            f"Missing required columns: {missing}\n"
            f"Required: {sorted(required)}\n"
            f"Found: {sorted(columns)}\n"
            f"\n"
            f"Note: Extra columns (metadata, audit, etc) are OK and will be ignored."
        )

    # Extra columns are silently ignored
    # This is intentional - users often have metadata columns like:
    # - catalog_name, schema_name, table_name (context)
    # - created_at, updated_at, created_by (audit)
    # - active, environment, priority (control)


def validate_rule_dict(rule: dict) -> None:
    """
    Validate that a single rule dict has correct structure.

    Args:
        rule: Dictionary with rule configuration

    Raises:
        ValueError: If rule is invalid

    Example:
        >>> rule = {'field': 'email', 'check_type': 'is_complete'}
        >>> validate_rule_dict(rule)  # OK
        >>>
        >>> rule = {'field': 'email'}  # Missing check_type
        >>> validate_rule_dict(rule)  # Raises ValueError
    """
    if not isinstance(rule, dict):
        raise TypeError(f"Expected dict, got {type(rule)}")

    # Required keys
    required = {"field", "check_type"}

    # Check required keys
    missing = required - set(rule.keys())
    if missing:
        raise ValueError(
            f"Missing required keys in rule: {missing}\n"
            f"Required: {sorted(required)}\n"
            f"Got: {sorted(rule.keys())}"
        )

    # Validate field
    field = rule.get("field")
    if not field or (isinstance(field, str) and not field.strip()):
        raise ValueError("'field' cannot be empty")

    # Validate check_type
    check_type = rule.get("check_type")
    if not check_type or (isinstance(check_type, str) and not check_type.strip()):
        raise ValueError("'check_type' cannot be empty")
