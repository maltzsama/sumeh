"""
PyFlink validation registry.

Maps check_type to UDF names and supported validations.
"""

# Map check_type to UDF function name
VALIDATION_REGISTRY = {
    # Completeness
    "is_complete": "sumeh_check_complete",
    "are_complete": "sumeh_check_complete",  # Multi-field not supported in streaming
    # Comparison
    "is_equal": "sumeh_check_equal",
    "is_greater_than": "sumeh_check_greater",
    "is_less_than": "sumeh_check_less",
    "is_positive": "sumeh_check_positive",
    "is_negative": "sumeh_check_negative",
    # Membership
    "is_in": "sumeh_check_in_list",
    "is_contained_in": "sumeh_check_in_list",
    "not_in": "sumeh_check_in_list",
    "not_contained_in": "sumeh_check_in_list",
    # Pattern
    "has_pattern": "sumeh_check_pattern",
    "is_legit": "sumeh_check_legit",
}


def get_udf_name(check_type: str) -> str:
    """Get UDF name for check_type."""
    if check_type not in VALIDATION_REGISTRY:
        raise KeyError(
            f"'{check_type}' not implemented in pyflink. "
            f"Available: {list(VALIDATION_REGISTRY.keys())}"
        )
    return VALIDATION_REGISTRY[check_type]


def list_implemented() -> list:
    """List all implemented check types."""
    return list(VALIDATION_REGISTRY.keys())


def is_supported(check_type: str) -> bool:
    """Check if validation type is supported."""
    return check_type in VALIDATION_REGISTRY
