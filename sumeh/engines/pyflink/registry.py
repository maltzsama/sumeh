"""
PyFlink validation registry - COMPLETE.

Maps check_type to UDF names for ALL row-level validations.
"""

VALIDATION_REGISTRY = {
    # Completeness
    "is_complete": "sumeh_check_complete",
    # Comparison
    "is_equal": "sumeh_check_equal",
    "is_equal_than": "sumeh_check_equal_column",
    "is_greater_than": "sumeh_check_greater",
    "is_less_than": "sumeh_check_less",
    "is_greater_or_equal_than": "sumeh_check_greater_or_equal",
    "is_less_or_equal_than": "sumeh_check_less_or_equal",
    "is_positive": "sumeh_check_positive",
    "is_negative": "sumeh_check_negative",
    "is_in_millions": "sumeh_check_in_millions",
    "is_in_billions": "sumeh_check_in_billions",
    "is_between": "sumeh_check_between",
    # Membership
    "is_in": "sumeh_check_in_list",
    "is_contained_in": "sumeh_check_in_list",
    "not_in": "sumeh_check_not_in_list",
    "not_contained_in": "sumeh_check_not_in_list",
    # Pattern
    "has_pattern": "sumeh_check_pattern",
    "is_legit": "sumeh_check_legit",
    # Date
    "is_today": "sumeh_check_date_today",
    "is_yesterday": "sumeh_check_date_yesterday",
    "is_t_minus_1": "sumeh_check_date_yesterday",
    "is_t_minus_2": "sumeh_check_date_t_minus_2",
    "is_t_minus_3": "sumeh_check_date_t_minus_3",
    "is_past_date": "sumeh_check_date_past",
    "is_future_date": "sumeh_check_date_future",
    "is_on_weekday": "sumeh_check_date_weekday",
    "is_on_weekend": "sumeh_check_date_weekend",
    "is_on_monday": "sumeh_check_date_monday",
    "is_on_tuesday": "sumeh_check_date_tuesday",
    "is_on_wednesday": "sumeh_check_date_wednesday",
    "is_on_thursday": "sumeh_check_date_thursday",
    "is_on_friday": "sumeh_check_date_friday",
    "is_on_saturday": "sumeh_check_date_saturday",
    "is_on_sunday": "sumeh_check_date_sunday",
    "is_date_between": "sumeh_check_date_between",
    "is_date_after": "sumeh_check_date_after",
    "is_date_before": "sumeh_check_date_before",
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
