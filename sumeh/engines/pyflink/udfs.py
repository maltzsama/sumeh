"""
PyFlink UDFs for data quality validation - COMPLETE.

All validation UDFs defined here (implements IStreamValidator protocol).
Each UDF is THREAD-mode compatible (PyFlink 1.15+).
"""

import re
from datetime import datetime, date, timedelta

from pyflink.table import DataTypes
from pyflink.table.udf import udf
from sumeh.engines.pyflink.protocols import stream_validator

# ============================================================================
# COMPLETENESS VALIDATORS
# ============================================================================


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_complete(value):
    """Check if value is not null."""
    if value is None:
        return "null_value"
    return None


# ============================================================================
# COMPARISON VALIDATORS
# ============================================================================


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_equal(value, expected):
    """Check if value == expected."""
    if value is None:
        return None
    if value != expected:
        return f"not_equal:{expected}"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_equal_column(value1, value2):
    """
    Check if value1 == value2 (column-to-column comparison).

    Used for is_equal_than validation.
    """
    if value1 is None or value2 is None:
        return None
    if value1 != value2:
        return "columns_not_equal"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_greater(value, threshold):
    """Check if value > threshold."""
    if value is None:
        return None
    if value <= threshold:
        return f"not_greater_than:{threshold}"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_greater_or_equal(value, threshold):
    """Check if value >= threshold."""
    if value is None:
        return None
    if value < threshold:
        return f"not_greater_or_equal:{threshold}"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_less(value, threshold):
    """Check if value < threshold."""
    if value is None:
        return None
    if value >= threshold:
        return f"not_less_than:{threshold}"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_less_or_equal(value, threshold):
    """Check if value <= threshold."""
    if value is None:
        return None
    if value > threshold:
        return f"not_less_or_equal:{threshold}"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_positive(value):
    """Check if value > 0."""
    if value is None:
        return None
    if value <= 0:
        return "not_positive"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_negative(value):
    """Check if value < 0."""
    if value is None:
        return None
    if value >= 0:
        return "not_negative"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_in_millions(value):
    """Check if value >= 1,000,000."""
    if value is None:
        return None
    if value < 1_000_000:
        return "not_in_millions"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_in_billions(value):
    """Check if value >= 1,000,000,000."""
    if value is None:
        return None
    if value < 1_000_000_000:
        return "not_in_billions"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_between(value, min_val, max_val):
    """Check if min_val <= value <= max_val."""
    if value is None:
        return None
    if value < min_val or value > max_val:
        return f"not_between:{min_val}-{max_val}"
    return None


# ============================================================================
# MEMBERSHIP VALIDATORS
# ============================================================================


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_in_list(value, allowed_values):
    """Check if value in allowed list."""
    if value is None:
        return None
    # allowed_values comes as string, parse it
    allowed = eval(allowed_values)  # Convert "['a','b']" to list
    if value not in allowed:
        return f"not_in_list:{allowed_values}"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_not_in_list(value, forbidden_values):
    """Check if value NOT in forbidden list."""
    if value is None:
        return None
    forbidden = eval(forbidden_values)
    if value in forbidden:
        return f"in_forbidden_list:{forbidden_values}"
    return None


# ============================================================================
# PATTERN VALIDATORS
# ============================================================================


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_pattern(value, pattern):
    """Check if value matches regex pattern."""
    if value is None:
        return None
    if not re.match(pattern, str(value)):
        return f"pattern_mismatch:{pattern}"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_legit(value):
    """Check if value is not null and not empty string."""
    if value is None:
        return "null_value"
    if isinstance(value, str) and value.strip() == "":
        return "empty_string"
    return None


# ============================================================================
# DATE VALIDATORS
# ============================================================================


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_date_today(value):
    """Check if date is today."""
    if value is None:
        return None
    try:
        val_date = value.date() if hasattr(value, "date") else value
        if val_date != date.today():
            return "not_today"
    except:
        return "invalid_date"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_date_yesterday(value):
    """Check if date is yesterday."""
    if value is None:
        return None
    try:
        val_date = value.date() if hasattr(value, "date") else value
        yesterday = date.today() - timedelta(days=1)
        if val_date != yesterday:
            return "not_yesterday"
    except:
        return "invalid_date"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_date_t_minus_2(value):
    """Check if date is 2 days ago."""
    if value is None:
        return None
    try:
        val_date = value.date() if hasattr(value, "date") else value
        target = date.today() - timedelta(days=2)
        if val_date != target:
            return "not_t_minus_2"
    except:
        return "invalid_date"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_date_t_minus_3(value):
    """Check if date is 3 days ago."""
    if value is None:
        return None
    try:
        val_date = value.date() if hasattr(value, "date") else value
        target = date.today() - timedelta(days=3)
        if val_date != target:
            return "not_t_minus_3"
    except:
        return "invalid_date"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_date_past(value):
    """Check if date is in the past."""
    if value is None:
        return None
    try:
        val_date = value.date() if hasattr(value, "date") else value
        if val_date >= date.today():
            return "not_past_date"
    except:
        return "invalid_date"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_date_future(value):
    """Check if date is in the future."""
    if value is None:
        return None
    try:
        val_date = value.date() if hasattr(value, "date") else value
        if val_date <= date.today():
            return "not_future_date"
    except:
        return "invalid_date"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_date_weekday(value):
    """Check if date is on weekday (Mon-Fri)."""
    if value is None:
        return None
    try:
        val_date = value.date() if hasattr(value, "date") else value
        if val_date.weekday() >= 5:  # 5=Sat, 6=Sun
            return "not_weekday"
    except:
        return "invalid_date"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_date_weekend(value):
    """Check if date is on weekend (Sat-Sun)."""
    if value is None:
        return None
    try:
        val_date = value.date() if hasattr(value, "date") else value
        if val_date.weekday() < 5:  # 0-4 = Mon-Fri
            return "not_weekend"
    except:
        return "invalid_date"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_date_monday(value):
    """Check if date is Monday."""
    if value is None:
        return None
    try:
        val_date = value.date() if hasattr(value, "date") else value
        if val_date.weekday() != 0:
            return "not_monday"
    except:
        return "invalid_date"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_date_tuesday(value):
    """Check if date is Tuesday."""
    if value is None:
        return None
    try:
        val_date = value.date() if hasattr(value, "date") else value
        if val_date.weekday() != 1:
            return "not_tuesday"
    except:
        return "invalid_date"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_date_wednesday(value):
    """Check if date is Wednesday."""
    if value is None:
        return None
    try:
        val_date = value.date() if hasattr(value, "date") else value
        if val_date.weekday() != 2:
            return "not_wednesday"
    except:
        return "invalid_date"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_date_thursday(value):
    """Check if date is Thursday."""
    if value is None:
        return None
    try:
        val_date = value.date() if hasattr(value, "date") else value
        if val_date.weekday() != 3:
            return "not_thursday"
    except:
        return "invalid_date"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_date_friday(value):
    """Check if date is Friday."""
    if value is None:
        return None
    try:
        val_date = value.date() if hasattr(value, "date") else value
        if val_date.weekday() != 4:
            return "not_friday"
    except:
        return "invalid_date"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_date_saturday(value):
    """Check if date is Saturday."""
    if value is None:
        return None
    try:
        val_date = value.date() if hasattr(value, "date") else value
        if val_date.weekday() != 5:
            return "not_saturday"
    except:
        return "invalid_date"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_date_sunday(value):
    """Check if date is Sunday."""
    if value is None:
        return None
    try:
        val_date = value.date() if hasattr(value, "date") else value
        if val_date.weekday() != 6:
            return "not_sunday"
    except:
        return "invalid_date"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_date_between(value, start_str, end_str):
    """Check if date is between start and end."""
    if value is None:
        return None
    try:
        val_date = value.date() if hasattr(value, "date") else value
        start = datetime.strptime(start_str, "%Y-%m-%d").date()
        end = datetime.strptime(end_str, "%Y-%m-%d").date()
        if val_date < start or val_date > end:
            return f"not_between:{start_str}-{end_str}"
    except:
        return "invalid_date"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_date_after(value, target_str):
    """Check if date is after target."""
    if value is None:
        return None
    try:
        val_date = value.date() if hasattr(value, "date") else value
        target = datetime.strptime(target_str, "%Y-%m-%d").date()
        if val_date <= target:
            return f"not_after:{target_str}"
    except:
        return "invalid_date"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_date_before(value, target_str):
    """Check if date is before target."""
    if value is None:
        return None
    try:
        val_date = value.date() if hasattr(value, "date") else value
        target = datetime.strptime(target_str, "%Y-%m-%d").date()
        if val_date >= target:
            return f"not_before:{target_str}"
    except:
        return "invalid_date"
    return None


# ============================================================================
# UDF REGISTRY
# ============================================================================

UDF_REGISTRY = {
    # Completeness
    "sumeh_check_complete": sumeh_check_complete,
    # Comparison
    "sumeh_check_equal": sumeh_check_equal,
    "sumeh_check_equal_column": sumeh_check_equal_column,
    "sumeh_check_greater": sumeh_check_greater,
    "sumeh_check_greater_or_equal": sumeh_check_greater_or_equal,
    "sumeh_check_less": sumeh_check_less,
    "sumeh_check_less_or_equal": sumeh_check_less_or_equal,
    "sumeh_check_positive": sumeh_check_positive,
    "sumeh_check_negative": sumeh_check_negative,
    "sumeh_check_in_millions": sumeh_check_in_millions,
    "sumeh_check_in_billions": sumeh_check_in_billions,
    "sumeh_check_between": sumeh_check_between,
    # Membership
    "sumeh_check_in_list": sumeh_check_in_list,
    "sumeh_check_not_in_list": sumeh_check_not_in_list,
    # Pattern
    "sumeh_check_pattern": sumeh_check_pattern,
    "sumeh_check_legit": sumeh_check_legit,
    # Date
    "sumeh_check_date_today": sumeh_check_date_today,
    "sumeh_check_date_yesterday": sumeh_check_date_yesterday,
    "sumeh_check_date_t_minus_2": sumeh_check_date_t_minus_2,
    "sumeh_check_date_t_minus_3": sumeh_check_date_t_minus_3,
    "sumeh_check_date_past": sumeh_check_date_past,
    "sumeh_check_date_future": sumeh_check_date_future,
    "sumeh_check_date_weekday": sumeh_check_date_weekday,
    "sumeh_check_date_weekend": sumeh_check_date_weekend,
    "sumeh_check_date_monday": sumeh_check_date_monday,
    "sumeh_check_date_tuesday": sumeh_check_date_tuesday,
    "sumeh_check_date_wednesday": sumeh_check_date_wednesday,
    "sumeh_check_date_thursday": sumeh_check_date_thursday,
    "sumeh_check_date_friday": sumeh_check_date_friday,
    "sumeh_check_date_saturday": sumeh_check_date_saturday,
    "sumeh_check_date_sunday": sumeh_check_date_sunday,
    "sumeh_check_date_between": sumeh_check_date_between,
    "sumeh_check_date_after": sumeh_check_date_after,
    "sumeh_check_date_before": sumeh_check_date_before,
}


def get_all_udfs():
    """Get all UDF functions for registration."""
    return UDF_REGISTRY


def validate_all_implement_protocol():
    """Verify all UDFs implement IStreamValidator protocol."""
    from sumeh.engines.pyflink.protocols import IStreamValidator

    for name, udf_func in UDF_REGISTRY.items():
        if not hasattr(udf_func, "__stream_validator__"):
            raise TypeError(f"{name} missing @stream_validator decorator")
