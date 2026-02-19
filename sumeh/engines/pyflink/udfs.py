"""
PyFlink UDFs for data quality validation.

All validation UDFs defined here (implements IStreamValidator protocol).
Each UDF is THREAD-mode compatible (PyFlink 1.15+).

Note: These are stream validators, not batch analyzers.
      They work row-by-row on unbounded streams.
"""

from pyflink.table import DataTypes
from pyflink.table.udf import udf
from sumeh.engines.pyflink.protocols import stream_validator
import re

# ============================================================================
# COMPLETENESS VALIDATORS
# ============================================================================


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_complete(value):
    """
    Check if value is not null.

    Implements: IStreamValidator
    """
    if value is None:
        return "null_value"
    return None


# ============================================================================
# PATTERN VALIDATORS
# ============================================================================


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_pattern(value, pattern):
    """
    Check if value matches regex pattern.

    Implements: IStreamValidator
    """
    if value is None:
        return None
    if not re.match(pattern, str(value)):
        return f"pattern_mismatch:{pattern}"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_legit(value):
    """
    Check if value is not null and not empty string.

    Implements: IStreamValidator
    """
    if value is None:
        return "null_value"
    if isinstance(value, str) and value.strip() == "":
        return "empty_string"
    return None


# ============================================================================
# COMPARISON VALIDATORS
# ============================================================================


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_equal(value, expected):
    """
    Check if value == expected.

    Implements: IStreamValidator
    """
    if value is None:
        return None
    if value != expected:
        return f"not_equal:{expected}"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_greater(value, threshold):
    """
    Check if value > threshold.

    Implements: IStreamValidator
    """
    if value is None:
        return None
    if value <= threshold:
        return f"not_greater_than:{threshold}"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_less(value, threshold):
    """
    Check if value < threshold.

    Implements: IStreamValidator
    """
    if value is None:
        return None
    if value >= threshold:
        return f"not_less_than:{threshold}"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_positive(value):
    """
    Check if value > 0.

    Implements: IStreamValidator
    """
    if value is None:
        return None
    if value <= 0:
        return "not_positive"
    return None


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_negative(value):
    """
    Check if value < 0.

    Implements: IStreamValidator
    """
    if value is None:
        return None
    if value >= 0:
        return "not_negative"
    return None


# ============================================================================
# MEMBERSHIP VALIDATORS
# ============================================================================


@udf(result_type=DataTypes.STRING())
@stream_validator
def sumeh_check_in_list(value, allowed_values):
    """
    Check if value in allowed list.

    Implements: IStreamValidator
    """
    if value is None:
        return None
    # allowed_values comes as string, parse it
    allowed = eval(allowed_values)  # Convert "['a','b']" to list
    if value not in allowed:
        return f"not_in_list:{allowed_values}"
    return None


# ============================================================================
# UDF REGISTRY
# ============================================================================

# Map UDF function name to function object
UDF_REGISTRY = {
    "sumeh_check_complete": sumeh_check_complete,
    "sumeh_check_pattern": sumeh_check_pattern,
    "sumeh_check_legit": sumeh_check_legit,
    "sumeh_check_equal": sumeh_check_equal,
    "sumeh_check_greater": sumeh_check_greater,
    "sumeh_check_less": sumeh_check_less,
    "sumeh_check_positive": sumeh_check_positive,
    "sumeh_check_negative": sumeh_check_negative,
    "sumeh_check_in_list": sumeh_check_in_list,
}


def get_all_udfs():
    """Get all UDF functions for registration."""
    return UDF_REGISTRY


def validate_all_implement_protocol():
    """
    Verify all UDFs implement IStreamValidator protocol.

    Raises:
        TypeError: If any UDF doesn't implement protocol
    """
    from sumeh.engines.pyflink.protocols import IStreamValidator

    for name, udf_func in UDF_REGISTRY.items():
        # Check if has __stream_validator__ marker
        if not hasattr(udf_func, "__stream_validator__"):
            raise TypeError(f"{name} missing @stream_validator decorator")
