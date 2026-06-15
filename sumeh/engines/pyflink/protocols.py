"""
PyFlink Engine Protocols.

Defines the stream_validator decorator and IStreamValidator Protocol
used to mark and verify Sumeh UDFs.
"""

import functools
from typing import Any, Callable, Protocol, runtime_checkable


@runtime_checkable
class IStreamValidator(Protocol):
    """
    Protocol for Sumeh PyFlink UDF validators.

    Any callable decorated with @stream_validator satisfies this protocol.
    Used by validate_all_implement_protocol() to verify UDF registration.
    """

    __stream_validator__: bool


def stream_validator(func: Callable) -> Callable:
    """
    Marks a PyFlink UDF as a Sumeh stream validator.

    Sets __stream_validator__ = True on the wrapped function so that
    validate_all_implement_protocol() can verify all UDFs are properly
    decorated before registration.

    Usage:
        @udf(result_type=DataTypes.STRING())
        @stream_validator
        def sumeh_check_complete(value):
            ...
    """

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    wrapper.__stream_validator__ = True  # type: ignore[attr-defined]
    return wrapper
