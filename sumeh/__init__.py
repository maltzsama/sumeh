"""Top-level package for Sumeh DQ."""

__author__ = "Demetrius Albuquerque"
__email__ = "demetrius.albuquerque@yahoo.com.br"
__version__ = "1.0.0"

from .core import (
    report,
    validate,
    summarize,
    get_rules_config,
    get_schema_config,
)
from .core.schema import extract_schema_data, validate_schema, types_are_compatible

__all__ = [
    "report",
    "validate",
    "summarize",
    "validate_schema",
    "get_rules_config",
    "get_schema_config",
    "extract_schema_data",
    "types_are_compatible",
    "__version__",
]
