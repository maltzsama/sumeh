"""Top-level package for Sumeh DQ."""

__author__ = "Demetrius Albuquerque"
__email__ = "demetrius.albuquerque@yahoo.com.br"
__version__ = "0.3.0"

from .core import (
    report,
    validate,
    summarize,
    validate_schema,
    get_rules_config,
    get_schema_config,
)

__all__ = [
    "report",
    "validate",
    "summarize",
    "validate_schema",
    "get_rules_config",
    "get_schema_config",
    "__version__",
]
