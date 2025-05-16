"""Top-level package for Sumeh DQ."""

__authors__ = [
    "Demetrius Albuquerque <demetrius.albuquerque@yahoo.com.br>",
    "Otávio Lara <>"
]

__email__ = "demetrius.albuquerque@yahoo.com.br"
__version__ = "0.2.3"

from .core import report
from .services.config import *
from .engine import *


"""Top-level package for Sumeh DQ."""

__author__ = "Demetrius Albuquerque"
__email__ = "demetrius.albuquerque@yahoo.com.br"
__version__ = "0.2.3"

from .core import report
from .services.config import get_rules_config, get_schema_config
from .engine import validate, summarize, validate_schema

__all__ = [
    "report",
    "validate",
    "summarize",
    "validate_schema",
    "get_rules_config",
    "get_schema_config",
    "__version__",
]
