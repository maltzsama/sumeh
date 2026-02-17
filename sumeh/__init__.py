"""
Sumeh - Unified Data Quality Framework

v2.0 - Breaking Changes:
  - New namespace-first API
  - Analyzer/Constraint architecture
  - ValidationReport replaces DataFrame tuples
  - No backwards compatibility with v1.x

API:
    from sumeh import pandas, csv
    
    rules = csv.get_rules_config("rules.csv")
    report = pandas.validate(df, rules)
    
    print(f"Pass rate: {report.pass_rate:.2%}")
    for result in report.failed:
        print(f"  - {result.check_type} on {result.field}: {result.message}")
"""

__author__ = "Demetrius Albuquerque"
__email__ = "demetrius.albuquerque@yahoo.com.br"
__version__ = "2.0.0"

# ============================================================================
# Core Models
# ============================================================================
from sumeh.core.models import (
    ValidationResult,
    ValidationReport,
    MetricResult,
    ValidationStatus,
    ValidationLevel,
)

# ============================================================================
# Engines (namespace-first API)
# ============================================================================
from sumeh.engines import pandas

# ============================================================================
# Config Sources (namespace-first API)
# ============================================================================
# Will be imported when config sources are implemented
# from sumeh.config import csv, s3, mysql, postgresql

__all__ = [
    # Meta
    "__version__",
    "__author__",
    "__email__",
    
    # Core Models
    "ValidationResult",
    "ValidationReport",
    "MetricResult",
    "ValidationStatus",
    "ValidationLevel",
    
    # Engines
    "pandas",
    
    # Config Sources (coming soon)
    # "csv", "s3", "mysql", "postgresql",
]