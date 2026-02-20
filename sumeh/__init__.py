"""
Sumeh - Unified Data Quality Framework

v2.0 - Complete Rewrite:
  - Namespace-first API
  - Analyzer/Constraint architecture
  - ValidationReport with .df property
  - Bifurcation Pattern (single-pass validation)
  - 16 engines: Batch + Streaming + SQL

Quick Start:
    from sumeh import pandas

    rules = [...]
    report = pandas.validate(df, rules)

    # View report
    print(f"Pass rate: {report.pass_rate:.2%}")

    # Get validated DataFrame (with _dq_errors column)
    validated_df = report.df

    # Or split good/bad
    good_df, bad_df = report.split()

Engines:
    Batch DataFrames:
        - pandas: Fast local processing
        - polars: Rust-powered performance
        - pyspark: Distributed Spark processing
        - dask: Out-of-core parallel computing

    Streaming & ML:
        - ray_data: ML/AI pipelines with GPU support
        - pyflink: Stream processing with Apache Flink

    SQL Engines:
        - duckdb: Embedded analytics
        - bigquery: Google Cloud
        - snowflake: Cloud data warehouse
        - redshift: AWS data warehouse
        - athena: AWS serverless query
        - trino: Distributed SQL
        - doris: Apache Doris
        - sql_core: Generic SQL generation

Coverage:
    - 50 validation rules
    - 16 engines
    - Single-pass execution
    - GPU acceleration (Ray Data)
    - Cross-dialect SQL generation
"""

__author__ = "Demetrius Albuquerque"
__email__ = "demetrius.albuquerque@yahoo.com.br"
__version__ = "2.0.0"

# ============================================================================
# Core Models
# ============================================================================
from sumeh.core.models.validation import (
    ValidationResult,
    ValidationReport,
    ValidationStatus,
    ValidationLevel,
)

from sumeh.core.models.metrics import MetricResult

from sumeh.generators import SQLGenerator

# ============================================================================
# Engines (namespace-first API with lazy loading)
# ============================================================================
# ALL engines are optional - only import if dependencies are installed
_all_engines = [
    # Batch DataFrames
    "pandas",
    "polars",
    "pyspark",
    "dask",
    # Streaming & ML
    "ray_data",
    "pyflink",
    # SQL Engines
    "duckdb",
    "bigquery",
    "snowflake",
    "redshift",
    "athena",
    "trino",
    "doris",
    "sql_core",
]

_available_engines = []
for _name in _all_engines:
    try:
        exec(f"from sumeh.engines import {_name}")
        _available_engines.append(_name)
    except (ImportError, AttributeError):
        # Engine not available (missing dependency or not implemented)
        exec(f"{_name} = None")

# Warn if no engines available
if not _available_engines:
    import warnings

    warnings.warn(
        "No validation engines available! "
        "Install at least one engine:\n"
        "  pip install sumeh[pandas]   # For pandas\n"
        "  pip install sumeh[pyspark]  # For PySpark\n"
        "  pip install sumeh[all]      # For all engines"
    )


# Utilities


# Build __all__ dynamically based on what was successfully imported
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
    # Utilities
    "SQLGenerator",
]

# Add engines that were successfully imported
__all__.extend(_available_engines)
