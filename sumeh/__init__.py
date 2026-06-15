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
        - ray: ML/AI pipelines with GPU support
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
__version__ = "3.0.0"

from sumeh.core.models.metrics import MetricResult

# ============================================================================
# Core Models
# ============================================================================
from sumeh.core.models.validation import (
    ValidationResult,
    ValidationReport,
    ValidationStatus,
    ValidationLevel,
)
from sumeh.generators import SQLGenerator

# ============================================================================
# Engines (explicit imports with fallback)
# ============================================================================
# Batch DataFrames
try:
    from sumeh.engines import pandas
except ImportError:
    pandas = None

try:
    from sumeh.engines import polars
except ImportError:
    polars = None

try:
    from sumeh.engines import pyspark
except ImportError:
    pyspark = None

try:
    from sumeh.engines import dask
except ImportError:
    dask = None

# Streaming & ML
try:
    from sumeh.engines import ray
except ImportError:
    ray = None

try:
    from sumeh.engines import pyflink
except ImportError:
    pyflink = None

# SQL Engines
try:
    from sumeh.engines import duckdb
except ImportError:
    duckdb = None

try:
    from sumeh.engines import bigquery
except ImportError:
    bigquery = None

try:
    from sumeh.engines import snowflake
except ImportError:
    snowflake = None

try:
    from sumeh.engines import redshift
except ImportError:
    redshift = None

try:
    from sumeh.engines import athena
except ImportError:
    athena = None

try:
    from sumeh.engines import trino
except ImportError:
    trino = None

try:
    from sumeh.engines import doris
except ImportError:
    doris = None

try:
    from sumeh.engines import sql_core
except ImportError:
    sql_core = None

# List of available engines (for warning)
_available_engines = [
    name
    for name, engine in locals().items()
    if engine is not None
    and name
    in [
        "pandas",
        "polars",
        "pyspark",
        "dask",
        "ray",
        "pyflink",
        "duckdb",
        "bigquery",
        "snowflake",
        "redshift",
        "athena",
        "trino",
        "doris",
        "sql_core",
    ]
]

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

# ============================================================================
# Public API
# ============================================================================
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
    # Engines (explicit list for auto-complete)
    "pandas",
    "polars",
    "pyspark",
    "dask",
    "ray",
    "pyflink",
    "duckdb",
    "bigquery",
    "snowflake",
    "redshift",
    "athena",
    "trino",
    "doris",
    "sql_core",
]
