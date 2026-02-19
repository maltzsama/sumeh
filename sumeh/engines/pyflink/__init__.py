"""
PyFlink Engine - Streaming Data Quality Validation.

Apache Flink's Python API for real-time stream processing.
Validates events one-by-one, adding _dq_errors column.

⚠️  ROW-LEVEL ONLY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Streaming validation is fundamentally different from batch.
We validate rows individually - no aggregations, no uniqueness checks.
Each row gets a _dq_errors column with a list of failed rules.

Installation:
    pip install apache-flink

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

from sumeh.engines.pyflink.engine import (
    register_udfs,
    validate,
)

CAPABILITIES = {
    'schema_validation': False,
    'profiling': False,     
    'aggregation_analyzers': False,
    'bifurcation': False, 
    'streaming': True,
}

__all__ = ["register_udfs", "validate", "CAPABILITIES"]