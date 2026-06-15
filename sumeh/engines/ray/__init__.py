"""
Ray Data Engine - ML/AI Data Quality Validation.

Ray Data is the distributed data processing library for Ray ecosystem.
Designed for ML/AI workloads with native GPU support and Python-first API.

⚠️  ROW-LEVEL ONLY (No Aggregations)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Ray Data focuses on map/filter operations for ML pipelines.
Like PyFlink, we support row-level validations only.

"""

from sumeh.engines.ray.engine import validate

CAPABILITIES = {
    "schema_validation": True,
    "profiling": True,
    "aggregation_analyzers": True,
    "bifurcation": True,
    "streaming": True,  # Supports both batch and streaming
}

__all__ = ["validate", "CAPABILITIES"]
