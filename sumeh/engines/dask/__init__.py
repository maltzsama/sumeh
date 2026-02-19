"""
Dask Engine - Distributed Data Validation.

Leverages Dask's lazy evaluation and task graph optimization to validate
datasets larger than memory.

Strategy:
    1. Analyzers build a computation graph (Lazy Scalars).
    2. Engine triggers a SINGLE .compute() for all rules simultaneously.
    3. Constraints validate the concrete results.

Example:
    >>> import dask.dataframe as dd
    >>> from sumeh import dask as sumeh_dask
    >>>
    >>> ddf = dd.read_parquet("s3://bucket/big-data/*.parquet")
    >>> report = sumeh_dask.validate(ddf, rules)
"""

from sumeh.engines.dask.engine import validate

CAPABILITIES = {
    "schema_validation": True,
    "profiling": True,
    "aggregation_analyzers": True,
    "bifurcation": True,
    "streaming": False,
}

__all__ = ["validate", "CAPABILITIES"]
