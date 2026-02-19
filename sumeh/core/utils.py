#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sumeh Core Utilities.
Engine detection and capability checking for cross-engine support.
"""

from typing import Any, Dict, List


# Engine Detection & Capabilities
def detect_engine(df: Any, **context) -> str:
    """
    Detect DataFrame engine type.
    
    Args:
        df: DataFrame object
        **context: Additional context (e.g., client, table_ref for BigQuery)
        
    Returns:
        Engine name: 'pandas', 'polars', 'pyspark', 'dask', 'pyflink', 
                     'ray', 'duckdb', 'bigquery'
        
    Raises:
        TypeError: If DataFrame type is unsupported
        
    Examples:
        >>> detect_engine(spark_df)
        'pyspark'
        >>> detect_engine(pd_df)
        'pandas'
    """
    # BigQuery with explicit context
    if "client" in context and "table_ref" in context:
        return "bigquery"
    
    mod = df.__class__.__module__
    class_name = df.__class__.__name__
    
    # PySpark - has .rdd attribute
    if mod.startswith("pyspark") or hasattr(df, 'rdd'):
        return "pyspark"
    
    # PyFlink
    if 'flink' in mod.lower() or 'flink' in class_name.lower():
        return "pyflink"
    
    # Ray
    if 'ray' in mod.lower() or 'dataset' in class_name.lower():
        return "ray"
    
    # Dask
    if mod.startswith("dask"):
        return "dask"
    
    # Polars
    if mod.startswith("polars"):
        return "polars"
    
    # Pandas
    if mod.startswith("pandas"):
        return "pandas"
    
    # DuckDB
    if mod.startswith("duckdb") or mod.startswith("_duckdb") or "DuckDB" in class_name:
        return "duckdb"
    
    # BigQuery
    if "bigquery" in mod.lower():
        return "bigquery"
    
    raise TypeError(
        f"Unsupported DataFrame type: {type(df)}. "
        f"Supported: pandas, polars, pyspark, dask, pyflink, ray, duckdb, bigquery"
    )


def has_capability(engine_name: str, capability: str) -> bool:
    """
    Check if engine supports a specific capability.
    
    Args:
        engine_name: Engine identifier (e.g., 'pyspark', 'pandas')
        capability: Capability name (e.g., 'profiling', 'schema_validation')
        
    Returns:
        True if supported, False otherwise
        
    Examples:
        >>> has_capability('pyspark', 'profiling')
        True
        >>> has_capability('pyflink', 'profiling')
        False
    """
    try:
        # Dynamic import to avoid circular dependencies
        if engine_name == 'pandas':
            from sumeh.engines.pandas import CAPABILITIES
        elif engine_name == 'polars':
            from sumeh.engines.polars import CAPABILITIES
        elif engine_name == 'pyspark':
            from sumeh.engines.pyspark import CAPABILITIES
        elif engine_name == 'dask':
            from sumeh.engines.dask import CAPABILITIES
        elif engine_name == 'pyflink':
            from sumeh.engines.pyflink import CAPABILITIES
        elif engine_name == 'ray':
            from sumeh.engines.ray_data import CAPABILITIES
        elif engine_name == 'duckdb':
            from sumeh.engines.duckdb import CAPABILITIES
        elif engine_name == 'bigquery':
            from sumeh.engines.bigquery import CAPABILITIES
        else:
            return False
        
        return CAPABILITIES.get(capability, False)
    
    except (ImportError, AttributeError):
        return False


def get_capabilities(engine_name: str) -> Dict[str, bool]:
    """
    Get all capabilities for an engine.
    
    Args:
        engine_name: Engine identifier
        
    Returns:
        Dict of capabilities, empty dict if unknown
        
    Examples:
        >>> get_capabilities('pyspark')
        {'schema_validation': True, 'profiling': True, ...}
    """
    try:
        if engine_name == 'pandas':
            from sumeh.engines.pandas import CAPABILITIES
        elif engine_name == 'polars':
            from sumeh.engines.polars import CAPABILITIES
        elif engine_name == 'pyspark':
            from sumeh.engines.pyspark import CAPABILITIES
        elif engine_name == 'dask':
            from sumeh.engines.dask import CAPABILITIES
        elif engine_name == 'pyflink':
            from sumeh.engines.pyflink import CAPABILITIES
        elif engine_name == 'ray':
            from sumeh.engines.ray_data import CAPABILITIES
        elif engine_name == 'duckdb':
            from sumeh.engines.duckdb import CAPABILITIES
        elif engine_name == 'bigquery':
            from sumeh.engines.bigquery import CAPABILITIES
        else:
            return {}
        
        return dict(CAPABILITIES)
    
    except (ImportError, AttributeError):
        return {}


def require_capability(df: Any, capability: str, feature_name: str = "This feature"):
    """
    Assert that DataFrame's engine has required capability.
    
    Args:
        df: DataFrame object
        capability: Required capability name
        feature_name: Feature name for error message
        
    Raises:
        NotImplementedError: If engine lacks capability
        
    Examples:
        >>> require_capability(df, 'profiling', 'Data profiling')
        # Raises if engine doesn't support profiling
    """
    engine = detect_engine(df)
    
    if not has_capability(engine, capability):
        caps = get_capabilities(engine)
        supported = list_engines_with_capability(capability)
        
        raise NotImplementedError(
            f"{feature_name} not supported by {engine} engine.\n"
            f"Supported engines: {', '.join(supported)}\n"
            f"Current engine capabilities: {caps}"
        )


def list_engines_with_capability(capability: str) -> List[str]:
    """
    List engines that support a capability.
    
    Args:
        capability: Capability name
        
    Returns:
        List of engine names
        
    Examples:
        >>> list_engines_with_capability('profiling')
        ['pandas', 'polars', 'pyspark', 'dask', 'ray', 'duckdb']
    """
    all_engines = [
        'pandas', 'polars', 'pyspark', 'dask',
        'pyflink', 'ray', 'duckdb', 'bigquery'
    ]
    return [eng for eng in all_engines if has_capability(eng, capability)]