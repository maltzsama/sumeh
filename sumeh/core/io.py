"""
Data loading utilities for CLI and programmatic use.

Supports: CSV, Parquet, JSON, Excel
"""
from pathlib import Path
from typing import Union, Any


def load_data(filepath: Union[str, Path], engine: str = "pandas") -> Any:
    """
    Load data file into DataFrame.
    
    Supports:
        - CSV (.csv)
        - Parquet (.parquet, .pq)
        - JSON (.json, .jsonl, .ndjson)
        - Excel (.xlsx, .xls)
    
    Args:
        filepath: Path to data file
        engine: DataFrame engine ('pandas', 'polars', 'dask')
    
    Returns:
        DataFrame (engine-specific type)
    
    Example:
        >>> df = load_data("data.csv", engine="pandas")
        >>> df = load_data("data.parquet", engine="polars")
    """
    filepath = Path(filepath)
    
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")
    
    # Determine file type
    suffix = filepath.suffix.lower()
    
    # Load based on engine
    if engine == "pandas":
        import pandas as pd
        
        if suffix == ".csv":
            return pd.read_csv(filepath)
        elif suffix in [".parquet", ".pq"]:
            return pd.read_parquet(filepath)
        elif suffix in [".json", ".jsonl", ".ndjson"]:
            return pd.read_json(filepath, lines=(suffix in [".jsonl", ".ndjson"]))
        elif suffix in [".xlsx", ".xls"]:
            return pd.read_excel(filepath)
        else:
            raise ValueError(f"Unsupported file type: {suffix}")
    
    elif engine == "polars":
        import polars as pl
        
        if suffix == ".csv":
            return pl.read_csv(filepath)
        elif suffix in [".parquet", ".pq"]:
            return pl.read_parquet(filepath)
        elif suffix in [".json", ".jsonl", ".ndjson"]:
            return pl.read_json(filepath)
        elif suffix in [".xlsx", ".xls"]:
            return pl.read_excel(filepath)
        else:
            raise ValueError(f"Unsupported file type: {suffix}")
    
    elif engine == "dask":
        import dask.dataframe as dd
        
        if suffix == ".csv":
            return dd.read_csv(filepath)
        elif suffix in [".parquet", ".pq"]:
            return dd.read_parquet(filepath)
        elif suffix in [".json", ".jsonl", ".ndjson"]:
            return dd.read_json(filepath)
        else:
            raise ValueError(f"Dask doesn't support {suffix}")
    
    else:
        raise ValueError(f"Unknown engine: {engine}")


def save_data(df: Any, filepath: Union[str, Path], engine: str = "pandas"):
    """
    Save DataFrame to file.
    
    Args:
        df: DataFrame to save
        filepath: Output path
        engine: DataFrame engine
    """
    filepath = Path(filepath)
    suffix = filepath.suffix.lower()
    
    if engine == "pandas":
        if suffix == ".csv":
            df.to_csv(filepath, index=False)
        elif suffix in [".parquet", ".pq"]:
            df.to_parquet(filepath, index=False)
        elif suffix == ".json":
            df.to_json(filepath, orient="records", lines=True)
        elif suffix in [".xlsx", ".xls"]:
            df.to_excel(filepath, index=False)
    
    elif engine == "polars":
        if suffix == ".csv":
            df.write_csv(filepath)
        elif suffix in [".parquet", ".pq"]:
            df.write_parquet(filepath)
        elif suffix == ".json":
            df.write_json(filepath)
        elif suffix in [".xlsx", ".xls"]:
            df.write_excel(filepath)
    
    elif engine == "dask":
        if suffix == ".csv":
            df.to_csv(filepath, index=False, single_file=True)
        elif suffix in [".parquet", ".pq"]:
            df.to_parquet(filepath)