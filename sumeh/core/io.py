"""I/O operations for loading data and saving results."""

import pandas as pd
from pathlib import Path
from typing import Dict, Any
import json


def load_data(source: str, engine: str = "pandas", **kwargs) -> Any:
    """
    Load data from various sources.

    Args:
        source: Path to file or connection string
        engine: pandas | polars | dask
        **kwargs: Additional parameters for the reader

    Returns:
        DataFrame (type depends on engine)

    Examples:
        >>> df = load_data("data.csv")
        >>> df = load_data("data.parquet", engine="polars")
    """
    source_path = Path(source)

    if engine == "pandas":
        return _load_pandas(source_path, **kwargs)
    elif engine == "polars":
        return _load_polars(source_path, **kwargs)
    elif engine == "dask":
        return _load_dask(source_path, **kwargs)
    else:
        raise ValueError(f"Unsupported engine: {engine}")


def _load_pandas(source: Path, **kwargs) -> pd.DataFrame:
    """Load data using pandas."""
    suffix = source.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(source, **kwargs)
    elif suffix == ".parquet":
        return pd.read_parquet(source, **kwargs)
    elif suffix == ".json":
        return pd.read_json(source, **kwargs)
    elif suffix in [".xls", ".xlsx"]:
        return pd.read_excel(source, **kwargs)
    else:
        raise ValueError(f"Unsupported file format: {suffix}")


def _load_polars(source: Path, **kwargs):
    """Load data using polars."""
    try:
        import polars as pl
    except ImportError:
        raise ImportError("polars not installed. Run: pip install polars")

    suffix = source.suffix.lower()

    if suffix == ".csv":
        return pl.read_csv(source, **kwargs)
    elif suffix == ".parquet":
        return pl.read_parquet(source, **kwargs)
    elif suffix == ".json":
        return pl.read_json(source, **kwargs)
    else:
        raise ValueError(f"Unsupported file format for polars: {suffix}")


def _load_dask(source: Path, **kwargs):
    """Load data using dask."""
    try:
        import dask.dataframe as dd
    except ImportError:
        raise ImportError("dask not installed. Run: pip install dask[dataframe]")

    suffix = source.suffix.lower()

    if suffix == ".csv":
        return dd.read_csv(source, **kwargs)
    elif suffix == ".parquet":
        return dd.read_parquet(source, **kwargs)
    elif suffix == ".json":
        return dd.read_json(source, **kwargs)
    else:
        raise ValueError(f"Unsupported file format for dask: {suffix}")


def save_results(
    results: Dict[str, Any], output_path: str, format: str = "json", **kwargs
) -> None:
    """
    Save validation results to file.

    Args:
        results: Dictionary with validation results
        output_path: Path to output file
        format: json | csv | html
        **kwargs: Additional parameters for the writer
    """
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    if format == "json":
        _save_json(results, output, **kwargs)
    elif format == "csv":
        _save_csv(results, output, **kwargs)
    elif format == "html":
        _save_html(results, output, **kwargs)
    else:
        raise ValueError(f"Unsupported format: {format}")


def _save_json(results: Dict[str, Any], output: Path, **kwargs) -> None:
    """Save results as JSON."""
    with open(output, "w") as f:
        serializable_results = {}
        for key, value in results.items():
            if isinstance(value, pd.DataFrame):
                serializable_results[key] = value.to_dict(orient="records")
            else:
                serializable_results[key] = value

        json.dump(serializable_results, f, indent=2, default=str)


def _save_csv(results: Dict[str, Any], output: Path, **kwargs) -> None:
    """Save summary as CSV."""
    summary = results.get("summary")
    if isinstance(summary, pd.DataFrame):
        summary.to_csv(output, index=False, **kwargs)
    else:
        raise ValueError("No DataFrame in results to save as CSV")


def _save_html(results: Dict[str, Any], output: Path, **kwargs) -> None:
    """Save results as HTML report."""
    from sumeh.core.report import generate_html_report

    html_content = generate_html_report(results)
    output.write_text(html_content)
