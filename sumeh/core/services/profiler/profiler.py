"""
Sumeh Auto-Profiler.
Automatically generates statistical profiles of datasets in a single pass.

This service leverages existing AggregationAnalyzers to compute statistics
without requiring separate or redundant data scans.
"""

import time
from typing import Any, Dict, Optional

from sumeh.core.rules.rule_model import RuleDef
from sumeh.core.services.schema.validator import extract_schema


class DataProfiler:
    """
    Scans a dataset and extracts statistical profiles in a single pass.

    Supports: Pandas, PySpark, Polars, Dask, and Ray.
    """

    @staticmethod
    def profile(
        df_target: Any, engine_module: Any, sample_size: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Generate a complete statistical profile of a DataFrame.

        Args:
            df_target: DataFrame object (Pandas, PySpark, Polars, Dask, Ray)
            engine_module: The specific engine module (e.g., sumeh.engines.pyspark)
            sample_size: Optional fraction (0.0-1.0) for sampling before profiling
        """
        start_time = time.time()

        # 1. Apply sampling if requested
        if sample_size and 0 < sample_size < 1.0:
            df_target = DataProfiler._sample_df(df_target, sample_size)

        # 2. Extract schema (O(1) operation)
        schema_info = extract_schema(df_target, as_json=False)

        # 3. Dynamic Rule Generation for Profiling
        profiling_rules = []

        for col_name, col_meta in schema_info.items():
            col_type = col_meta.get("type", "unknown")

            # Basic metrics for all columns
            profiling_rules.extend(
                [
                    RuleDef(field=col_name, check_type="is_complete"),
                    RuleDef(field=col_name, check_type="has_cardinality"),
                ]
            )

            # Statistical metrics for numeric columns only
            if col_type in ["integer", "float"]:
                profiling_rules.extend(
                    [
                        RuleDef(field=col_name, check_type="has_min"),
                        RuleDef(field=col_name, check_type="has_max"),
                        RuleDef(field=col_name, check_type="has_mean"),
                        RuleDef(field=col_name, check_type="has_std"),
                        RuleDef(field=col_name, check_type="has_sum"),
                    ]
                )

        # 4. Execution (O(N) - Single scan via Engine)
        report = engine_module.validate(df_target, profiling_rules)

        # 5. Format results into structured JSON
        return DataProfiler._format_output(
            report, schema_info, start_time, sampled=bool(sample_size)
        )

    @staticmethod
    def _format_output(
        report: Any,
        schema_info: Dict[str, dict],
        start_time: float,
        sampled: bool = False,
    ) -> Dict[str, Any]:
        """Format ValidationReport into an analytical JSON structure."""
        column_profiles = {}

        metric_map = {
            "is_complete": "completeness",
            "has_cardinality": "distinct_count",
            "has_min": "min",
            "has_max": "max",
            "has_mean": "mean",
            "has_std": "std_dev",
            "has_sum": "sum",
        }

        for res in report.results:
            col = res.field
            metric = res.check_type
            val = res.actual_value  # Might be None if engine fails

            if col not in column_profiles:
                column_profiles[col] = {
                    "type": schema_info.get(col, {}).get("type", "unknown"),
                    "nullable": schema_info.get(col, {}).get("nullable", True),
                    "row_count": report.total_rows or 0,
                }

            friendly_key = metric_map.get(metric, metric)
            column_profiles[col][friendly_key] = val

            # Safe null_count calculation
            if metric == "is_complete" and (report.total_rows or 0) > 0:
                try:
                    # Handle potential None from Spark/Ray
                    rate = float(val) if val is not None else 0.0
                    column_profiles[col]["null_count"] = int(
                        report.total_rows * (1 - rate)
                    )
                except (TypeError, ValueError):
                    column_profiles[col]["null_count"] = report.total_rows

        # Calculate derived metrics (e.g., Uniqueness)
        for col, stats in column_profiles.items():
            if "distinct_count" in stats and "row_count" in stats:
                # Defensive check: Ensure no division by zero or NoneType operations
                distinct = stats.get("distinct_count") or 0
                total = stats.get("row_count") or 0

                if total > 0:
                    stats["uniqueness"] = round(float(distinct) / total, 4)
                else:
                    stats["uniqueness"] = 0.0

        execution_time_ms = round((time.time() - start_time) * 1000, 2)

        return {
            "table_stats": {
                "total_rows": report.total_rows,
                "execution_time_ms": execution_time_ms,
                "columns_count": len(schema_info),
                "sampled": sampled,
            },
            "column_profiles": column_profiles,
        }

    @staticmethod
    def _sample_df(df: Any, fraction: float) -> Any:
        """Sample DataFrame across different engines (Pandas, Spark, Polars, Ray, DuckDB)."""

        # Ray Data support
        if hasattr(df, "random_sample"):
            return df.random_sample(fraction)

        # DuckDB Relation support
        if "duckdb" in str(type(df)).lower() and hasattr(df, "sample"):
            return df.sample(fraction)

        # PySpark support
        if hasattr(df, "sample") and hasattr(df, "rdd"):
            return df.sample(fraction=fraction, seed=42)

        # Pandas / Polars / Dask support
        if hasattr(df, "sample"):
            try:
                # Pandas and Dask use 'frac'
                return df.sample(frac=fraction, random_state=42)
            except TypeError:
                # Polars uses 'fraction'
                return df.sample(fraction=fraction, seed=42)

        return df
