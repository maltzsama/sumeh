"""
Sumeh Auto-Profiler.
Automatically generates statistical profiles of datasets in a single pass.

Leverages existing AggregationAnalyzers (has_min, has_max, has_mean, etc)
to compute statistics without separate aggregation queries.
"""

import time
from typing import Any, Dict, Optional

from sumeh.core.rules.rule_model import RuleDef
from sumeh.core.services.schema.validator import extract_schema


class DataProfiler:
    """
    Scans a dataset and automatically extracts statistical profiles
    without requiring manual rule definitions.

    Key features:
    - Single-pass execution (all metrics computed in one scan)
    - Cross-engine support (Pandas, PySpark, Polars, Dask)
    - Reuses existing validation infrastructure
    - Clean JSON output
    """

    @staticmethod
    def profile(
        df_target: Any, engine_module: Any, sample_size: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Generate complete statistical profile of DataFrame.

        Args:
            df_target: DataFrame (Pandas, PySpark, Polars, Dask)
            engine_module: Engine module for execution (e.g., sumeh.engines.pyspark)
            sample_size: Optional fraction (0.0-1.0) to sample before profiling

        Returns:
            JSON-friendly dict with statistics for each column

        Example:
            >>> from sumeh.profiler import DataProfiler
            >>> from sumeh.engines import pyspark
            >>> profile = DataProfiler.profile(spark_df, pyspark)
            >>> print(profile['column_profiles']['age']['mean'])
            34.5
        """
        start_time = time.time()

        # Sample if requested
        if sample_size and 0 < sample_size < 1.0:
            df_target = DataProfiler._sample_df(df_target, sample_size)

        # 1. Extract schema (O(1) - no data scan)
        # Returns: {'age': {'type': 'integer', 'nullable': True}}
        schema_info = extract_schema(df_target, as_json=False)

        # 2. Auto-Rule Generation (dynamic rule creation)
        profiling_rules = []

        for col_name, col_meta in schema_info.items():
            col_type = col_meta.get("type", "unknown")

            # Universal metrics (all columns)
            profiling_rules.extend(
                [
                    RuleDef(field=col_name, check_type="is_complete"),
                    RuleDef(field=col_name, check_type="has_cardinality"),
                ]
            )

            # Numeric metrics
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

        # 3. Execute (O(N) - Single Pass)
        # Sumeh batches all rules into a single .agg() call
        report = engine_module.validate(df_target, profiling_rules)

        # 4. Format output as clean analytical JSON
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
        """
        Transform ValidationReport into structured Profiler Report.

        Args:
            report: ValidationReport from engine
            schema_info: Extracted schema metadata
            start_time: Profile start timestamp
            sampled: Whether data was sampled

        Returns:
            Structured profile report
        """
        column_profiles = {}

        # Map check_type to friendly names
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

            # Initialize column if not present
            if col not in column_profiles:
                column_profiles[col] = {
                    "type": schema_info.get(col, {}).get("type", "unknown"),
                    "nullable": schema_info.get(col, {}).get("nullable", True),
                    "row_count": report.total_rows,
                }

            # Add metric with friendly name
            friendly_key = metric_map.get(metric, metric)
            column_profiles[col][friendly_key] = res.actual_value

            # Derive null_count from completeness
            if metric == "is_complete" and report.total_rows > 0:
                completeness_rate = float(res.actual_value)
                null_count = int(report.total_rows * (1 - completeness_rate))
                column_profiles[col]["null_count"] = null_count

        # Calculate derived metrics
        for col, stats in column_profiles.items():
            # Uniqueness ratio
            if "distinct_count" in stats and "row_count" in stats:
                distinct = stats["distinct_count"]
                total = stats["row_count"]
                stats["uniqueness"] = round(distinct / total, 4) if total > 0 else 0.0

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
        """
        Sample DataFrame (engine-agnostic).

        Args:
            df: DataFrame to sample
            fraction: Sample fraction (0.0-1.0)

        Returns:
            Sampled DataFrame
        """
        # PySpark
        if hasattr(df, "sample") and hasattr(df, "rdd"):
            return df.sample(fraction=fraction, seed=42)

        # Pandas / Polars / Dask
        elif hasattr(df, "sample"):
            try:
                return df.sample(frac=fraction, random_state=42)
            except TypeError:
                # Polars uses different API
                return df.sample(fraction=fraction, seed=42)

        return df
