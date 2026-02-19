"""
Dask Engine.
Orchestrates lazy validation and efficient distributed computation.
"""

import logging
import time
from typing import List

import dask
import dask.dataframe as dd
import numpy as np

from sumeh.core.models import ValidationReport, MetricResult
from sumeh.core.rules.rule_model import RuleDef
from sumeh.engines.dask.registry import get_analyzer, get_constraint

# Configure logger
logger = logging.getLogger("sumeh.dask")


def validate(
    df: dd.DataFrame, rules: List[RuleDef], baseline_provider=None
) -> ValidationReport:
    start_time = time.time()

    lazy_total_rows = df.map_partitions(len).sum()

    lazy_metrics = []
    active_rules = []

    for rule in rules:
        analyzer = get_analyzer(rule.check_type)

        if not analyzer:
            # Log warning via logging, not print
            r_id = getattr(rule, "id", f"{rule.check_type}:{rule.field}")
            logger.warning(f"Analyzer not found for rule: {r_id}")
            continue

        try:
            # Analyzer returns a Dask Scalar (Promise)
            metric_promise = analyzer.analyze(df, rule)
            lazy_metrics.append(metric_promise)
            active_rules.append(rule)
        except Exception as e:
            # Defensive ID access
            r_id = getattr(rule, "id", f"{rule.check_type}:{rule.field}")
            logger.error(f"Error preparing rule {r_id}: {e}")

    # 2. COMPUTE (The Single Pass)
    try:
        if not lazy_metrics:
            return ValidationReport(
                results=[], total_rows=0, engine="dask", execution_time_ms=0
            )

        # Trigger DAG execution: Computes count + all metrics in one go
        results_tuple = dask.compute(lazy_total_rows, *lazy_metrics)

    except Exception as e:
        return ValidationReport(
            results=[],
            total_rows=0,
            engine="dask",
            error_message=f"Dask Compute Error: {str(e)}",
        )

    # Unpack results
    total_rows = int(results_tuple[0])  # Ensure native int
    computed_metrics = results_tuple[1:]

    # 3. EVALUATE & CAST TYPES
    validation_results = []

    for i, rule in enumerate(active_rules):
        raw_val = computed_metrics[i]

        # FIX: Numpy Safety (Dask often returns numpy scalars)
        if hasattr(raw_val, "item"):
            raw_val = raw_val.item()

        metadata = {}
        metric_value = 0.0

        if rule.check_type.startswith("has_"):
            metric_value = float(raw_val)
            metadata = {"metric": rule.check_type, "value": metric_value}
        else:
            fail_count = int(raw_val)
            metric_value = (
                (total_rows - fail_count) / total_rows if total_rows > 0 else 1.0
            )
            metadata = {"fail_count": fail_count, "total_count": total_rows}

        metric_obj = MetricResult(
            metric_type=rule.check_type,
            field=str(rule.field),
            value=metric_value,
            total_rows=total_rows,
            affected_row_ids=[],
            metadata=metadata,
        )

        constraint = get_constraint(rule.check_type)
        res = constraint.check(metric_obj, rule)
        validation_results.append(res)

    return ValidationReport(
        results=validation_results,
        total_rows=total_rows,
        execution_time_ms=(time.time() - start_time) * 1000,
        engine="dask",
    )
