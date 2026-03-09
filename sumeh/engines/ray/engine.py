"""
Ray Data Engine.
Orchestrates validation using Ray's distributed processing.
"""

import time
from typing import List

from sumeh.engines.ray_data.registry import get_analyzer, get_constraint

from sumeh.core.models.validation import (
    ValidationReport,
    ValidationResult,
    ValidationLevel,
    ValidationStatus,
)
from sumeh.core.models.metrics import MetricResult

from sumeh.core.rules.rule_model import RuleDefinition


def validate(
    ds, rules: List[RuleDefinition], baseline_provider=None
) -> ValidationReport:
    """
    Validate Ray Dataset with row-level validation rules.

    Args:
        ds: Ray Dataset
        rules: Validation rules
        baseline_provider: Optional baseline provider

    Returns:
        ValidationReport with validation results

    Example:
        >>> import ray
        >>> ds = ray.data.read_parquet("data.parquet")
        >>> report = validate(ds, rules)
        >>> print(f"Pass rate: {report.pass_rate:.2%}")
    """
    start_time = time.time()

    # Get total rows
    total_rows = ds.count()

    # Process rules
    validation_results = []

    for rule in rules:
        # Only row-level rules supported
        if not rule.is_applicable_for_level("ROW"):
            validation_results.append(
                ValidationResult(
                    rule_id=getattr(rule, "id", ""),
                    level=ValidationLevel.TABLE,
                    category=rule.category or "unknown",
                    check_type=rule.check_type,
                    field=str(rule.field),
                    status=ValidationStatus.ERROR,
                    message="Table-level not supported in Ray Data",
                )
            )
            continue

        # Get analyzer
        analyzer = get_analyzer(rule.check_type)
        if not analyzer:
            validation_results.append(
                ValidationResult(
                    rule_id=getattr(rule, "id", ""),
                    level=ValidationLevel.ROW,
                    category=rule.category or "unknown",
                    check_type=rule.check_type,
                    field=str(rule.field),
                    status=ValidationStatus.ERROR,
                    message=f"Check type '{rule.check_type}' not implemented",
                )
            )
            continue

        try:
            # Analyze (computes immediately in Ray Data)
            raw_val = analyzer.analyze(ds, rule)

            # Build MetricResult
            if rule.check_type == "is_complete":
                null_count = int(raw_val)
                completeness_rate = (
                    (total_rows - null_count) / total_rows if total_rows > 0 else 1.0
                )
                metric_obj = MetricResult(
                    metric_type="completeness",
                    field=str(rule.field),
                    value=completeness_rate,
                    total_rows=total_rows,
                    affected_row_ids=[],
                    metadata={"null_count": null_count, "total_count": total_rows},
                )
            elif rule.check_type.startswith("has_"):
                # Aggregation - raw_val is the metric value
                metric_value = float(raw_val)
                metric_obj = MetricResult(
                    metric_type=rule.check_type,
                    field=str(rule.field),
                    value=metric_value,
                    total_rows=total_rows,
                    affected_row_ids=[],
                    metadata={"metric": rule.check_type, "value": metric_value},
                )
            else:
                fail_count = int(raw_val)
                pass_rate = (
                    (total_rows - fail_count) / total_rows if total_rows > 0 else 1.0
                )
                metric_obj = MetricResult(
                    metric_type=rule.check_type,
                    field=str(rule.field),
                    value=pass_rate,
                    total_rows=total_rows,
                    affected_row_ids=[],
                    metadata={"fail_count": fail_count, "total_count": total_rows},
                )

            # Check constraint
            constraint = get_constraint(rule.check_type)
            res = constraint.check(metric_obj, rule)
            validation_results.append(res)

        except Exception as e:
            validation_results.append(
                ValidationResult(
                    rule_id=getattr(rule, "id", ""),
                    level=ValidationLevel.ROW,
                    category=rule.category or "unknown",
                    check_type=rule.check_type,
                    field=str(rule.field),
                    status=ValidationStatus.ERROR,
                    message=f"Error: {str(e)}",
                )
            )

    return ValidationReport(
        results=validation_results,
        total_rows=total_rows,
        execution_time_ms=(time.time() - start_time) * 1000,
        engine="ray_data",
    )
