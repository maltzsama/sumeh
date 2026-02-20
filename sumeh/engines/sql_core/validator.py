"""
SQLCore Validator.
Decouples the validation logic from the execution engine.
Converts SQL result rows (tuples) into Sumeh ValidationResults.
"""

from typing import List, Tuple, Any, Dict

from sumeh.core.models.validation import ValidationResult
from sumeh.core.models.metrics import MetricResult
from sumeh.core.rules.rule_model import RuleDef
from sumeh.engines.sql_core.registry import get_constraint


def validate_results(
    metrics_row: Tuple[Any, ...],
    rule_ids: List[str],
    rules: List[RuleDef],
    total_rows: int = 0,
) -> List[ValidationResult]:
    """
    Maps the raw metrics row from SQL execution back to Rule Constraints.
    Wraps raw SQL values into MetricResult objects to satisfy Constraint interface.
    """
    results = []

    # Create a lookup map for rules
    rule_map: Dict[str, RuleDef] = {
        getattr(r, "id", None) or f"{r.check_type}:{r.field}": r for r in rules
    }

    for i, r_id in enumerate(rule_ids):
        rule = rule_map.get(r_id)
        if not rule:
            continue

        # Raw value from SQL (usually float pass_rate or aggregation result)
        raw_val = metrics_row[i]

        # ---------------------------------------------------------------------
        # ADAPTER: Wrap scalar into MetricResult
        # ---------------------------------------------------------------------
        # Constraints expect an object with .value and .metadata

        # Estimate failure count based on pass rate (if it's a rate check)
        # Note: SQLCore analyzers usually return Pass Rate (0.0 to 1.0)
        metadata = {}
        affected_ids = []

        try:
            val_float = float(raw_val) if raw_val is not None else 0.0

            # Heuristic: If it's a row-level check, infer fail count
            if rule.check_type.startswith(("is_", "has_pattern", "not_")):
                fail_count = int(total_rows * (1.0 - val_float))
                metadata = {
                    "fail_count": fail_count,
                    "null_count": fail_count,  # For completeness
                    "duplicate_count": fail_count,  # For uniqueness
                    "total_count": total_rows,
                }
            else:
                # Table level (aggregations)
                metadata = {"metric": rule.check_type, "value": val_float}

            metric_obj = MetricResult(
                metric_type=rule.check_type,
                field=str(rule.field),
                value=val_float,
                total_rows=total_rows,
                affected_row_ids=[],  # SQL aggregation doesn't return IDs
                metadata=metadata,
            )

            # -----------------------------------------------------------------
            # JUDGMENT
            # -----------------------------------------------------------------
            constraint_cls = get_constraint(rule.check_type)
            result = constraint_cls.check(metric=metric_obj, rule=rule)
            results.append(result)

        except Exception as e:
            # Em debug, é bom printar isso pra não ficar cego
            # print(f"DEBUG: Error validating rule {r_id}: {e}")

            # Create an error result instead of silently skipping
            results.append(
                ValidationResult(
                    rule_id=r_id,
                    level="ROW",  # Default assumption
                    check_type=rule.check_type,
                    field=str(rule.field),
                    status="ERROR",
                    message=f"Validation error: {str(e)}",
                )
            )

    return results
