"""
Polars validation engine - Bifurcation Pattern (Optimized).

Uses Bulk Error Aggregation to avoid loop overhead and schema mismatches.
"""

import time
from datetime import datetime
from typing import List, Dict, Any

import polars as pl

from sumeh.core.models import (
    ValidationReport,
    ValidationResult,
    ValidationLevel,
    ValidationStatus,
)
from sumeh.core.rules.rule_model import RuleDef
from sumeh.engines.polars.dataframe import ValidatedPolarsDataFrame
from sumeh.engines.polars.registry import get_analyzer, get_constraint

# Centralized Schema Definition to avoid drift
ERROR_SCHEMA = {
    "rule_id": pl.Utf8,
    "check_type": pl.Utf8,
    "field": pl.Utf8,
    "category": pl.Utf8,
    "expected": pl.Utf8,
    "actual": pl.Utf8,
    "message": pl.Utf8,
    "timestamp": pl.Utf8,
}


def validate(
    df: pl.DataFrame, rules: List[RuleDef], baseline_provider=None
) -> ValidationReport:
    start_time = time.time()

    # 1. Add temporary row_id for join later if not exists
    if "_row_id" not in df.columns:
        df = df.with_row_index("_row_id")

    row_rules = [r for r in rules if r.is_applicable_for_level("ROW")]
    table_rules = [r for r in rules if r.is_applicable_for_level("TABLE")]

    results = []

    # 2. Execute Validations
    row_results = []
    if row_rules:
        row_results = _validate_row_level(df, row_rules)
        results.extend(row_results)

    # 3. Bulk Error Collection (The "Senior" Way)
    all_errors: List[Dict[str, Any]] = []

    for result in row_results:
        if result.status == ValidationStatus.FAIL and result.violating_row_ids:
            # Create the error payload structure (normalized)
            base_payload = {
                "rule_id": str(result.rule_id),
                "check_type": str(result.check_type),
                "field": str(result.field),
                "category": str(result.category),
                "expected": str(result.expected_value),
                "actual": str(result.actual_value),
                "message": str(result.message),
                "timestamp": datetime.utcnow().isoformat(),
            }

            # Expand to one record per violating row
            for rid in result.violating_row_ids:
                record = base_payload.copy()
                record["_row_id"] = rid
                all_errors.append(record)

    # 4. Join Errors back to DataFrame
    if all_errors:
        # Schema for creation includes _row_id
        creation_schema = {"_row_id": pl.UInt32, **ERROR_SCHEMA}

        errors_df = pl.from_dicts(all_errors, schema=creation_schema)

        # 4b. Group by row_id and collect into List[Struct]
        struct_cols = list(ERROR_SCHEMA.keys())

        errors_agg = (
            errors_df.group_by("_row_id")
            .agg(
                pl.col(struct_cols)
                .struct.rename_fields(struct_cols)
                .alias("_dq_errors_struct")
            )
            .select(pl.col("_row_id"), pl.col("_dq_errors_struct").alias("_dq_errors"))
        )

        errors_agg = errors_df.group_by("_row_id").agg(
            pl.struct(struct_cols).alias("_dq_errors")
        )

        # 4c. Join back to main DF
        df = df.join(errors_agg, on="_row_id", how="left")

        # 4d. Fill nulls safely
        dq_errors_dtype = errors_agg.schema["_dq_errors"]
        df = df.with_columns(
            pl.col("_dq_errors").fill_null(pl.lit([], dtype=dq_errors_dtype))
        )

    else:
        # No errors found at all, create empty column with correct type
        # Construct Struct Dtype from ERROR_SCHEMA
        fields = [pl.Field(k, v) for k, v in ERROR_SCHEMA.items()]
        error_list_dtype = pl.List(pl.Struct(fields))

        df = df.with_columns(pl.lit([], dtype=error_list_dtype).alias("_dq_errors"))

    # Table validations
    if table_rules:
        results.extend(_validate_table_level(df, table_rules, baseline_provider))

    # Clean up
    if "_row_id" in df.columns:
        df = df.drop("_row_id")

    execution_time_ms = (time.time() - start_time) * 1000

    return ValidationReport(
        results=results,
        total_rows=len(df),
        execution_time_ms=execution_time_ms,
        engine="polars",
        df_validated=ValidatedPolarsDataFrame(df),
    )


def _validate_row_level(
    df: pl.DataFrame, rules: List[RuleDef]
) -> List[ValidationResult]:
    results = []
    for rule in rules:
        try:
            analyzer_class = get_analyzer(rule.check_type)
            metric = analyzer_class.analyze(df, rule)
            constraint_class = get_constraint(rule.check_type)
            result = constraint_class.check(metric, rule)
            results.append(result)
        except Exception as e:
            # Error handling simples
            results.append(
                ValidationResult(
                    rule_id=getattr(rule, "id", ""),
                    level=ValidationLevel.ROW,
                    check_type=rule.check_type,
                    field=str(rule.field),
                    status=ValidationStatus.ERROR,
                    message=str(e),
                )
            )
    return results


def _validate_table_level(
    df: pl.DataFrame, rules: List[RuleDef], baseline_provider
) -> List[ValidationResult]:
    """Execute table-level validations."""
    results = []

    for rule in rules:
        skip_reason = rule.get_skip_reason("TABLE", "polars")
        if skip_reason:
            results.append(
                ValidationResult(
                    rule_id=getattr(rule, "id", ""),
                    level=ValidationLevel.TABLE,
                    category=rule.category or "unknown",
                    check_type=rule.check_type,
                    field=str(rule.field),
                    status=ValidationStatus.ERROR,
                    message=f"Skipped: {skip_reason}",
                )
            )
            continue

        try:
            analyzer_class = get_analyzer(rule.check_type)
            metric = analyzer_class.analyze(df, rule)

            constraint_class = get_constraint(rule.check_type)
            result = constraint_class.check(metric, rule)

            results.append(result)

        except KeyError as e:
            results.append(
                ValidationResult(
                    rule_id=getattr(rule, "id", ""),
                    level=ValidationLevel.TABLE,
                    category=rule.category or "unknown",
                    check_type=rule.check_type,
                    field=str(rule.field),
                    status=ValidationStatus.ERROR,
                    message=f"Not implemented: {str(e)}",
                )
            )

        except Exception as e:
            results.append(
                ValidationResult(
                    rule_id=getattr(rule, "id", ""),
                    level=ValidationLevel.TABLE,
                    category=rule.category or "unknown",
                    check_type=rule.check_type,
                    field=str(rule.field),
                    status=ValidationStatus.ERROR,
                    message=f"Error: {str(e)}",
                )
            )

    return results
