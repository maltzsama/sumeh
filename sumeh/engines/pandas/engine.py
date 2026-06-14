"""
Pandas validation engine - Bifurcation Pattern (Optimized).

Returns ValidationReport with ValidatedPandasDataFrame wrapper.
Optimized to avoid row-by-row iteration using bulk aggregation.
"""

import time
from datetime import datetime
from typing import List, Dict, Any, Union

import numpy as np
import pandas as pd

from sumeh.core.models.validation import (
    ValidationReport,
    ValidationResult,
    ValidationLevel,
    ValidationStatus,
)
from sumeh.core.rules.rule_model import RuleDefinition
from sumeh.engines.pandas.dataframe import ValidatedPandasDataFrame
from sumeh.engines.pandas.registry import get_analyzer, get_constraint


def validate(
    df: pd.DataFrame, rules: List[RuleDefinition], baseline_provider=None
) -> ValidationReport:
    """
    Validate pandas DataFrame using Bifurcation Pattern (Optimized).

    Returns ValidationReport with ValidatedPandasDataFrame wrapper.
    Use report.split() to get (good_df, bad_df).
    """
    start_time = time.time()

    # Ensure strict integer index for performance and alignment
    df = df.copy().reset_index(drop=True)

    row_rules = [r for r in rules if r.is_applicable_for_level("ROW")]
    table_rules = [r for r in rules if r.is_applicable_for_level("TABLE")]

    results = []

    # 1. Row-Level Validations
    if row_rules:
        row_results = _validate_row_level(df, row_rules)
        results.extend(row_results)

        all_errors: List[Dict[str, Any]] = []

        for result in row_results:
            if result.status == ValidationStatus.FAIL and result.violating_row_ids:
                # Create the error payload structure
                error_payload = {
                    "rule_id": str(getattr(result, "rule_id", "")),
                    "check_type": str(result.check_type),
                    "field": str(result.field),
                    "category": str(result.category),
                    "expected": str(result.expected_value),
                    "actual": str(result.actual_value),
                    "message": str(result.message),
                    "timestamp": datetime.utcnow().isoformat(),
                }

                # Flatten: (Row ID, Error Object)
                for rid in result.violating_row_ids:
                    all_errors.append({"_idx": rid, "error": error_payload})

        # --- MERGE BACK STRATEGY ---
        if all_errors:
            # Create a lightweight DF for grouping
            err_df = pd.DataFrame(all_errors)

            # Group by Index and aggregate errors into a list
            error_series = err_df.groupby("_idx")["error"].apply(list)

            # Join back to main DF
            df = df.join(error_series.rename("_dq_errors"))

            # Fill NaN (rows with no errors) with empty lists
            mask = df["_dq_errors"].isna()
            if mask.any():
                # ✅ FIX: Use Series with explicit index to avoid shape mismatch
                df.loc[mask, "_dq_errors"] = pd.Series(
                    [[] for _ in range(mask.sum())], index=df.index[mask]
                )
        else:
            # No errors found at all
            df["_dq_errors"] = [[] for _ in range(len(df))]

    else:
        # No row rules
        df["_dq_errors"] = [[] for _ in range(len(df))]

    # 2. Table-Level Validations
    if table_rules:
        results.extend(_validate_table_level(df, table_rules, baseline_provider))

    execution_time_ms = (time.time() - start_time) * 1000

    # Wrap DataFrame in ValidatedPandasDataFrame
    validated_wrapper = ValidatedPandasDataFrame(df)

    return ValidationReport(
        results=results,
        total_rows=len(df),
        execution_time_ms=execution_time_ms,
        engine="pandas",
        df_validated=validated_wrapper,  # Wrapper, not raw DataFrame
    )


def _validate_row_level(
    df: pd.DataFrame, rules: List[RuleDefinition]
) -> List[ValidationResult]:
    """Execute row-level validations."""
    results = []

    for rule in rules:
        skip_reason = rule.get_skip_reason("ROW", "pandas")
        if skip_reason:
            results.append(_create_skipped_result(rule, "ROW", skip_reason))
            continue

        try:
            analyzer_class = get_analyzer(rule.check_type)
            metric = analyzer_class.analyze(df, rule)

            constraint_class = get_constraint(rule.check_type)
            result = constraint_class.check(metric, rule)

            results.append(result)

        except KeyError as e:
            results.append(
                _create_error_result(rule, "ROW", f"Not implemented: {str(e)}")
            )
        except Exception as e:
            results.append(_create_error_result(rule, "ROW", f"Error: {str(e)}"))

    return results


def _validate_table_level(
    df: pd.DataFrame, rules: List[RuleDefinition], baseline_provider
) -> List[ValidationResult]:
    """Execute table-level validations."""
    results = []

    for rule in rules:
        skip_reason = rule.get_skip_reason("TABLE", "pandas")
        if skip_reason:
            results.append(_create_skipped_result(rule, "TABLE", skip_reason))
            continue

        try:
            analyzer_class = get_analyzer(rule.check_type)
            metric = analyzer_class.analyze(df, rule)

            constraint_class = get_constraint(rule.check_type)
            result = constraint_class.check(metric, rule)

            results.append(result)

        except KeyError as e:
            results.append(
                _create_error_result(rule, "TABLE", f"Not implemented: {str(e)}")
            )
        except Exception as e:
            results.append(_create_error_result(rule, "TABLE", f"Error: {str(e)}"))

    return results


# --- Helper Methods to keep main logic clean ---
def _create_skipped_result(
    rule: RuleDefinition, level: str, reason: str
) -> ValidationResult:
    return ValidationResult(
        rule_id=getattr(rule, "id", ""),
        level=ValidationLevel[level],
        category=rule.category or "unknown",
        check_type=rule.check_type,
        field=str(rule.field),
        status=ValidationStatus.ERROR,
        message=f"Skipped: {reason}",
    )


def _create_error_result(
    rule: RuleDefinition, level: str, message: str
) -> ValidationResult:
    return ValidationResult(
        rule_id=getattr(rule, "id", ""),
        level=ValidationLevel[level],
        category=rule.category or "unknown",
        check_type=rule.check_type,
        field=str(rule.field),
        status=ValidationStatus.ERROR,
        message=message,
    )
