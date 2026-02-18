"""
Polars validation engine - Bifurcation Pattern (Optimized).

Uses Bulk Error Aggregation to avoid loop overhead and schema mismatches.
"""
import polars as pl
import time
from typing import List, Dict, Any
from datetime import datetime
from sumeh.core.models import ValidationReport, ValidationResult, ValidationLevel, ValidationStatus
from sumeh.core.rules.rule_model import RuleDef
from sumeh.engines.polars.registry import get_analyzer, get_constraint
from sumeh.engines.polars.dataframe import ValidatedPolarsDataFrame


def validate(df: pl.DataFrame, rules: List[RuleDef], baseline_provider=None) -> ValidationReport:
    start_time = time.time()
    
    # 1. Add temporary row_id for join later
    df = df.with_row_count("_row_id")
    
    row_rules = [r for r in rules if r.is_applicable_for_level("ROW")]
    table_rules = [r for r in rules if r.is_applicable_for_level("TABLE")]
    
    results = []
    
    # 2. Execute Validations
    if row_rules:
        row_results = _validate_row_level(df, row_rules)
        results.extend(row_results)
        
        # 3. Bulk Error Collection (The "Senior" Way)
        # Instead of iterating and updating df N times, we collect all errors first.
        all_errors: List[Dict[str, Any]] = []
        
        for result in row_results:
            if result.status == ValidationStatus.FAIL and result.violating_row_ids:
                # Create the error payload structure (normalized)
                error_payload = {
                    "rule_id": str(result.rule_id),
                    "check_type": str(result.check_type),
                    "field": str(result.field),
                    "category": str(result.category),
                    "expected": str(result.expected_value), # Stringify to ensure schema consistency
                    "actual": str(result.actual_value),     # Stringify to ensure schema consistency
                    "message": str(result.message),
                    "timestamp": datetime.utcnow().isoformat(),
                }
                
                # Expand to one record per violating row
                for rid in result.violating_row_ids:
                    # Flatten for DataFrame creation
                    record = error_payload.copy()
                    record["_row_id"] = rid
                    all_errors.append(record)

        # 4. Join Errors back to DataFrame
        if all_errors:
            # Create a separate DataFrame for errors
            errors_schema = {
                "_row_id": pl.UInt32, # Matches default row_count type
                "rule_id": pl.Utf8,
                "check_type": pl.Utf8,
                "field": pl.Utf8,
                "category": pl.Utf8,
                "expected": pl.Utf8,
                "actual": pl.Utf8,
                "message": pl.Utf8,
                "timestamp": pl.Utf8,
            }
            
            # 4a. Create Errors DF
            errors_df = pl.from_dicts(all_errors, schema=errors_schema)
            
            # 4b. Group by row_id and collect into List[Struct]
            # We structify all columns EXCEPT _row_id
            struct_cols = [c for c in errors_df.columns if c != "_row_id"]
            
            errors_agg = errors_df.group_by("_row_id").agg(
                pl.struct(struct_cols).alias("_dq_errors")
            )
            
            # 4c. Join back to main DF
            df = df.join(errors_agg, on="_row_id", how="left")
            
            # 4d. Fill nulls (rows with no errors) with empty lists
            # Note: We need to respect the schema of the list
            df = df.with_columns(
                pl.col("_dq_errors").fill_null(pl.lit([], dtype=df.schema["_dq_errors"]))
            )
            
        else:
            # No errors found at all, create empty column with correct type
            # We define the schema manually to match the structure above
            error_struct_dtype = pl.Struct([
                pl.Field("rule_id", pl.Utf8),
                pl.Field("check_type", pl.Utf8),
                pl.Field("field", pl.Utf8),
                pl.Field("category", pl.Utf8),
                pl.Field("expected", pl.Utf8),
                pl.Field("actual", pl.Utf8),
                pl.Field("message", pl.Utf8),
                pl.Field("timestamp", pl.Utf8),
            ])
            df = df.with_columns(
                pl.lit([], dtype=pl.List(error_struct_dtype)).alias("_dq_errors")
            )

    # Table validations
    if table_rules:
        results.extend(_validate_table_level(df, table_rules, baseline_provider))
    
    # Clean up
    df = df.drop("_row_id")
    
    execution_time_ms = (time.time() - start_time) * 1000
    
    return ValidationReport(
        results=results,
        total_rows=len(df),
        execution_time_ms=execution_time_ms,
        engine="polars",
        df_validated=ValidatedPolarsDataFrame(df),
    )


def _validate_row_level(df: pl.DataFrame, rules: List[RuleDef]) -> List[ValidationResult]:
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
            results.append(ValidationResult(
                rule_id=getattr(rule, 'id', ''),
                level=ValidationLevel.ROW,
                check_type=rule.check_type,
                field=str(rule.field),
                status=ValidationStatus.ERROR,
                message=str(e)
            ))
    return results


def _validate_table_level(df: pl.DataFrame, rules: List[RuleDef], baseline_provider) -> List[ValidationResult]:
    """Execute table-level validations."""
    results = []
    
    for rule in rules:
        skip_reason = rule.get_skip_reason("TABLE", "polars")
        if skip_reason:
            results.append(ValidationResult(
                rule_id=getattr(rule, 'id', ''),
                level=ValidationLevel.TABLE,
                category=rule.category or "unknown",
                check_type=rule.check_type,
                field=str(rule.field),
                status=ValidationStatus.ERROR,
                message=f"Skipped: {skip_reason}",
            ))
            continue
        
        try:
            analyzer_class = get_analyzer(rule.check_type)
            metric = analyzer_class.analyze(df, rule)
            
            constraint_class = get_constraint(rule.check_type)
            result = constraint_class.check(metric, rule)
            
            results.append(result)
            
        except KeyError as e:
            results.append(ValidationResult(
                rule_id=getattr(rule, 'id', ''),
                level=ValidationLevel.TABLE,
                category=rule.category or "unknown",
                check_type=rule.check_type,
                field=str(rule.field),
                status=ValidationStatus.ERROR,
                message=f"Not implemented: {str(e)}",
            ))
        
        except Exception as e:
            results.append(ValidationResult(
                rule_id=getattr(rule, 'id', ''),
                level=ValidationLevel.TABLE,
                category=rule.category or "unknown",
                check_type=rule.check_type,
                field=str(rule.field),
                status=ValidationStatus.ERROR,
                message=f"Error: {str(e)}",
            ))
    
    return results