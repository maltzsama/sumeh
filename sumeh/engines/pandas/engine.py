"""
Pandas validation engine - Bifurcation Pattern.

Returns ValidationReport with ValidatedPandasDataFrame wrapper.
"""
import pandas as pd
import time
from typing import List
from datetime import datetime
from sumeh.core.models import ValidationReport, ValidationResult, ValidationLevel, ValidationStatus
from sumeh.core.rules.rule_model import RuleDef
from sumeh.engines.pandas.registry import get_analyzer, get_constraint
from sumeh.engines.pandas.dataframe import ValidatedPandasDataFrame


def validate(df: pd.DataFrame, rules: List[RuleDef], baseline_provider=None) -> ValidationReport:
    """
    Validate pandas DataFrame using Bifurcation Pattern.
    
    Returns ValidationReport with ValidatedPandasDataFrame wrapper.
    Use report.split() to get (good_df, bad_df).
    
    Example:
        >>> report = validate(df, rules)
        >>> good_df, bad_df = report.split()  # Delegates to wrapper
        >>> good_df.to_parquet("clean/")
        >>> bad_df.to_parquet("quarantine/")
    """
    start_time = time.time()
    
    # Initialize _dq_errors column
    df = df.copy().reset_index(drop=True)
    df['_dq_errors'] = [[] for _ in range(len(df))]
    
    row_rules = [r for r in rules if r.is_applicable_for_level("ROW")]
    table_rules = [r for r in rules if r.is_applicable_for_level("TABLE")]
    
    results = []
    
    # Row-level validations
    if row_rules:
        row_results = _validate_row_level(df, row_rules)
        results.extend(row_results)
        
        # Populate _dq_errors with DLQ format
        for result in row_results:
            if result.status == ValidationStatus.FAIL:
                error_obj = {
                    "rule_id": result.rule_id,
                    "check_type": result.check_type,
                    "field": result.field,
                    "category": result.category,
                    "expected": result.expected_value,
                    "actual": result.actual_value,
                    "message": result.message,
                    "timestamp": datetime.utcnow().isoformat(),
                }
                
                for row_id in result.violating_row_ids:
                    df.at[row_id, '_dq_errors'].append(error_obj)
    
    # Table-level validations
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


def _validate_row_level(df: pd.DataFrame, rules: List[RuleDef]) -> List[ValidationResult]:
    """Execute row-level validations."""
    results = []
    
    for rule in rules:
        skip_reason = rule.get_skip_reason("ROW", "pandas")
        if skip_reason:
            results.append(ValidationResult(
                rule_id=getattr(rule, 'id', ''),
                level=ValidationLevel.ROW,
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
                level=ValidationLevel.ROW,
                category=rule.category or "unknown",
                check_type=rule.check_type,
                field=str(rule.field),
                status=ValidationStatus.ERROR,
                message=f"Not implemented: {str(e)}",
            ))
        
        except Exception as e:
            results.append(ValidationResult(
                rule_id=getattr(rule, 'id', ''),
                level=ValidationLevel.ROW,
                category=rule.category or "unknown",
                check_type=rule.check_type,
                field=str(rule.field),
                status=ValidationStatus.ERROR,
                message=f"Error: {str(e)}",
            ))
    
    return results


def _validate_table_level(df: pd.DataFrame, rules: List[RuleDef], baseline_provider) -> List[ValidationResult]:
    """Execute table-level validations."""
    results = []
    
    for rule in rules:
        skip_reason = rule.get_skip_reason("TABLE", "pandas")
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