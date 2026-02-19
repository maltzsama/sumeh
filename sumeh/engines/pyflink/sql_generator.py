"""
SQL generator for PyFlink validations.

Generates Flink SQL with UDF calls based on validation rules.
"""
from typing import List
from sumeh.core.rules.rule_model import RuleDef


def generate_validation_sql(
    rules: List[RuleDef],
    table_name: str
) -> str:
    """
    Generate Flink SQL with validation UDFs.
    
    Adds _dq_errors column (ARRAY of error messages).
    
    Args:
        rules: Validation rules (ROW-LEVEL ONLY)
        table_name: Source table name
    
    Returns:
        Flink SQL query string
    """
    # Filter only row-level rules
    row_rules = [r for r in rules if r.is_applicable_for_level("ROW")]
    
    if not row_rules:
        return f"SELECT *, ARRAY[] as _dq_errors FROM {table_name}"
    
    # Build validation checks
    checks = []
    
    for rule in row_rules:
        check_sql = _build_check_sql(rule)
        if check_sql:
            checks.append(check_sql)
    
    if not checks:
        return f"SELECT *, ARRAY[] as _dq_errors FROM {table_name}"
    
    # Generate SQL
    checks_sql = ",\n        ".join(checks)
    
    sql = f"""SELECT 
    *,
    ARRAY[
        {checks_sql}
    ] as _dq_errors
FROM {table_name}"""
    
    return sql


def _build_check_sql(rule: RuleDef) -> str:
    """
    Build SQL fragment for a single rule.
    
    Returns UDF call string or empty if unsupported.
    """
    field = rule.field
    check_type = rule.check_type
    
    if check_type == "is_complete":
        return f"sumeh_check_complete({field})"
    
    elif check_type == "has_pattern":
        pattern = rule.value.replace("'", "''")  # Escape quotes
        return f"sumeh_check_pattern({field}, '{pattern}')"
    
    elif check_type == "is_greater_than":
        return f"sumeh_check_greater({field}, {rule.value})"
    
    elif check_type == "is_less_than":
        return f"sumeh_check_less({field}, {rule.value})"
    
    elif check_type == "is_equal":
        if isinstance(rule.value, str):
            return f"sumeh_check_equal({field}, '{rule.value}')"
        else:
            return f"sumeh_check_equal({field}, {rule.value})"
    
    elif check_type == "is_positive":
        return f"sumeh_check_positive({field})"
    
    elif check_type == "is_negative":
        return f"sumeh_check_negative({field})"
    
    elif check_type in ["is_in", "is_contained_in"]:
        allowed = str(rule.value)
        return f"sumeh_check_in_list({field}, '{allowed}')"
    
    elif check_type == "is_legit":
        return f"sumeh_check_legit({field})"
    
    else:
        # Unsupported check type (skip)
        return ""