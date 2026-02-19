"""
SQL generator for PyFlink validations.

Generates Flink SQL with UDF calls based on validation rules.
"""

from typing import List

from sumeh.core.rules.rule_model import RuleDef


def generate_validation_sql(rules: List[RuleDef], table_name: str) -> str:
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

    # Completeness
    if check_type == "is_complete":
        return f"sumeh_check_complete({field})"

    # Pattern
    elif check_type == "has_pattern":
        pattern = rule.value.replace("'", "''")  # Escape quotes
        return f"sumeh_check_pattern({field}, '{pattern}')"

    elif check_type == "is_legit":
        return f"sumeh_check_legit({field})"

    # Comparison
    elif check_type == "is_equal":
        if isinstance(rule.value, str):
            return f"sumeh_check_equal({field}, '{rule.value}')"
        else:
            return f"sumeh_check_equal({field}, {rule.value})"

    elif check_type == "is_equal_than":
        # Column-to-column comparison
        other_column = rule.value
        return f"sumeh_check_equal_column({field}, {other_column})"

    elif check_type == "is_greater_than":
        return f"sumeh_check_greater({field}, {rule.value})"

    elif check_type == "is_less_than":
        return f"sumeh_check_less({field}, {rule.value})"

    elif check_type == "is_greater_or_equal_than":
        return f"sumeh_check_greater_or_equal({field}, {rule.value})"

    elif check_type == "is_less_or_equal_than":
        return f"sumeh_check_less_or_equal({field}, {rule.value})"

    elif check_type == "is_positive":
        return f"sumeh_check_positive({field})"

    elif check_type == "is_negative":
        return f"sumeh_check_negative({field})"

    elif check_type == "is_in_millions":
        return f"sumeh_check_in_millions({field})"

    elif check_type == "is_in_billions":
        return f"sumeh_check_in_billions({field})"

    elif check_type == "is_between":
        if isinstance(rule.value, list) and len(rule.value) == 2:
            min_val, max_val = rule.value
            return f"sumeh_check_between({field}, {min_val}, {max_val})"
        return ""

    # Membership
    elif check_type in ["is_in", "is_contained_in"]:
        allowed = str(rule.value)
        return f"sumeh_check_in_list({field}, '{allowed}')"

    elif check_type in ["not_in", "not_contained_in"]:
        forbidden = str(rule.value)
        return f"sumeh_check_not_in_list({field}, '{forbidden}')"

    # Date
    elif check_type == "is_today":
        return f"sumeh_check_date_today({field})"

    elif check_type in ["is_yesterday", "is_t_minus_1"]:
        return f"sumeh_check_date_yesterday({field})"

    elif check_type == "is_t_minus_2":
        return f"sumeh_check_date_t_minus_2({field})"

    elif check_type == "is_t_minus_3":
        return f"sumeh_check_date_t_minus_3({field})"

    elif check_type == "is_past_date":
        return f"sumeh_check_date_past({field})"

    elif check_type == "is_future_date":
        return f"sumeh_check_date_future({field})"

    elif check_type == "is_on_weekday":
        return f"sumeh_check_date_weekday({field})"

    elif check_type == "is_on_weekend":
        return f"sumeh_check_date_weekend({field})"

    elif check_type == "is_on_monday":
        return f"sumeh_check_date_monday({field})"

    elif check_type == "is_on_tuesday":
        return f"sumeh_check_date_tuesday({field})"

    elif check_type == "is_on_wednesday":
        return f"sumeh_check_date_wednesday({field})"

    elif check_type == "is_on_thursday":
        return f"sumeh_check_date_thursday({field})"

    elif check_type == "is_on_friday":
        return f"sumeh_check_date_friday({field})"

    elif check_type == "is_on_saturday":
        return f"sumeh_check_date_saturday({field})"

    elif check_type == "is_on_sunday":
        return f"sumeh_check_date_sunday({field})"

    elif check_type == "is_date_between":
        if isinstance(rule.value, list) and len(rule.value) == 2:
            start, end = rule.value
            return f"sumeh_check_date_between({field}, '{start}', '{end}')"
        return ""

    elif check_type == "is_date_after":
        return f"sumeh_check_date_after({field}, '{rule.value}')"

    elif check_type == "is_date_before":
        return f"sumeh_check_date_before({field}, '{rule.value}')"

    else:
        # Unsupported check type (skip)
        return ""
