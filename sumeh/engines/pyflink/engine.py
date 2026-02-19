"""
PyFlink Engine - Orchestration layer.

Coordinates UDF registration, SQL generation, and validation execution.
"""

from typing import List
from sumeh.core.rules.rule_model import RuleDef
from sumeh.engines.pyflink.udfs import get_all_udfs
from sumeh.engines.pyflink.dataframe import ValidatedFlinkTable


def register_udfs(table_env):
    """
    Register Sumeh validation UDFs in PyFlink TableEnvironment.

    Must be called before using validate() or get_validation_sql().

    Args:
        table_env: PyFlink StreamTableEnvironment

    Example:
        >>> from pyflink.table import StreamTableEnvironment
        >>> t_env = StreamTableEnvironment.create(...)
        >>> sumeh_pyflink.register_udfs(t_env)
    """
    try:
        from pyflink.table import DataTypes
        from pyflink.table.udf import udf
    except ImportError:
        raise ImportError("PyFlink not installed. Run: pip install apache-flink")

    # Get all UDFs from udfs.py
    udfs = get_all_udfs()

    # Register each UDF
    for udf_name, udf_func in udfs.items():
        table_env.create_temporary_function(udf_name, udf_func)



def validate(table_env, rules: List[RuleDef], table_name: str):
    """
    Validate PyFlink Table with automatic UDF registration.

    Returns ValidatedFlinkTable wrapper with .split() method.
    User handles execution and sinks.

    Args:
        table_env: PyFlink StreamTableEnvironment
        rules: Validation rules
        table_name: Source table name

    Returns:
        ValidatedFlinkTable with .split() method

    Example - Bifurcation:
        >>> result = sumeh_pyflink.validate(t_env, rules, "users")
        >>> good, bad = result.split()
        >>> good.execute_insert("clean_topic")
        >>> bad.execute_insert("dlq_topic")

    Example - Single sink:
        >>> result = sumeh_pyflink.validate(t_env, rules, "users")
        >>> result.execute_insert("output_with_errors")
    """
    # Register UDFs
    register_udfs(table_env)

    # Generate SQL
    sql = get_validation_sql(rules, table_name)

    # Execute query and wrap
    table = table_env.sql_query(sql)
    return ValidatedFlinkTable(table)
