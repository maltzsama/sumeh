#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module provides utilities for generating and validating SQL expressions 
and data quality rules using DuckDB. It includes functions for building SQL 
expressions, validating dataframes against rules, summarizing rule violations, 
and schema validation.

Classes:
    __RuleCtx: A dataclass representing the context required to generate SQL 
              expressions for data quality rules.

Functions:
    __escape_single_quotes(txt: str) -> str:
        Escapes single quotes in a string for SQL compatibility.

    _format_sequence(value: Any) -> str:
        Formats a sequence (list, tuple, or string) into a SQL-compatible 
        representation for IN/NOT IN clauses.

    _is_complete(r: __RuleCtx) -> str:
        Generates a SQL expression to check if a column is not NULL.

    _are_complete(r: __RuleCtx) -> str:
        Generates a SQL expression to check if all columns in a list are not NULL.

    _is_unique(r: __RuleCtx) -> str:
        Generates a SQL expression to check if a column has unique values.

    _are_unique(r: __RuleCtx) -> str:
        Generates a SQL expression to check if a combination of columns has unique values.

    _is_greater_than(r: __RuleCtx) -> str:
        Generates a SQL expression to check if a column's value is greater than a given value.

    _is_less_than(r: __RuleCtx) -> str:
        Generates a SQL expression to check if a column's value is less than a given value.

    _is_greater_or_equal_than(r: __RuleCtx) -> str:
        Generates a SQL expression to check if a column's value is greater than or equal to a given value.

    _is_less_or_equal_than(r: __RuleCtx) -> str:
        Generates a SQL expression to check if a column's value is less than or equal to a given value.

    _is_equal_than(r: __RuleCtx) -> str:
        Generates a SQL expression to check if a column's value is equal to a given value.

    _is_between(r: __RuleCtx) -> str:
        Generates a SQL expression to check if a column's value is between two values.

    _has_pattern(r: __RuleCtx) -> str:
        Generates a SQL expression to check if a column's value matches a regular expression pattern.

    _is_contained_in(r: __RuleCtx) -> str:
        Generates a SQL expression to check if a column's value is in a given sequence.

    _not_contained_in(r: __RuleCtx) -> str:
        Generates a SQL expression to check if a column's value is not in a given sequence.

    _satisfies(r: __RuleCtx) -> str:
        Generates a SQL expression based on a custom condition provided as a string.

    _build_union_sql(rules: List[Dict]) -> str:
        Builds a SQL query that combines multiple rule-based conditions into a UNION ALL query.

    validate(
        conn: dk.DuckDBPyConnection
        Validates a DuckDB dataframe against a set of rules and returns the results.

    summarize(
        total_rows: Optional[int] = None
        Summarizes rule violations and calculates pass rates for each rule.

    validate_schema(
        expected: List[Dict[str, Any]], 
        table: str
        Validates the schema of a DuckDB table against an expected schema.
"""

from __future__ import annotations

import duckdb as dk
import ast, warnings
from dataclasses import dataclass
from typing import List, Dict, Callable, Any, Optional, Tuple
from sumeh.services.utils import __compare_schemas

__RULE_DISPATCH: dict[str, Callable[[__RuleCtx], str]] = {
    "is_complete": _is_complete,
    "are_complete": _are_complete,
    "is_unique": _is_unique,
    "are_unique": _are_unique,
    "is_greater_than": _is_greater_than,
    "is_less_than": _is_less_than,
    "is_greater_or_equal_than": _is_greater_or_equal_than,
    "is_less_or_equal_than": _is_less_or_equal_than,
    "is_equal_than": _is_equal_than,
    "is_between": _is_between,
    "has_pattern": _has_pattern,
    "is_contained_in": _is_contained_in,
    "not_contained_in": _not_contained_in,
    "satisfies": _satisfies,
}


def __escape_single_quotes(txt: str) -> str:
    """
    Escapes single quotes in a given string by replacing each single quote
    with two single quotes. This is commonly used to sanitize strings for
    use in SQL queries.

    Args:
        txt (str): The input string to process.

    Returns:
        str: The processed string with single quotes escaped.
    """
    return txt.replace("'", "''")


def _format_sequence(value: Any) -> str:
    """
    Formats a sequence-like input into a string representation suitable for SQL queries.

    Converts inputs into a tuple-like string format:
        - 'BR,US' -> "('BR','US')"
        - ['BR', 'US'] -> "('BR','US')"
        - ('BR', 'US') -> "('BR','US')"

    Args:
        value (Any): The input value to be formatted. Can be a string, list, or tuple.

    Returns:
        str: A string representation of the input in the format "('item1','item2',...)".

    Raises:
        ValueError: If the input value is None or cannot be interpreted as a sequence.

    Notes:
        - If the input is a string, it attempts to parse it as a Python literal.
        - If parsing fails, it splits the string by commas and processes the resulting parts.
        - Empty or invalid elements are ignored in the output.
    """

    if value is None:
        raise ValueError("value cannot be None for IN/NOT IN")

    if isinstance(value, (list, tuple)):
        seq = value
    else:
        try:  # tenta interpretar como literal Python
            seq = ast.literal_eval(value)
            if not isinstance(seq, (list, tuple)):
                raise ValueError
        except Exception:
            seq = [v.strip(" []()'\"") for v in str(value).split(",")]

    return "(" + ",".join(repr(str(x).strip()) for x in seq if x != "") + ")"


@dataclass(slots=True)
class __RuleCtx:
    """
    __RuleCtx is a context class used to define rules for processing data.

    Attributes:
        column (Any): Represents the column(s) to which the rule applies. 
                      It can be a string (for a single column) or a list of strings (for multiple columns).
        value (Any): The value associated with the rule. The type of this value depends on the specific rule implementation.
        name (str): The name of the rule, typically used to indicate the type of check being performed.
    """
    column: Any  # str ou list[str]
    value: Any
    name: str  # check_type


def _is_complete(r: __RuleCtx) -> str:
    """
    Constructs a SQL condition to check if a column is not NULL.

    Args:
        r (__RuleCtx): An object containing context information, including the column name.

    Returns:
        str: A SQL condition string in the format "<column> IS NOT NULL".
    """
    return f"{r.column} IS NOT NULL"


def _are_complete(r: __RuleCtx) -> str:
    """
    Constructs a SQL condition string that checks if all specified columns in a rule context are not NULL.

    Args:
        r (__RuleCtx): The rule context containing the list of column names to check.

    Returns:
        str: A SQL condition string in the format "(column1 IS NOT NULL AND column2 IS NOT NULL ...)".
    """
    parts = " AND ".join(f"{c} IS NOT NULL" for c in r.column)
    return f"({parts})"


def _is_unique(r: __RuleCtx) -> str:
    """
    Generates a SQL expression to check if a column value is unique within a table.

    Args:
        r (__RuleCtx): A context object containing metadata, including the column name to check.

    Returns:
        str: A SQL string that evaluates to True if the column value is unique, otherwise False.
    """
    return (
        f"(SELECT COUNT(*)                            \n"
        f"   FROM tbl AS d2                           \n"
        f"   WHERE d2.{r.column} = tbl.{r.column}     \n"
        f") = 1"
    )


def _are_unique(r: __RuleCtx) -> str:
    """
    Generates a SQL query string to check if the combination of specified columns
    in a table is unique for each row.

    Args:
        r (__RuleCtx): A context object containing the column names to be checked
                       for uniqueness.

    Returns:
        str: A SQL query string that evaluates whether the combination of the
             specified columns is unique for each row in the table.
    """
    combo_outer = " || '|' || ".join(f"tbl.{c}" for c in r.column)
    combo_inner = " || '|' || ".join(f"d2.{c}" for c in r.column)

    return (
        f"(SELECT COUNT(*)                  \n"
        f"   FROM tbl AS d2                 \n"
        f"   WHERE ({combo_inner}) = ({combo_outer})\n"
        f") = 1"
    )


def _is_greater_than(r: __RuleCtx) -> str:
    """
    Generates a SQL condition string to check if a column's value is greater than a specified value.

    Args:
        r (__RuleCtx): A context object containing the column name and the value to compare.

    Returns:
        str: A SQL condition string in the format "<column> > <value>".
    """
    return f"{r.column} > {r.value}"


def _is_less_than(r: __RuleCtx) -> str:
    """
    Generates a SQL condition string that checks if a column's value is less than a specified value.

    Args:
        r (__RuleCtx): A context object containing the column name and the value to compare against.

    Returns:
        str: A SQL condition string in the format "<column> < <value>".
    """
    return f"{r.column} < {r.value}"


def _is_greater_or_equal_than(r: __RuleCtx) -> str:
    """
    Generates a SQL expression to check if a column's value is greater than or equal to a specified value.

    Args:
        r (__RuleCtx): A context object containing the column name and the value to compare.

    Returns:
        str: A SQL expression in the format "<column> >= <value>".
    """
    return f"{r.column} >= {r.value}"


def _is_less_or_equal_than(r: __RuleCtx) -> str:
    """
    Generates a SQL condition string that checks if a column's value is less than or equal to a specified value.

    Args:
        r (__RuleCtx): A context object containing the column name and the value to compare.

    Returns:
        str: A SQL condition string in the format "<column> <= <value>".
    """
    return f"{r.column} <= {r.value}"


def _is_equal_than(r: __RuleCtx) -> str:
    """
    Generates a SQL equality condition string for a given rule context.

    Args:
        r (__RuleCtx): The rule context containing the column and value to compare.

    Returns:
        str: A string representing the SQL equality condition in the format "column = value".
    """
    return f"{r.column} = {r.value}"


def _is_between(r: __RuleCtx) -> str:
    """
    Constructs a SQL BETWEEN clause for a given rule context.

    Args:
        r (__RuleCtx): The rule context containing the column name and value(s).
                       The `value` attribute can be a list, tuple, or a string
                       representation of a range (e.g., "lo, hi").

    Returns:
        str: A SQL BETWEEN clause in the format "column BETWEEN lo AND hi".

    Notes:
        - If `r.value` is a list or tuple, it is expected to contain exactly two elements
          representing the lower (lo) and upper (hi) bounds.
        - If `r.value` is a string, it will be split by commas and stripped of any
          surrounding brackets, parentheses, or quotes to extract the bounds.
    """
    val = r.value
    if isinstance(val, (list, tuple)):
        lo, hi = val
    else:
        lo, hi, *_ = [v.strip(" []()'\"") for v in str(val).split(",")]
    return f"{r.column} BETWEEN {lo} AND {hi}"


def _has_pattern(r: __RuleCtx) -> str:
    """
    Constructs a SQL expression to check if a column's value matches a given regular expression pattern.

    Args:
        r (__RuleCtx): An object containing the column name and the value to be used as the pattern.
                       The `value` attribute is expected to be a string or convertible to a string,
                       and the `column` attribute is the name of the column to be checked.

    Returns:
        str: A SQL expression string that uses the REGEXP_MATCHES function to evaluate
             whether the column matches the escaped pattern.
    """
    pat = __escape_single_quotes(str(r.value))
    return f"REGEXP_MATCHES({r.column}, '{pat}')"


def _is_contained_in(r: __RuleCtx) -> str:
    """
    Generates a SQL fragment that checks if a column's value is contained within a sequence of values.

    Args:
        r (__RuleCtx): A context object containing the column name and the sequence of values.

    Returns:
        str: A SQL fragment in the format "<column> IN (<value1>, <value2>, ...)".
    """
    return f"{r.column} IN {_format_sequence(r.value)}"


def _not_contained_in(r: __RuleCtx) -> str:
    """
    Generates a SQL expression that checks if a column's value is not contained 
    within a specified sequence of values.

    Args:
        r (__RuleCtx): A context object containing the column name and the sequence 
                       of values to check against.

    Returns:
        str: A SQL string in the format "<column> NOT IN (<value1>, <value2>, ...)".
    """
    return f"{r.column} NOT IN {_format_sequence(r.value)}"


def _satisfies(r: __RuleCtx) -> str:
    """
    Constructs a string representation of the given rule context.

    Args:
        r (__RuleCtx): The rule context containing a value to be formatted.

    Returns:
        str: A string in the format "(value)" where 'value' is the value of the rule context.
    """
    return f"({r.value})"

def _build_union_sql(rules: List[Dict]) -> str:
    """
    Constructs a SQL query that combines multiple rule-based checks into a single query
    using UNION ALL. Each rule specifies a condition to be checked on a table, and the
    resulting query flags rows that do not satisfy the condition.

    Args:
        rules (List[Dict]): A list of dictionaries where each dictionary represents a rule.
            Each rule should contain the following keys:
            - "check_type" (str): The type of check to perform.
            - "field" (str): The column name to apply the check on.
            - "value" (optional): The value to use in the check.
            - "execute" (optional, bool): Whether to execute the rule. Defaults to True.

    Returns:
        str: A SQL query string that combines all active rules using UNION ALL. If no
        rules are active, returns a query that produces an empty result set.

    Notes:
        - If a rule's "check_type" is not recognized, a warning is issued, and the rule
          is skipped.
        - The resulting query includes a "dq_status" column that indicates the rule
          that flagged the row, formatted as "column:check_type:value".
        - If no rules are active, the query returns an empty result set with the same
          structure as the input table.
    """
    pieces: list[str] = []

    for r in rules:
        if not r.get("execute", True):
            continue

        check = r["check_type"]
        builder = __RULE_DISPATCH.get(check)
        if builder is None:
            warnings.warn(f"Regra desconhecida: {check}")
            continue

        ctx = __RuleCtx(
            column=r["field"],
            value=r.get("value"),
            name=check,
        )

        expr_ok = builder(ctx)  # condição “passa”
        dq_tag = __escape_single_quotes(f"{ctx.column}:{check}:{ctx.value}")
        pieces.append(
            f"SELECT *, '{dq_tag}' AS dq_status FROM tbl WHERE NOT ({expr_ok})"
        )

    # se não há regras ativas: devolve DF vazio
    if not pieces:
        return "SELECT *, '' AS dq_status FROM tbl WHERE 1=0"

    return "\nUNION ALL\n".join(pieces)


def validate(df_rel: dk.DuckDBPyRelation, rules: List[Dict], conn: dk.DuckDBPyConnection) -> dk.DuckDBPyRelation:
    """
    Validates a DuckDB relation against a set of rules and returns the processed relation.

    Args:
        df_rel (dk.DuckDBPyRelation): The input DuckDB relation to be validated.
        rules (List[Dict]): A list of dictionaries representing validation rules.
        conn (dk.DuckDBPyConnection): The DuckDB connection object used for executing SQL queries.

    Returns:
        dk.DuckDBPyRelation: A tuple containing:
            - The final DuckDB relation with aggregated validation statuses.
            - The raw DuckDB relation resulting from applying the validation rules.

    Notes:
        - The function creates a temporary view of the input relation named "tbl".
        - Validation rules are combined into a union SQL query using `_build_union_sql`.
        - The final relation includes all original columns and an aggregated `dq_status` column.
    """
    df_rel.create_view("tbl")

    union_sql = _build_union_sql(rules)

    cols_df = conn.sql("PRAGMA table_info('tbl')").df()
    colnames = cols_df["name"].tolist()
    cols_sql = ", ".join(colnames)

    raw_sql = f"""
        {union_sql}
    """

    raw = conn.sql(raw_sql)

    final_sql = f"""
    SELECT
        {cols_sql},
        STRING_AGG(dq_status, ';') AS dq_status
    FROM raw
    GROUP BY {cols_sql}
    """
    final = conn.sql(final_sql)

    return final, raw


def __rules_to_duckdb_df(rules: List[Dict]) -> str:
    """
    Converts a list of rule dictionaries into a DuckDB-compatible SQL query string.
    Each rule in the input list is processed to generate a SQL `SELECT` statement
    with the following fields:
    - `col`: The column name(s) associated with the rule.
    - `rule`: The name of the rule (check type).
    - `pass_threshold`: A numeric threshold value for the rule, defaulting to 1.0 if not provided.
    - `value`: The value associated with the rule, which can be a string, list, tuple, or `NULL`.
    Rules with the `execute` field set to `False` are skipped.
    If the input list is empty or all rules are skipped, the function returns a SQL query
    that selects `NULL` values with a `LIMIT 0`.
    Args:
        rules (List[Dict]): A list of dictionaries, where each dictionary represents a rule
            with the following keys:
            - `field` (str or list): The column(s) associated with the rule.
            - `check_type` (str): The name of the rule.
            - `value` (optional): The value associated with the rule.
            - `threshold` (optional, float): The numeric threshold for the rule.
            - `execute` (optional, bool): Whether the rule should be executed (default is `True`).
    Returns:
        str: A DuckDB-compatible SQL query string representing the rules.
    """
    
    parts: List[str] = []

    for r in rules:
        if not r.get("execute", True):
            continue

        ctx = __RuleCtx(column=r["field"], value=r.get("value"), name=r["check_type"])

        # Formatação da coluna (string ou lista)
        col = ", ".join(ctx.column) if isinstance(ctx.column, list) else ctx.column
        col_sql = f"'{__escape_single_quotes(col.strip())}'"

        # Formatação do nome da regra
        rule_sql = f"'{__escape_single_quotes(ctx.name)}'"

        # Threshold com fallback seguro
        try:
            thr = float(r.get("threshold", 1.0))
        except (TypeError, ValueError):
            thr = 1.0

        # Formatação do valor
        if ctx.value is None:
            val_sql = "NULL"
        elif isinstance(ctx.value, str):
            val_sql = f"'{__escape_single_quotes(ctx.value)}'"
        elif isinstance(ctx.value, (list, tuple)):
            try:
                val_sql = _format_sequence(ctx.value)
            except ValueError:
                val_sql = "NULL"
        else:
            val_sql = str(ctx.value)

        parts.append(
            f"SELECT {col_sql} AS col, "
            f"{rule_sql} AS rule, "
            f"{thr} AS pass_threshold, "
            f"{val_sql} AS value"
        )

    if not parts:
        return "SELECT NULL AS col, NULL AS rule, NULL AS pass_threshold, NULL AS value LIMIT 0"

    union_sql = "\nUNION ALL\n".join(parts)
    return (
        "SELECT DISTINCT col, rule, pass_threshold, value\n"
        "FROM (\n"
        f"{union_sql}\n"
        ") AS t"
    )


def summarize(df_rel: dk.DuckDBPyRelation,rules: List[Dict],conn: dk.DuckDBPyConnection,total_rows: Optional[int] = None) -> dk.DuckDBPyRelation:
    """
    Summarizes data quality checks for a given DuckDB relation based on specified rules.

    Args:
        df_rel (dk.DuckDBPyRelation): The DuckDB relation containing the data to be analyzed.
        rules (List[Dict]): A list of dictionaries defining the data quality rules to be applied.
        conn (dk.DuckDBPyConnection): The DuckDB connection used to execute SQL queries.
        total_rows (Optional[int]): The total number of rows in the dataset. If not provided, 
                                    it must be calculated externally.

    Returns:
        dk.DuckDBPyRelation: A DuckDB relation containing the summary of data quality checks, 
                                including pass rates, violation counts, and statuses for each rule.

    Notes:
        - The function creates a temporary view named "violations_raw" from the input relation.
        - It uses SQL to compute violations, pass rates, and statuses based on the provided rules.
        - The output includes metadata such as timestamps, rule thresholds, and overall status 
            (PASS/FAIL) for each rule.
    """
    rules_sql = __rules_to_duckdb_df(rules)

    df_rel.create_view("violations_raw")

    sql = f"""
        WITH
        rules      AS ({rules_sql}),
        violations AS (
            SELECT
                split_part(dq_status, ':', 1) AS col,
                split_part(dq_status, ':', 2) AS rule,
                split_part(dq_status, ':', 2) AS value,
                COUNT(*)               AS violations
            FROM violations_raw
            WHERE dq_status IS NOT NULL
                AND dq_status <> ''
            GROUP BY col, rule, value, value
        ),
        total_rows AS (
            SELECT {total_rows} AS cnt
        )
        SELECT
        ROW_NUMBER() OVER ()                            AS id,
        date_trunc('minute', NOW())                     AS timestamp,
        'Quality Check'                                 AS check,
        'WARNING'                                       AS level,
        r.col         AS col,
        r.rule,
        r.value,
        tr.cnt                                         AS rows,
        COALESCE(v.violations, 0)                      AS violations,
        (tr.cnt - COALESCE(v.violations, 0))::DOUBLE / tr.cnt          AS pass_rate,
        r.pass_threshold,
        CASE
            WHEN (tr.cnt - COALESCE(v.violations,0))::DOUBLE / tr.cnt 
             >= r.pass_threshold THEN 'PASS'
            ELSE 'FAIL'
        END                                            AS status
        FROM rules r
        LEFT JOIN violations v ON r.col = v.col AND r.rule = v.rule,
            total_rows tr
    """
    return conn.sql(sql)


def __duckdb_schema_to_list(conn: dk.DuckDBPyConnection, table: str) -> List[Dict[str, Any]]:
    """
    Retrieve the schema of a DuckDB table as a list of dictionaries.
    This function queries the schema of the specified table in a DuckDB database
    and returns a list of dictionaries where each dictionary represents a column
    in the table, including its name, data type, nullability, and maximum length.
    Args:
        conn (dk.DuckDBPyConnection): The DuckDB connection object.
        table (str): The name of the table whose schema is to be retrieved.
    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each containing the following keys:
            - "field" (str): The name of the column.
            - "data_type" (str): The data type of the column in lowercase.
            - "nullable" (bool): Whether the column allows NULL values.
            - "max_length" (None): Always None, as DuckDB does not provide maximum length information.
    """
    
    df_info = conn.execute(f"PRAGMA table_info('{table}')").fetchdf()
    return [
        {
            "field": row["name"],
            "data_type": row["type"].lower(),
            "nullable": not bool(row["notnull"]),
            "max_length": None,
        }
        for _, row in df_info.iterrows()
    ]


def validate_schema(conn: dk.DuckDBPyConnection, expected: List[Dict[str, Any]], table: str) -> Tuple[bool, List[Tuple[str, str]]]:
    """
    Validates the schema of a DuckDB table against an expected schema.

    Args:
        conn (dk.DuckDBPyConnection): The DuckDB connection object.
        expected (List[Dict[str, Any]]): A list of dictionaries representing the expected schema.
            Each dictionary should define the expected attributes of the schema, such as column names and types.
        table (str): The name of the table whose schema is to be validated.

    Returns:
        Tuple[bool, List[Tuple[str, str]]]: A tuple where the first element is a boolean indicating
            whether the actual schema matches the expected schema, and the second element is a list
            of tuples describing the mismatches (if any). Each tuple contains the column name and
            a description of the mismatch.
    """
    actual = __duckdb_schema_to_list(conn, table)
    return __compare_schemas(actual, expected)
