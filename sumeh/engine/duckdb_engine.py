#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import duckdb as dk
import ast, warnings
from dataclasses import dataclass
from typing import List, Dict, Callable, Any, Optional
from sumeh.services.utils import __compare_schemas


def _escape_single_quotes(txt: str) -> str:
    return txt.replace("'", "''")


def _format_sequence(value: Any) -> str:
    """
    Converte ’BR,US’  ->  ('BR','US')
             ['BR','US']  -> ('BR','US')
             ('BR','US')  -> ('BR','US')
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
class _RuleCtx:
    """Contexto mínimo necessário para gerar a expressão SQL."""

    column: Any  # str ou list[str]
    value: Any
    name: str  # check_type


# --------------------------------------------------------------------------- #
# Geração das expressões “OK” (verdadeiras quando a linha passa)              #
# --------------------------------------------------------------------------- #
def _is_complete(r: _RuleCtx) -> str:
    return f"{r.column} IS NOT NULL"


def _are_complete(r: _RuleCtx) -> str:
    parts = " AND ".join(f"{c} IS NOT NULL" for c in r.column)
    return f"({parts})"


def _is_unique(r: _RuleCtx) -> str:
    return (
        f"(SELECT COUNT(*)                            \n"
        f"   FROM tbl AS d2                           \n"
        f"   WHERE d2.{r.column} = tbl.{r.column}     \n"
        f") = 1"
    )


def _are_unique(r: _RuleCtx) -> str:
    combo_outer = " || '|' || ".join(f"tbl.{c}" for c in r.column)
    combo_inner = " || '|' || ".join(f"d2.{c}" for c in r.column)

    return (
        f"(SELECT COUNT(*)                  \n"
        f"   FROM tbl AS d2                 \n"
        f"   WHERE ({combo_inner}) = ({combo_outer})\n"
        f") = 1"
    )


def _is_greater_than(r: _RuleCtx) -> str:
    return f"{r.column} > {r.value}"


def _is_less_than(r: _RuleCtx) -> str:
    return f"{r.column} < {r.value}"


def _is_greater_or_equal_than(r: _RuleCtx) -> str:
    return f"{r.column} >= {r.value}"


def _is_less_or_equal_than(r: _RuleCtx) -> str:
    return f"{r.column} <= {r.value}"


def _is_equal_than(r: _RuleCtx) -> str:
    return f"{r.column} = {r.value}"


def _is_between(r: _RuleCtx) -> str:
    val = r.value
    if isinstance(val, (list, tuple)):
        lo, hi = val
    else:
        lo, hi, *_ = [v.strip(" []()'\"") for v in str(val).split(",")]
    return f"{r.column} BETWEEN {lo} AND {hi}"


def _has_pattern(r: _RuleCtx) -> str:
    pat = _escape_single_quotes(str(r.value))
    return f"REGEXP_MATCHES({r.column}, '{pat}')"


def _is_contained_in(r: _RuleCtx) -> str:
    return f"{r.column} IN {_format_sequence(r.value)}"


def _not_contained_in(r: _RuleCtx) -> str:
    return f"{r.column} NOT IN {_format_sequence(r.value)}"


def _satisfies(r: _RuleCtx) -> str:
    """Recebe string do tipo `'a'='b' and "c"='d'` etc."""
    return f"({r.value})"


# -- mapeamento central ------------------------------------------------------ #
_RULE_DISPATCH: dict[str, Callable[[_RuleCtx], str]] = {
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
    # adicione aqui as demais regras numéricas (has_min, has_max, …)
}


def _build_union_sql(rules: List[Dict]) -> str:
    pieces: list[str] = []

    for r in rules:
        if not r.get("execute", True):
            continue

        check = r["check_type"]
        builder = _RULE_DISPATCH.get(check)
        if builder is None:
            warnings.warn(f"Regra desconhecida: {check}")
            continue

        ctx = _RuleCtx(
            column=r["field"],
            value=r.get("value"),
            name=check,
        )

        expr_ok = builder(ctx)  # condição “passa”
        dq_tag = _escape_single_quotes(f"{ctx.column}:{check}:{ctx.value}")
        pieces.append(
            f"SELECT *, '{dq_tag}' AS dq_status FROM tbl WHERE NOT ({expr_ok})"
        )

    # se não há regras ativas: devolve DF vazio
    if not pieces:
        return "SELECT *, '' AS dq_status FROM tbl WHERE 1=0"

    return "\nUNION ALL\n".join(pieces)


def validate(
    df_rel: dk.DuckDBPyRelation, rules: List[Dict], conn: dk.DuckDBPyConnection
) -> dk.DuckDBPyRelation:

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


def _rules_to_duckdb_df(rules: List[Dict]) -> str:
    """
    Gera o SQL completo para um CTE 'rules' em DuckDB, com valores inline.
    Utiliza as funções _escape_single_quotes e _format_sequence existentes.
    Retorna apenas a string SQL montada.
    """
    parts: List[str] = []

    for r in rules:
        if not r.get("execute", True):
            continue

        ctx = _RuleCtx(column=r["field"], value=r.get("value"), name=r["check_type"])

        # Formatação da coluna (string ou lista)
        col = ", ".join(ctx.column) if isinstance(ctx.column, list) else ctx.column
        col_sql = f"'{_escape_single_quotes(col.strip())}'"

        # Formatação do nome da regra
        rule_sql = f"'{_escape_single_quotes(ctx.name)}'"

        # Threshold com fallback seguro
        try:
            thr = float(r.get("threshold", 1.0))
        except (TypeError, ValueError):
            thr = 1.0

        # Formatação do valor
        if ctx.value is None:
            val_sql = "NULL"
        elif isinstance(ctx.value, str):
            val_sql = f"'{_escape_single_quotes(ctx.value)}'"
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


def summarize(
    df_rel: dk.DuckDBPyRelation,
    rules: List[Dict],
    conn: dk.DuckDBPyConnection,
    total_rows: Optional[int] = None,
) -> dk.DuckDBPyRelation:

    rules_sql = _rules_to_duckdb_df(rules)

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


def __duckdb_schema_to_list(
    conn: duckdb.DuckDBPyConnection, table: str
) -> List[Dict[str, Any]]:
    """
    Introspects a DuckDB table via PRAGMA and returns a list of column specs.
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


def validate_schema(
    conn: duckdb.DuckDBPyConnection, table: str, expected: List[Dict[str, Any]]
) -> Tuple[bool, List[Tuple[str, str]]]:
    actual = __duckdb_schema_to_list(conn, table)
    return __compare_schemas(actual, expected)
