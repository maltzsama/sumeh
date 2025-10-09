#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module provides utility functions for working with Google BigQuery schemas and validating them.

Functions:
    extract_schema(table: bigquery.Table) -> List[Dict[str, Any]]:

    validate_schema(
        Validates the schema of a BigQuery table against an expected schema.

Dependencies:
    - google.cloud.bigquery: Provides the BigQuery client and table schema functionality.
    - typing: Used for type annotations.
    - sumeh.core.utils.__compare_schemas: A utility function for comparing schemas.
"""

# bigquery_engine.py - VERSÃO SQLGLOT HARDCORE

from google.cloud import bigquery
from typing import List, Dict, Any, Tuple, Callable
import warnings
import ast
from dataclasses import dataclass
import uuid
from datetime import datetime
import sqlglot
from sqlglot import exp
from sqlglot.expressions import Identifier, Literal, Table, Column, Star, Select, Alias

from sumeh.core.utils import __compare_schemas


@dataclass(slots=True)
class __RuleCtx:
    column: Any
    value: Any
    name: str


def _parse_table_ref(table_ref: str) -> exp.Table:
    """
    Cria expressão de tabela do sqlglot a partir de table_ref.
    Suporta: project.dataset.table ou dataset.table
    """
    parts = table_ref.split(".")

    if len(parts) == 3:
        # project.dataset.table
        return exp.Table(
            catalog=exp.Identifier(this=parts[0], quoted=False),
            db=exp.Identifier(this=parts[1], quoted=False),
            this=exp.Identifier(this=parts[2], quoted=False),
        )
    elif len(parts) == 2:
        # dataset.table
        return exp.Table(
            db=exp.Identifier(this=parts[0], quoted=False),
            this=exp.Identifier(this=parts[1], quoted=False),
        )
    else:
        # table
        return exp.Table(this=exp.Identifier(this=parts[0], quoted=False))


def _is_complete(r: __RuleCtx) -> exp.Expression:
    return exp.Is(this=exp.Column(this=r.column), expression=exp.Not(this=exp.Null()))


def _are_complete(r: __RuleCtx) -> exp.Expression:
    conditions = [
        exp.Is(this=exp.Column(this=c), expression=exp.Not(this=exp.Null()))
        for c in r.column
    ]
    return exp.And(expressions=conditions)


def _is_unique(r: __RuleCtx, table_expr: exp.Table) -> exp.Expression:
    """Subquery que verifica unicidade"""
    subquery = (
        exp.Select(expressions=[exp.Count(this=exp.Star())])
        .from_(exp.alias_(table_expr, "d2", copy=True))
        .where(
            exp.EQ(
                this=exp.Column(this=r.column, table="d2"),
                expression=exp.Column(this=r.column, table="tbl"),
            )
        )
    )
    return exp.EQ(this=exp.Paren(this=subquery), expression=exp.Literal.number(1))


def _are_unique(r: __RuleCtx, table_expr: exp.Table) -> exp.Expression:
    """Subquery que verifica unicidade composta"""

    def concat_cols(table_alias):
        parts = [
            exp.Cast(
                this=exp.Column(this=c, table=table_alias),
                to=exp.DataType.build("STRING"),
            )
            for c in r.column
        ]

        if len(parts) == 1:
            return parts[0]

        result = parts[0]
        for part in parts[1:]:
            result = exp.DPipe(this=result, expression=exp.Literal.string("|"))
            result = exp.DPipe(this=result, expression=part)

        return result

    subquery = (
        exp.Select(expressions=[exp.Count(this=exp.Star())])
        .from_(exp.alias_(table_expr, "d2", copy=True))
        .where(exp.EQ(this=concat_cols("d2"), expression=concat_cols("tbl")))
    )

    return exp.EQ(this=exp.Paren(this=subquery), expression=exp.Literal.number(1))


def _is_positive(r: __RuleCtx) -> exp.Expression:
    return exp.LT(this=exp.Column(this=r.column), expression=exp.Literal.number(0))


def _is_negative(r: __RuleCtx) -> exp.Expression:
    return exp.GTE(this=exp.Column(this=r.column), expression=exp.Literal.number(0))


def _is_greater_than(r: __RuleCtx) -> exp.Expression:
    return exp.LTE(
        this=exp.Column(this=r.column), expression=exp.Literal.number(r.value)
    )


def _is_less_than(r: __RuleCtx) -> exp.Expression:
    return exp.GTE(
        this=exp.Column(this=r.column), expression=exp.Literal.number(r.value)
    )


def _is_greater_or_equal_than(r: __RuleCtx) -> exp.Expression:
    return exp.LT(
        this=exp.Column(this=r.column), expression=exp.Literal.number(r.value)
    )


def _is_less_or_equal_than(r: __RuleCtx) -> exp.Expression:
    return exp.GT(
        this=exp.Column(this=r.column), expression=exp.Literal.number(r.value)
    )


def _is_equal_than(r: __RuleCtx) -> exp.Expression:
    return exp.EQ(
        this=exp.Column(this=r.column), expression=exp.Literal.number(r.value)
    )


def _is_in_millions(r: __RuleCtx) -> exp.Expression:
    return exp.GTE(
        this=exp.Column(this=r.column), expression=exp.Literal.number(1000000)
    )


def _is_in_billions(r: __RuleCtx) -> exp.Expression:
    return exp.GTE(
        this=exp.Column(this=r.column), expression=exp.Literal.number(1000000000)
    )


def _is_between(r: __RuleCtx) -> exp.Expression:
    val = r.value
    if isinstance(val, (list, tuple)):
        lo, hi = val
    else:
        lo, hi, *_ = [v.strip(" []()'\"") for v in str(val).split(",")]

    return exp.Between(
        this=exp.Column(this=r.column),
        low=exp.Literal.number(float(lo)),
        high=exp.Literal.number(float(hi)),
    )


def _has_pattern(r: __RuleCtx) -> exp.Expression:
    return exp.RegexpLike(
        this=exp.Cast(this=exp.Column(this=r.column), to=exp.DataType.build("STRING")),
        expression=exp.Literal.string(str(r.value)),
    )


def _is_contained_in(r: __RuleCtx) -> exp.Expression:
    if isinstance(r.value, (list, tuple)):
        seq = r.value
    else:
        seq = [v.strip() for v in str(r.value).split(",")]

    literals = [exp.Literal.string(str(x)) for x in seq if x]
    return exp.In(this=exp.Column(this=r.column), expressions=literals)


def _is_in(r: __RuleCtx) -> exp.Expression:
    return _is_contained_in(r)


def _not_contained_in(r: __RuleCtx) -> exp.Expression:
    if isinstance(r.value, (list, tuple)):
        seq = r.value
    else:
        seq = [v.strip() for v in str(r.value).split(",")]

    literals = [exp.Literal.string(str(x)) for x in seq if x]
    return exp.Not(this=exp.In(this=exp.Column(this=r.column), expressions=literals))


def _not_in(r: __RuleCtx) -> exp.Expression:
    return _not_contained_in(r)


def _satisfies(r: __RuleCtx) -> exp.Expression:
    return sqlglot.parse_one(str(r.value), dialect="bigquery")


def _validate_date_format(r: __RuleCtx) -> exp.Expression:
    fmt = r.value
    token_map = {
        "DD": r"(0[1-9]|[12][0-9]|3[01])",
        "MM": r"(0[1-9]|1[0-2])",
        "YYYY": r"(19|20)\d\d",
        "YY": r"\d\d",
    }
    regex = fmt
    for tok, pat in token_map.items():
        regex = regex.replace(tok, pat)
    regex = regex.replace(".", r"\.").replace(" ", r"\s")

    return exp.Or(
        this=exp.Is(this=exp.Column(this=r.column), expression=exp.Null()),
        expression=exp.Not(
            this=exp.RegexpLike(
                this=exp.Column(this=r.column),
                expression=exp.Literal.string(f"^{regex}$"),
            )
        ),
    )


def _is_future_date(r: __RuleCtx) -> exp.Expression:
    return exp.GT(this=exp.Column(this=r.column), expression=exp.CurrentDate())


def _is_past_date(r: __RuleCtx) -> exp.Expression:
    return exp.LT(this=exp.Column(this=r.column), expression=exp.CurrentDate())


def _is_date_after(r: __RuleCtx) -> exp.Expression:
    return exp.LT(
        this=exp.Column(this=r.column),
        expression=exp.Anonymous(
            this="DATE", expressions=[exp.Literal.string(r.value)]
        ),
    )


def _is_date_before(r: __RuleCtx) -> exp.Expression:
    return exp.GT(
        this=exp.Column(this=r.column),
        expression=exp.Anonymous(
            this="DATE", expressions=[exp.Literal.string(r.value)]
        ),
    )


def _is_date_between(r: __RuleCtx) -> exp.Expression:
    start, end = [d.strip() for d in r.value.strip("[]").split(",")]
    return exp.Not(
        this=exp.Between(
            this=exp.Column(this=r.column),
            low=exp.Anonymous(this="DATE", expressions=[exp.Literal.string(start)]),
            high=exp.Anonymous(this="DATE", expressions=[exp.Literal.string(end)]),
        )
    )


def _all_date_checks(r: __RuleCtx) -> exp.Expression:
    return _is_past_date(r)


def _is_today(r: __RuleCtx) -> exp.Expression:
    return exp.EQ(this=exp.Column(this=r.column), expression=exp.CurrentDate())


def _is_yesterday(r: __RuleCtx) -> exp.Expression:
    return exp.EQ(
        this=exp.Column(this=r.column),
        expression=exp.DateSub(
            this=exp.CurrentDate(),
            expression=exp.Interval(
                this=exp.Literal.number(1), unit=exp.Var(this="DAY")
            ),
        ),
    )


def _is_t_minus_1(r: __RuleCtx) -> exp.Expression:
    return _is_yesterday(r)


def _is_t_minus_2(r: __RuleCtx) -> exp.Expression:
    return exp.EQ(
        this=exp.Column(this=r.column),
        expression=exp.DateSub(
            this=exp.CurrentDate(),
            expression=exp.Interval(
                this=exp.Literal.number(2), unit=exp.Var(this="DAY")
            ),
        ),
    )


def _is_t_minus_3(r: __RuleCtx) -> exp.Expression:
    return exp.EQ(
        this=exp.Column(this=r.column),
        expression=exp.DateSub(
            this=exp.CurrentDate(),
            expression=exp.Interval(
                this=exp.Literal.number(3), unit=exp.Var(this="DAY")
            ),
        ),
    )


def _is_on_weekday(r: __RuleCtx) -> exp.Expression:
    dayofweek = exp.Extract(
        this=exp.Var(this="DAYOFWEEK"), expression=exp.Column(this=r.column)
    )
    return exp.Between(
        this=dayofweek, low=exp.Literal.number(2), high=exp.Literal.number(6)
    )


def _is_on_weekend(r: __RuleCtx) -> exp.Expression:
    dayofweek = exp.Extract(
        this=exp.Var(this="DAYOFWEEK"), expression=exp.Column(this=r.column)
    )
    return exp.Or(
        this=exp.EQ(this=dayofweek, expression=exp.Literal.number(1)),
        expression=exp.EQ(this=dayofweek, expression=exp.Literal.number(7)),
    )


def _is_on_monday(r: __RuleCtx) -> exp.Expression:
    return exp.EQ(
        this=exp.Extract(
            this=exp.Var(this="DAYOFWEEK"), expression=exp.Column(this=r.column)
        ),
        expression=exp.Literal.number(2),
    )


def _is_on_tuesday(r: __RuleCtx) -> exp.Expression:
    return exp.EQ(
        this=exp.Extract(
            this=exp.Var(this="DAYOFWEEK"), expression=exp.Column(this=r.column)
        ),
        expression=exp.Literal.number(3),
    )


def _is_on_wednesday(r: __RuleCtx) -> exp.Expression:
    return exp.EQ(
        this=exp.Extract(
            this=exp.Var(this="DAYOFWEEK"), expression=exp.Column(this=r.column)
        ),
        expression=exp.Literal.number(4),
    )


def _is_on_thursday(r: __RuleCtx) -> exp.Expression:
    return exp.EQ(
        this=exp.Extract(
            this=exp.Var(this="DAYOFWEEK"), expression=exp.Column(this=r.column)
        ),
        expression=exp.Literal.number(5),
    )


def _is_on_friday(r: __RuleCtx) -> exp.Expression:
    return exp.EQ(
        this=exp.Extract(
            this=exp.Var(this="DAYOFWEEK"), expression=exp.Column(this=r.column)
        ),
        expression=exp.Literal.number(6),
    )


def _is_on_saturday(r: __RuleCtx) -> exp.Expression:
    return exp.EQ(
        this=exp.Extract(
            this=exp.Var(this="DAYOFWEEK"), expression=exp.Column(this=r.column)
        ),
        expression=exp.Literal.number(7),
    )


def _is_on_sunday(r: __RuleCtx) -> exp.Expression:
    return exp.EQ(
        this=exp.Extract(
            this=exp.Var(this="DAYOFWEEK"), expression=exp.Column(this=r.column)
        ),
        expression=exp.Literal.number(1),
    )


# Dispatch table - algumas funções precisam da table_expr
__RULE_DISPATCH_SIMPLE: dict[str, Callable[[__RuleCtx], exp.Expression]] = {
    "is_complete": _is_complete,
    "are_complete": _are_complete,
    "is_greater_than": _is_greater_than,
    "is_less_than": _is_less_than,
    "is_greater_or_equal_than": _is_greater_or_equal_than,
    "is_less_or_equal_than": _is_less_or_equal_than,
    "is_equal_than": _is_equal_than,
    "is_in_millions": _is_in_millions,
    "is_in_billions": _is_in_billions,
    "is_between": _is_between,
    "has_pattern": _has_pattern,
    "is_contained_in": _is_contained_in,
    "is_in": _is_in,
    "not_contained_in": _not_contained_in,
    "not_in": _not_in,
    "satisfies": _satisfies,
    "validate_date_format": _validate_date_format,
    "is_future_date": _is_future_date,
    "is_past_date": _is_past_date,
    "is_date_after": _is_date_after,
    "is_date_before": _is_date_before,
    "is_date_between": _is_date_between,
    "all_date_checks": _all_date_checks,
    "is_positive": _is_positive,
    "is_negative": _is_negative,
    "is_on_weekday": _is_on_weekday,
    "is_on_weekend": _is_on_weekend,
    "is_on_monday": _is_on_monday,
    "is_on_tuesday": _is_on_tuesday,
    "is_on_wednesday": _is_on_wednesday,
    "is_on_thursday": _is_on_thursday,
    "is_on_friday": _is_on_friday,
    "is_on_saturday": _is_on_saturday,
    "is_on_sunday": _is_on_sunday,
}

__RULE_DISPATCH_WITH_TABLE: dict[
    str, Callable[[__RuleCtx, exp.Table], exp.Expression]
] = {
    "is_unique": _is_unique,
    "are_unique": _are_unique,
    "is_primary_key": _is_unique,  # Alias
    "is_composite_key": _are_unique,  # Alias
}


def _build_union_sql(rules: List[Dict], table_ref: str) -> str:
    """
    Constrói SQL de validação usando SOMENTE sqlglot.
    Até o céu cair, até o preto virar fluorescente! 🚀
    """

    # Parse table reference
    table_expr = _parse_table_ref(table_ref)

    queries = []

    for r in rules:
        if not r.get("execute", True):
            continue

        check = r["check_type"]

        # Verifica qual dispatcher usar
        if check in __RULE_DISPATCH_SIMPLE:
            builder = __RULE_DISPATCH_SIMPLE[check]
            needs_table = False
        elif check in __RULE_DISPATCH_WITH_TABLE:
            builder = __RULE_DISPATCH_WITH_TABLE[check]
            needs_table = True
        else:
            warnings.warn(f"Unknown rule: {check}")
            continue

        ctx = __RuleCtx(
            column=r["field"],
            value=r.get("value"),
            name=check,
        )

        # Gera expressão de validação
        if needs_table:
            expr_ok = builder(ctx, table_expr)
        else:
            expr_ok = builder(ctx)

        # Cria dq_status tag
        dq_tag = f"{ctx.column}:{check}:{ctx.value}"

        # Constrói query COMPLETAMENTE com sqlglot
        query = (
            exp.Select(
                expressions=[
                    exp.Star(),
                    exp.alias_(exp.Literal.string(dq_tag), "dq_status"),
                ]
            )
            .from_(exp.alias_(table_expr, "tbl", copy=True))
            .where(exp.Not(this=expr_ok))
        )

        queries.append(query)

    if not queries:
        # Query vazia
        empty = (
            exp.Select(
                expressions=[
                    exp.Star(),
                    exp.alias_(exp.Literal.string(""), "dq_status"),
                ]
            )
            .from_(table_expr)
            .where(exp.false())
        )

        return empty.sql(dialect="bigquery")

    # Combina com UNION ALL usando sqlglot
    union_query = queries[0]
    for q in queries[1:]:
        union_query = exp.union(union_query, q, distinct=False)

    return union_query.sql(dialect="bigquery")


# bigquery_engine.py (continuação)


def validate(
    client: bigquery.Client, table_ref: str, rules: List[Dict]
) -> Tuple[bigquery.table.RowIterator, bigquery.table.RowIterator]:
    """Valida dados usando regras especificadas - 100% sqlglot"""

    # Gera UNION de violações com sqlglot
    union_sql = _build_union_sql(rules, table_ref)

    # Parse o union_sql como subquery
    violations_subquery = sqlglot.parse_one(union_sql, dialect="bigquery")

    # Pega schema da tabela
    table = client.get_table(table_ref)
    cols = [exp.Column(this=f.name) for f in table.schema]

    # Query RAW: WITH violations AS (...) SELECT cols, dq_status FROM violations
    raw_query = (
        exp.Select(expressions=cols + [exp.Column(this="dq_status")])
        .with_("violations", as_=violations_subquery)
        .from_("violations")
    )

    raw_sql = raw_query.sql(dialect="bigquery")

    # Query FINAL: WITH violations AS (...) SELECT cols, STRING_AGG(dq_status) GROUP BY cols
    final_query = (
        exp.Select(
            expressions=cols
            + [
                exp.alias_(
                    exp.Anonymous(
                        this="STRING_AGG",
                        expressions=[
                            exp.Column(this="dq_status"),
                            exp.Literal.string(";"),
                        ],
                    ),
                    "dq_status",
                )
            ]
        )
        .with_("violations", as_=violations_subquery)
        .from_("violations")
        .group_by(*cols)
    )

    final_sql = final_query.sql(dialect="bigquery")

    raw = client.query(raw_sql).result()
    final = client.query(final_sql).result()

    return final, raw


def __rules_to_bq_sql(rules: List[Dict]) -> str:
    """Gera SQL das regras usando 100% sqlglot"""

    queries = []

    for r in rules:
        if not r.get("execute", True):
            continue

        ctx = __RuleCtx(column=r["field"], value=r.get("value"), name=r["check_type"])

        # Formata coluna
        col = ", ".join(ctx.column) if isinstance(ctx.column, list) else ctx.column

        # Threshold
        try:
            thr = float(r.get("threshold", 1.0))
        except (TypeError, ValueError):
            thr = 1.0

        # Formata value
        if ctx.value is None:
            val_literal = exp.Null()
        elif isinstance(ctx.value, str):
            val_literal = exp.Literal.string(ctx.value)
        elif isinstance(ctx.value, (list, tuple)):
            val_literal = exp.Literal.string(",".join(str(x) for x in ctx.value))
        else:
            val_literal = exp.Literal.number(ctx.value)

        # Constrói SELECT usando sqlglot
        query = exp.Select(
            expressions=[
                exp.alias_(exp.Literal.string(col.strip()), "col"),
                exp.alias_(exp.Literal.string(ctx.name), "rule"),
                exp.alias_(exp.Literal.number(thr), "pass_threshold"),
                exp.alias_(val_literal, "value"),
            ]
        )

        queries.append(query)

    if not queries:
        # Query vazia
        empty = exp.Select(
            expressions=[
                exp.alias_(exp.Null(), "col"),
                exp.alias_(exp.Null(), "rule"),
                exp.alias_(exp.Null(), "pass_threshold"),
                exp.alias_(exp.Null(), "value"),
            ]
        ).limit(0)
        return empty.sql(dialect="bigquery")

    # UNION ALL de todas as queries
    union_query = queries[0]
    for q in queries[1:]:
        union_query = exp.union(union_query, q, distinct=False)

    # Wrapper com DISTINCT
    final_query = (
        exp.Select(
            expressions=[
                exp.Column(this="col"),
                exp.Column(this="rule"),
                exp.Column(this="pass_threshold"),
                exp.Column(this="value"),
            ]
        )
        .from_(exp.alias_(exp.Subquery(this=union_query), "t"))
        .distinct()
    )

    return final_query.sql(dialect="bigquery")


def summarize(
    df: bigquery.table.RowIterator,
    rules: List[Dict],
    total_rows: int,
    client: bigquery.Client,
) -> List[Dict[str, Any]]:

    violations_count = {}
    for row in df:
        dq_status = row.get("dq_status", "")
        if dq_status and dq_status.strip():
            parts = dq_status.split(":", 2)
            if len(parts) >= 2:
                col, rule = parts[0], parts[1]
                val = parts[2] if len(parts) > 2 else "N/A"
                key = (col, rule, val)
                violations_count[key] = violations_count.get(key, 0) + 1

    results = []
    for r in rules:
        if not r.get("execute", True):
            continue

        col = r["field"]
        if isinstance(col, list):
            col = ", ".join(col)

        rule = r["check_type"]
        val = str(r.get("value")) if r.get("value") is not None else "N/A"
        threshold = float(r.get("threshold", 1.0))

        key = (str(col), rule, val)
        violations = violations_count.get(key, 0)

        pass_rate = (total_rows - violations) / total_rows if total_rows > 0 else 1.0
        status = "PASS" if pass_rate >= threshold else "FAIL"

        results.append(
            {
                "id": str(uuid.uuid4()),
                "timestamp": datetime.now(),
                "check": "Quality Check",
                "level": "WARNING",
                "column": col,
                "rule": rule,
                "value": val,
                "rows": total_rows,
                "violations": violations,
                "pass_rate": pass_rate,
                "pass_threshold": threshold,
                "status": status,
            }
        )

    return results


def count_rows(client: bigquery.Client, table_ref: str) -> int:

    table_expr = _parse_table_ref(table_ref)

    query = exp.Select(
        expressions=[exp.alias_(exp.Count(this=exp.Star()), "total")]
    ).from_(table_expr)

    sql = query.sql(dialect="bigquery")
    result = client.query(sql).result()
    return list(result)[0]["total"]


def extract_schema(table: bigquery.Table) -> List[Dict[str, Any]]:
    """
    Converts a BigQuery table schema into a list of dictionaries representing the schema fields.

    Args:
        table (bigquery.Table): The BigQuery table whose schema is to be converted.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries where each dictionary represents a field in the schema.
            Each dictionary contains the following keys:
                - "field": The name of the field.
                - "data_type": The data type of the field, converted to lowercase.
                - "nullable": A boolean indicating whether the field is nullable.
                - "max_length": Always set to None (reserved for future use).
    """
    return [
        {
            "field": fld.name,
            "data_type": fld.field_type,
            "nullable": fld.is_nullable,
            "max_length": None,
        }
        for fld in table.schema
    ]


def validate_schema(
    client: bigquery.Client, expected: List[Dict[str, Any]], table_ref: str
) -> tuple[bool, list[dict[str, Any]]]:
    """
    Validates the schema of a BigQuery table against an expected schema.

    Args:
        client (bigquery.Client): The BigQuery client used to interact with the BigQuery service.
        expected (List[Dict[str, Any]]): The expected schema as a list of dictionaries, where each dictionary
            represents a field with its attributes (e.g., name, type, mode).
        table_ref (str): The reference to the BigQuery table (e.g., "project.dataset.table").

    Returns:
        Tuple[bool, List[Tuple[str, str]]]: A tuple where the first element is a boolean indicating whether
            the actual schema matches the expected schema, and the second element is a list of tuples
            describing the differences (if any) between the schemas. Each tuple contains a description
            of the difference and the corresponding field name.
    """

    table = client.get_table(table_ref)
    actual = extract_schema(table)

    result, errors = __compare_schemas(actual, expected)
    return result, errors
