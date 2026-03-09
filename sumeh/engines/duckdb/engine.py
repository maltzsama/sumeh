"""
DuckDB Engine - Strict Mode.
No file reading magic. User must provide loaded object or table name.
"""

import time
from typing import List, Union, Any, Optional

import duckdb

from sumeh.core.models.validation import ValidationReport, ValidationStatus
from sumeh.engines.duckdb.validator import ValidatedDuckDBDataFrame
from sumeh.core.rules.rule_model import RuleDefinition
from sumeh.engines.sql_core.compiler import compile_rules_to_sql
from sumeh.engines.sql_core.validator import validate_results


def validate(
    df: Union[Any, str, duckdb.DuckDBPyRelation],
    rules: List[RuleDefinition],
    connection: Optional[duckdb.DuckDBPyConnection] = None,
    baseline_provider=None,
    table_name: str = "dataset",
    bifurcate: bool = False,
) -> ValidationReport:
    """
    Validates data using DuckDB.

    Args:
        df: Data source. Must be:
            - duckdb.DuckDBPyRelation (Native object)
            - Pandas/Polars/Arrow DataFrame (In-memory)
            - String: NAME of an existing table in 'connection'.
        rules: Validation rules
        connection: Optional existing connection. Required if df is a table name string.
    """
    start_time = time.time()
    con = None
    should_close = False

    try:
        if isinstance(df, duckdb.DuckDBPyRelation):
            con = connection or duckdb.connect(":memory:")
            should_close = connection is None
            df.create_view(table_name, replace=True)
            total_rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        elif isinstance(df, str):
            if not connection:
                raise ValueError(
                    "If 'df' is a table name (str), you MUST provide the 'connection'."
                )
            con = connection
            table_name = df
            try:
                total_rows = con.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
            except Exception:
                return ValidationReport(
                    results=[],
                    total_rows=0,
                    engine="duckdb",
                    error_message=f"Table '{table_name}' not found in provided connection.",
                )

        else:  # In-memory DataFrame (pandas, polars, arrow)
            con = connection or duckdb.connect(":memory:")
            should_close = connection is None
            con.register(table_name, df)
            total_rows = (
                len(df)
                if hasattr(df, "__len__")
                else con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            )

        # --- SQL aggregated validation ---
        sql_query, rule_ids = compile_rules_to_sql(rules, table_name, dialect="duckdb")
        if not sql_query:
            return ValidationReport(results=[], total_rows=total_rows, engine="duckdb")

        metrics_row = con.execute(sql_query).fetchone()
        if metrics_row is None:
            raise ValueError("Validation query returned no results.")

        results = validate_results(metrics_row, rule_ids, rules, total_rows=total_rows)

        # --- Bifurcation (optional) ---
        df_validated = None
        if bifurcate:
            df_validated = _bifurcate(con, table_name, rules, results)

        return ValidationReport(
            results=results,
            total_rows=total_rows,
            execution_time_ms=(time.time() - start_time) * 1000,
            engine="duckdb",
            df_validated=df_validated,
        )

    except Exception as e:
        return ValidationReport(
            results=[],
            total_rows=0,
            execution_time_ms=(time.time() - start_time) * 1000,
            engine="duckdb",
            error_message=f"DuckDB Engine Error: {str(e)}",
        )
    finally:
        if should_close and con:
            try:
                con.close()
            except Exception:
                pass


def _bifurcate(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    rules: List[RuleDefinition],
    results: list,
) -> ValidatedDuckDBDataFrame:

    from sumeh.engines.sql_core.registry import get_analyzer
    import sqlglot.expressions as exp

    rule_map = {getattr(r, "id", None) or f"{r.check_type}:{r.field}": r for r in rules}

    fail_conditions = []

    for result in results:
        if result.status != ValidationStatus.FAIL:
            continue
        rule = rule_map.get(str(result.rule_id))
        if not rule:
            continue

        analyzer = get_analyzer(rule.check_type)
        if not analyzer:
            continue

        pass_rate_expr = analyzer.analyze(rule)

        try:
            condition = pass_rate_expr.this.ifs[0].this  # o pass condition
            fail_cond = exp.Not(this=condition).sql(dialect="duckdb")
            fail_conditions.append(fail_cond)
        except Exception:
            continue

    if not fail_conditions:
        good = con.execute(f"SELECT * FROM {table_name}").df()
        bad = con.execute(f"SELECT * FROM {table_name} WHERE 1=0").df()
        return ValidatedDuckDBDataFrame(good, bad)

    combined = " OR ".join(f"({c})" for c in fail_conditions)
    good = con.execute(f"SELECT * FROM {table_name} WHERE NOT ({combined})").df()
    bad = con.execute(f"SELECT * FROM {table_name} WHERE {combined}").df()

    return ValidatedDuckDBDataFrame(good, bad)


def get_validation_sql(rules, table_name, global_filter=None):
    """Generate DuckDB SQL."""
    return compile_rules_to_sql(
        rules, table_name, dialect="duckdb", global_filter=global_filter
    )[0]
