"""
DuckDB Engine - Strict Mode.
No file reading magic. User must provide loaded object or table name.
"""
import duckdb
import time
from typing import List, Union, Any, Optional

from sumeh.core.models import ValidationReport
from sumeh.core.rules.rule_model import RuleDef
from sumeh.engines.sql_core.compiler import compile_rules_to_sql
from sumeh.engines.sql_core.validator import validate_results


def validate(
    df: Union[Any, str, duckdb.DuckDBPyRelation],
    rules: List[RuleDef],
    connection: Optional[duckdb.DuckDBPyConnection] = None,
    baseline_provider=None,
    table_name: str = "dataset"
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
        # Connection and data registration
        if isinstance(df, duckdb.DuckDBPyRelation):
            if connection:
                con = connection
                df.create_view(table_name, replace=True)
            else:
                con = duckdb.connect(":memory:")
                should_close = True
                con.register(table_name, df.arrow())

            total_rows = df.count("*").fetchone()[0] if hasattr(df, 'count') \
                else con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        elif isinstance(df, str):
            if not connection:
                raise ValueError("If 'df' is a table name (str), you MUST provide the 'connection'.")

            con = connection
            table_name = df

            try:
                total_rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            except Exception:
                return ValidationReport(
                    results=[], total_rows=0, engine="duckdb",
                    error_message=f"Table '{table_name}' not found in provided connection."
                )

        else:  # In-memory DataFrame
            con = connection or duckdb.connect(":memory:")
            should_close = connection is None
            con.register(table_name, df)
            total_rows = len(df) if hasattr(df, '__len__') \
                else con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        # Compile and execute validation
        sql_query, rule_ids = compile_rules_to_sql(rules, table_name, dialect="duckdb")
        if not sql_query:
            return ValidationReport(results=[], total_rows=total_rows, engine="duckdb")

        metrics_row = con.execute(sql_query).fetchone()
        if metrics_row is None:
            raise ValueError("Validation query returned no results.")

        results = validate_results(metrics_row, rule_ids, rules)

        return ValidationReport(
            results=results,
            total_rows=total_rows,
            execution_time_ms=(time.time() - start_time) * 1000,
            engine="duckdb"
        )

    except Exception as e:
        return ValidationReport(
            results=[],
            total_rows=0,
            execution_time_ms=(time.time() - start_time) * 1000,
            engine="duckdb",
            error_message=f"DuckDB Engine Error: {str(e)}"
        )
    finally:
        if should_close and con:
            try:
                con.close()
            except:
                pass