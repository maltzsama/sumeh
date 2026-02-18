"""
DuckDB Engine - Production Ready.
Supports:
- Native DuckDB Relations (DuckDBPyRelation)
- Existing Connections (DuckDBPyConnection)
- In-Memory DataFrames (Pandas/Polars/Arrow)
- File Paths (Parquet/CSV/JSON)
"""
import duckdb
import time
import os
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
        df: Data source. Can be:
            - duckdb.DuckDBPyRelation (Native DuckDB object)
            - Pandas/Polars/Arrow DataFrame
            - String file path
            - String table name (if connection provided)
        rules: Validation rules
        connection: Optional existing DuckDB connection. 
                    If None, a temporary in-memory connection is created.
        table_name: Name to register the view/table as for SQL generation.
    """
    start_time = time.time()
    
    # 1. CONNECTION MANAGEMENT
    # Se o usuário passou uma conexão, usamos ela. Senão, criamos uma efêmera.
    if connection:
        con = connection
        should_close = False
    else:
        con = duckdb.connect(database=":memory:")
        should_close = True
    
    try:
        # 2. SMART REGISTRATION
        total_rows = 0
        is_registered = False

        # Caso A: Native DuckDB Relation (O "DataFrame" do DuckDB)
        if isinstance(df, duckdb.DuckDBPyRelation):
            # Cria uma VIEW temporária apontando para essa relação
            # Isso permite que nossa query SQL gerada (SELECT ... FROM table_name) funcione
            df.create_view(table_name, replace=True)
            # Se a relation veio de outra conexão, isso pode dar erro, 
            # mas assumimos que se o user passou relation, passou a conn certa ou tá em memória.
            
            # Pega o count rápido da relation
            total_rows = df.count("*").fetchone()[0]
            is_registered = True

        # Caso B: String (Path ou Table Name)
        elif isinstance(df, str):
            if os.path.exists(df) or df.startswith("s3://") or df.startswith("http"):
                # File Path Strategy
                create_view_sql = f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM '{df}'"
                try:
                    con.execute(create_view_sql)
                except Exception:
                    # Fallbacks para formatos específicos se a inferência falhar
                    if df.endswith(".parquet"):
                        con.execute(f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{df}')")
                    elif df.endswith((".csv", ".tsv")):
                        con.execute(f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_csv_auto('{df}')")
                    elif df.endswith(".json"):
                        con.execute(f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_json_auto('{df}')")
                    else:
                        raise ValueError(f"Unsupported file format or path: {df}")
                
                total_rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                is_registered = True
            else:
                # Assume que é o nome de uma tabela que JÁ EXISTE na conexão fornecida
                table_name = df
                # Valida se existe e pega count
                try:
                    total_rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    is_registered = True
                except:
                    # Tabela não encontrada
                    if should_close: con.close()
                    return ValidationReport(
                        results=[], total_rows=0, engine="duckdb", 
                        error_message=f"Table '{table_name}' not found in connection."
                    )

        # Caso C: Pandas/Polars/Arrow (In-Memory)
        else:
            # DuckDB register aceita qualquer coisa que tenha __arrow_array__ ou suporte buffer protocol
            con.register(table_name, df)
            total_rows = len(df)
            is_registered = True

        # 3. COMPILE SQL
        sql_query, rule_ids = compile_rules_to_sql(rules, table_name, dialect="duckdb")
        
        if not sql_query:
            if should_close: con.close()
            return ValidationReport(results=[], total_rows=total_rows, engine="duckdb")

        # 4. EXECUTE
        # fetchone retorna tupla: (0.9, 100, 50.5...)
        metrics_row = con.execute(sql_query).fetchone()
        
        # 5. EVALUATE (Validator)
        results = validate_results(metrics_row, rule_ids, rules)

        execution_time_ms = (time.time() - start_time) * 1000
        
        return ValidationReport(
            results=results,
            total_rows=total_rows,
            execution_time_ms=execution_time_ms,
            engine="duckdb"
        )

    except Exception as e:
        return ValidationReport(
            results=[],
            total_rows=0,
            execution_time_ms=(time.time() - start_time) * 1000,
            engine="duckdb",
            error_message=f"Engine Error: {str(e)}"
        )
    finally:
        if should_close:
            con.close()