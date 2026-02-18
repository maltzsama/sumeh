"""
BigQuery Engine.
Connects to Google Cloud BigQuery and executes optimized validation queries.
"""
import time
from typing import List, Optional, Any, Union
try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None

from sumeh.core.models import ValidationReport
from sumeh.core.rules.rule_model import RuleDef
from sumeh.engines.sql_core.compiler import compile_rules_to_sql
from sumeh.engines.sql_core.validator import validate_results


def validate(
    table_id: str,
    rules: List[RuleDef],
    client: Optional[Any] = None,
    project_id: Optional[str] = None,
    location: Optional[str] = None,
) -> ValidationReport:
    """
    Validates a BigQuery table.

    Args:
        table_id: Fully qualified table ID (e.g., "project.dataset.table")
        rules: List of validation rules
        client: Optional bigquery.Client instance (uses default creds if None)
        project_id: Optional project ID (if not providing client)
        location: Optional job location (e.g., "US", "EU")

    Returns:
        ValidationReport
    """
    if bigquery is None:
        raise ImportError("google-cloud-bigquery not installed. Run: pip install google-cloud-bigquery")

    start_time = time.time()
    
    # 1. SETUP CLIENT
    if client is None:
        client = bigquery.Client(project=project_id, location=location)

    # 2. COMPILE SQL (Dialect = BigQuery)
    # O SQLGlot vai tratar crases, funções de data (CURRENT_DATE vs CURRENT_DATE())
    # e sintaxe específica do BQ automaticamente.
    try:
        sql_query, rule_ids = compile_rules_to_sql(
            rules=rules, 
            table_name=f"`{table_id}`",  # Enforce backticks for BQ safety
            dialect="bigquery"
        )
    except Exception as e:
        return ValidationReport(
            results=[], total_rows=0, engine="bigquery", 
            execution_time_ms=0, error_message=f"Compilation Error: {str(e)}"
        )

    if not sql_query:
        return ValidationReport(
            results=[], total_rows=0, engine="bigquery", execution_time_ms=0
        )

    # 3. GET TOTAL ROWS (Fast Metadata Check)
    # No BQ, COUNT(*) pode custar dinheiro se não for metadados.
    # Tentamos pegar da tabela primeiro.
    try:
        table_ref = client.get_table(table_id)
        total_rows = table_ref.num_rows
    except Exception:
        # Fallback se for view ou query
        try:
            total_rows_job = client.query(f"SELECT COUNT(*) FROM `{table_id}`")
            total_rows = list(total_rows_job.result())[0][0]
        except Exception:
            total_rows = 0

    # 4. EXECUTE VALIDATION (The "Single Pass")
    try:
        query_job = client.query(sql_query)
        rows = list(query_job.result())
        
        if not rows:
            raise ValueError("Query returned no results")
            
        # O resultado do BQ vem como um Row object que se comporta como tupla/dict
        metrics_row = rows[0]

    except Exception as e:
        return ValidationReport(
            results=[],
            total_rows=total_rows,
            execution_time_ms=(time.time() - start_time) * 1000,
            engine="bigquery",
            error_message=f"Execution Error: {str(e)}"
        )

    # 5. VALIDATE (Delegate to SQL Core)
    # Precisamos converter o Row do BQ para tupla simples para o validator
    metrics_tuple = tuple(metrics_row.values())
    
    results = validate_results(
        metrics_row=metrics_tuple, 
        rule_ids=rule_ids, 
        rules=rules, 
        total_rows=total_rows
    )

    return ValidationReport(
        results=results,
        total_rows=total_rows,
        execution_time_ms=(time.time() - start_time) * 1000,
        engine="bigquery"
    )