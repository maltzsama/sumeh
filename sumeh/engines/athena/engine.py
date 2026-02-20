"""
Athena Engine - SQL-based validation with AWS Athena.

AWS Athena is a serverless, interactive query service using athena.
Queries are asynchronous and stored in S3.

Two modes:
1. validate() - Execute validation and return ValidationReport (with polling)
2. get_validation_sql() - Generate SQL without executing (Open SQL)
"""

import time
from typing import List, Optional, Any

from sumeh.core.models.validation import ValidationReport
from sumeh.core.rules.rule_model import RuleDef
from sumeh.engines.sql_core.compiler import compile_rules_to_sql
from sumeh.engines.sql_core.validator import validate_results


def get_validation_sql(
    rules: List[RuleDef], table_name: str, global_filter: Optional[str] = None
) -> str:
    """
    Generate Athena validation SQL without executing (Open SQL mode).

    Returns the SQL query as a string for manual execution or inspection.

    Args:
        rules: Validation rules
        table_name: Target table name (database.table)
        global_filter: Optional WHERE clause (e.g., "date = '2024-01-01'")

    Returns:
        SQL query string (Athena dialect)

    Example:
        >>> from sumeh import athena as sumeh_athena
        >>>
        >>> sql = sumeh_athena.get_validation_sql(
        ...     rules=rules,
        ...     table_name="default.users",
        ...     global_filter="created_at >= DATE '2024-01-01'"
        ... )
        >>> print(sql)
        >>> # Copy-paste into Athena Query Editor
    """
    sql_query, _ = compile_rules_to_sql(
        rules=rules,
        table_name=table_name,
        dialect="athena",
        global_filter=global_filter,
    )
    return sql_query


def validate(
    table_name: str,
    rules: List[RuleDef],
    client: Any,
    s3_output_location: str,
    database: str = "default",
    baseline_provider=None,
    poll_interval: int = 1,
    max_wait: int = 300,
) -> ValidationReport:
    """
    Validates data using AWS Athena (asynchronous with polling).

    Args:
        table_name: Target table name (database.table or just table)
        rules: Validation rules
        client: boto3 Athena client
        s3_output_location: S3 path for query results (e.g., "s3://bucket/athena-results/")
        database: Database name (default: "default")
        baseline_provider: Optional baseline provider
        poll_interval: Seconds between status checks (default: 1)
        max_wait: Maximum seconds to wait for query (default: 300)

    Returns:
        ValidationReport with .generated_sql for inspection

    Example:
        >>> import boto3
        >>> from sumeh import athena as sumeh_athena
        >>>
        >>> client = boto3.client('athena', region_name='us-east-1')
        >>>
        >>> report = sumeh_athena.validate(
        ...     table_name="default.users",
        ...     rules=rules,
        ...     client=client,
        ...     s3_output_location="s3://my-bucket/athena-results/",
        ...     database="default"
        ... )
        >>> print(report.generated_sql)
        >>> report.explain()
    """
    start_time = time.time()

    try:
        # Get total rows (async query)
        try:
            count_response = client.start_query_execution(
                QueryString=f"SELECT COUNT(*) FROM {table_name}",
                QueryExecutionContext={"Database": database},
                ResultConfiguration={"OutputLocation": s3_output_location},
            )
            count_id = count_response["QueryExecutionId"]

            # Poll for count query
            _wait_for_query(client, count_id, poll_interval, max_wait)

            count_results = client.get_query_results(QueryExecutionId=count_id)
            total_rows = int(
                count_results["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"]
            )

        except Exception:
            return ValidationReport(
                results=[],
                total_rows=0,
                execution_time_ms=(time.time() - start_time) * 1000,
                engine="athena",
            )

        # Compile validation SQL
        sql_query, rule_ids = compile_rules_to_sql(rules, table_name, dialect="athena")
        if not sql_query:
            return ValidationReport(
                results=[],
                total_rows=total_rows,
                execution_time_ms=(time.time() - start_time) * 1000,
                engine="athena",
                generated_sql="",
            )

        # Execute validation query (async)
        response = client.start_query_execution(
            QueryString=sql_query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": s3_output_location},
        )
        query_id = response["QueryExecutionId"]

        # Poll for results
        _wait_for_query(client, query_id, poll_interval, max_wait)

        # Get results
        results_response = client.get_query_results(QueryExecutionId=query_id)

        # Parse metrics row (skip header row)
        if len(results_response["ResultSet"]["Rows"]) < 2:
            raise ValueError("Validation query returned no results.")

        metrics_row = tuple(
            col.get("VarCharValue")
            for col in results_response["ResultSet"]["Rows"][1]["Data"]
        )

        # Convert string values to float
        metrics_row = tuple(float(v) if v else 0.0 for v in metrics_row)

        results = validate_results(metrics_row, rule_ids, rules, total_rows)

        return ValidationReport(
            results=results,
            total_rows=total_rows,
            execution_time_ms=(time.time() - start_time) * 1000,
            engine="athena",
            generated_sql=sql_query,
        )

    except Exception as e:
        return ValidationReport(
            results=[],
            total_rows=0,
            execution_time_ms=(time.time() - start_time) * 1000,
            engine="athena",
        )


def _wait_for_query(client, query_id: str, poll_interval: int, max_wait: int):
    """
    Poll Athena query until completion.

    Raises:
        TimeoutError: If query doesn't complete within max_wait
        RuntimeError: If query fails or is cancelled
    """
    elapsed = 0

    while elapsed < max_wait:
        response = client.get_query_execution(QueryExecutionId=query_id)
        state = response["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            return
        elif state in ["FAILED", "CANCELLED"]:
            reason = response["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown"
            )
            raise RuntimeError(f"Athena query {state}: {reason}")

        time.sleep(poll_interval)
        elapsed += poll_interval

    raise TimeoutError(f"Athena query timed out after {max_wait} seconds")
