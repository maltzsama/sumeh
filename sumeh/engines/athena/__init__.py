"""
Athena Engine - SQL-based validation with AWS Athena.

AWS Athena is a serverless, interactive query service using athena.
Queries are asynchronous and results stored in S3.

Two modes:
1. validate() - Execute and return ValidationReport (with async polling)
2. get_validation_sql() - Generate SQL without executing (Open SQL)

Example - Execute (Async):
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
    ...     database="default",
    ...     poll_interval=2,  # Check every 2 seconds
    ...     max_wait=300      # Timeout after 5 minutes
    ... )
    >>> print(f"Pass rate: {report.pass_rate:.2%}")
    >>> report.explain()

Example - Generate SQL (Open SQL):
    >>> sql = sumeh_athena.get_validation_sql(
    ...     rules=rules,
    ...     table_name="default.users"
    ... )
    >>> print(sql)
    >>> # Execute manually in Athena Query Editor

Notes:
    - Athena queries are asynchronous (uses polling)
    - Results stored in S3 (s3_output_location required)
    - Uses Presto SQL dialect (same as Trino)
    - Charges per TB scanned (partition filters recommended)
"""

from sumeh.engines.athena.engine import validate, get_validation_sql

CAPABILITIES = {
    'schema_validation': True,
    'profiling': True,
    'aggregation_analyzers': True,
    'bifurcation': True,
    'streaming': False,
}

__all__ = ['validate', 'get_validation_sql', 'CAPABILITIES']