"""
BigQuery Engine - Serverless SQL Validation.

Executes validation rules directly on Google BigQuery tables using SQLGlot translation.
Leverages BigQuery's distributed compute for massive datasets.

Example:
    >>> from sumeh import bigquery as sumeh_bq
    >>>
    >>> # Authenticates using Google Default Credentials
    >>> report = sumeh_bq.validate(
    ...     table_id="my-project.analytics.users",
    ...     rules=rules
    ... )
"""

from sumeh.engines.bigquery.engine import validate, get_validation_sql

CAPABILITIES = {
    'schema_validation': True,
    'profiling': True,
    'aggregation_analyzers': True,
    'bifurcation': True,
    'streaming': False,
}

__all__ = ['validate', 'get_validation_sql', 'CAPABILITIES']

