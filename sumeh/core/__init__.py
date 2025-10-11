#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Sumeh Core Module - Data Quality Validation Framework

This module provides dispatchers for data validation, summarization, and configuration
retrieval across multiple data sources and engines (Pandas, Dask, PySpark, Polars,
DuckDB, BigQuery).

Classes:
    _ValidateDispatcher: Dispatcher for validate functions across engines
    _SummarizeDispatcher: Dispatcher for summarize functions across engines
    _RulesConfigDispatcher: Dispatcher for retrieving validation rules
    _SchemaConfigDispatcher: Dispatcher for retrieving schema configurations

Exports:
    validate: Dispatcher instance for validation
    summarize: Dispatcher instance for summarization
    get_rules_config: Dispatcher instance for rules retrieval
    get_schema_config: Dispatcher instance for schema retrieval
    report: Legacy cuallee-based validation (deprecated)

Usage:
    from sumeh import validate, summarize, get_rules_config, get_schema_config

    # Get rules
    rules = get_rules_config.csv("rules.csv")

    # Validate
    result = validate.pandas(df, rules)

    # Summarize
    summary = summarize.pandas(result, rules, total_rows=len(df))
"""

from cuallee import Check, CheckLevel
import warnings
import re
from typing import List, Dict, Any, Optional
from .utils import __convert_value
from sumeh.core.config import (
    get_config_from_s3,
    get_config_from_csv,
    get_config_from_mysql,
    get_config_from_postgresql,
    get_config_from_bigquery,
    get_config_from_glue_data_catalog,
    get_config_from_duckdb,
    get_config_from_databricks,
    get_schema_from_duckdb,
    get_schema_from_bigquery,
    get_schema_from_s3,
    get_schema_from_csv,
    get_schema_from_mysql,
    get_schema_from_postgresql,
    get_schema_from_databricks,
    get_schema_from_glue,
)

import pandas as pd

# ============================================================================
# VALIDATION DISPATCHER
# ============================================================================


class _ValidateDispatcher:
    """
    Dispatcher for validate functions across different engines.

    Usage:
        from sumeh import validate

        # Direct engine call
        result = validate.pandas(df, rules)
        result = validate.bigquery(table_ref, rules, client=bq_client)
        result = validate.duckdb(df_rel, rules, conn=duck_conn)

        # Fallback with engine string
        result = validate(engine="pandas", df, rules)

    Returns:
        Tuple[DataFrame, DataFrame, DataFrame]:
            - df_with_errors: DataFrame with only rows that failed (with dq_status)
            - violations: DataFrame with detailed violations (exploded)
            - table_summary: DataFrame with table-level validation results
    """

    @property
    def pandas(self):
        """Validate for pandas DataFrame."""
        from sumeh.engines import pandas_engine

        return pandas_engine.validate

    @property
    def dask(self):
        """Validate for Dask DataFrame."""
        from sumeh.engines import dask_engine

        return dask_engine.validate

    @property
    def pyspark(self):
        """Validate for PySpark DataFrame."""
        from sumeh.engines import pyspark_engine

        return pyspark_engine.validate

    @property
    def polars(self):
        """Validate for Polars DataFrame."""
        from sumeh.engines import polars_engine

        return polars_engine.validate

    @property
    def duckdb(self):
        """
        Validate for DuckDB Relation.

        Args:
            df_rel: DuckDB relation
            rules: List[RuleDef]
            conn: DuckDB connection
        """
        from sumeh.engines import duckdb_engine

        return duckdb_engine.validate

    @property
    def bigquery(self):
        """
        Validate for BigQuery Table.

        Args:
            table_ref: BigQuery table reference
            rules: List[RuleDef]
            client: BigQuery client
        """
        from sumeh.engines import bigquery_engine

        return bigquery_engine.validate

    def __call__(self, engine: str, *args, **kwargs):
        """
        Fallback for calling with engine as string.

        Usage:
            validate(engine="pandas", df, rules)
        """
        engine_method = getattr(self, engine, None)
        if engine_method is None:
            available = ["pandas", "dask", "pyspark", "polars", "duckdb", "bigquery"]
            raise ValueError(
                f"Unknown engine '{engine}'. Available: {', '.join(available)}"
            )
        return engine_method(*args, **kwargs)

    def __repr__(self):
        return (
            "Sumeh Validate Dispatcher\n"
            "Available engines: pandas, dask, pyspark, polars, duckdb, bigquery\n\n"
            "Usage:\n"
            "  validate.pandas(df, rules)\n"
            "  validate.bigquery(table_ref, rules, client=client)\n"
            "  validate.duckdb(df_rel, rules, conn=conn)\n"
            "  validate(engine='pandas', df, rules)  # fallback\n"
        )


# ============================================================================
# SUMMARIZATION DISPATCHER
# ============================================================================


class _SummarizeDispatcher:
    """
    Dispatcher for summarize functions across different engines.

    Usage:
        from sumeh import summarize

        # After validation
        df_errors, violations, table_sum = validate.pandas(df, rules)

        # Generate summary
        summary = summarize.pandas(
            validation_result=(df_errors, violations, table_sum),
            rules=rules,
            total_rows=len(df)
        )

        # Or with engine string
        summary = summarize(
            engine="pandas",
            validation_result=(df_errors, violations, table_sum),
            rules=rules,
            total_rows=len(df)
        )

    Returns:
        DataFrame with consolidated summary containing:
        - id, timestamp, level, category, check_type, field
        - rows, violations, pass_rate, pass_threshold (row-level)
        - expected, actual (table-level)
        - status, message
    """

    @property
    def pandas(self):
        from sumeh.engines import pandas_engine

        def _call(
                rules: list,
                total_rows: int,
                df_with_errors: pd.DataFrame = None,
                table_error: Optional[pd.DataFrame] = None,
                **kwargs,
        ):
            invalid_args = [k for k in kwargs.keys() if k in ["table_summary", "summary_df"]]
            if invalid_args:
                raise ValueError(
                    f"Invalid parameter(s): {invalid_args}. "
                    f"Did you mean 'table_error' instead?"
                )

            if not isinstance(total_rows, int) or total_rows <= 0:
                raise ValueError("'total_rows' must be a positive integer")

            if not isinstance(rules, list) or not all(isinstance(r, dict) or hasattr(r, "check_type") for r in rules):
                raise TypeError("'rules' must be a list of Rule objects or dicts")

            return pandas_engine.summarize(
                rules=rules,
                total_rows=total_rows,
                df_with_errors=df_with_errors,
                table_error=table_error,
                **kwargs,
            )

        return _call

    @property
    def dask(self):
        """Summarize for Dask validation results."""
        from sumeh.engines import dask_engine

        return dask_engine.summarize

    @property
    def pyspark(self):
        """Summarize for PySpark validation results."""
        from sumeh.engines import pyspark_engine

        return pyspark_engine.summarize

    @property
    def polars(self):
        """Summarize for Polars validation results."""
        from sumeh.engines import polars_engine

        return polars_engine.summarize

    @property
    def duckdb(self):
        """
        Summarize for DuckDB validation results.

        Args:
            validation_result: Tuple from validate.duckdb()
            rules: List[RuleDef]
            total_rows: int
            conn: DuckDB connection
        """
        from sumeh.engines import duckdb_engine

        return duckdb_engine.summarize

    @property
    def bigquery(self):
        """
        Summarize for BigQuery validation results.

        Args:
            validation_result: Tuple from validate.bigquery()
            rules: List[RuleDef]
            total_rows: int
            client: BigQuery client
        """
        from sumeh.engines import bigquery_engine

        return bigquery_engine.summarize

    def __call__(self, engine: str, *args, **kwargs):
        """
        Fallback for calling with engine as string.

        Usage:
            summarize(engine="pandas", validation_result, rules, total_rows=1000)
        """
        engine_method = getattr(self, engine, None)
        if engine_method is None:
            available = ["pandas", "dask", "pyspark", "polars", "duckdb", "bigquery"]
            raise ValueError(
                f"Unknown engine '{engine}'. Available: {', '.join(available)}"
            )
        return engine_method(*args, **kwargs)

    def __repr__(self):
        return (
            "Sumeh Summarize Dispatcher\n"
            "Available engines: pandas, dask, pyspark, polars, duckdb, bigquery\n\n"
            "Usage:\n"
            "  summary = summarize.pandas(validation_result, rules, total_rows=1000)\n"
            "  summary = summarize.bigquery(validation_result, rules, total_rows=1000, client=client)\n"
            "  summary = summarize(engine='pandas', ...)  # fallback\n"
        )


# ============================================================================
# RULES CONFIG DISPATCHER
# ============================================================================


class _RulesConfigDispatcher:
    """
    Dispatcher for retrieving validation rules from different sources.

    Usage:
        from sumeh import get_rules_config

        # Direct source call
        rules = get_rules_config.bigquery(project_id="proj", dataset_id="ds", table_id="rules")
        rules = get_rules_config.s3("s3://bucket/rules.csv")
        rules = get_rules_config.csv("rules.csv")
        rules = get_rules_config.mysql(conn=conn, table="rules")

        # Fallback with source string
        rules = get_rules_config(source="bigquery", project_id="proj", ...)
    """

    @property
    def bigquery(self):
        """
        Get rules from BigQuery.

        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset
            table_id: Table name
        """
        return get_config_from_bigquery

    @property
    def mysql(self):
        """
        Get rules from MySQL.

        Args:
            conn: MySQL connection OR (host, user, password, database)
        """
        return get_config_from_mysql

    @property
    def postgresql(self):
        """
        Get rules from PostgreSQL.

        Args:
            conn: PostgreSQL connection OR (host, user, password, database, schema, table)
        """
        return get_config_from_postgresql

    @property
    def glue(self):
        """
        Get rules from AWS Glue Data Catalog.

        Args:
            glue_context: Glue context
            database_name: Database name
            table_name: Table name
        """
        return get_config_from_glue_data_catalog

    @property
    def duckdb(self):
        """
        Get rules from DuckDB.

        Args:
            conn: DuckDB connection
            table: Table name
        """
        return get_config_from_duckdb

    @property
    def databricks(self):
        """
        Get rules from Databricks.

        Args:
            spark: Spark session
            catalog: Catalog name
            schema: Schema name
            table: Table name
        """
        return get_config_from_databricks

    def s3(self, s3_path: str, **kwargs):
        """
        Get rules from S3 CSV file.

        Args:
            s3_path: S3 path (s3://bucket/path/file.csv)
            **kwargs: Additional parameters
        """
        return get_config_from_s3(s3_path, **kwargs)

    def csv(self, file_path: str, delimiter: str = ",", **kwargs):
        """
        Get rules from local CSV file.

        Args:
            file_path: Path to CSV file
            delimiter: CSV delimiter (optional)
            **kwargs: Additional parameters
        """
        return get_config_from_csv(file_path, delimiter=delimiter, **kwargs)

    def __call__(self, source: str, **kwargs) -> List[Dict[str, Any]]:
        """
        Fallback for calling with source as string (backwards compatibility).

        Args:
            source: Source identifier
            **kwargs: Source-specific parameters
        """
        match source:
            case "bigquery":
                required_params = ["project_id", "dataset_id", "table_id"]
                for param in required_params:
                    if param not in kwargs:
                        raise ValueError(
                            f"BigQuery source requires '{param}' in kwargs"
                        )
                return self.bigquery(**kwargs)

            case s if s.startswith("s3://"):
                return self.s3(s, **kwargs)

            case s if re.search(r"\.csv$", s, re.IGNORECASE):
                return self.csv(s, **kwargs)

            case "mysql":
                if "conn" not in kwargs:
                    required_params = ["host", "user", "password", "database"]
                    for param in required_params:
                        if param not in kwargs:
                            raise ValueError(
                                f"MySQL requires 'conn' OR all of {required_params} in kwargs"
                            )
                return self.mysql(**kwargs)

            case "postgresql":
                if "conn" not in kwargs:
                    required_params = [
                        "host",
                        "user",
                        "password",
                        "database",
                        "schema",
                        "table",
                    ]
                    for param in required_params:
                        if param not in kwargs:
                            raise ValueError(f"PostgreSQL requires '{param}' in kwargs")
                return self.postgresql(**kwargs)

            case "glue":
                required_params = ["glue_context", "database_name", "table_name"]
                for param in required_params:
                    if param not in kwargs:
                        raise ValueError(f"Glue requires '{param}' in kwargs")
                return self.glue(**kwargs)

            case s if s.startswith("duckdb://"):
                _, path = s.split("://", 1)
                _, table = path.rsplit(".", 1)
                conn = kwargs.pop("conn", None)
                return self.duckdb(conn=conn, table=table)

            case "databricks":
                required_params = ["spark", "catalog", "schema", "table"]
                for param in required_params:
                    if param not in kwargs:
                        raise ValueError(f"Databricks requires '{param}' in kwargs")
                return self.databricks(**kwargs)

            case _:
                raise ValueError(f"Unknown source: {source}")

    def __repr__(self):
        return (
            "Sumeh Rules Config Dispatcher\n"
            "Available sources: bigquery, mysql, postgresql, glue, duckdb, databricks, s3, csv\n\n"
            "Usage:\n"
            "  get_rules_config.bigquery(project_id='proj', dataset_id='ds', table_id='rules')\n"
            "  get_rules_config.s3('s3://bucket/rules.csv')\n"
            "  get_rules_config.csv('rules.csv')\n"
            "  get_rules_config.mysql(conn=conn)\n"
            "  get_rules_config(source='bigquery', ...)  # fallback\n"
        )


# ============================================================================
# SCHEMA CONFIG DISPATCHER
# ============================================================================


class _SchemaConfigDispatcher:
    """
    Dispatcher for retrieving schema configuration from different sources.

    Usage:
        from sumeh import get_schema_config

        # Direct source call
        schema = get_schema_config.bigquery(project_id="proj", dataset_id="ds", table_id="schema_registry")
        schema = get_schema_config.s3("s3://bucket/schema.csv", table="users")
        schema = get_schema_config.csv("schema.csv", table="users")
        schema = get_schema_config.mysql(conn=conn, table="users")

        # Fallback with source string
        schema = get_schema_config(source="bigquery", ...)
    """

    @property
    def bigquery(self):
        """
        Get schema from BigQuery.

        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset
            table_id: Schema registry table name
        """
        return get_schema_from_bigquery

    @property
    def mysql(self):
        """
        Get schema from MySQL.

        Args:
            conn: MySQL connection OR (host, user, password, database)
            table: Table name to look up
        """
        return get_schema_from_mysql

    @property
    def postgresql(self):
        """
        Get schema from PostgreSQL.

        Args:
            conn: PostgreSQL connection OR (host, user, password, database)
            table: Table name to look up
        """
        return get_schema_from_postgresql

    @property
    def glue(self):
        """
        Get schema from AWS Glue Data Catalog.

        Args:
            glue_context: Glue context
            database_name: Database name
            table_name: Table name
        """
        return get_schema_from_glue

    @property
    def duckdb(self):
        """
        Get schema from DuckDB.

        Args:
            conn: DuckDB connection
            table: Table name to look up
        """
        return get_schema_from_duckdb

    @property
    def databricks(self):
        """
        Get schema from Databricks.

        Args:
            spark: Spark session
            catalog: Catalog name
            schema: Schema name
            table: Table name to look up
        """
        return get_schema_from_databricks

    def s3(self, s3_path: str, table: str, environment: str = "prod", **kwargs):
        """
        Get schema from S3 CSV file.

        Args:
            s3_path: S3 path to schema registry CSV
            table: Table name to look up
            environment: Environment filter (default: 'prod')
            **kwargs: Additional parameters
        """
        return get_schema_from_s3(
            s3_path, table=table, environment=environment, **kwargs
        )

    def csv(self, file_path: str, table: str, environment: str = "prod", **kwargs):
        """
        Get schema from local CSV file.

        Args:
            file_path: Path to schema registry CSV
            table: Table name to look up
            environment: Environment filter (default: 'prod')
            **kwargs: Additional parameters
        """
        return get_schema_from_csv(
            file_path, table=table, environment=environment, **kwargs
        )

    def __call__(self, source: str, **kwargs) -> List[Dict[str, Any]]:
        """
        Fallback for calling with source as string (backwards compatibility).

        Args:
            source: Source identifier
            **kwargs: Source-specific parameters (must include 'table' for most sources)
        """
        match source:
            case "bigquery":
                required_params = ["project_id", "dataset_id", "table_id"]
                for param in required_params:
                    if param not in kwargs:
                        raise ValueError(
                            f"BigQuery schema requires '{param}' in kwargs"
                        )
                return self.bigquery(**kwargs)

            case s if s.startswith("s3://"):
                if "table" not in kwargs:
                    raise ValueError("S3 schema requires 'table' in kwargs")
                return self.s3(s, **kwargs)

            case s if re.search(r"\.csv$", s, re.IGNORECASE):
                if "table" not in kwargs:
                    raise ValueError("CSV schema requires 'table' in kwargs")
                return self.csv(s, **kwargs)

            case "mysql":
                if "conn" not in kwargs:
                    required_params = ["host", "user", "password", "database"]
                    for param in required_params:
                        if param not in kwargs:
                            raise ValueError(
                                f"MySQL schema requires 'conn' OR all of {required_params} in kwargs"
                            )
                if "table" not in kwargs:
                    raise ValueError("MySQL schema requires 'table' in kwargs")
                return self.mysql(**kwargs)

            case "postgresql":
                if "conn" not in kwargs:
                    required_params = ["host", "user", "password", "database"]
                    for param in required_params:
                        if param not in kwargs:
                            raise ValueError(
                                f"PostgreSQL schema requires 'conn' OR all of {required_params} in kwargs"
                            )
                if "table" not in kwargs:
                    raise ValueError("PostgreSQL schema requires 'table' in kwargs")
                return self.postgresql(**kwargs)

            case "glue":
                required_params = ["glue_context", "database_name", "table_name"]
                for param in required_params:
                    if param not in kwargs:
                        raise ValueError(f"Glue schema requires '{param}' in kwargs")
                return self.glue(**kwargs)

            case "duckdb":
                required_params = ["conn", "table"]
                for param in required_params:
                    if param not in kwargs:
                        raise ValueError(f"DuckDB schema requires '{param}' in kwargs")
                return self.duckdb(**kwargs)

            case "databricks":
                required_params = ["spark", "catalog", "schema", "table"]
                for param in required_params:
                    if param not in kwargs:
                        raise ValueError(
                            f"Databricks schema requires '{param}' in kwargs"
                        )
                return self.databricks(**kwargs)

            case _:
                raise ValueError(f"Unknown source: {source}")

    def __repr__(self):
        return (
            "Sumeh Schema Config Dispatcher\n"
            "Available sources: bigquery, mysql, postgresql, glue, duckdb, databricks, s3, csv\n\n"
            "Usage:\n"
            "  get_schema_config.bigquery(project_id='proj', dataset_id='ds', table_id='schema_registry')\n"
            "  get_schema_config.s3('s3://bucket/schema.csv', table='users')\n"
            "  get_schema_config.csv('schema.csv', table='users')\n"
            "  get_schema_config.mysql(conn=conn, table='users')\n"
            "  get_schema_config(source='bigquery', ...)  # fallback\n"
        )


# ============================================================================
# INSTANTIATE DISPATCHERS
# ============================================================================

validate = _ValidateDispatcher()
summarize = _SummarizeDispatcher()
get_rules_config = _RulesConfigDispatcher()
get_schema_config = _SchemaConfigDispatcher()


# ============================================================================
# LEGACY FUNCTION (DEPRECATED)
# ============================================================================


def report(df, rules: list[dict], name: str = "Quality Check"):
    """
    [DEPRECATED] Performs a quality check using cuallee library.

    This function is legacy and will be removed in future versions.
    Use `validate.pandas()` or other engine-specific validators instead.

    The function iterates over a list of rules and applies different checks to the
    specified fields of the DataFrame using the cuallee library.

    Parameters:
        df (DataFrame): The DataFrame to be validated.
        rules (list of dict): A list of rules defining the checks to be performed.
        name (str): The name of the quality check (default is "Quality Check").

    Returns:
        quality_check (CheckResult): The result of the quality validation.

    Warnings:
        This function is deprecated. Use validate.pandas() instead.
    """
    warnings.warn(
        "report() is deprecated and will be removed in a future version. "
        "Use validate.pandas() or other engine-specific validators instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    check = Check(CheckLevel.WARNING, name)
    for rule in rules:
        rule_name = rule["check_type"]
        field = rule["field"]
        threshold = rule.get("threshold", 1.0)
        threshold = 1.0 if threshold is None else threshold

        match rule_name:
            case "is_complete":
                check = check.is_complete(field, pct=threshold)

            case "is_unique":
                check = check.is_unique(field, pct=threshold)

            case "is_primary_key":
                check = check.is_primary_key(field, pct=threshold)

            case "are_complete":
                check = check.are_complete(field, pct=threshold)

            case "are_unique":
                check = check.are_complete(field, pct=threshold)

            case "is_composite_key":
                check = check.are_complete(field, pct=threshold)

            case "is_greater_than":
                value = __convert_value(rule["value"])
                check = check.is_greater_than(field, value, pct=threshold)

            case "is_positive":
                check = check.is_positive(field, pct=threshold)

            case "is_negative":
                check = check.is_negative(field, pct=threshold)

            case "is_greater_or_equal_than":
                value = __convert_value(rule["value"])
                check = check.is_greater_or_equal_than(field, value, pct=threshold)

            case "is_less_than":
                value = __convert_value(rule["value"])
                check = check.is_less_than(field, value, pct=threshold)

            case "is_less_or_equal_than":
                value = __convert_value(rule["value"])
                check = check.is_less_or_equal_than(field, value, pct=threshold)

            case "is_equal_than":
                value = __convert_value(rule["value"])
                check = check.is_equal_than(field, value, pct=threshold)

            case "is_contained_in" | "is_in":
                values = rule["value"]
                values = values.replace("[", "").replace("]", "").split(",")
                values = tuple([value.strip() for value in values])
                check = check.is_contained_in(field, values, pct=threshold)

            case "not_contained_in" | "not_in":
                values = rule["value"]
                values = values.replace("[", "").replace("]", "").split(",")
                values = tuple([value.strip() for value in values])
                check = check.is_contained_in(field, values, pct=threshold)

            case "is_between":
                values = rule["value"]
                values = values.replace("[", "").replace("]", "").split(",")
                values = tuple(__convert_value(value) for value in values)
                check = check.is_between(field, values, pct=threshold)

            case "has_pattern":
                pattern = rule["value"]
                check = check.has_pattern(field, pattern, pct=threshold)

            case "is_legit":
                check = check.is_legit(field, pct=threshold)

            case "has_min":
                value = __convert_value(rule["value"])
                check = check.has_min(field, value)

            case "has_max":
                value = __convert_value(rule["value"])
                check = check.has_max(field, value)

            case "has_std":
                value = __convert_value(rule["value"])
                check = check.has_std(field, value)

            case "has_mean":
                value = __convert_value(rule["value"])
                check = check.has_mean(field, value)

            case "has_sum":
                value = __convert_value(rule["value"])
                check = check.has_sum(field, value)

            case "has_cardinality":
                value = __convert_value(rule["value"])
                check = check.has_cardinality(field, value)

            case "has_infogain":
                check = check.has_infogain(field, pct=threshold)

            case "has_entropy":
                value = __convert_value(rule["value"])
                check = check.has_entropy(field, value)

            case "is_in_millions":
                check = check.is_in_millions(field, pct=threshold)

            case "is_in_billions":
                check = check.is_in_millions(field, pct=threshold)

            case "is_t_minus_1":
                check = check.is_t_minus_1(field, pct=threshold)

            case "is_t_minus_2":
                check = check.is_t_minus_2(field, pct=threshold)

            case "is_t_minus_3":
                check = check.is_t_minus_3(field, pct=threshold)

            case "is_today":
                check = check.is_today(field, pct=threshold)

            case "is_yesterday":
                check = check.is_yesterday(field, pct=threshold)

            case "is_on_weekday":
                check = check.is_on_weekday(field, pct=threshold)

            case "is_on_weekend":
                check = check.is_on_weekend(field, pct=threshold)

            case "is_on_monday":
                check = check.is_on_monday(field, pct=threshold)

            case "is_on_tuesday":
                check = check.is_on_tuesday(field, pct=threshold)

            case "is_on_wednesday":
                check = check.is_on_wednesday(field, pct=threshold)

            case "is_on_thursday":
                check = check.is_on_thursday(field, pct=threshold)

            case "is_on_friday":
                check = check.is_on_friday(field, pct=threshold)

            case "is_on_saturday":
                check = check.is_on_saturday(field, pct=threshold)

            case "is_on_sunday":
                check = check.is_on_sunday(field, pct=threshold)

            case "satisfies":
                predicate = rule["value"]
                check = check.satisfies(field, predicate, pct=threshold)

            case _:
                warnings.warn(f"Unknown rule name: {rule_name}, {field}")

    quality_check = check.validate(df)
    return quality_check
