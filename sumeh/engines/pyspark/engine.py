"""
PySpark validation engine - Bifurcation Pattern.

Zero UDFs. Pure Column API. Scales to Petabytes.
"""

import time
from datetime import datetime
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

from sumeh.core.models.validation import (
    ValidationReport,
    ValidationResult,
    ValidationLevel,
    ValidationStatus,
)
from sumeh.core.rules.rule_model import RuleDefinition
from sumeh.engines.pyspark.dataframe import ValidatedSparkDataFrame
from sumeh.engines.pyspark.registry import get_analyzer, get_constraint


def validate(
    df: DataFrame, rules: List[RuleDefinition], baseline_provider=None
) -> ValidationReport:
    """
    Validate PySpark DataFrame using Bifurcation Pattern.

    Returns ValidationReport with ValidatedSparkDataFrame wrapper.

    Example:
        >>> report = pyspark.validate(spark_df, rules)
        >>> good_df, bad_df = report.split()
        >>> good_df.write.parquet("clean/")
        >>> bad_df.write.parquet("quarantine/")
    """
    start_time = time.time()

    # Initialize _dq_errors column (array of structs)
    # Use monotonically_increasing_id for row tracking
    df = df.withColumn("_row_id", F.monotonically_increasing_id())

    error_schema = __get_error_schema()
    df = df.withColumn("_dq_errors", F.array().cast(ArrayType(error_schema)))

    row_rules = [r for r in rules if r.is_applicable_for_level("ROW")]
    table_rules = [r for r in rules if r.is_applicable_for_level("TABLE")]

    results = []

    # Row-level validations
    if row_rules:
        row_results = _validate_row_level(df, row_rules)
        results.extend(row_results)

        # Populate _dq_errors (PySpark way)
        for result in row_results:
            if result.status == ValidationStatus.FAIL and result.violating_row_ids:
                error_struct = F.struct(
                    F.lit(result.rule_id).alias("rule_id"),
                    F.lit(result.check_type).alias("check_type"),
                    F.lit(result.field).alias("field"),
                    F.lit(result.category).alias("category"),
                    F.lit(str(result.expected_value)).alias("expected"),
                    F.lit(str(result.actual_value)).alias("actual"),
                    F.lit(result.message).alias("message"),
                    F.lit(datetime.utcnow().isoformat()).alias("timestamp"),
                )

                # Append error to array for matching rows
                # Uses ALL violating_row_ids (no sampling here)
                # Sampling happens in report.summary()
                df = df.withColumn(
                    "_dq_errors",
                    F.when(
                        F.col("_row_id").isin(result.violating_row_ids),
                        F.array_union(F.col("_dq_errors"), F.array(error_struct)),
                    ).otherwise(F.col("_dq_errors")),
                )

    # Drop _row_id (temporary column)
    df = df.drop("_row_id")

    # Table-level validations
    if table_rules:
        results.extend(_validate_table_level(df, table_rules, baseline_provider))

    execution_time_ms = (time.time() - start_time) * 1000

    # Wrap in ValidatedSparkDataFrame
    validated_wrapper = ValidatedSparkDataFrame(df)

    return ValidationReport(
        results=results,
        total_rows=df.count(),  # Triggers action
        execution_time_ms=execution_time_ms,
        engine="pyspark",
        df_validated=validated_wrapper,
    )


def _validate_row_level(df: DataFrame, rules: List[RuleDefinition]) -> List[ValidationResult]:
    """Execute row-level validations using Column API."""
    results = []

    for rule in rules:
        skip_reason = rule.get_skip_reason("ROW", "pyspark")
        if skip_reason:
            results.append(
                ValidationResult(
                    rule_id=getattr(rule, "id", ""),
                    level=ValidationLevel.ROW,
                    category=rule.category or "unknown",
                    check_type=rule.check_type,
                    field=str(rule.field),
                    status=ValidationStatus.ERROR,
                    message=f"Skipped: {skip_reason}",
                )
            )
            continue

        try:
            analyzer_class = get_analyzer(rule.check_type)
            metric = analyzer_class.analyze(df, rule)

            constraint_class = get_constraint(rule.check_type)
            result = constraint_class.check(metric, rule)

            results.append(result)

        except KeyError as e:
            results.append(
                ValidationResult(
                    rule_id=getattr(rule, "id", ""),
                    level=ValidationLevel.ROW,
                    category=rule.category or "unknown",
                    check_type=rule.check_type,
                    field=str(rule.field),
                    status=ValidationStatus.ERROR,
                    message=f"Not implemented: {str(e)}",
                )
            )

        except Exception as e:
            results.append(
                ValidationResult(
                    rule_id=getattr(rule, "id", ""),
                    level=ValidationLevel.ROW,
                    category=rule.category or "unknown",
                    check_type=rule.check_type,
                    field=str(rule.field),
                    status=ValidationStatus.ERROR,
                    message=f"Error: {str(e)}",
                )
            )

    return results


def _validate_table_level(
    df: DataFrame, rules: List[RuleDefinition], baseline_provider
) -> List[ValidationResult]:
    """Execute table-level validations."""
    results = []

    for rule in rules:
        skip_reason = rule.get_skip_reason("TABLE", "pyspark")
        if skip_reason:
            results.append(
                ValidationResult(
                    rule_id=getattr(rule, "id", ""),
                    level=ValidationLevel.TABLE,
                    category=rule.category or "unknown",
                    check_type=rule.check_type,
                    field=str(rule.field),
                    status=ValidationStatus.ERROR,
                    message=f"Skipped: {skip_reason}",
                )
            )
            continue

        try:
            analyzer_class = get_analyzer(rule.check_type)
            metric = analyzer_class.analyze(df, rule)

            constraint_class = get_constraint(rule.check_type)
            result = constraint_class.check(metric, rule)

            results.append(result)

        except KeyError as e:
            results.append(
                ValidationResult(
                    rule_id=getattr(rule, "id", ""),
                    level=ValidationLevel.TABLE,
                    category=rule.category or "unknown",
                    check_type=rule.check_type,
                    field=str(rule.field),
                    status=ValidationStatus.ERROR,
                    message=f"Not implemented: {str(e)}",
                )
            )

        except Exception as e:
            results.append(
                ValidationResult(
                    rule_id=getattr(rule, "id", ""),
                    level=ValidationLevel.TABLE,
                    category=rule.category or "unknown",
                    check_type=rule.check_type,
                    field=str(rule.field),
                    status=ValidationStatus.ERROR,
                    message=f"Error: {str(e)}",
                )
            )

    return results


def __get_error_schema():
    return StructType(
        [
            StructField("check_type", StringType(), True),
            StructField("field", StringType(), True),
            StructField("message", StringType(), True),
            StructField("expected", StringType(), True),
            StructField("actual", StringType(), True),
        ]
    )
