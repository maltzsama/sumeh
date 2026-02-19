"""
Core data models for Sumeh validation framework.

All validation results flow through these models.
No DataFrame string concatenation. No dq_status column bullshit.
"""

import uuid
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import datetime
from enum import Enum
from typing import Any, List, Optional


class ValidationLevel(Enum):
    """Validation level: ROW or TABLE."""

    ROW = "ROW"
    TABLE = "TABLE"


class ValidationStatus(Enum):
    """Validation status: PASS, FAIL, or ERROR."""

    PASS = "PASS"
    FAIL = "FAIL"
    ERROR = "ERROR"


@dataclass
class MetricResult:
    """
    Output of an Analyzer - pure computation, no opinion.

    An Analyzer computes a metric on data:
      - CompletenessAnalyzer → null_count, completeness_rate
      - MeanAnalyzer → mean_value
      - PatternAnalyzer → matching_row_ids, non_matching_row_ids

    Analyzers are PURE - same input always gives same output.
    Analyzers DON'T know about thresholds or rules.

    Example:
        >>> analyzer = CompletenessAnalyzer()
        >>> metric = analyzer.analyze(df, "email")
        >>> print(metric.value)  # 0.95 (95% complete)
        >>> print(metric.metadata["null_count"])  # 50
    """

    metric_type: str  # "completeness", "mean", "pattern", "cardinality"
    field: str  # "email", "salary", "age"
    value: Any  # Primary metric value (float, int, list, etc)

    # Row tracking
    total_rows: int
    affected_row_ids: List[int] = dataclass_field(
        default_factory=list
    )  # Row indices that violate

    # Metadata
    metadata: dict = dataclass_field(
        default_factory=dict
    )  # Extra context (null_count, distribution, etc)

    def __repr__(self):
        return f"MetricResult(type={self.metric_type}, field={self.field}, value={self.value})"


@dataclass
class ValidationResult:
    """
    Output of a Constraint - compares metric to rule expectation.

    A Constraint takes a MetricResult + RuleDef and decides PASS/FAIL:
      - is_complete: checks if completeness_rate >= threshold
      - has_mean: checks if mean >= expected_value

    This is the atomic unit of validation output.
    One ValidationResult per rule executed.

    Example:
        >>> constraint = CompletenessConstraint()
        >>> result = constraint.check(metric, rule)
        >>> print(result.status)  # ValidationStatus.FAIL
        >>> print(result.message)  # "50 null values found"
    """

    id: str = dataclass_field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = dataclass_field(default_factory=datetime.utcnow)

    # Rule identification
    rule_id: str = ""
    level: ValidationLevel = ValidationLevel.ROW
    category: str = "unknown"
    check_type: str = ""
    field: str = ""

    # Validation result
    status: ValidationStatus = ValidationStatus.ERROR
    pass_rate: Optional[float] = None  # For row-level: % of rows that passed

    # Expected vs Actual
    expected_value: Any = None  # What the rule expected (threshold, value, etc)
    actual_value: Any = None  # What was actually measured

    # Details
    violating_row_ids: List[int] = dataclass_field(
        default_factory=list
    )  # Row indices that failed
    message: Optional[str] = None  # Human-readable explanation
    metadata: dict = dataclass_field(default_factory=dict)  # Extra context

    def __repr__(self):
        return (
            f"ValidationResult({self.check_type} on {self.field}: {self.status.value})"
        )


@dataclass
class ValidationReport:
    """
    Collection of ValidationResults for a single validation run.

    This is what engines return from validate().
    Replaces the old (df_errors, violations, table_summary) tuple nightmare.

    **Bifurcation Pattern:**
    - df_validated: IValidatedDataFrame wrapper (engine-specific)
    - results: Aggregated statistics per rule
    - Use .split() to separate good vs bad data (delegates to wrapper)
    - Use .summary() to get lightweight JSON for Deletron

    Example:
        >>> from sumeh import pandas, csv
        >>> rules = csv.get_rules_config("rules.csv")
        >>> report = pandas.validate(df, rules)
        >>>
        >>> # Split for storage (delegates to engine wrapper)
        >>> good_df, bad_df = report.split()
        >>> good_df.to_parquet("clean/users.parquet")
        >>> bad_df.to_parquet("quarantine/users.parquet")
        >>>
        >>> # Send lightweight metadata to Deletron
        >>> deletron_sink.push(report.summary())
    """

    results: List[ValidationResult]
    total_rows: int
    execution_time_ms: float
    engine: str
    error_message: Optional[str] = None

    timestamp: datetime = dataclass_field(default_factory=datetime.utcnow)
    df_validated: Any = None

    @property
    def passed(self) -> List[ValidationResult]:
        """Get all passed validations."""
        return [r for r in self.results if r.status == ValidationStatus.PASS]

    @property
    def failed(self) -> List[ValidationResult]:
        """Get all failed validations."""
        return [r for r in self.results if r.status == ValidationStatus.FAIL]

    @property
    def errors(self) -> List[ValidationResult]:
        """Get all errored validations (execution failures)."""
        return [r for r in self.results if r.status == ValidationStatus.ERROR]

    @property
    def pass_rate(self) -> float:
        """Overall pass rate across all validations."""
        if not self.results:
            return 1.0
        return len(self.passed) / len(self.results)

    @property
    def df(self):
        """
        Get validated DataFrame with _dq_errors column.

        Shortcut for get_validated_df().
        Returns the DataFrame with all original columns plus _dq_errors.

        Returns:
            DataFrame with _dq_errors column (engine-specific type)
            None if engine doesn't support DataFrame output (e.g. SQL engines)

        Example:
            >>> report = validate(df, rules)
            >>> validated_df = report.df
            >>>
            >>> # DataFrame has _dq_errors column:
            >>> # | id | name  | age | _dq_errors                    |
            >>> # |----|-------|-----|-------------------------------|
            >>> # | 1  | John  | 25  | []                            |
            >>> # | 2  | Jane  | -5  | [{"rule": "is_positive"...}]  |
            >>>
            >>> # Filter to see only errors
            >>> errors = validated_df.filter("size(_dq_errors) > 0")
        """
        return self.get_validated_df()

    def get_validated_df(self):
        """
        Get the validated DataFrame with _dq_errors column.

        Returns DataFrame with all original columns plus _dq_errors column.
        Use .split() if you want to separate good/bad rows instead.

        Returns:
            DataFrame with _dq_errors column (engine-specific type)
            None if engine doesn't support DataFrame output (e.g. SQL engines)

        Example:
            >>> # Option 1: Get DataFrame with errors (no split)
            >>> report = validate(df, rules)
            >>> validated_df = report.get_validated_df()
            >>> validated_df.write.parquet("output_with_errors/")
            >>>
            >>> # Option 2: Split good/bad
            >>> good_df, bad_df = report.split()
            >>> good_df.write.parquet("clean/")
            >>> bad_df.write.parquet("quarantine/")
        """
        if self.df_validated is None:
            return None

        # Delegate to engine-specific wrapper's to_native()
        if hasattr(self.df_validated, "to_native"):
            return self.df_validated.to_native()

        # Fallback: return wrapper directly
        return self.df_validated

    def split(self):
        """
        Split validated DataFrame into good and bad data.

        Delegates to the engine-specific wrapper's split_by_errors() method.
        Each engine (pandas, pyspark, polars) implements this differently.

        Returns:
            (good_df, bad_df) tuple where types depend on the engine

        Raises:
            ValueError: If df_validated is None
            AttributeError: If wrapper doesn't implement split_by_errors()
        """
        if self.df_validated is None:
            raise ValueError("No validated DataFrame available")

        # Delegate to engine-specific wrapper
        if not hasattr(self.df_validated, "split_by_errors"):
            raise AttributeError(
                f"DataFrame wrapper doesn't implement split_by_errors(). "
                f"Got type: {type(self.df_validated)}"
            )

        return self.df_validated.split_by_errors()

    def summary(self, max_sample_ids: int = 100) -> dict:
        """
        Get lightweight summary for Deletron (no big data).

        Returns JSON with aggregated statistics + sample IDs.
        Typically ~10KB, suitable for HTTP POST.

        Args:
            max_sample_ids: Maximum IDs to include per rule (default: 100)

        Returns:
            dict: Lightweight summary
        """
        return {
            "timestamp": self.timestamp.isoformat(),
            "engine": self.engine,
            "total_rows": self.total_rows,
            "execution_time_ms": self.execution_time_ms,
            "total_validations": len(self.results),
            "passed": len(self.passed),
            "failed": len(self.failed),
            "errors": len(self.errors),
            "pass_rate": self.pass_rate,
            "validations": [
                {
                    "rule_id": r.rule_id,
                    "check_type": r.check_type,
                    "field": r.field,
                    "category": r.category,
                    "level": r.level.value,
                    "status": r.status.value,
                    "pass_rate": r.pass_rate,
                    "expected": r.expected_value,
                    "actual": r.actual_value,
                    "message": r.message,
                    "fail_count": len(r.violating_row_ids),
                    "sample_violating_ids": r.violating_row_ids[:max_sample_ids],
                }
                for r in self.results
            ],
        }

    def get_good_df(self):
        """Shortcut for split()[0]"""
        return self.split()[0]

    def get_bad_df(self):
        """Shortcut for split()[1]"""
        return self.split()[1]

    def __repr__(self):
        return (
            f"ValidationReport("
            f"{len(self.results)} rules, "
            f"{len(self.failed)} failed, "
            f"pass_rate={self.pass_rate:.2%})"
        )

    def __len__(self):
        """Number of validation results."""
        return len(self.results)

    def __iter__(self):
        """Iterate over validation results."""
        return iter(self.results)

    def __getitem__(self, index):
        """Get validation result by index."""
        return self.results[index]


@dataclass
class SinkResult:
    """
    Structured response for metadata export operations.
    Allows the orchestrator to monitor health and performance.
    """

    success: bool
    sink_name: str
    duration_ms: float
    records_sent: int
    timestamp: datetime
    error: Optional[str] = None

    def __bool__(self) -> bool:
        return self.success
