from typing import Protocol, runtime_checkable
from typing import Tuple, Any, Optional, Dict

from sumeh.core.models.metrics import MetricResult
from sumeh.core.models.validation import ValidationResult
from sumeh.core.rules.rule_model import RuleDefinition


@runtime_checkable
class IDataFrameValidator(Protocol):
    """
    Protocol for validated DataFrames across all engines.

    Any engine that implements this protocol can be used with
    ValidationReport.split() and other generic operations.

    Each engine (pandas, pyspark, polars) creates a wrapper class
    that implements this protocol.
    """

    def split_by_errors(self, error_column: str = "_dq_errors") -> Tuple[Any, Any]:
        """
        Split DataFrame into good and bad rows based on error column.

        Args:
            error_column: Name of the column containing error lists

        Returns:
            (good_df, bad_df) where:
              - good_df: Rows with no errors (without error column)
              - bad_df: Rows with errors (WITH error column for DLQ)
        """
        ...

    def to_native(self) -> Any:
        """
        Return the underlying native DataFrame.

        Returns:
            pd.DataFrame, pyspark.DataFrame, pl.DataFrame, etc
        """
        ...


@runtime_checkable
class IStreamValidator(Protocol):
    """
    Protocol for streaming validators (PyFlink UDFs).

    Stream validators work row-by-row on unbounded streams.
    They are fundamentally different from batch analyzers.

    Key Differences from IAnalyzer:
      - Works on SINGLE value, not DataFrame
      - Returns error string or None, not MetricResult
      - Stateless (no aggregations, no uniqueness)
      - No "total_rows" concept (stream is unbounded)

    Implementation Requirements:
      1. Must have a validate_value() static method
      2. Must return Optional[str] (error message or None)
      3. Must be stateless (pure function of inputs)
      4. Must be fast (runs per-event in stream)

    Example Implementation:
        # >>> class CompletenessValidator:
        # >>>     @staticmethod
        # >>>     def validate_value(value) -> Optional[str]:
        # >>>         if value is None:
        # >>>             return "null_value"
        # >>>         return None
        # >>>
        # >>> class PatternValidator:
        # >>>     @staticmethod
        # >>>     def validate_value(value, pattern: str) -> Optional[str]:
        # >>>         if not re.match(pattern, str(value)):
        # >>>             return f"pattern_mismatch:{pattern}"
        # >>>         return None
    """

    @staticmethod
    def validate_value(value: Any, *args, **kwargs) -> Optional[str]:
        """
        Validate a single value from stream.

        Args:
            value: Single value to validate (from one row/event)
            *args: Validator-specific parameters (e.g., threshold, pattern)
            **kwargs: Additional validator parameters

        Returns:
            Error message if validation fails, None if passes

        Notes:
            - Must be stateless (no cross-row dependencies)
            - Must be fast (called per-event)
            - Cannot access other rows or aggregate
            - Should handle None gracefully

        Example:
            # >>> validate_value(None)  # → "null_value"
            # >>> validate_value("test@example.com")  # → None
            # >>> validate_value(42, threshold=100)  # → "not_greater_than:100"
        """
        ...


def stream_validator(func):
    """
    Decorator to mark a function as a stream validator.

    Validates that function follows IStreamValidator protocol.

    Example:
        # >>> @stream_validator
        # >>> def check_positive(value):
        # >>>     return "not_positive" if value <= 0 else None
    """
    # Check if function follows protocol (basic check)
    if not callable(func):
        raise TypeError(f"{func} is not callable")

    # Add metadata
    func.__stream_validator__ = True
    return func


def udf_validator(result_type):
    """
    Decorator to create PyFlink UDF from stream validator.

    Combines @stream_validator with PyFlink's @udf decorator.

    Example:
        # >>> from pyflink.table import DataTypes
        # >>>
        # >>> @udf_validator(DataTypes.STRING())
        # >>> def check_complete(value):
        # >>>     return "null_value" if value is None else None
    """

    def decorator(func):
        from pyflink.table.udf import udf

        func.__stream_validator__ = True

        return udf(result_type=result_type)(func)

    return decorator


@runtime_checkable
class IConstraint(Protocol):
    """
    Protocol for all constraints.

    Constraints validate metrics against rules.
    They make PASS/FAIL decisions.

    Implementation Requirements:
      1. Must have a check() static method
      2. Must accept MetricResult + RuleDef
      3. Must return ValidationResult
      4. Must set status to PASS, FAIL, or ERROR

    Example Implementation:
        # >>> class MyConstraint:
        # >>>     @staticmethod
        # >>>     def check(metric: MetricResult, rule: RuleDef) -> ValidationResult:
        # >>>         threshold = rule.threshold or 1.0
        # >>>         passed = metric.value >= threshold
        # >>>
        # >>>         return ValidationResult(
        # >>>             check_type=rule.check_type,
        # >>>             field=rule.field,
        # >>>             status=ValidationStatus.PASS if passed else ValidationStatus.FAIL,
        # >>>             expected_value=threshold,
        # >>>             actual_value=metric.value,
        # >>>         )
    """

    @staticmethod
    def check(metric: MetricResult, rule: RuleDefinition) -> ValidationResult:
        """
        Validate metric against rule.

        Args:
            metric: Computed metric from an Analyzer
            rule: Validation rule with expectations (threshold, value, etc)

        Returns:
            ValidationResult with PASS/FAIL decision

        Note:
            - Set status to ERROR if metric is invalid or check fails to execute
            - Always populate expected_value and actual_value for traceability
            - Include helpful message for failures
        """
        ...


@runtime_checkable
class IExporter(Protocol):
    """
    Protocol for exporting results to external systems.
    """

    def profile(self, data: Dict[str, Any]) -> Any:
        """
        Process and profile the given data.

        Args:
            data: A dictionary containing arbitrary key-value pairs to be profiled.

        Returns:
            The profiling result or analysis of the input data.
        """
        ...

    def validation(self, report: Any) -> Any:
        """
        Validate the provided report.

        Args:
            report: The report object to be validated.

        Returns:
            The validation result or processed report.

        Raises:
            ValidationError: If the report fails validation.
        """
        ...
