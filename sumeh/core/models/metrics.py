from dataclasses import dataclass, field as dataclass_field
from typing import Any, List, Union


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
    field: Union[str, List[str]]  # "email", "salary", "age"
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
