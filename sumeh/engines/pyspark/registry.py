"""
PySpark engine registry - COMPLETE (48+ rules).
"""

from sumeh.core.logic.comparators import (
    CompletenessConstraint,
    MultiFieldCompletenessConstraint,
    UniquenessConstraint,
    GenericConstraint,
    AggregationConstraint,
)
from sumeh.engines.pyspark.analyzers import (
    CompletenessAnalyzer,
    MultiFieldCompletenessAnalyzer,
    UniquenessAnalyzer,
    MultiFieldUniquenessAnalyzer,
    ComparisonAnalyzer,
    BetweenAnalyzer,
    ColumnComparisonAnalyzer,
    MembershipAnalyzer,
    PatternAnalyzer,
    LegitAnalyzer,
    DateAnalyzer,
    DateBetweenAnalyzer,
    DateComparisonAnalyzer,
    AggregationAnalyzer,
)

VALIDATION_REGISTRY = {
    # Completeness
    "is_complete": (CompletenessAnalyzer, CompletenessConstraint),
    "are_complete": (MultiFieldCompletenessAnalyzer, MultiFieldCompletenessConstraint),
    # Uniqueness
    "is_unique": (UniquenessAnalyzer, UniquenessConstraint),
    "are_unique": (MultiFieldUniquenessAnalyzer, UniquenessConstraint),
    "is_primary_key": (UniquenessAnalyzer, UniquenessConstraint),
    "is_composite_key": (MultiFieldUniquenessAnalyzer, UniquenessConstraint),
    # Comparison
    "is_equal": (ComparisonAnalyzer, GenericConstraint),
    "is_equal_than": (ColumnComparisonAnalyzer, GenericConstraint),
    "is_between": (BetweenAnalyzer, GenericConstraint),
    "is_greater_than": (ComparisonAnalyzer, GenericConstraint),
    "is_less_than": (ComparisonAnalyzer, GenericConstraint),
    "is_greater_or_equal_than": (ComparisonAnalyzer, GenericConstraint),
    "is_less_or_equal_than": (ComparisonAnalyzer, GenericConstraint),
    "is_positive": (ComparisonAnalyzer, GenericConstraint),
    "is_negative": (ComparisonAnalyzer, GenericConstraint),
    "is_in_millions": (ComparisonAnalyzer, GenericConstraint),
    "is_in_billions": (ComparisonAnalyzer, GenericConstraint),
    # Membership
    "is_contained_in": (MembershipAnalyzer, GenericConstraint),
    "not_contained_in": (MembershipAnalyzer, GenericConstraint),
    "is_in": (MembershipAnalyzer, GenericConstraint),
    "not_in": (MembershipAnalyzer, GenericConstraint),
    # Pattern
    "has_pattern": (PatternAnalyzer, GenericConstraint),
    "is_legit": (LegitAnalyzer, GenericConstraint),
    # Date
    "is_today": (DateAnalyzer, GenericConstraint),
    "is_t_minus_1": (DateAnalyzer, GenericConstraint),
    "is_t_minus_2": (DateAnalyzer, GenericConstraint),
    "is_t_minus_3": (DateAnalyzer, GenericConstraint),
    "is_yesterday": (DateAnalyzer, GenericConstraint),
    "is_past_date": (DateAnalyzer, GenericConstraint),
    "is_future_date": (DateAnalyzer, GenericConstraint),
    "is_date_between": (DateBetweenAnalyzer, GenericConstraint),
    "is_date_after": (DateComparisonAnalyzer, GenericConstraint),
    "is_date_before": (DateComparisonAnalyzer, GenericConstraint),
    "is_on_weekday": (DateAnalyzer, GenericConstraint),
    "is_on_weekend": (DateAnalyzer, GenericConstraint),
    "is_on_monday": (DateAnalyzer, GenericConstraint),
    "is_on_tuesday": (DateAnalyzer, GenericConstraint),
    "is_on_wednesday": (DateAnalyzer, GenericConstraint),
    "is_on_thursday": (DateAnalyzer, GenericConstraint),
    "is_on_friday": (DateAnalyzer, GenericConstraint),
    "is_on_saturday": (DateAnalyzer, GenericConstraint),
    "is_on_sunday": (DateAnalyzer, GenericConstraint),
    # Table-level aggregations
    "has_min": (AggregationAnalyzer, AggregationConstraint),
    "has_max": (AggregationAnalyzer, AggregationConstraint),
    "has_sum": (AggregationAnalyzer, AggregationConstraint),
    "has_mean": (AggregationAnalyzer, AggregationConstraint),
    "has_std": (AggregationAnalyzer, AggregationConstraint),
    "has_cardinality": (AggregationAnalyzer, AggregationConstraint),
}


def get_analyzer(check_type: str):
    if check_type not in VALIDATION_REGISTRY:
        raise KeyError(
            f"'{check_type}' not implemented in pyspark. Available: {list(VALIDATION_REGISTRY.keys())}"
        )
    return VALIDATION_REGISTRY[check_type][0]


def get_constraint(check_type: str):
    if check_type not in VALIDATION_REGISTRY:
        raise KeyError(
            f"'{check_type}' not implemented. Available: {list(VALIDATION_REGISTRY.keys())}"
        )
    return VALIDATION_REGISTRY[check_type][1]


def list_implemented() -> list:
    return list(VALIDATION_REGISTRY.keys())
