"""
Ray Data Registry - EXPANDED.
Maps check_types to Ray Data Analyzers and Constraints.
"""

from sumeh.engines.ray_data.analyzers import (
    CompletenessAnalyzer,
    MultiFieldCompletenessAnalyzer,
    ComparisonAnalyzer,
    BetweenAnalyzer,
    MembershipAnalyzer,
    PatternAnalyzer,
    LegitAnalyzer,
    DateAnalyzer,
    DateBetweenAnalyzer,
    DateComparisonAnalyzer,
    AggregationAnalyzer,
)
from sumeh.core.constraints.comparators import (
    CompletenessConstraint,
    MultiFieldCompletenessConstraint,
    GenericConstraint,
    AggregationConstraint,
)

VALIDATION_REGISTRY = {
    # Completeness
    "is_complete": (CompletenessAnalyzer, CompletenessConstraint),
    "are_complete": (MultiFieldCompletenessAnalyzer, MultiFieldCompletenessConstraint),
    # Comparison
    "is_equal": (ComparisonAnalyzer, GenericConstraint),
    "is_greater_than": (ComparisonAnalyzer, GenericConstraint),
    "is_less_than": (ComparisonAnalyzer, GenericConstraint),
    "is_greater_or_equal_than": (ComparisonAnalyzer, GenericConstraint),
    "is_less_or_equal_than": (ComparisonAnalyzer, GenericConstraint),
    "is_positive": (ComparisonAnalyzer, GenericConstraint),
    "is_negative": (ComparisonAnalyzer, GenericConstraint),
    "is_in_millions": (ComparisonAnalyzer, GenericConstraint),
    "is_in_billions": (ComparisonAnalyzer, GenericConstraint),
    "is_between": (BetweenAnalyzer, GenericConstraint),
    # Membership
    "is_in": (MembershipAnalyzer, GenericConstraint),
    "is_contained_in": (MembershipAnalyzer, GenericConstraint),
    "not_in": (MembershipAnalyzer, GenericConstraint),
    "not_contained_in": (MembershipAnalyzer, GenericConstraint),
    # Pattern
    "has_pattern": (PatternAnalyzer, GenericConstraint),
    "is_legit": (LegitAnalyzer, GenericConstraint),
    # Date - Basic
    "is_today": (DateAnalyzer, GenericConstraint),
    "is_yesterday": (DateAnalyzer, GenericConstraint),
    "is_t_minus_1": (DateAnalyzer, GenericConstraint),
    "is_t_minus_2": (DateAnalyzer, GenericConstraint),
    "is_t_minus_3": (DateAnalyzer, GenericConstraint),
    "is_past_date": (DateAnalyzer, GenericConstraint),
    "is_future_date": (DateAnalyzer, GenericConstraint),
    # Date - Weekday
    "is_on_weekday": (DateAnalyzer, GenericConstraint),
    "is_on_weekend": (DateAnalyzer, GenericConstraint),
    "is_on_monday": (DateAnalyzer, GenericConstraint),
    "is_on_tuesday": (DateAnalyzer, GenericConstraint),
    "is_on_wednesday": (DateAnalyzer, GenericConstraint),
    "is_on_thursday": (DateAnalyzer, GenericConstraint),
    "is_on_friday": (DateAnalyzer, GenericConstraint),
    "is_on_saturday": (DateAnalyzer, GenericConstraint),
    "is_on_sunday": (DateAnalyzer, GenericConstraint),
    # Date - Range/Comparison
    "is_date_between": (DateBetweenAnalyzer, GenericConstraint),
    "is_date_after": (DateComparisonAnalyzer, GenericConstraint),
    "is_date_before": (DateComparisonAnalyzer, GenericConstraint),
    # Aggregations
    "has_min": (AggregationAnalyzer, AggregationConstraint),
    "has_max": (AggregationAnalyzer, AggregationConstraint),
    "has_sum": (AggregationAnalyzer, AggregationConstraint),
    "has_mean": (AggregationAnalyzer, AggregationConstraint),
    "has_std": (AggregationAnalyzer, AggregationConstraint),
    "has_cardinality": (AggregationAnalyzer, AggregationConstraint),
}


def get_analyzer(check_type: str):
    """Get analyzer for check_type."""
    entry = VALIDATION_REGISTRY.get(check_type)
    return entry[0] if entry else None


def get_constraint(check_type: str):
    """Get constraint for check_type."""
    entry = VALIDATION_REGISTRY.get(check_type)
    return entry[1] if entry else None
