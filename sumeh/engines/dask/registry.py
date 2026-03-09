"""
Dask Registry - COMPLETE IMPLEMENTATION.

Maps check_type → (Analyzer, Constraint) for ALL supported rules in Dask engine.
Ensures feature parity with Pandas engine.
"""

from sumeh.core.logic.comparators import (
    CompletenessConstraint,
    MultiFieldCompletenessConstraint,
    UniquenessConstraint,
    GenericConstraint,
    AggregationConstraint,
)
from sumeh.engines.dask.analyzers import (
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
    LogicAnalyzer,
)

VALIDATION_REGISTRY = {
    # ========================================================================
    # COMPLETENESS
    # ========================================================================
    "is_complete": (CompletenessAnalyzer, CompletenessConstraint),
    "are_complete": (MultiFieldCompletenessAnalyzer, MultiFieldCompletenessConstraint),
    # ========================================================================
    # UNIQUENESS
    # ========================================================================
    "is_unique": (UniquenessAnalyzer, UniquenessConstraint),
    "are_unique": (MultiFieldUniquenessAnalyzer, UniquenessConstraint),
    "is_primary_key": (UniquenessAnalyzer, UniquenessConstraint),
    "is_composite_key": (MultiFieldUniquenessAnalyzer, UniquenessConstraint),
    # ========================================================================
    # COMPARISON (ROW LEVEL)
    # ========================================================================
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
    # ========================================================================
    # MEMBERSHIP
    # ========================================================================
    "is_contained_in": (MembershipAnalyzer, GenericConstraint),
    "not_contained_in": (MembershipAnalyzer, GenericConstraint),
    "is_in": (MembershipAnalyzer, GenericConstraint),
    "not_in": (MembershipAnalyzer, GenericConstraint),
    # ========================================================================
    # PATTERN & LOGIC
    # ========================================================================
    "has_pattern": (PatternAnalyzer, GenericConstraint),
    "is_legit": (LegitAnalyzer, GenericConstraint),
    "logic_check": (LogicAnalyzer, GenericConstraint),
    # ========================================================================
    # DATE CHECKS
    # ========================================================================
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
    # ========================================================================
    # TABLE-LEVEL AGGREGATIONS
    # ========================================================================
    "has_min": (AggregationAnalyzer, AggregationConstraint),
    "has_max": (AggregationAnalyzer, AggregationConstraint),
    "has_sum": (AggregationAnalyzer, AggregationConstraint),
    "has_mean": (AggregationAnalyzer, AggregationConstraint),
    "has_std": (AggregationAnalyzer, AggregationConstraint),
    "has_cardinality": (AggregationAnalyzer, AggregationConstraint),
}


def get_analyzer(check_type: str):
    """Retrieve Analyzer class for a specific check type."""
    if check_type not in VALIDATION_REGISTRY:
        return None
    return VALIDATION_REGISTRY[check_type][0]


def get_constraint(check_type: str):
    """Retrieve Constraint class for a specific check type."""
    if check_type not in VALIDATION_REGISTRY:
        return None
    return VALIDATION_REGISTRY[check_type][1]


def list_implemented() -> list:
    """List all rules supported by Dask engine."""
    return list(VALIDATION_REGISTRY.keys())
