"""
Polars engine registry.

Maps check_type → (Analyzer, Constraint) for Polars implementations.
Uses same constraints as pandas (they're generic).
"""
from sumeh.engines.polars.analyzers import (
    CompletenessAnalyzer,
    MultiFieldCompletenessAnalyzer,
    UniquenessAnalyzer,
    MultiFieldUniquenessAnalyzer,
    ComparisonAnalyzer,
    BetweenAnalyzer,
    MembershipAnalyzer,
    PatternAnalyzer,
)
from sumeh.core.constraints.comparators import (
    CompletenessConstraint,
    MultiFieldCompletenessConstraint,
    UniquenessConstraint,
    GenericConstraint,
    AggregationConstraint,
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
    
    # TODO: Add date and aggregation analyzers
}


def get_analyzer(check_type: str):
    if check_type not in VALIDATION_REGISTRY:
        raise KeyError(f"'{check_type}' not implemented in polars. Available: {list(VALIDATION_REGISTRY.keys())}")
    return VALIDATION_REGISTRY[check_type][0]


def get_constraint(check_type: str):
    if check_type not in VALIDATION_REGISTRY:
        raise KeyError(f"'{check_type}' not implemented. Available: {list(VALIDATION_REGISTRY.keys())}")
    return VALIDATION_REGISTRY[check_type][1]


def list_implemented() -> list:
    return list(VALIDATION_REGISTRY.keys())