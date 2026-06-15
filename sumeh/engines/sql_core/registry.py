"""
SQLCore Registry.

Maps check_types to:
1. Analyzer (Generates the SQL AST)
2. Constraint (Validates the result against the rule)
"""

from sumeh.core.logic.comparators import (
    CompletenessConstraint,
    MultiFieldCompletenessConstraint,
    UniquenessConstraint,
    GenericConstraint,
    AggregationConstraint,
)
from sumeh.engines.sql_core.analyzers import (
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
    SatisfiesAnalyzer,
    DateAnalyzer,
    DateBetweenAnalyzer,
    DateComparisonAnalyzer,
    WeekdayAnalyzer,
    ValidateDateFormatAnalyzer,
    AggregationAnalyzer,
)

VALIDATION_REGISTRY = {
    # ------------------------------------------------------------------
    # Completeness
    # ------------------------------------------------------------------
    "is_complete":  (CompletenessAnalyzer,           CompletenessConstraint),
    "are_complete": (MultiFieldCompletenessAnalyzer,  MultiFieldCompletenessConstraint),

    # ------------------------------------------------------------------
    # Uniqueness
    # ------------------------------------------------------------------
    "is_unique":        (UniquenessAnalyzer,           UniquenessConstraint),
    "are_unique":       (MultiFieldUniquenessAnalyzer, UniquenessConstraint),
    "is_primary_key":   (UniquenessAnalyzer,           UniquenessConstraint),
    "is_composite_key": (MultiFieldUniquenessAnalyzer, UniquenessConstraint),  # alias

    # ------------------------------------------------------------------
    # Comparison (row-level)
    # ------------------------------------------------------------------
    "is_equal":               (ComparisonAnalyzer,      GenericConstraint),
    "is_greater_than":        (ComparisonAnalyzer,      GenericConstraint),
    "is_less_than":           (ComparisonAnalyzer,      GenericConstraint),
    "is_greater_or_equal_than": (ComparisonAnalyzer,    GenericConstraint),
    "is_less_or_equal_than":  (ComparisonAnalyzer,      GenericConstraint),
    "is_positive":            (ComparisonAnalyzer,      GenericConstraint),
    "is_negative":            (ComparisonAnalyzer,      GenericConstraint),
    "is_in_millions":         (ComparisonAnalyzer,      GenericConstraint),
    "is_in_billions":         (ComparisonAnalyzer,      GenericConstraint),
    "is_between":             (BetweenAnalyzer,         GenericConstraint),
    "is_equal_than":          (ColumnComparisonAnalyzer, GenericConstraint),  # manifest name
    "is_equal_to_column":     (ColumnComparisonAnalyzer, GenericConstraint),  # legacy alias

    # ------------------------------------------------------------------
    # Membership
    # ------------------------------------------------------------------
    "is_contained_in": (MembershipAnalyzer, GenericConstraint),
    "not_contained_in": (MembershipAnalyzer, GenericConstraint),
    "is_in":           (MembershipAnalyzer, GenericConstraint),
    "not_in":          (MembershipAnalyzer, GenericConstraint),

    # ------------------------------------------------------------------
    # Pattern
    # ------------------------------------------------------------------
    "has_pattern": (PatternAnalyzer, GenericConstraint),
    "is_legit":    (LegitAnalyzer,   GenericConstraint),

    # ------------------------------------------------------------------
    # Custom SQL
    # ------------------------------------------------------------------
    "satisfies": (SatisfiesAnalyzer, GenericConstraint),

    # ------------------------------------------------------------------
    # Date (absolute)
    # ------------------------------------------------------------------
    "is_today":      (DateAnalyzer, GenericConstraint),
    "is_yesterday":  (DateAnalyzer, GenericConstraint),
    "is_t_minus_1":  (DateAnalyzer, GenericConstraint),
    "is_t_minus_2":  (DateAnalyzer, GenericConstraint),
    "is_t_minus_3":  (DateAnalyzer, GenericConstraint),
    "is_past_date":  (DateAnalyzer, GenericConstraint),
    "is_future_date": (DateAnalyzer, GenericConstraint),
    "is_on_weekday": (DateAnalyzer, GenericConstraint),
    "is_on_weekend": (DateAnalyzer, GenericConstraint),

    # ------------------------------------------------------------------
    # Date (weekday — Sun=1/Mon=2…Sat=7; see WeekdayAnalyzer docstring)
    # ------------------------------------------------------------------
    "is_on_monday":    (WeekdayAnalyzer, GenericConstraint),
    "is_on_tuesday":   (WeekdayAnalyzer, GenericConstraint),
    "is_on_wednesday": (WeekdayAnalyzer, GenericConstraint),
    "is_on_thursday":  (WeekdayAnalyzer, GenericConstraint),
    "is_on_friday":    (WeekdayAnalyzer, GenericConstraint),
    "is_on_saturday":  (WeekdayAnalyzer, GenericConstraint),
    "is_on_sunday":    (WeekdayAnalyzer, GenericConstraint),

    # ------------------------------------------------------------------
    # Date (range / format)
    # ------------------------------------------------------------------
    "is_date_between":      (DateBetweenAnalyzer,         GenericConstraint),
    "is_date_after":        (DateComparisonAnalyzer,       GenericConstraint),
    "is_date_before":       (DateComparisonAnalyzer,       GenericConstraint),
    "validate_date_format": (ValidateDateFormatAnalyzer,   GenericConstraint),

    # ------------------------------------------------------------------
    # Table-level aggregations
    # ------------------------------------------------------------------
    "has_min":         (AggregationAnalyzer, AggregationConstraint),
    "has_max":         (AggregationAnalyzer, AggregationConstraint),
    "has_sum":         (AggregationAnalyzer, AggregationConstraint),
    "has_mean":        (AggregationAnalyzer, AggregationConstraint),
    "has_std":         (AggregationAnalyzer, AggregationConstraint),
    "has_cardinality": (AggregationAnalyzer, AggregationConstraint),

    # NOTE: has_entropy and has_infogain require frequency-distribution
    # subqueries and cannot be expressed as a single scalar in the current
    # single-pass SELECT model. Not registered; manifest should not list
    # sql/bigquery as supported engines for these two.
}


def get_analyzer(check_type: str):
    """Returns the Analyzer class for generating SQL AST."""
    entry = VALIDATION_REGISTRY.get(check_type)
    return entry[0] if entry else None


def get_constraint(check_type: str):
    """Returns the Constraint class for validating the result."""
    if check_type not in VALIDATION_REGISTRY:
        raise KeyError(f"Constraint not found for check_type: '{check_type}'")
    return VALIDATION_REGISTRY[check_type][1]


def list_implemented() -> list:
    return list(VALIDATION_REGISTRY.keys())