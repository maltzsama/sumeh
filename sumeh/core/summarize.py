"""Summarization functions - wrapper around existing core functionality."""

from typing import Any, List, Dict


def summarize(df: Any, rules: List[Dict], **context) -> Any:
    """
    Summarize validation results.

    Args:
        df: DataFrame with validation results
        rules: List of validation rules
        **context: Additional context (total_rows, conn, etc.)

    Returns:
        Summary DataFrame
    """
    import sumeh

    return sumeh.summarize(df, rules, **context)
