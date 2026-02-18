"""
Pandas validation engine.

API:
    from sumeh.engines.pandas import validate

    report = validate(df, rules)
"""

from sumeh.engines.pandas.engine import validate

__all__ = ["validate"]
