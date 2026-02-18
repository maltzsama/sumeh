"""
Validation engines.

Each engine is a namespace with a validate() function.

Usage:
    from sumeh import pandas

    report = pandas.validate(df, rules)
"""

from sumeh.engines import pandas

__all__ = ["pandas"]
