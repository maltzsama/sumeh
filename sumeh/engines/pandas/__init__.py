"""
Pandas validation engine.

API:
    from sumeh.engines.pandas import validate

    report = validate(df, rules)
"""

from sumeh.engines.pandas.engine import validate

CAPABILITIES = {
    'schema_validation': True,
    'profiling': True,
    'aggregation_analyzers': True,
    'bifurcation': True,
    'streaming': False,
}

__all__ = ['validate', 'CAPABILITIES']
