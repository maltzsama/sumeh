# tests/conftest.py - A BASE DE TUDO
"""Fixtures compartilhadas para todos os testes"""
import pytest
import pandas as pd
import dask.dataframe as dd
from datetime import datetime, timedelta
from sumeh.core.rules.rule_model import RuleDef


# ==================== FACTORIES ====================

@pytest.fixture
def rule_factory():
    """Factory para criar regras rapidamente"""
    def _create(field, check_type, value="", threshold=1.0, **kwargs):
        return RuleDef.from_dict({
            "field": field,
            "check_type": check_type,
            "value": value,
            "threshold": threshold,
            **kwargs
        })
    return _create


# ==================== SAMPLE DATA ====================

@pytest.fixture
def sample_pandas_df():
    """DataFrame Pandas com dados diversos para testes"""
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", None, "Eve"],
        "age": [25, -30, 35, 40, -45],
        "salary": [5000, 6000, 7000, 8000, 9000],
        "email": ["alice@example.com", "invalid", "charlie@test.com", "david@test.com", "eve@example.com"],
        "department": ["HR", "HR", "IT", "IT", "Finance"],
        "join_date": pd.to_datetime(["2022-01-01", "2022-06-15", "2023-01-01", "2023-06-01", "2024-01-01"]),
        "active": [True, True, False, True, True],
    })


@pytest.fixture
def sample_dask_df(sample_pandas_df):
    """Converte pandas para Dask"""
    return dd.from_pandas(sample_pandas_df, npartitions=2)


@pytest.fixture
def empty_pandas_df():
    """DataFrame vazio para testes de edge cases"""
    return pd.DataFrame(columns=["id", "name", "value"])


@pytest.fixture
def sample_dates_df():
    """DataFrame com várias situações de datas"""
    today = datetime.now().date()
    return pd.DataFrame({
        "id": range(1, 8),
        "date_col": [
            today,                           # Hoje
            today - timedelta(days=1),       # Ontem
            today - timedelta(days=2),       # T-2
            today + timedelta(days=1),       # Amanhã (futuro)
            today - timedelta(days=365),     # 1 ano atrás (passado)
            pd.NaT,                          # Nulo
            today - timedelta(days=3),       # T-3
        ],
    })

@pytest.fixture
def basic_rules(rule_factory):
    """Conjunto básico de regras para testes"""
    return [
        rule_factory("age", "is_positive"),
        rule_factory("name", "is_complete", threshold=0.8),
        rule_factory("email", "has_pattern", r"^[\w\.-]+@[\w\.-]+\.\w+$", threshold=0.9),
    ]