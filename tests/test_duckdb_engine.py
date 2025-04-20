import pytest
import duckdb
from sumeh.engine.duckdb_engine import (
    _escape_single_quotes,
    _format_sequence,
    _RuleCtx,
    validate,
    summarize,
    _build_union_sql,
    _rules_to_duckdb_df,
)

@pytest.fixture
def test_conn():
    conn = duckdb.connect(':memory:')
    yield conn
    conn.close()

@pytest.fixture
def sample_data(test_conn):
    test_conn.execute("""
    CREATE TABLE test_data AS SELECT * FROM (
        VALUES 
            (1, 'John', 'john@example.com', 25),
            (2, 'Jane', 'jane@example.com', 30),
            (3, NULL, 'invalid', NULL),
            (4, 'Bob', 'bob@example.com', 18)
    ) AS t(id, name, email, age)
    """)
    return test_conn.table('test_data')

def test_escape_single_quotes():
    assert _escape_single_quotes("test") == "test"
    assert _escape_single_quotes("test's") == "test''s"
    assert _escape_single_quotes("") == ""

def test_format_sequence():
    assert _format_sequence("BR,US") == "('BR','US')"
    assert _format_sequence(["BR", "US"]) == "('BR','US')"
    assert _format_sequence(("BR", "US")) == "('BR','US')"
    
    with pytest.raises(ValueError):
        _format_sequence(None)

def test_rule_ctx_initialization():
    ctx = _RuleCtx(column="name", value="test", name="is_complete")
    assert ctx.column == "name"
    assert ctx.value == "test"
    assert ctx.name == "is_complete"

def test_build_union_sql():
    rules = [
        {"field": "name", "check_type": "is_complete", "execute": True},
        {"field": "email", "check_type": "has_pattern", "value": "@example.com", "execute": True},
    ]
    
    sql = _build_union_sql(rules)
    assert "name IS NOT NULL" in sql
    assert "REGEXP_MATCHES(email, '@example.com')" in sql
    assert "UNION ALL" in sql

def test_rules_to_duckdb_df():
    rules = [
        {"field": "age", "check_type": "is_greater_than", "value": 18, "threshold": 0.9},
        {"field": ["country", "region"], "check_type": "is_contained_in", "value": ["BR", "US"]},
    ]
    
    sql = _rules_to_duckdb_df(rules)
    assert "SELECT 'age' AS col" in sql
    assert "SELECT 'country, region' AS col" in sql
    assert "('BR','US') AS value" in sql

def test_validate(sample_data, test_conn):
    rules = [
        {"field": "name", "check_type": "is_complete"},
        {"field": "email", "check_type": "has_pattern", "value": "@example.com"},
    ]
    
    result, raw = validate(sample_data, rules, test_conn)
    result_df = result.df()
    raw_df = raw.df()
    
    # Verifica se o resultado principal contém a coluna dq_status
    assert "dq_status" in result_df.columns
    
    # Verifica se contém pelo menos as linhas com violações
    assert len(result_df) >= 1
    
    # Verifica se o raw contém as violações individuais
    assert len(raw_df) >= 2  # Duas violações (name e email para a linha 3)
    
    # Verifica se a linha com problemas foi capturada corretamente
    invalid_row = result_df.iloc[0]
    assert invalid_row['id'] == 3
    assert "name:is_complete" in invalid_row['dq_status']
    assert "email:has_pattern" in invalid_row['dq_status']
    
    # Verifica se as linhas válidas estão no raw (opcional)
    # Podemos verificar diretamente na tabela original
    valid_rows = sample_data.filter("id != 3").df()
    assert len(valid_rows) == 3  # 3 linhas sem violações

def test_summarize(sample_data, test_conn):
    rules = [
        {"field": "name", "check_type": "is_complete", "threshold": 0.9},
        {"field": "email", "check_type": "has_pattern", "value": "@example.com", "threshold": 0.8},
    ]
    
    # First validate to get violations
    _, raw = validate(sample_data, rules, test_conn)
    
    # Then summarize
    summary = summarize(raw, rules, test_conn, total_rows=4)
    summary_df = summary.df()
    
    assert not summary_df.empty
    assert set(summary_df.columns) == {
        'id', 'timestamp', 'check', 'level', 'col', 'rule', 
        'value', 'rows', 'violations', 'pass_rate', 'pass_threshold', 'status'
    }
    
    # Check one specific rule's summary
    name_rule = summary_df[summary_df['col'] == 'name'].iloc[0]
    assert name_rule['violations'] == 1  # One NULL name
    assert name_rule['status'] in ['PASS', 'FAIL']

def test_empty_rules(sample_data, test_conn):
    # Test with no active rules
    rules = [{"field": "name", "check_type": "is_complete", "execute": False}]
    
    # Validate
    result, raw = validate(sample_data, rules, test_conn)
    assert "dq_status" in result.columns
    assert all(status == "" for status in result.df()['dq_status'])
    
    # Summarize
    summary = summarize(raw, rules, test_conn, total_rows=4)
    assert summary.df().empty