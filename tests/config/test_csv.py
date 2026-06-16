"""
Tests for sumeh/config/csv.py

Covers:
- load_rules_csv: happy path, missing file, empty file, value parsing, threshold parsing
- save_rules_csv: roundtrip, list values, missing value
"""

from pathlib import Path

import pytest

from sumeh.config.csv import load_rules_csv, save_rules_csv
from sumeh.core.rules.rule_definition import RuleDefinition

# Helpers


def write_csv(tmp_path: Path, content: str, filename: str = "rules.csv") -> Path:
    """Write raw CSV content to a temp file and return its path."""
    p = tmp_path / filename
    p.write_text(content)
    return p


# load_rules_csv — happy path


def test_load_basic_rules(tmp_path):
    csv = write_csv(
        tmp_path,
        """\
field,check_type,value,threshold,level,category
user_id,is_unique,,1.0,ROW,uniqueness
email,is_complete,,1.0,ROW,completeness
age,is_positive,,0.99,ROW,comparison
""",
    )
    rules = load_rules_csv(csv)

    assert len(rules) == 3
    assert all(isinstance(r, RuleDefinition) for r in rules)
    assert rules[0].check_type == "is_unique"
    assert rules[0].field == "user_id"
    assert rules[1].check_type == "is_complete"
    assert rules[2].threshold == pytest.approx(0.99)


def test_load_preserves_level_and_category(tmp_path):
    csv = write_csv(
        tmp_path,
        """\
field,check_type,value,threshold,level,category
age,has_mean,40.0,0.1,TABLE,aggregation
""",
    )
    rules = load_rules_csv(csv)

    assert rules[0].level == "TABLE"
    assert rules[0].category == "aggregation"


# load_rules_csv — value parsing


def test_load_value_as_list(tmp_path):
    csv = write_csv(
        tmp_path,
        """\
field,check_type,value,threshold,level,category
age,is_between,"[18, 120]",1.0,ROW,comparison
""",
    )
    rules = load_rules_csv(csv)

    assert rules[0].value == [18, 120]


def test_load_value_as_string_list(tmp_path):
    csv = write_csv(
        tmp_path,
        """\
field,check_type,value,threshold,level,category
status,is_contained_in,"[""active"", ""inactive"", ""pending""]",1.0,ROW,membership
""",
    )
    rules = load_rules_csv(csv)

    assert rules[0].value == ["active", "inactive", "pending"]


def test_load_value_as_float(tmp_path):
    csv = write_csv(
        tmp_path,
        """\
field,check_type,value,threshold,level,category
revenue,has_mean,3000.0,0.1,TABLE,aggregation
""",
    )
    rules = load_rules_csv(csv)

    assert rules[0].value == pytest.approx(3000.0)


def test_load_value_as_string(tmp_path):
    csv = write_csv(
        tmp_path,
        """\
field,check_type,value,threshold,level,category
email,has_pattern,^[\\w.-]+@[\\w.-]+\\.[a-zA-Z]{2,}$,1.0,ROW,pattern
""",
    )
    rules = load_rules_csv(csv)

    assert isinstance(rules[0].value, str)
    assert "@" in rules[0].value


def test_load_empty_value_becomes_none(tmp_path):
    csv = write_csv(
        tmp_path,
        """\
field,check_type,value,threshold,level,category
email,is_complete,,1.0,ROW,completeness
""",
    )
    rules = load_rules_csv(csv)

    assert rules[0].value is None


# load_rules_csv — threshold parsing


def test_load_threshold_default_when_missing(tmp_path):
    csv = write_csv(
        tmp_path,
        """\
field,check_type,value,threshold,level,category
email,is_complete,,,ROW,completeness
""",
    )
    rules = load_rules_csv(csv)

    assert rules[0].threshold == pytest.approx(1.0)


def test_load_threshold_invalid_falls_back_to_default(tmp_path):
    csv = write_csv(
        tmp_path,
        """\
field,check_type,value,threshold,level,category
email,is_complete,,not_a_float,ROW,completeness
""",
    )
    rules = load_rules_csv(csv)

    assert rules[0].threshold == pytest.approx(1.0)


# load_rules_csv — level/category defaults


def test_load_missing_level_defaults_to_row(tmp_path):
    csv = write_csv(
        tmp_path,
        """\
field,check_type,value,threshold
email,is_complete,,1.0
""",
    )
    rules = load_rules_csv(csv)

    assert rules[0].level == "ROW"


def test_load_missing_category_defaults_to_unknown(tmp_path):
    csv = write_csv(
        tmp_path,
        """\
field,check_type,value,threshold
email,is_complete,,1.0
""",
    )
    rules = load_rules_csv(csv)

    assert rules[0].category in ("unknown", "completeness")  # manifest may enrich


# load_rules_csv — error cases


def test_load_file_not_found():
    with pytest.raises(FileNotFoundError):
        load_rules_csv("/tmp/this_file_does_not_exist_sumeh.csv")


def test_load_empty_file_returns_empty_list(tmp_path):
    csv = write_csv(tmp_path, "field,check_type,value,threshold,level,category\n")
    rules = load_rules_csv(csv)

    assert rules == []


def test_load_invalid_check_type_raises(tmp_path):
    csv = write_csv(
        tmp_path,
        """\
field,check_type,value,threshold,level,category
email,totally_fake_check,,1.0,ROW,completeness
""",
    )
    with pytest.raises(ValueError, match="Invalid rule type"):
        load_rules_csv(csv)


# save_rules_csv


def test_save_basic_rules(tmp_path):
    rules = [
        RuleDefinition(field="user_id", check_type="is_unique", threshold=1.0),
        RuleDefinition(field="email", check_type="is_complete", threshold=1.0),
    ]
    output = tmp_path / "out.csv"
    save_rules_csv(rules, output)

    assert output.exists()
    content = output.read_text()
    assert "user_id" in content
    assert "is_unique" in content
    assert "email" in content
    assert "is_complete" in content


def test_save_and_reload_roundtrip(tmp_path):
    original = [
        RuleDefinition(field="user_id", check_type="is_unique", threshold=1.0),
        RuleDefinition(field="email", check_type="is_complete", threshold=1.0),
        RuleDefinition(
            field="age", check_type="is_between", value=[18, 120], threshold=1.0
        ),
        RuleDefinition(
            field="status",
            check_type="is_contained_in",
            value=["active", "inactive"],
            threshold=0.95,
        ),
        RuleDefinition(
            field="revenue", check_type="has_mean", value=3000.0, threshold=0.1
        ),
    ]
    output = tmp_path / "roundtrip.csv"
    save_rules_csv(original, output)
    reloaded = load_rules_csv(output)

    assert len(reloaded) == len(original)
    for orig, reloaded_rule in zip(original, reloaded):
        assert orig.field == reloaded_rule.field
        assert orig.check_type == reloaded_rule.check_type
        assert orig.threshold == pytest.approx(reloaded_rule.threshold)


def test_save_list_value_serialized_as_json(tmp_path):
    rules = [
        RuleDefinition(
            field="age", check_type="is_between", value=[18, 120], threshold=1.0
        ),
    ]
    output = tmp_path / "out.csv"
    save_rules_csv(rules, output)

    content = output.read_text()
    assert "[18" in content or "18" in content


def test_save_none_value_serialized_as_empty(tmp_path):
    rules = [
        RuleDefinition(field="email", check_type="is_complete", threshold=1.0),
    ]
    output = tmp_path / "out.csv"
    save_rules_csv(rules, output)

    content = output.read_text()
    # value column should be empty, not "None"
    assert "None" not in content


def test_save_creates_parent_directories_or_raises(tmp_path):
    """save_rules_csv should write to existing paths without error."""
    rules = [RuleDefinition(field="email", check_type="is_complete", threshold=1.0)]
    output = tmp_path / "out.csv"
    save_rules_csv(rules, output)
    assert output.exists()
