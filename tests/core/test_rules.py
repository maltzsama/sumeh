"""
Tests for sumeh/core/rules/registry.py and rule_model.py

Covers:
- RuleRegistry: loading, get_rule, list_rules, is_rule_supported
- RuleDefinition: instantiation, validation, parsing, enrichment, methods
"""

import pytest
from datetime import date
from sumeh.core.rules.registry import RuleRegistry
from sumeh.core.rules.rule_model import RuleDefinition

# RuleRegistry


class TestRuleRegistry:

    def test_manifest_loads(self):
        rules = RuleRegistry.list_rules()
        assert len(rules) > 0

    def test_list_rules_returns_strings(self):
        rules = RuleRegistry.list_rules()
        assert all(isinstance(r, str) for r in rules)

    def test_known_rules_exist(self):
        known = [
            "is_complete",
            "are_complete",
            "is_unique",
            "are_unique",
            "is_between",
            "is_positive",
            "is_negative",
            "is_contained_in",
            "not_contained_in",
            "has_pattern",
            "is_legit",
            "is_today",
            "is_past_date",
            "is_future_date",
            "has_mean",
            "has_min",
            "has_max",
            "has_sum",
            "has_cardinality",
            "has_entropy",
            "satisfies",
            "validate_schema",
        ]
        registered = RuleRegistry.list_rules()
        for rule in known:
            assert rule in registered, f"Expected '{rule}' in registry"

    def test_get_rule_returns_dict(self):
        rule = RuleRegistry.get_rule("is_complete")
        assert isinstance(rule, dict)

    def test_get_rule_has_required_keys(self):
        rule = RuleRegistry.get_rule("is_complete")
        assert "check_type" in rule
        assert "level" in rule
        assert "category" in rule
        assert "engines" in rule

    def test_get_rule_unknown_returns_none(self):
        result = RuleRegistry.get_rule("totally_fake_rule_xyz")
        assert result is None

    def test_get_rule_level_values(self):
        row_rule = RuleRegistry.get_rule("is_complete")
        table_rule = RuleRegistry.get_rule("has_mean")
        assert row_rule["level"] == "ROW"
        assert table_rule["level"] == "TABLE"

    def test_is_rule_supported_known_engine(self):
        assert RuleRegistry.is_rule_supported("is_complete", "pandas") is True
        assert RuleRegistry.is_rule_supported("is_complete", "pyspark") is True
        assert RuleRegistry.is_rule_supported("is_complete", "bigquery") is True

    def test_is_rule_supported_unknown_engine(self):
        assert RuleRegistry.is_rule_supported("is_complete", "oracle") is False

    def test_is_rule_supported_unknown_rule(self):
        assert RuleRegistry.is_rule_supported("fake_rule", "pandas") is False

    def test_aliases_exist(self):
        """Alias rules like is_primary_key and is_in must be registered."""
        aliases = [
            "is_primary_key",
            "is_composite_key",
            "is_in",
            "not_in",
            "is_yesterday",
        ]
        for alias in aliases:
            assert RuleRegistry.get_rule(alias) is not None, f"Alias '{alias}' missing"


# RuleDefinition — instantiation


class TestRuleDefinitionInstantiation:

    def test_basic_instantiation(self):
        rule = RuleDefinition(field="email", check_type="is_complete")
        assert rule.field == "email"
        assert rule.check_type == "is_complete"
        assert rule.threshold == 1.0
        assert rule.execute is True

    def test_invalid_check_type_raises(self):
        with pytest.raises(ValueError, match="Invalid rule type"):
            RuleDefinition(field="email", check_type="fake_check_xyz")

    def test_metadata_defaults_to_empty_dict(self):
        rule = RuleDefinition(field="email", check_type="is_complete")
        assert rule.metadata == {}

    def test_level_auto_enriched_row(self):
        rule = RuleDefinition(field="email", check_type="is_complete")
        assert rule.level == "ROW"

    def test_level_auto_enriched_table(self):
        rule = RuleDefinition(field="age", check_type="has_mean", value=40.0)
        assert rule.level == "TABLE"

    def test_category_auto_enriched(self):
        rule = RuleDefinition(field="email", check_type="is_complete")
        assert rule.category == "completeness"

    def test_execute_defaults_to_true(self):
        rule = RuleDefinition(field="email", check_type="is_complete")
        assert rule.execute is True


# RuleDefinition — field parsing


class TestRuleDefinitionFieldParsing:

    def test_simple_string_field(self):
        rule = RuleDefinition(field="email", check_type="is_complete")
        assert rule.field == "email"

    def test_list_field(self):
        rule = RuleDefinition(field=["name", "email"], check_type="are_complete")
        assert rule.field == ["name", "email"]

    def test_bracket_notation_field(self):
        rule = RuleDefinition(field="[name, email]", check_type="are_complete")
        assert rule.field == ["name", "email"]

    def test_single_item_list_unwrapped(self):
        rule = RuleDefinition(field=["email"], check_type="is_complete")
        assert rule.field == "email"

    def test_quoted_field_stripped(self):
        rule = RuleDefinition(field='"email"', check_type="is_complete")
        assert rule.field == "email"


# RuleDefinition — value parsing


class TestRuleDefinitionValueParsing:

    def test_none_value(self):
        rule = RuleDefinition(field="email", check_type="is_complete", value=None)
        assert rule.value is None

    def test_float_value(self):
        rule = RuleDefinition(field="age", check_type="has_mean", value=40.0)
        assert rule.value == pytest.approx(40.0)

    def test_int_value(self):
        rule = RuleDefinition(field="category", check_type="has_cardinality", value=3)
        assert rule.value == 3

    def test_list_value(self):
        rule = RuleDefinition(
            field="status", check_type="is_contained_in", value=["active", "inactive"]
        )
        assert rule.value == ["active", "inactive"]

    def test_string_value_kept_as_string(self):
        rule = RuleDefinition(
            field="email",
            check_type="has_pattern",
            value=r"^[\w.-]+@[\w.-]+\.[a-zA-Z]{2,}$",
        )
        assert isinstance(rule.value, str)

    def test_date_value(self):
        rule = RuleDefinition(
            field="created_at", check_type="is_date_after", value=date(2020, 1, 1)
        )
        assert rule.value == date(2020, 1, 1)


# RuleDefinition — from_dict


class TestRuleDefinitionFromDict:

    def test_from_dict_basic(self):
        rule = RuleDefinition.from_dict(
            {
                "field": "email",
                "check_type": "is_complete",
                "threshold": "1.0",
            }
        )
        assert rule.field == "email"
        assert rule.check_type == "is_complete"
        assert rule.threshold == pytest.approx(1.0)

    def test_from_dict_preserves_extra_fields_as_metadata(self):
        rule = RuleDefinition.from_dict(
            {
                "field": "email",
                "check_type": "is_complete",
                "threshold": "1.0",
                "environment": "prod",
                "table_name": "users",
            }
        )
        assert rule.metadata.get("environment") == "prod"
        assert rule.metadata.get("table_name") == "users"

    def test_from_dict_execute_string_true(self):
        rule = RuleDefinition.from_dict(
            {
                "field": "email",
                "check_type": "is_complete",
                "execute": "true",
            }
        )
        assert rule.execute is True

    def test_from_dict_execute_string_false(self):
        rule = RuleDefinition.from_dict(
            {
                "field": "email",
                "check_type": "is_complete",
                "execute": "false",
            }
        )
        assert rule.execute is False

    def test_from_dict_invalid_threshold_defaults(self):
        rule = RuleDefinition.from_dict(
            {
                "field": "email",
                "check_type": "is_complete",
                "threshold": "not_a_number",
            }
        )
        assert rule.threshold == pytest.approx(1.0)

    def test_from_dict_list_value_parsed(self):
        rule = RuleDefinition.from_dict(
            {
                "field": "age",
                "check_type": "is_between",
                "value": "[18, 120]",
            }
        )
        assert rule.value == [18, 120]


# RuleDefinition — methods


class TestRuleDefinitionMethods:

    def test_get_description(self):
        rule = RuleDefinition(field="email", check_type="is_complete")
        desc = rule.get_description()
        assert isinstance(desc, str)
        assert len(desc) > 0

    def test_get_supported_engines(self):
        rule = RuleDefinition(field="email", check_type="is_complete")
        engines = rule.get_supported_engines()
        assert "pandas" in engines
        assert "pyspark" in engines

    def test_is_supported_by_engine_true(self):
        rule = RuleDefinition(field="email", check_type="is_complete")
        assert rule.is_supported_by_engine("pandas") is True

    def test_is_supported_by_engine_false(self):
        rule = RuleDefinition(field="email", check_type="is_complete")
        assert rule.is_supported_by_engine("oracle") is False

    def test_is_applicable_for_level_row(self):
        rule = RuleDefinition(field="email", check_type="is_complete")
        assert rule.is_applicable_for_level("ROW") is True
        assert rule.is_applicable_for_level("TABLE") is False

    def test_is_applicable_for_level_table(self):
        rule = RuleDefinition(field="age", check_type="has_mean", value=40.0)
        assert rule.is_applicable_for_level("TABLE") is True
        assert rule.is_applicable_for_level("ROW") is False

    def test_get_skip_reason_execute_false(self):
        rule = RuleDefinition(field="email", check_type="is_complete", execute=False)
        reason = rule.get_skip_reason("ROW", "pandas")
        assert reason == "execute=False"

    def test_get_skip_reason_wrong_level(self):
        rule = RuleDefinition(field="email", check_type="is_complete")
        reason = rule.get_skip_reason("TABLE", "pandas")
        assert reason is not None
        assert "level" in reason.lower() or "Wrong" in reason

    def test_get_skip_reason_unsupported_engine(self):
        rule = RuleDefinition(field="email", check_type="is_complete")
        reason = rule.get_skip_reason("ROW", "oracle")
        assert reason is not None
        assert "oracle" in reason

    def test_get_skip_reason_none_when_applicable(self):
        rule = RuleDefinition(field="email", check_type="is_complete")
        reason = rule.get_skip_reason("ROW", "pandas")
        assert reason is None

    def test_to_dict_roundtrip(self):
        rule = RuleDefinition(
            field="email",
            check_type="is_complete",
            threshold=0.99,
        )
        d = rule.to_dict()
        assert d["field"] == "email"
        assert d["check_type"] == "is_complete"
        assert d["threshold"] == pytest.approx(0.99)

    def test_repr_contains_field_and_check_type(self):
        rule = RuleDefinition(field="email", check_type="is_complete")
        r = repr(rule)
        assert "email" in r
        assert "is_complete" in r
