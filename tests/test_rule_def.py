# tests/test_rule_model.py
"""
Tests for RuleDef model
"""
from datetime import datetime, date

import pytest

from sumeh.core.rules.rule_model import RuleDef


class TestRuleDefCreation:

    def test_create_from_dict_basic(self):
        data = {
            "field": "age",
            "check_type": "is_positive",
            "value": "",
            "threshold": 1.0,
        }

        rule = RuleDef.from_dict(data)

        assert rule.field == "age"
        assert rule.check_type == "is_positive"
        assert rule.threshold == 1.0
        assert rule.execute is True

    def test_create_with_invalid_check_type(self):
        data = {
            "field": "age",
            "check_type": "invalid_check",
        }

        with pytest.raises(ValueError, match="Invalid rule type"):
            RuleDef.from_dict(data)

    def test_metadata_enrichment(self):
        data = {
            "field": "age",
            "check_type": "is_positive",
        }

        rule = RuleDef.from_dict(data)

        # Should auto-populate from manifest
        assert rule.level is not None
        assert rule.category is not None

    def test_direct_instantiation(self):
        rule = RuleDef(
            field="name",
            check_type="is_complete",
            value="",
            threshold=0.9,
        )

        assert rule.field == "name"
        assert rule.check_type == "is_complete"
        assert rule.threshold == 0.9


class TestFieldParsing:

    def test_parse_single_field(self):
        data = {"field": "column_name", "check_type": "is_complete"}
        rule = RuleDef.from_dict(data)

        assert rule.field == "column_name"

    def test_parse_list_notation(self):
        data = {"field": "[col1, col2]", "check_type": "are_unique"}
        rule = RuleDef.from_dict(data)

        assert rule.field == ["col1", "col2"]

    def test_parse_quoted_list(self):
        data = {"field": "['col1', 'col2']", "check_type": "are_unique"}
        rule = RuleDef.from_dict(data)

        assert rule.field == ["col1", "col2"]

    def test_parse_comma_separated(self):
        data = {"field": "col1, col2, col3", "check_type": "are_unique"}
        rule = RuleDef.from_dict(data)

        assert rule.field == ["col1", "col2", "col3"]

    def test_parse_single_item_list_returns_string(self):
        data = {"field": "[single_col]", "check_type": "is_complete"}
        rule = RuleDef.from_dict(data)

        assert rule.field == "single_col"

    def test_parse_already_list(self):
        data = {"field": ["col1", "col2"], "check_type": "are_unique"}
        rule = RuleDef.from_dict(data)

        assert rule.field == ["col1", "col2"]


class TestValueParsing:

    def test_parse_empty_value(self):
        data = {"field": "age", "check_type": "is_positive", "value": ""}
        rule = RuleDef.from_dict(data)

        assert rule.value is None

    def test_parse_integer(self):
        data = {"field": "age", "check_type": "is_greater_than", "value": "18"}
        rule = RuleDef.from_dict(data)

        assert rule.value == 18
        assert isinstance(rule.value, int)

    def test_parse_float(self):
        data = {"field": "price", "check_type": "is_greater_than", "value": "19.99"}
        rule = RuleDef.from_dict(data)

        assert rule.value == 19.99
        assert isinstance(rule.value, float)

    def test_parse_date_iso(self):
        data = {
            "field": "date_col",
            "check_type": "is_date_after",
            "value": "2025-05-16",
        }
        rule = RuleDef.from_dict(data)

        assert isinstance(rule.value, date)
        assert rule.value == date(2025, 5, 16)

    def test_parse_list_of_strings(self):
        data = {
            "field": "status",
            "check_type": "is_contained_in",
            "value": "[active, pending]",
        }
        rule = RuleDef.from_dict(data)

        assert rule.value == ["active", "pending"]

    def test_parse_list_of_numbers(self):
        data = {"field": "age", "check_type": "is_between", "value": "[18, 65]"}
        rule = RuleDef.from_dict(data)

        assert rule.value == [18, 65]

    def test_parse_regex_pattern(self):
        data = {
            "field": "email",
            "check_type": "has_pattern",
            "value": r"^\w+@\w+\.\w+$",
        }
        rule = RuleDef.from_dict(data)

        assert isinstance(rule.value, str)
        assert rule.value == r"^\w+@\w+\.\w+$"


class TestThresholdParsing:

    def test_default_threshold(self):
        data = {"field": "age", "check_type": "is_positive"}
        rule = RuleDef.from_dict(data)

        assert rule.threshold == 1.0

    def test_parse_threshold_float(self):
        data = {"field": "age", "check_type": "is_positive", "threshold": "0.95"}
        rule = RuleDef.from_dict(data)

        assert rule.threshold == 0.95

    def test_parse_threshold_int(self):
        data = {"field": "age", "check_type": "is_positive", "threshold": 1}
        rule = RuleDef.from_dict(data)

        assert rule.threshold == 1.0


class TestExecuteFlag:

    def test_default_execute_true(self):
        data = {"field": "age", "check_type": "is_positive"}
        rule = RuleDef.from_dict(data)

        assert rule.execute is True

    def test_parse_execute_string(self):
        data = {"field": "age", "check_type": "is_positive", "execute": "true"}
        rule = RuleDef.from_dict(data)

        assert rule.execute is True

    def test_parse_execute_false(self):
        data = {"field": "age", "check_type": "is_positive", "execute": False}
        rule = RuleDef.from_dict(data)

        assert rule.execute is False


class TestEngineValidation:

    def test_supported_engine(self):
        data = {"field": "age", "check_type": "is_positive"}
        rule = RuleDef.from_dict(data, engine="pandas")

        assert rule.is_supported_by_engine("pandas")

    def test_get_supported_engines(self):
        data = {"field": "age", "check_type": "is_positive"}
        rule = RuleDef.from_dict(data)

        engines = rule.get_supported_engines()
        assert isinstance(engines, list)
        assert len(engines) > 0

    def test_unsupported_engine_warning(self):
        data = {"field": "age", "check_type": "is_positive"}

        with pytest.warns(UserWarning, match="not supported"):
            rule = RuleDef.from_dict(data, engine="unsupported_engine")

        assert rule.execute is False


class TestLevelAndCategory:

    def test_is_applicable_for_level(self):
        data = {"field": "age", "check_type": "is_positive"}
        rule = RuleDef.from_dict(data)

        # is_positive should be ROW level
        assert rule.is_applicable_for_level("ROW")
        assert not rule.is_applicable_for_level("TABLE")

    def test_get_skip_reason_wrong_level(self):
        data = {"field": "age", "check_type": "is_positive"}
        rule = RuleDef.from_dict(data)

        reason = rule.get_skip_reason("TABLE", "pandas")
        assert reason is not None
        assert "Wrong level" in reason

    def test_get_skip_reason_execute_false(self):
        data = {"field": "age", "check_type": "is_positive", "execute": False}
        rule = RuleDef.from_dict(data)

        reason = rule.get_skip_reason("ROW", "pandas")
        assert reason == "execute=False"

    def test_get_skip_reason_none_when_applicable(self):
        data = {"field": "age", "check_type": "is_positive"}
        rule = RuleDef.from_dict(data)

        reason = rule.get_skip_reason("ROW", "pandas")
        assert reason is None


class TestHelperMethods:

    def test_to_dict(self):
        data = {"field": "age", "check_type": "is_positive"}
        rule = RuleDef.from_dict(data)

        result = rule.to_dict()

        assert isinstance(result, dict)
        assert result["field"] == "age"
        assert result["check_type"] == "is_positive"

    def test_get_description(self):
        data = {"field": "age", "check_type": "is_positive"}
        rule = RuleDef.from_dict(data)

        description = rule.get_description()
        assert isinstance(description, str)

    def test_repr(self):
        data = {"field": "age", "check_type": "is_positive"}
        rule = RuleDef.from_dict(data)

        repr_str = repr(rule)
        assert "RuleDef" in repr_str
        assert "age" in repr_str
        assert "is_positive" in repr_str


class TestTimestamps:

    def test_auto_created_at(self):
        data = {"field": "age", "check_type": "is_positive"}
        rule = RuleDef.from_dict(data)

        assert rule.created_at is not None
        assert isinstance(rule.created_at, datetime)

    def test_parse_timestamp_string(self):
        data = {
            "field": "age",
            "check_type": "is_positive",
            "created_at": "2025-05-16T10:00:00",
        }
        rule = RuleDef.from_dict(data)

        assert rule.created_at is not None
        assert isinstance(rule.created_at, datetime)


class TestEdgeCases:

    def test_empty_field_string(self):
        data = {"field": "", "check_type": "is_positive"}
        rule = RuleDef.from_dict(data)

        assert rule.field == ""

    def test_null_value(self):
        data = {"field": "age", "check_type": "is_positive", "value": "NULL"}
        rule = RuleDef.from_dict(data)

        assert rule.value is None

    def test_complex_list_parsing(self):
        data = {
            "field": "status",
            "check_type": "is_contained_in",
            "value": "['active', 'pending', 'closed']",
        }
        rule = RuleDef.from_dict(data)

        assert rule.value == ["active", "pending", "closed"]

    def test_mixed_type_list(self):
        data = {
            "field": "codes",
            "check_type": "is_contained_in",
            "value": "[1, 'a', 3.5]",
        }
        rule = RuleDef.from_dict(data)

        assert rule.value == [1, "a", 3.5]
