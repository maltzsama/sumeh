"""
Tests for sumeh/config/validate.py

Covers:
- validate_rules_schema()
- validate_rule_dict()
"""

import pandas as pd
import pytest

from sumeh.config.validate import validate_rules_schema, validate_rule_dict

# validate_rules_schema


class TestValidateRulesSchema:

    def test_minimal_valid_df(self):
        df = pd.DataFrame([{"field": "email", "check_type": "is_complete"}])
        validate_rules_schema(df)  # should not raise

    def test_extra_columns_ignored(self):
        df = pd.DataFrame(
            [
                {
                    "field": "email",
                    "check_type": "is_complete",
                    "catalog_name": "prod",
                    "created_at": "2024-01-01",
                    "created_by": "john",
                    "active": True,
                }
            ]
        )
        validate_rules_schema(df)  # should not raise

    def test_missing_field_column_raises(self):
        df = pd.DataFrame([{"check_type": "is_complete"}])
        with pytest.raises(ValueError, match="Missing required columns"):
            validate_rules_schema(df)

    def test_missing_check_type_column_raises(self):
        df = pd.DataFrame([{"field": "email"}])
        with pytest.raises(ValueError, match="Missing required columns"):
            validate_rules_schema(df)

    def test_missing_both_columns_raises(self):
        df = pd.DataFrame([{"catalog": "prod", "table": "users"}])
        with pytest.raises(ValueError) as exc_info:
            validate_rules_schema(df)
        assert "field" in str(exc_info.value) or "check_type" in str(exc_info.value)

    def test_error_message_includes_missing_columns(self):
        df = pd.DataFrame([{"catalog": "prod"}])
        with pytest.raises(ValueError) as exc_info:
            validate_rules_schema(df)
        msg = str(exc_info.value)
        assert "field" in msg or "check_type" in msg

    def test_non_dataframe_raises_type_error(self):
        with pytest.raises(TypeError):
            validate_rules_schema({"field": "email", "check_type": "is_complete"})

    def test_list_raises_type_error(self):
        with pytest.raises(TypeError):
            validate_rules_schema([{"field": "email", "check_type": "is_complete"}])

    def test_string_raises_type_error(self):
        with pytest.raises(TypeError):
            validate_rules_schema("not a dataframe")

    def test_none_raises_type_error(self):
        with pytest.raises(TypeError):
            validate_rules_schema(None)

    def test_empty_df_with_correct_columns_passes(self):
        df = pd.DataFrame(columns=["field", "check_type"])
        validate_rules_schema(df)  # should not raise

    def test_multiple_rows_valid(self):
        df = pd.DataFrame(
            [
                {"field": "email", "check_type": "is_complete"},
                {"field": "user_id", "check_type": "is_unique"},
                {"field": "age", "check_type": "is_positive"},
            ]
        )
        validate_rules_schema(df)  # should not raise

    def test_polars_df_accepted(self):
        try:
            import polars as pl

            df = pl.DataFrame({"field": ["email"], "check_type": ["is_complete"]})
            validate_rules_schema(df)  # should not raise
        except ImportError:
            pytest.skip("polars not installed")


# validate_rule_dict


class TestValidateRuleDict:

    def test_minimal_valid_rule(self):
        rule = {"field": "email", "check_type": "is_complete"}
        validate_rule_dict(rule)  # should not raise

    def test_extra_keys_ignored(self):
        rule = {
            "field": "email",
            "check_type": "is_complete",
            "threshold": 0.95,
            "value": None,
            "execute": True,
        }
        validate_rule_dict(rule)  # should not raise

    def test_missing_field_raises(self):
        rule = {"check_type": "is_complete"}
        with pytest.raises(ValueError, match="Missing required keys"):
            validate_rule_dict(rule)

    def test_missing_check_type_raises(self):
        rule = {"field": "email"}
        with pytest.raises(ValueError, match="Missing required keys"):
            validate_rule_dict(rule)

    def test_missing_both_raises(self):
        rule = {"threshold": 0.95}
        with pytest.raises(ValueError, match="Missing required keys"):
            validate_rule_dict(rule)

    def test_empty_field_raises(self):
        rule = {"field": "", "check_type": "is_complete"}
        with pytest.raises(ValueError, match="field"):
            validate_rule_dict(rule)

    def test_whitespace_field_raises(self):
        rule = {"field": "   ", "check_type": "is_complete"}
        with pytest.raises(ValueError, match="field"):
            validate_rule_dict(rule)

    def test_empty_check_type_raises(self):
        rule = {"field": "email", "check_type": ""}
        with pytest.raises(ValueError, match="check_type"):
            validate_rule_dict(rule)

    def test_whitespace_check_type_raises(self):
        rule = {"field": "email", "check_type": "   "}
        with pytest.raises(ValueError, match="check_type"):
            validate_rule_dict(rule)

    def test_non_dict_raises_type_error(self):
        with pytest.raises(TypeError):
            validate_rule_dict("not a dict")

    def test_list_raises_type_error(self):
        with pytest.raises(TypeError):
            validate_rule_dict(["field", "check_type"])

    def test_none_raises_type_error(self):
        with pytest.raises(TypeError):
            validate_rule_dict(None)

    def test_list_field_accepted(self):
        rule = {"field": ["name", "email"], "check_type": "are_complete"}
        validate_rule_dict(rule)  # list fields are valid

    def test_error_message_includes_missing_key(self):
        rule = {"field": "email"}
        with pytest.raises(ValueError) as exc_info:
            validate_rule_dict(rule)
        assert "check_type" in str(exc_info.value)
