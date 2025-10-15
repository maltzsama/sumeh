#!/usr/bin/env python
import os
import unittest
import pandas as pd
import numpy as np
import ast
from sumeh import get_rules_config, report


class TestQualityFunction(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.mock_data_file = os.path.join(os.path.dirname(__file__), "mock", "data.csv")
        cls.mock_config_file = os.path.join(os.path.dirname(__file__), "mock", "config.csv")

        cls.mock_data = pd.read_csv(cls.mock_data_file, sep=",", header=0)
        cls.mock_data["performance"] = pd.to_numeric(cls.mock_data["performance"], errors="coerce").fillna(0)
        cls.mock_data["last_updated"] = pd.to_datetime(cls.mock_data["last_updated"], errors="coerce")
        cls.mock_data["id"] = cls.mock_data["id"].astype(str)

    @staticmethod
    def _is_null_like(value):
        if value is None:
            return True
        if isinstance(value, str) and value.strip() == "":
            return True
        if isinstance(value, float):
            return np.isnan(value)
        if isinstance(value, (pd.Series, np.ndarray)):
            return False
        return False

    @staticmethod
    def _parse_field(field_value):
        if isinstance(field_value, list):
            return field_value

        field_str = str(field_value).strip()

        if not field_str:
            return ""

        if field_str.startswith("[") and field_str.endswith("]"):
            try:
                parsed = ast.literal_eval(field_str)
                if isinstance(parsed, list):
                    return parsed if len(parsed) > 1 else parsed[0]
            except (ValueError, SyntaxError):
                pass

        return field_str

    def test_quality_with_mock_data(self):
        raw_rules = get_rules_config.csv(self.mock_config_file, delimiter=";")
        clean_rules = []

        for r in raw_rules:
            rule = dict(r.__dict__) if hasattr(r, "__dict__") else dict(r)

            rule["field"] = self._parse_field(rule.get("field", ""))

            value = rule.get("value", "")
            if self._is_null_like(value):
                rule["value"] = ""
            else:
                rule["value"] = str(value).strip()

            rule["check_type"] = str(rule.get("check_type", "")).strip()

            clean_rules.append(rule)

        result = report(df=self.mock_data, rules=clean_rules)

        print("\nSample result:\n", result.head())

        self.assertIsNotNone(result)
        self.assertFalse(result.empty, "Report result should not be empty")

        # USE AS COLUNAS CORRETAS QUE O REPORT REALMENTE RETORNA
        expected_cols = {
            "id", "timestamp", "level", "column", "rule",
            "status", "rows", "violations", "pass_rate"
        }

        missing = expected_cols - set(result.columns)
        self.assertTrue(
            expected_cols.issubset(result.columns),
            f"Missing columns: {missing}. Available: {list(result.columns)}"
        )


if __name__ == "__main__":
    unittest.main()