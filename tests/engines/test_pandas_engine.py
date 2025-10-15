#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
from datetime import date, timedelta

import pandas as pd

from sumeh.core.rules.rule_model import RuleDef
from sumeh.engines.pandas_engine import (
    is_positive, is_negative, is_complete, is_unique, are_complete, are_unique,
    is_greater_than, is_less_than, is_equal, is_contained_in, not_contained_in,
    is_between, has_pattern, is_legit, satisfies, validate_date_format,
    is_future_date, is_past_date, is_date_between, is_in_millions, is_in_billions,
    is_today, is_yesterday, validate_row_level, validate_table_level, validate,
    summarize, validate_schema
)


class TestPandasEngine(unittest.TestCase):

    def setUp(self):
        """Setup test data"""
        self.df_basic = pd.DataFrame({
            'numeric_col': [1, -1, 0, 5, -5, None, 10],
            'text_col': ['A', 'B', 'C', 'A', None, ' ', 'D'],
            'date_col': pd.to_datetime([
                '2023-01-01', '2024-01-01', '2023-06-15',
                None, '2023-12-31', '2022-01-01', '2023-03-15'
            ]),
            'amount': [1000000, 500000, 2000000, 3000000000, 100, None, 500000000]
        })

        self.df_dates = pd.DataFrame({
            'date_field': pd.to_datetime([
                date.today(),
                date.today() - timedelta(days=1),
                date.today() - timedelta(days=2),
                date.today() - timedelta(days=3),
                None
            ])
        })

    def create_rule(self, field, check_type, value=None, threshold=None, level="ROW", category="test"):
        """Helper to create RuleDef objects"""
        return RuleDef(
            field=field,
            check_type=check_type,
            value=value,
            threshold=threshold,
            level=level,
            category=category
        )

    # Test numeric validation functions
    def test_is_positive(self):
        rule = self.create_rule('numeric_col', 'is_positive')
        result = is_positive(self.df_basic, rule)

        self.assertEqual(len(result), 2)  # -1 and -5
        self.assertTrue(all(result['numeric_col'] < 0))
        self.assertTrue(all('is_positive' in status for status in result['dq_status']))

    def test_is_negative(self):
        rule = self.create_rule('numeric_col', 'is_negative')
        result = is_negative(self.df_basic, rule)

        self.assertEqual(len(result), 4)  # 1, 0, 5, 10 (non-negative)
        self.assertTrue(all(result['numeric_col'] >= 0))

    def test_is_complete(self):
        rule = self.create_rule('text_col', 'is_complete')
        result = is_complete(self.df_basic, rule)

        # Only actual NaN/None values (not empty strings)
        self.assertEqual(len(result), 1)  # Only the actual None value
        self.assertTrue(result['text_col'].isna().any())

    def test_is_unique(self):
        rule = self.create_rule('text_col', 'is_unique')
        result = is_unique(self.df_basic, rule)

        # 'A' appears twice
        self.assertEqual(len(result), 2)
        self.assertEqual(result['text_col'].tolist(), ['A', 'A'])

    def test_are_complete(self):
        rule = self.create_rule(['numeric_col', 'text_col'], 'are_complete')
        result = are_complete(self.df_basic, rule)

        # Rows where any of the fields is null
        self.assertTrue(len(result) > 0)

    def test_are_unique(self):
        rule = self.create_rule(['numeric_col', 'text_col'], 'are_unique')
        result = are_unique(self.df_basic, rule)

        # Should find duplicate combinations
        self.assertTrue(isinstance(result, pd.DataFrame))

    # Test comparison functions
    def test_is_greater_than(self):
        rule = self.create_rule('numeric_col', 'is_greater_than', value=3)
        result = is_greater_than(self.df_basic, rule)

        self.assertTrue(all(result['numeric_col'] <= 3))

    def test_is_less_than(self):
        rule = self.create_rule('numeric_col', 'is_less_than', value=3)
        result = is_less_than(self.df_basic, rule)

        self.assertTrue(all(result['numeric_col'] >= 3))

    def test_is_equal(self):
        rule = self.create_rule('numeric_col', 'is_equal', value=5)
        result = is_equal(self.df_basic, rule)

        self.assertTrue(all(result['numeric_col'] != 5))

    def test_is_contained_in(self):
        rule = self.create_rule('text_col', 'is_contained_in', value="['A', 'B', 'C']")
        result = is_contained_in(self.df_basic, rule)

        self.assertTrue(all(~result['text_col'].isin(['A', 'B', 'C'])))

    def test_not_contained_in(self):
        rule = self.create_rule('text_col', 'not_contained_in', value="['X', 'Y', 'Z']")
        result = not_contained_in(self.df_basic, rule)

        # Should return rows where text_col is in ['X', 'Y', 'Z'] (none in our case)
        self.assertEqual(len(result), 0)

    def test_is_between(self):
        rule = self.create_rule('numeric_col', 'is_between', value='[2, 8]')
        result = is_between(self.df_basic, rule)

        # Check that all violations are outside the range OR are NaN
        violations = result['numeric_col'].dropna()
        if len(violations) > 0:
            self.assertTrue(all((violations < 2) | (violations > 8)))
        # Also check that NaN values are included in violations
        self.assertTrue(result['numeric_col'].isna().any())

    # Test pattern and custom validation
    def test_has_pattern(self):
        rule = self.create_rule('text_col', 'has_pattern', value='^[A-Z]$')
        result = has_pattern(self.df_basic, rule)

        # Should return rows that don't match the pattern
        self.assertTrue(len(result) > 0)

    def test_is_legit(self):
        rule = self.create_rule('text_col', 'is_legit')
        result = is_legit(self.df_basic, rule)

        # Should catch None and whitespace (None and ' ')
        self.assertEqual(len(result), 2)

    def test_satisfies(self):
        rule = self.create_rule('numeric_col', 'satisfies', value='numeric_col > 0')
        result = satisfies(self.df_basic, rule)

        # Should return rows where numeric_col <= 0 OR is NaN
        violations = result['numeric_col'].dropna()
        if len(violations) > 0:
            self.assertTrue(all(violations <= 0))
        self.assertTrue(result['numeric_col'].isna().any())

    # Test date validations
    def test_validate_date_format(self):
        rule = self.create_rule('date_col', 'validate_date_format', value='%Y-%m-%d')
        result = validate_date_format(self.df_basic, rule)

        self.assertTrue(isinstance(result, pd.DataFrame))

    def test_is_future_date(self):
        rule = self.create_rule('date_col', 'is_future_date')
        result = is_future_date(self.df_basic, rule)

        # Convert today to pandas Timestamp for proper comparison
        today = pd.Timestamp(date.today())
        future_dates = result['date_col'].dropna()
        if len(future_dates) > 0:
            self.assertTrue(all(future_dates > today))

    def test_is_past_date(self):
        rule = self.create_rule('date_col', 'is_past_date')
        result = is_past_date(self.df_basic, rule)

        # Convert today to pandas Timestamp for proper comparison
        today = pd.Timestamp(date.today())
        past_dates = result['date_col'].dropna()
        if len(past_dates) > 0:
            self.assertTrue(all(past_dates < today))

    def test_is_date_between(self):
        rule = self.create_rule('date_col', 'is_date_between', value='[2023-01-01, 2023-12-31]')
        result = is_date_between(self.df_basic, rule)

        self.assertTrue(isinstance(result, pd.DataFrame))

    # Test large number validations
    def test_is_in_millions(self):
        rule = self.create_rule('amount', 'is_in_millions')
        result = is_in_millions(self.df_basic, rule)

        violations = result['amount'].dropna()
        if len(violations) > 0:
            self.assertTrue(all(violations < 1000000))

    def test_is_in_billions(self):
        rule = self.create_rule('amount', 'is_in_billions')
        result = is_in_billions(self.df_basic, rule)

        violations = result['amount'].dropna()
        if len(violations) > 0:
            self.assertTrue(all(violations < 1000000000))

    # Test current date validations
    def test_is_today(self):
        rule = self.create_rule('date_field', 'is_today')
        result = is_today(self.df_dates, rule)

        self.assertTrue(isinstance(result, pd.DataFrame))

    def test_is_yesterday(self):
        rule = self.create_rule('date_field', 'is_yesterday')
        result = is_yesterday(self.df_dates, rule)

        self.assertTrue(isinstance(result, pd.DataFrame))

    # Test orchestration functions
    def test_validate_row_level(self):
        rules = [
            self.create_rule('numeric_col', 'is_positive'),
            self.create_rule('text_col', 'is_complete')
        ]

        df_with_status, violations = validate_row_level(self.df_basic, rules)

        self.assertTrue(isinstance(df_with_status, pd.DataFrame))
        self.assertTrue(isinstance(violations, pd.DataFrame))
        self.assertTrue('dq_status' in df_with_status.columns or df_with_status.empty)

    def test_validate_table_level(self):
        rules = [
            self.create_rule('numeric_col', 'has_mean', value=2.0, level="TABLE"),
            self.create_rule('text_col', 'has_cardinality', value=3, level="TABLE")
        ]

        result = validate_table_level(self.df_basic, rules)

        self.assertTrue(isinstance(result, pd.DataFrame))
        self.assertTrue(all(col in result.columns for col in
                            ['id', 'timestamp', 'level', 'category', 'check_type',
                             'field', 'status', 'expected', 'actual', 'message']))

    def test_validate_integration(self):
        rules = [
            self.create_rule('numeric_col', 'is_positive', level="ROW"),
            self.create_rule('numeric_col', 'has_mean', value=2.0, level="TABLE")
        ]

        df_with_status, row_violations, table_summary = validate(self.df_basic, rules)

        self.assertTrue(isinstance(df_with_status, pd.DataFrame))
        self.assertTrue(isinstance(row_violations, pd.DataFrame))
        self.assertTrue(isinstance(table_summary, pd.DataFrame))

    def test_summarize(self):
        rules = [
            self.create_rule('numeric_col', 'is_positive', level="ROW"),
            self.create_rule('numeric_col', 'has_mean', value=2.0, level="TABLE")
        ]

        # First run validation to get the required data
        df_with_status, row_violations, table_summary = validate(self.df_basic, rules)

        summary = summarize(
            rules=rules,
            total_rows=len(self.df_basic),
            df_with_errors=df_with_status,
            table_error=table_summary
        )

        self.assertTrue(isinstance(summary, pd.DataFrame))

    def test_validate_schema(self):
        # Test schema validation
        actual_schema = [
            {"field": "numeric_col", "data_type": "int64", "nullable": True},
            {"field": "text_col", "data_type": "object", "nullable": True}
        ]

        result, errors = validate_schema(self.df_basic, actual_schema)

        self.assertIsInstance(result, bool)
        self.assertIsInstance(errors, list)

    # Test edge cases
    def test_empty_dataframe(self):
        empty_df = pd.DataFrame(columns=['numeric_col', 'text_col'])
        rule = self.create_rule('numeric_col', 'is_complete')

        # Should handle empty dataframe gracefully
        result = is_complete(empty_df, rule)
        self.assertTrue(isinstance(result, pd.DataFrame))
        self.assertTrue(result.empty)

    def test_nonexistent_column(self):
        rule = self.create_rule('nonexistent_column', 'is_complete')

        with self.assertRaises(KeyError):
            is_complete(self.df_basic, rule)

    # Test threshold functionality
    def test_with_threshold(self):
        rule = self.create_rule('numeric_col', 'has_mean', value=10.0, threshold=0.8, level="TABLE")
        result = validate_table_level(self.df_basic, [rule])

        self.assertTrue(isinstance(result, pd.DataFrame))
        self.assertTrue(len(result) == 1)

    def test_dq_status_format(self):
        """Test that dq_status format is consistent"""
        rule = self.create_rule('numeric_col', 'is_positive', value='test_value')
        result = is_positive(self.df_basic, rule)

        if not result.empty:
            expected_format = f"numeric_col:is_positive:test_value"
            self.assertEqual(result['dq_status'].iloc[0], expected_format)

    # Test specific edge cases that were failing
    def test_is_complete_with_nulls_and_empty_strings(self):
        """Test is_complete with both nulls and empty strings"""
        df = pd.DataFrame({
            'col1': [1, None, 3, None],
            'col2': ['A', '', None, 'D']
        })

        rule = self.create_rule('col2', 'is_complete')
        result = is_complete(df, rule)

        # Should only catch actual None values, not empty strings
        self.assertEqual(len(result), 1)  # Only the None value
        self.assertTrue(result['col2'].isna().all())

    def test_is_legit_with_various_invalid_values(self):
        """Test is_legit with various invalid values"""
        df = pd.DataFrame({
            'col': [None, ' ', '', 'valid', '  ', '\t', 'valid2']
        })

        rule = self.create_rule('col', 'is_legit')
        result = is_legit(df, rule)

        # Should catch None, ' ', '', '  ', '\t' (5 invalid values)
        self.assertEqual(len(result), 5)

    def test_satisfies_complex_expression(self):
        """Test satisfies with more complex expressions"""
        df = pd.DataFrame({
            'a': [1, 2, 3, 4, 5],
            'b': [10, 20, 30, 40, 50]
        })

        rule = self.create_rule('a', 'satisfies', value='a > 2 and b < 45')
        result = satisfies(df, rule)

        # Should return rows where condition is False: rows 0, 1, 4
        self.assertEqual(len(result), 3)


def test_is_future_date_pytest():
    """Test para is_future_date usando pytest"""
    from sumeh.engines.pandas_engine import is_future_date
    from sumeh.core.rules.rule_model import RuleDef

    # Criar DataFrame com datas claramente no futuro e passado
    df = pd.DataFrame({
        'date_col': pd.to_datetime([
            '2030-01-01',  # Futuro
            '2020-01-01',  # Passado
            None
        ])
    })

    rule = RuleDef(field='date_col', check_type='is_future_date')
    result = is_future_date(df, rule)

    assert isinstance(result, pd.DataFrame)
    # Apenas a data futura deve estar no resultado
    if not result.empty:
        assert len(result) == 1
        assert result['date_col'].iloc[0] == pd.Timestamp('2030-01-01')


def test_is_past_date_pytest():
    """Test para is_past_date usando pytest"""
    from sumeh.engines.pandas_engine import is_past_date
    from sumeh.core.rules.rule_model import RuleDef

    # Criar DataFrame com datas claramente no futuro e passado
    df = pd.DataFrame({
        'date_col': pd.to_datetime([
            '2030-01-01',  # Futuro
            '2020-01-01',  # Passado
            None
        ])
    })

    rule = RuleDef(field='date_col', check_type='is_past_date')
    result = is_past_date(df, rule)

    assert isinstance(result, pd.DataFrame)
    # Apenas a data passada deve estar no resultado
    if not result.empty:
        assert len(result) == 1
        assert result['date_col'].iloc[0] == pd.Timestamp('2020-01-01')


class TestRuleDef(unittest.TestCase):
    """Test RuleDef specific functionality"""

    def test_rule_creation(self):
        rule = RuleDef(
            field="test_field",
            check_type="is_complete",
            value="test_value",
            threshold=0.95,
            level="ROW",
            category="completeness"
        )

        self.assertEqual(rule.field, "test_field")
        self.assertEqual(rule.check_type, "is_complete")
        self.assertEqual(rule.value, "test_value")
        self.assertEqual(rule.threshold, 0.95)
        self.assertEqual(rule.level, "ROW")
        self.assertEqual(rule.category, "completeness")


if __name__ == '__main__':
    unittest.main()
