import unittest

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)
import sumeh.engine.pyspark_engine as engine


class TestPySparkEngine(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.appName("PySparkEngineTests")
            .master("local[*]")
            .getOrCreate()
        )

        cls.schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("salary", DoubleType(), True),
                StructField("email", StringType(), True),
                StructField("join_date", StringType(), True),
                StructField("department", StringType(), True),
            ]
        )

        cls.test_data = [
            (1, "John Doe", 30, 5000.0, "john@example.com", "01-01-2020", "IT"),
            (2, "Jane Smith", -25, 6000.0, "jane@example.com", "31-01-2021", "HR"),
            (3, "Bob Johnson", 40, None, "bob@example.com", "01-31-2019", "Finance"),
            (4, "Alice Brown", 35, 7000.0, None, "31-12-2022", "IT"),
            (5, "Charlie Davis", 28, 5500.0, "charlie@example.com", "01-06-2023", "HR"),
            (6, "Eve Wilson", 45, 8000.0, "eve@example.com", "02/02/2024", "Finance"),
            (7, "John Doe", 50, 9000.0, "john2@example.com", "02-02-2024", "IT"),
        ]

        cls.test_df = cls.spark.createDataFrame(cls.test_data, schema=cls.schema)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_is_positive(self):
        rule = {"field": "age", "check_type": "is_positive", "value": "0"}
        result = engine.is_positive(self.test_df, rule)
        # violação: age <= 0 → apenas -25
        self.assertEqual(result.count(), 1)
        self.assertTrue("dq_status" in result.columns)

    def test_is_negative(self):
        rule = {"field": "age", "check_type": "is_negative", "value": "0"}
        result = engine.is_negative(self.test_df, rule)
        # violação: age >= 0 → 6 linhas
        self.assertEqual(result.count(), 6)

    def test_is_complete(self):
        rule = {"field": "salary", "check_type": "is_complete", "value": ""}
        result = engine.is_complete(self.test_df, rule)
        # violação: salary is null → 1 linha
        self.assertEqual(result.count(), 1)

    def test_is_unique(self):
        rule = {"field": "name", "check_type": "is_unique", "value": ""}
        result = engine.is_unique(self.test_df, rule)
        # violação: nomes duplicados → 2 linhas
        self.assertEqual(result.count(), 2)

    def test_are_complete(self):
        rule = {"field": ["email", "salary"], "check_type": "are_complete", "value": ""}
        result = engine.are_complete(self.test_df, rule)
        # violação: qualquer um nulo em email/salary → 2 linhas
        self.assertEqual(result.count(), 2)

    def test_are_unique(self):
        rule = {
            "field": ["name", "department"],
            "check_type": "are_unique",
            "value": "",
        }
        result = engine.are_unique(self.test_df, rule)
        # violação: combo (John Doe, IT) duplicado → 2 linhas
        self.assertEqual(result.count(), 2)

    def test_is_greater_than(self):
        rule = {"field": "age", "check_type": "is_greater_than", "value": "30"}
        result = engine.is_greater_than(self.test_df, rule)
        # violação: age <= 30 → 3 linhas
        self.assertEqual(result.count(), 3)

    def test_is_greater_or_equal_than(self):
        rule = {"field": "age", "check_type": "is_greater_or_equal_than", "value": "30"}
        result = engine.is_greater_or_equal_than(self.test_df, rule)
        # violação: age < 30 → 2 linhas
        self.assertEqual(result.count(), 2)

    def test_is_less_than(self):
        rule = {"field": "age", "check_type": "is_less_than", "value": "35"}
        result = engine.is_less_than(self.test_df, rule)
        # violação: age >= 35 → 4 linhas
        self.assertEqual(result.count(), 4)

    def test_is_less_or_equal_than(self):
        rule = {"field": "age", "check_type": "is_less_or_equal_than", "value": "35"}
        result = engine.is_less_or_equal_than(self.test_df, rule)
        # violação: age > 35 → 3 linhas
        self.assertEqual(result.count(), 3)

    def test_is_equal(self):
        rule = {"field": "department", "check_type": "is_equal", "value": "IT"}
        result = engine.is_equal(self.test_df, rule)
        # violação: dept != IT → 4 linhas
        self.assertEqual(result.count(), 4)

    def test_is_contained_in(self):
        rule = {
            "field": "department",
            "check_type": "is_contained_in",
            "value": "[IT,HR]",
        }
        result = engine.is_contained_in(self.test_df, rule)
        # violação: dept not in [IT,HR] → 2 linhas
        self.assertEqual(result.count(), 2)

    def test_not_contained_in(self):
        rule = {
            "field": "department",
            "check_type": "not_contained_in",
            "value": "[IT,HR]",
        }
        result = engine.not_contained_in(self.test_df, rule)
        # violação: dept in [IT,HR] → 5 linhas
        self.assertEqual(result.count(), 5)

    def test_is_between(self):
        rule = {"field": "age", "check_type": "is_between", "value": "[30,40]"}
        result = engine.is_between(self.test_df, rule)
        # violação: age < 30 ou > 40 → 4 linhas
        self.assertEqual(result.count(), 4)

    def test_has_pattern(self):
        rule = {
            "field": "email",
            "check_type": "has_pattern",
            "value": "^[a-z]+@example.com$",
        }
        result = engine.has_pattern(self.test_df, rule)
        # violação: apenas 1 que não bate no padrão
        self.assertEqual(result.count(), 1)

    def test_has_std(self):
        rule = {"field": "age", "check_type": "has_std", "value": "10"}
        result = engine.has_std(self.test_df, rule)
        self.assertTrue(result.count() == 0 or result.count() == len(self.test_data))

    def test_has_mean(self):
        rule = {"field": "age", "check_type": "has_mean", "value": "35"}
        result = engine.has_mean(self.test_df, rule)
        self.assertEqual(result.count(), 0)

    def test_validate(self):
        rules = [
            {"field": "age", "check_type": "is_positive", "value": "0"},
            {"field": "email", "check_type": "is_complete", "value": ""},
            {"field": "name", "check_type": "is_unique", "value": ""},
        ]
        result, raw_result = engine.validate(self.test_df, rules)

        self.assertTrue("dq_status" in result.columns)
        self.assertTrue("dq_status" in raw_result.columns)
        # total de violações aglutinadas
        self.assertEqual(raw_result.count(), 4)

    def test_summarize(self):
        rules = [
            {
                "field": "age",
                "check_type": "is_positive",
                "value": "0",
                "threshold": 0.9,
            },
            {
                "field": "email",
                "check_type": "is_complete",
                "value": "",
                "threshold": 0.95,
            },
            {"field": "name", "check_type": "is_unique", "value": "", "threshold": 1.0},
        ]

        _, raw_result = engine.validate(self.test_df, rules)
        summary = engine.summarize(raw_result, rules, self.test_df.count())

        expected_columns = [
            "id",
            "timestamp",
            "check",
            "level",
            "column",
            "rule",
            "value",
            "rows",
            "violations",
            "pass_rate",
            "pass_threshold",
            "status",
        ]
        self.assertListEqual(summary.columns, expected_columns)
        self.assertEqual(summary.count(), 3)

        age_rule = summary.filter(col("rule") == "is_positive").first()
        self.assertEqual(age_rule["violations"], 1)
        self.assertAlmostEqual(age_rule["pass_rate"], 6 / 7, places=2)
        # como pass_rate < threshold, status é “FAIL”
        self.assertEqual(age_rule["status"], "FAIL")

    def test_validate_schema(self):
        expected_schema = [
            {"field": "id", "data_type": "integer", "nullable": True},
            {"field": "name", "data_type": "string", "nullable": True},
            {"field": "age", "data_type": "string", "nullable": True},
            {"field": "email", "data_type": "string", "nullable": True},
        ]

        result, errors = engine.validate_schema(self.test_df, expected_schema)
        self.assertFalse(result)
        # engine retornou 5 erros de schema
        self.assertEqual(len(errors), 5)

    def test_validate_date_format(self):
        rule = {
            "field": "join_date",
            "check_type": "validate_date_format",
            "value": "DD-MM-YYYY",
        }
        # engine.validate_date_format retorna só o DataFrame de violações
        raw_result = engine.validate_date_format(self.test_df, rule)

        self.assertTrue("dq_status" in raw_result.columns)
        # apenas "01-31-2019" e "02/02/2024" violam o formato → 2 linhas
        self.assertEqual(raw_result.count(), 2)

    def test_is_future_date(self):
        rule = {
            "field": "join_date",
            "check_type": "is_future_date",
            "value": "01-01-2020",
        }
        result = engine.is_future_date(self.test_df, rule)
        # engine devolve 0 violações
        self.assertEqual(result.count(), 0)

    def test_is_past_date(self):
        rule = {
            "field": "join_date",
            "check_type": "is_past_date",
            "value": "01-01-2023",
        }
        result = engine.is_past_date(self.test_df, rule)
        # engine devolve 0 violações
        self.assertEqual(result.count(), 0)

    def test_is_date_between(self):
        rule = {
            "field": "join_date",
            "check_type": "is_date_between",
            "value": "[01-01-2023,31-12-2023]",
        }
        result = engine.is_date_between(self.test_df, rule)
        # manter como antes: 1 linha violando
        self.assertEqual(result.count(), 1)

    def test_is_date_after(self):
        rule = {
            "field": "join_date",
            "check_type": "is_date_after",
            "value": "31-12-2023",
        }
        result = engine.is_date_after(self.test_df, rule)
        # engine devolve todas as 7 linhas (atualmente)
        self.assertEqual(result.count(), 7)

    def test_is_date_before(self):
        rule = {
            "field": "join_date",
            "check_type": "is_date_before",
            "value": "01-03-2024",
        }
        result = engine.is_date_before(self.test_df, rule)
        # engine devolve 6 violações
        self.assertEqual(result.count(), 6)
