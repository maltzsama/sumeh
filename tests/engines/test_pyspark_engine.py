# tests/engines/test_pyspark_engine.py
"""
Quick tests for PySpark Engine
"""
import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

from sumeh.engines.pyspark_engine import (
    validate,
    validate_row_level,
    validate_table_level,
    summarize,
    is_positive,
    is_complete,
    is_unique,
    has_max,
    has_min,
    has_mean,
    has_cardinality,
)


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for testing"""
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("sumeh-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def sample_df(spark, sample_pandas_df):
    """Convert pandas sample to Spark DataFrame"""
    return spark.createDataFrame(sample_pandas_df)


@pytest.fixture
def empty_df(spark):
    """Empty Spark DataFrame"""
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("value", DoubleType(), True),
        ]
    )
    return spark.createDataFrame([], schema)


class TestRowLevelFunctions:

    def test_is_positive_finds_violations(self, sample_df, rule_factory):
        rule = rule_factory("age", "is_positive")
        result = is_positive(sample_df, rule)

        assert result.count() == 2  # -30 and -45
        assert "dq_status" in result.columns

    def test_is_complete_finds_nulls(self, sample_df, rule_factory):
        rule = rule_factory("name", "is_complete")
        result = is_complete(sample_df, rule)

        assert result.count() == 1
        assert "dq_status" in result.columns

    def test_is_unique_finds_duplicates(self, sample_df, rule_factory):
        rule = rule_factory("department", "is_unique")
        result = is_unique(sample_df, rule)

        assert result.count() > 0


class TestTableLevelFunctions:

    def test_has_max(self, sample_df, rule_factory):
        rule = rule_factory("salary", "has_max", value=10000, level="TABLE")
        result = has_max(sample_df, rule)

        assert result["status"] == "PASS"
        assert result["actual"] == 9000.0

    def test_has_min(self, sample_df, rule_factory):
        rule = rule_factory("salary", "has_min", value=4000, level="TABLE")
        result = has_min(sample_df, rule)

        assert result["status"] == "PASS"
        assert result["actual"] == 5000.0

    def test_has_mean(self, sample_df, rule_factory):
        rule = rule_factory("salary", "has_mean", value=6000, level="TABLE")
        result = has_mean(sample_df, rule)

        assert result["status"] == "PASS"
        assert result["actual"] == 7000.0

    def test_has_cardinality(self, sample_df, rule_factory):
        rule = rule_factory("department", "has_cardinality", value=3, level="TABLE")
        result = has_cardinality(sample_df, rule)

        assert result["status"] == "PASS"
        assert result["actual"] == 3.0


class TestValidateRowLevel:

    def test_returns_two_dataframes(self, spark, sample_df, rule_factory):
        rules = [rule_factory("age", "is_positive", level="ROW")]

        df_with_status, raw_violations = validate_row_level(spark, sample_df, rules)

        assert df_with_status is not None
        assert raw_violations is not None
        assert "dq_status" in df_with_status.columns

    def test_finds_violations(self, spark, sample_df, rule_factory):
        rules = [rule_factory("age", "is_positive", level="ROW")]

        df_with_status, raw_violations = validate_row_level(spark, sample_df, rules)

        assert df_with_status.count() > 0
        assert raw_violations.count() > 0


class TestValidateTableLevel:

    def test_returns_summary_dataframe(self, spark, sample_df, rule_factory):
        rules = [rule_factory("salary", "has_max", value=10000, level="TABLE")]

        summary = validate_table_level(spark, sample_df, rules)

        assert summary.count() == 1
        assert "status" in summary.columns
        assert "expected" in summary.columns
        assert "actual" in summary.columns


class TestValidate:

    def test_returns_three_values(self, spark, sample_df, rule_factory):
        rules = [rule_factory("age", "is_positive", level="ROW")]

        result = validate(spark, sample_df, rules)

        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_with_row_rules(self, spark, sample_df, rule_factory):
        rules = [
            rule_factory("age", "is_positive", level="ROW"),
            rule_factory("name", "is_complete", level="ROW"),
        ]

        df_with_status, violations, table_summary = validate(spark, sample_df, rules)

        assert violations.count() > 0
        assert "dq_status" in violations.columns

    def test_with_table_rules(self, spark, sample_df, rule_factory):
        rules = [
            rule_factory("salary", "has_max", value=10000, level="TABLE"),
            rule_factory("salary", "has_min", value=4000, level="TABLE"),
        ]

        _, _, table_summary = validate(spark, sample_df, rules)

        assert table_summary.count() == 2

    def test_with_mixed_rules(self, spark, sample_df, rule_factory):
        rules = [
            rule_factory("age", "is_positive", level="ROW"),
            rule_factory("salary", "has_max", value=10000, level="TABLE"),
        ]

        df_with_status, violations, table_summary = validate(spark, sample_df, rules)

        assert violations.count() > 0
        assert table_summary.count() == 1


class TestSummarize:

    def test_with_row_rules(self, spark, sample_df, rule_factory):
        rules = [rule_factory("age", "is_positive", level="ROW")]

        _, violations, _ = validate(spark, sample_df, rules)
        total_rows = sample_df.count()

        summary = summarize(spark, rules, total_rows, df_with_errors=violations)

        assert summary.count() == 1
        assert "pass_rate" in summary.columns
        assert "status" in summary.columns

    def test_with_table_rules(self, spark, sample_df, rule_factory):
        rules = [rule_factory("salary", "has_max", value=10000, level="TABLE")]

        _, _, table_summary = validate(spark, sample_df, rules)
        total_rows = sample_df.count()

        summary = summarize(spark, rules, total_rows, table_error=table_summary)

        assert summary.count() == 1
        row = summary.collect()[0]
        assert row["level"] == "TABLE"

    def test_with_mixed_rules(self, spark, sample_df, rule_factory):
        rules = [
            rule_factory("age", "is_positive", level="ROW"),
            rule_factory("salary", "has_max", value=10000, level="TABLE"),
        ]

        _, violations, table_summary = validate(spark, sample_df, rules)
        total_rows = sample_df.count()

        summary = summarize(
            spark,
            rules,
            total_rows,
            df_with_errors=violations,
            table_error=table_summary,
        )

        assert summary.count() == 2


class TestEdgeCases:

    def test_empty_dataframe(self, spark, empty_df, rule_factory):
        rules = [rule_factory("name", "is_complete", level="ROW")]

        df_with_status, violations, _ = validate(spark, empty_df, rules)

        assert violations.count() == 0

    def test_no_rules(self, spark, sample_df):
        df_with_status, violations, table_summary = validate(spark, sample_df, [])

        assert violations.count() == 0
        assert table_summary.count() == 0
