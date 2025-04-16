#!/usr/bin/env python

import unittest
import os
from pyspark.sql import SparkSession
from sumeh.engine.pyspark_engine import quality_checker
from sumeh import get_config_from_csv
from pyspark.sql.functions import col


class TestQualityFunction(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[2]").appName("testes").getOrCreate()

        cls.mock_data_file = os.path.join(os.path.dirname(__file__), "mock", "data.csv")
        cls.mock_config_file = get_config_from_csv(file_path=os.path.join(
            os.path.dirname(__file__), "mock", "config.csv"
        ), delimiter=";")

        cls.mock_data = cls.spark.read.csv(cls.mock_data_file, header=True)
        cls.mock_data = (
            cls.mock_data.withColumn("performance", col("performance").cast("float"))
                .withColumn("age", col("age").cast("integer"))
                .withColumn("salary", col("salary").cast("float"))
                .withColumn("last_updated", col("last_updated").cast("date"))
        )

    def test_quality_with_mock_data(self):
        result = quality_checker(df=self.mock_data, rules=self.mock_config_file)
        result.show(truncate=False, n=100)


# if __name__ == "__main__":
#     unittest.main()
