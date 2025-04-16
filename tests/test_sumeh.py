#!/usr/bin/env python

import unittest
import pandas as pd
import os
from unittest.mock import patch
from sumeh.sumeh import quality_resume
from sumeh.sumeh import get_config_from_csv


class TestQualityFunction(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Load the mock data from the CSV file
        cls.mock_data_file = os.path.join(os.path.dirname(__file__), "mock", "data.csv")
        cls.mock_config_file = os.path.join(
            os.path.dirname(__file__), "mock", "config.csv"
        )

        cls.mock_data = pd.read_csv(cls.mock_data_file, sep=",", header=0)
        cls.mock_data["performance"] = pd.to_numeric(
            cls.mock_data["performance"], errors="coerce"
        )
        cls.mock_data["performance"] = cls.mock_data["performance"].fillna(0)
        cls.mock_data["last_updated"] = pd.to_datetime(
            cls.mock_data["last_updated"], errors="coerce"
        )
        cls.mock_data["id"] = cls.mock_data["id"].astype(str)

    def test_quality_with_mock_data(self):
        rules = get_config_from_csv(file_path=self.mock_config_file, delimiter=";")
        print(rules)
        result = quality_resume(
            df=self.mock_data,
            rules=rules,
        )

        print(result)


if __name__ == "__main__":
    unittest.main()
