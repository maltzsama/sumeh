"""
PySpark validation engine.

Distributed DataFrame processing using Spark.
Zero UDFs - pure Column API for JVM performance.
Scales to Petabytes.

Example:
    >>> from pyspark.sql import SparkSession
    >>> from sumeh import pyspark as sumeh_pyspark
    >>>
    >>> spark = SparkSession.builder.getOrCreate()
    >>> df = spark.read.parquet("s3://bucket/data.parquet")
    >>> rules = [...]
    >>> report = sumeh_pyspark.validate(df, rules)
    >>>
    >>> good_df, bad_df = report.split()
    >>> good_df.write.parquet("s3://bucket/clean/")
    >>> bad_df.write.parquet("s3://bucket/quarantine/")
"""

from sumeh.engines.pyspark.engine import validate

__all__ = ["validate"]
