#!/usr/bin/env python
# -*- coding: utf-8 -*-

import warnings
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col, concat, when, count
from pyspark.sql import Window


def is_complete(df: DataFrame, field: str, threshold: float = 1.0) -> DataFrame:
    result = df.withColumn(
        "dq_status",
        concat(
            col(field),
            when(col(field).isNotNull(), lit(":true")).otherwise(
                lit(":false")
            ),
        ),
    )
    return result



def quality_checker(df: DataFrame, rules: list[dict]) -> DataFrame:
    result = df.limit(0)
    for rule in rules:
        rule_name = rule["check_type"]
        field = rule["field"]
        threshold = rule.get("threshold", 1.0)
        # print(f"field:{field}\n rule_name:{rule_name}\n threshold:{threshold}")
        match rule_name:
            case "is_complete":
                print("Completeness check")
                result = df.withColumn(
                    "dq_status",
                    concat(
                        col(field),
                        when(col(field).isNotNull(), lit(":true")).otherwise(
                            lit(":false")
                        ),
                    ),
                )

            case "is_unique":
                window_spec = Window.partitionBy(col(field))
                count_col = count(col(field)).over(window_spec)
                result = df.withColumn(
                    "dq_status",
                    concat(
                        col(field),
                        when(count_col == 1, lit(":true")).otherwise(lit(":false"))
                    ),
                )


            case _:
                warnings.warn(f"Unknown rule name: {rule_name}, {field}")
    return result


# rs = quality_checker(df=df, rules=config)
