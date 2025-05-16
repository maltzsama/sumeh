# Quickstart 🚀

A concise guide to get started with Sumeh’s unified data quality framework.

## 1. Installation 💻

Install Sumeh via pip (recommended) or conda:

```bash
pip install sumeh
# or
conda install -c conda-forge sumeh
```

## 2. Loading Rules and Schema Configuration ⚙️

Use **`get_rules_config`** and **`get_schema_config`** to fetch your validation rules and expected schema from various sources.

```python
from sumeh import get_rules_config, get_schema_config

# Load rules from CSV
rules = get_rules_config("path/to/rules.csv", delimiter=';')

# Load expected schema from Glue Data Catalog
schema = get_schema_config(
    "glue",
    catalog_name="my_catalog",
    database_name="my_db",
    table_name="my_table"
)
```

Supported rule/schema sources include:

* `bigquery://project.dataset.table` 🌐
* `s3://bucket/path` ☁️
* Local CSV (`*.csv`) 📄
* Relational ("mysql", "postgresql") via kwargs 🗄️
* AWS Glue (`"glue"`) 🔥
* DuckDB (`duckdb://db_path.table`) 🦆
* Databricks (`databricks://catalog.schema.table`) 💎

## 3. Schema Validation 📐

Before validating data, ensure your DataFrame or connection matches the expected schema:

```python
from sumeh import validate_schema

# For a Spark DataFrame:
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("data.parquet")

is_valid, errors = validate_schema(
    df,
    expected=schema,
    engine="pyspark_engine"
)
if not is_valid:
    print("Schema mismatches:", errors)
```

## 4. Data Validation 🔍

Apply your loaded rules to any supported DataFrame using **`validate`**:

```python
from sumeh import validate

# Example with Pandas:
import pandas as pd
df = pd.read_csv("data.csv")

# Validate (detects engine automatically)
result = validate(df, rules)
# `result` structure depends on engine (e.g., CheckResult for cuallee engines)
```

## 5. Summarization 📊

Generate a tabular summary of violations and pass rates with **`summarize`**:

```python
from sumeh import summarize

# For DataFrames requiring manual total_rows (e.g., Pandas):
total = len(df)
summary_df = summarize(
    df=result,       # could be validation output or raw DataFrame
    rules=rules,
    total_rows=total
)
print(summary_df)
```

## 6. One-Step Reporting 📝

Use **`report`** for an end-to-end quality check and summary in one call:

```python
from sumeh import report

report_df = report(
    df,              # your DataFrame or connection
    rules,
    name="My Quality Check"
)
print(report_df)
```

---

*For deeper customization and engine-specific options, explore the full API and examples in the [Sumeh repository](https://github.com/maltzsama/sumeh).*
