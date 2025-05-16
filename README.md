![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)
![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)

# <h1 style="display: flex; align-items: center; gap: 0.5rem;"><img src="https://raw.githubusercontent.com/maltzsama/sumeh/refs/heads/main/docs/img/sumeh.svg" alt="Logo" style="height: 40px; width: auto; vertical-align: middle;" /> <span>Sumeh DQ</span> </h1>

Sumeh is a unified data quality validation framework supporting multiple backends (PySpark, Dask, Polars, DuckDB) with centralized rule configuration.

## 🚀 Installation

```bash
# Using pip
pip install sumeh

# Or with conda-forge
conda install -c conda-forge sumeh
```

**Prerequisites:**  
- Python 3.10+  
- One or more of: `pyspark`, `dask[dataframe]`, `polars`, `duckdb`, `cuallee`

## 🔍 Core API

- **`report(df, rules, name="Quality Check")`**  
  Apply your validation rules over any DataFrame (Pandas, Spark, Dask, Polars, or DuckDB).  
- **`validate(df, rules)`** *(per-engine)*  
  Returns a DataFrame with a `dq_status` column listing violations.  
- **`summarize(qc_df, rules, total_rows)`** *(per-engine)*  
  Consolidates violations into a summary report.

## ⚙️ Supported Engines

Each engine implements the `validate()` + `summarize()` pair:

| Engine                | Module                                  | Status          |
|-----------------------|-----------------------------------------|-----------------|
| PySpark               | `sumeh.engine.pyspark_engine`           | ✅ Fully implemented |
| Dask                  | `sumeh.engine.dask_engine`              | ✅ Fully implemented |
| Polars                | `sumeh.engine.polars_engine`            | ✅ Fully implemented |
| DuckDB                | `sumeh.engine.duckdb_engine`            | ✅ Fully implemented |
| Pandas                | `sumeh.engine.pandas_engine`            | 🔧 Stub implementation |
| BigQuery (SQL)        | `sumeh.engine.bigquery_engine`          | 🔧 Stub implementation |

## 🏗 Configuration Sources

Load rules from CSV, S3, MySQL, Postgres, BigQuery table, or AWS Glue:

```python
from sumeh.services.config import (
    get_config_from_csv,
    get_config_from_s3,
    get_config_from_mysql,
    get_config_from_postgresql,
    get_config_from_bigquery,
    get_config_from_glue_data_catalog,
)

rules = get_config_from_csv("rules.csv", delimiter=";")
```

## 🏃‍♂️ Typical Workflow

```python
from sumeh import report
from sumeh.engine.polars_engine import validate, summarize
import polars as pl

# 1) Load data
df = pl.read_csv("data.csv")

# 2) Run validation
qc_df = validate(df, rules)

# 3) Generate summary
total = df.height
report = summarize(qc_df, rules, total)
print(report)
```

Or simply:

```python
from sumeh import report

report = report(df, rules, name="My Check")
```

## 📋 Rule Definition Example

```json
{
  "field": "customer_id",
  "check_type": "is_complete",
  "threshold": 0.99,
  "value": null,
  "execute": true
}
```

**Supported Validation Rules**

The following data quality checks are available:

| Test                       | Description                                                                                                         |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `is_positive`              | Filters rows where the specified column is less than zero.                                                          |
| `is_negative`              | Filters rows where the specified column is greater than or equal to zero.                                           |
| `is_complete`              | Filters rows where the specified column is null.                                                                    |
| `validate_date_format`     | Filters rows where the specified column does not match the expected date format or is null.                         |
| `is_future_date`           | Filters rows where the specified date column is after today’s date.                                                 |
| `is_past_date`             | Filters rows where the specified date column is before today’s date.                                                |
| `is_date_between`          | Filters rows where the specified date column is not within the given start–end range.                               |
| `is_date_after`            | Filters rows where the specified date column is before the date provided in the rule.                               |
| `is_date_before`           | Filters rows where the specified date column is after the date provided in the rule.                                |
| `is_unique`                | Identifies rows with duplicate values in the specified column.                                                      |
| `are_complete`             | Filters rows where any of the specified columns is null.                                                            |
| `are_unique`               | Identifies rows with duplicate combinations of the specified columns.                                               |
| `is_greater_than`          | Filters rows where the specified column is less than or equal to the threshold value.                               |
| `is_greater_or_equal_than` | Filters rows where the specified column is less than the threshold value.                                           |
| `is_less_than`             | Filters rows where the specified column is greater than or equal to the threshold value.                            |
| `is_less_or_equal_than`    | Filters rows where the specified column is greater than the threshold value.                                        |
| `is_equal`                 | Filters rows where the specified column is not equal (null-safe) to the given value.                                |
| `is_equal_than`            | Alias of `is_equal`.                                                                                                |
| `is_contained_in`          | Filters rows where the specified column is not in the provided list of values.                                      |
| `not_contained_in`         | Filters rows where the specified column is in the provided list of values.                                          |
| `is_between`               | Filters rows where the specified column is not within the given numeric range.                                      |
| `has_pattern`              | Filters rows where the specified column does not match the given regular-expression pattern.                        |
| `is_legit`                 | Filters rows where the specified column is null or does not match a non-whitespace pattern (`\S*`).                 |
| `is_primary_key`           | Alias of `is_unique` (checks uniqueness of a single column).                                                        |
| `is_composite_key`         | Alias of `are_unique` (checks uniqueness across multiple columns).                                                  |
| `has_max`                  | Filters rows where the specified column exceeds the maximum threshold.                                              |
| `has_min`                  | Filters rows where the specified column is below the minimum threshold.                                             |
| `has_std`                  | Returns all rows if the standard deviation of the specified column exceeds the threshold; otherwise empty.          |
| `has_mean`                 | Returns all rows if the mean of the specified column exceeds the threshold; otherwise empty.                        |
| `has_sum`                  | Returns all rows if the sum of the specified column exceeds the threshold; otherwise empty.                         |
| `has_cardinality`          | Returns all rows if the distinct count of the specified column exceeds the threshold; otherwise empty.              |
| `has_infogain`             | Uses distinct-count as a proxy for information gain; returns all rows if it exceeds the threshold; otherwise empty. |
| `has_entropy`              | Uses distinct-count as a proxy for entropy; returns all rows if it exceeds the threshold; otherwise empty.          |
| `all_date_checks`          | Filters rows where the specified date column is before today’s date (similar to `is_past_date`).                    |
| `satisfies`                | Filters rows where the given SQL expression (via `expr(value)`) is not satisfied.                                   |
| `validate`                 | Applies a list of named validation rules and returns aggregated and raw result DataFrames.                          |
| `validate_schema`          | Compares the actual schema of a DataFrame against an expected schema and returns a match flag and errors.           |


## 📂 Project Layout

```
sumeh/
├── poetry.lock
├── pyproject.toml
├── README.md
├── sumeh
│   ├── __init__.py
│   ├── cli.py
│   ├── core.py
│   ├── engine
│   │   ├── __init__.py
│   │   ├── bigquery_engine.py
│   │   ├── dask_engine.py
│   │   ├── duckdb_engine.py
│   │   ├── polars_engine.py
│   │   └── pyspark_engine.py
│   └── services
│       ├── __init__.py
│       ├── config.py
│       ├── index.html
│       └── utils.py
└── tests
    ├── __init__.py
    ├── mock
    │   ├── config.csv
    │   └── data.csv
    ├── test_dask_engine.py
    ├── test_duckdb_engine.py
    ├── test_polars_engine.py
    ├── test_pyspark_engine.py
    └── test_sumeh.py
```

## 📈 Roadmap

- [ ] Complete BigQuery engine implementation
- [ ] Complete Pandas engine implementation
- ✅ Enhanced documentation
- [ ] More validation rule types
- [ ] Performance optimizations

## 🤝 Contributing

1. Fork & create a feature branch  
2. Implement new checks or engines, following existing signatures  
3. Add tests under `tests/`  
4. Open a PR and ensure CI passes

## 📜 License

Licensed under the [Apache License 2.0](LICENSE).
