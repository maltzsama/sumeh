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
| Pandas                | `sumeh.engine.pandas_engine`            | ✅ Fully implemented |
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
result, result_raw = validate(df, rules)

# 3) Generate summary
total = df.height
report = summarize(result_raw, rules, total)
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

## Supported Validation Rules

### Numeric checks

| Test                 | Description                                                                                               |
| -------------------- | --------------------------------------------------------------------------------------------------------- |
| **is_in_millions**   | Retains rows where the column value is **less than** 1,000,000 (fails the "in millions" criteria).        |
| **is_in_billions**   | Retains rows where the column value is **less than** 1,000,000,000 (fails the "in billions" criteria).    |

---

### Completeness & Uniqueness

| Test                   | Description                                                                 |
| ---------------------- | --------------------------------------------------------------------------- |
| **is_complete**        | Filters rows where the column value is null.                                |
| **are_complete**       | Filters rows where **any** of the specified columns are null.               |
| **is_unique**          | Identifies rows with duplicate values in the specified column.              |
| **are_unique**         | Identifies rows with duplicate combinations of the specified columns.       |
| **is_primary_key**     | Alias for `is_unique` (checks uniqueness of a single column).               |
| **is_composite_key**   | Alias for `are_unique` (checks combined uniqueness of multiple columns).    |

---

### Comparison & Range

| Test                             | Description                                                                             |
| -------------------------------- | --------------------------------------------------------------------------------------- |
| **is_equal**                     | Filters rows where the column is not equal to the provided value (null-safe).           |
| **is_equal_than**                | Alias for `is_equal`.                                                                   |
| **is_between**                   | Filters rows where the column value is **outside** the numeric range `[min, max]`.       |
| **is_greater_than**              | Filters rows where the column value is **≤** the threshold (fails "greater than").       |
| **is_greater_or_equal_than**     | Filters rows where the column value is **<** the threshold (fails "greater or equal").   |
| **is_less_than**                 | Filters rows where the column value is **≥** the threshold (fails "less than").          |
| **is_less_or_equal_than**        | Filters rows where the column value is **>** the threshold (fails "less or equal").      |
| **is_positive**                  | Filters rows where the column value is **< 0** (fails "positive").                       |
| **is_negative**                  | Filters rows where the column value is **≥ 0** (fails "negative").                       |

---

### Membership & Pattern

| Test                   | Description                                                                               |
| ---------------------- | ----------------------------------------------------------------------------------------- |
| **is_contained_in**    | Filters rows where the column value is **not** in the provided list.                       |
| **not_contained_in**   | Filters rows where the column value **is** in the provided list.                           |
| **has_pattern**        | Filters rows where the column value does **not** match the specified regex.                |
| **is_legit**           | Filters rows where the column value is null or contains whitespace (i.e., not `\S+`).      |

---

### Aggregate checks

| Test                 | Description                                                                                                      |
| -------------------- | ---------------------------------------------------------------------------------------------------------------- |
| **has_min**          | Returns all rows if the column's minimum value **causes failure** (value < threshold); otherwise returns empty.   |
| **has_max**          | Returns all rows if the column's maximum value **causes failure** (value > threshold); otherwise returns empty.   |
| **has_sum**          | Returns all rows if the column's sum **causes failure** (sum > threshold); otherwise returns empty.               |
| **has_mean**         | Returns all rows if the column's mean **causes failure** (mean > threshold); otherwise returns empty.             |
| **has_std**          | Returns all rows if the column's standard deviation **causes failure** (std > threshold); otherwise returns empty.|
| **has_cardinality**  | Returns all rows if the number of distinct values **causes failure** (count > threshold); otherwise returns empty.|
| **has_infogain**     | Same logic as `has_cardinality` (proxy for information gain).                                                    |
| **has_entropy**      | Same logic as `has_cardinality` (proxy for entropy).                                                             |

---

### SQL & Schema

| Test                 | Description                                                                                                     |
| -------------------- | --------------------------------------------------------------------------------------------------------------- |
| **satisfies**        | Filters rows where the SQL expression (based on `rule["value"]`) is **not** satisfied.                          |
| **validate_schema**  | Compares the DataFrame's actual schema against the expected one and returns a match flag + error list.          |
| **validate**         | Executes a list of named rules and returns two DataFrames: one with aggregated status and one with raw violations. |

---

### Date-related checks

| Test                       | Description                                                                                     |
| -------------------------- | ----------------------------------------------------------------------------------------------- |
| **is_t_minus_1**           | Retains rows where the date in the column is **not** equal to yesterday (T–1).                       |
| **is_t_minus_2**           | Retains rows where the date in the column is **not** equal to two days ago (T–2).                    |
| **is_t_minus_3**           | Retains rows where the date in the column is **not** equal to three days ago (T–3).                  |
| **is_today**               | Retains rows where the date in the column is **not** equal to today.                                 |
| **is_yesterday**           | Retains rows where the date in the column is **not** equal to yesterday.                             |
| **is_on_weekday**          | Retains rows where the date in the column **NOT FALLS** on a weekend (fails "weekday").              |
| **is_on_weekend**          | Retains rows where the date in the column **NOT FALLS** on a weekday (fails "weekend").              |
| **is_on_monday**           | Retains rows where the date in the column is **not** Monday.                                         |
| **is_on_tuesday**          | Retains rows where the date in the column is **not** Tuesday.                                        |
| **is_on_wednesday**        | Retains rows where the date in the column is **not** Wednesday.                                      |
| **is_on_thursday**         | Retains rows where the date in the column is **not** Thursday.                                       |
| **is_on_friday**           | Retains rows where the date in the column is **not** Friday.                                         |
| **is_on_saturday**         | Retains rows where the date in the column is **not** Saturday.                                       |
| **is_on_sunday**           | Retains rows where the date in the column is **not** Sunday.                                         |
| **validate_date_format**   | Filters rows where the date doesn't match the expected format or is null.                        |
| **is_future_date**         | Filters rows where the date in the column is **not** after today.                                    |
| **is_past_date**           | Filters rows where the date in the column is **not** before today.                                   |
| **is_date_after**          | Filters rows where the date in the column is **not** before the date provided in the rule.           |
| **is_date_before**         | Filters rows where the date in the column is **not** after the date provided in the rule.            |
| **is_date_between**        | Filters rows where the date in the column is **not** outside the range `[start, end]`.               |
| **all_date_checks**        | Alias for `is_past_date` (same logic: date before today).                                        |



## 📂 Project Layout

```
sumeh/
├── poetry.lock
├── pyproject.toml
├── README.md
└── sumeh
    ├── __init__.py
    ├── cli.py
    ├── core.py
    ├── engine
    │   ├── __init__.py
    │   ├── bigquery_engine.py
    │   ├── dask_engine.py
    │   ├── duckdb_engine.py
    │   ├── pandas_engine.py
    │   ├── polars_engine.py
    │   └── pyspark_engine.py
    └── services
        ├── __init__.py
        ├── config.py
        ├── index.html
        └── utils.py

```

## 📈 Roadmap

- [ ] Complete BigQuery engine implementation
- ✅ Complete Pandas engine implementation
- ✅ Enhanced documentation
- ✅ More validation rule types
- [ ] Performance optimizations

## 🤝 Contributing

1. Fork & create a feature branch  
2. Implement new checks or engines, following existing signatures  
3. Add tests under `tests/`  
4. Open a PR and ensure CI passes

## 📜 License

Licensed under the [Apache License 2.0](LICENSE).
