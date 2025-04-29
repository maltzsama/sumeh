![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)
![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)

# <h1 style="display: flex; align-items: center; gap: 0.5rem;"><img src="https://raw.githubusercontent.com/maltzsama/sumeh/refs/heads/feature/docs/docs/img/sumeh.svg" alt="Logo" style="height: 40px; width: auto; vertical-align: middle;" /> <span>Sumeh DQ</span> </h1>

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

## Supported Validation Rules

Sumeh supports a wide variety of validation checks including:
- Completeness checks (`is_complete`, `are_complete`)
- Uniqueness checks (`is_unique`, `are_unique`, `is_primary_key`, `is_composite_key`)
- Value comparisons (`is_greater_than`, `is_less_than`, `is_equal`, `is_between`)
- Set operations (`is_contained_in`, `not_contained_in`)
- Pattern matching (`has_pattern`)
- Statistical checks (`has_min`, `has_max`, `has_mean`, `has_std`, `has_sum`)
- Date validations (`is_today`, `is_yesterday`, `is_on_weekday`, etc.)
- Custom expressions (`satisfies`)

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
- [ ] Enhanced documentation
- [ ] More validation rule types
- [ ] Performance optimizations

## 🤝 Contributing

1. Fork & create a feature branch  
2. Implement new checks or engines, following existing signatures  
3. Add tests under `tests/`  
4. Open a PR and ensure CI passes

## 📜 License

Licensed under the [Apache License 2.0](LICENSE).
