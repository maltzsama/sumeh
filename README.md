![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)
![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)


# Sumeh — Unified Data Quality Framework

Sumeh is a unified data‑quality validation framework supporting multiple backends (PySpark, Dask, BigQuery) with centralized rule configuration.

## 🚀 Installation

```bash
# Using pip
pip install sumeh

# Or with conda-forge
conda install -c conda-forge sumeh
```

**Prerequisites:**  
- Python 3.10+  
- One or more of: `pyspark`, `dask[dataframe]`, `google-cloud-bigquery`, `cuallee`

## 🔍 Core API

- **`report(df, rules, name="Quality Check")`**  
  Apply your validation rules over any DataFrame (Pandas, Spark, Dask, or SQL via BigQuery).  
- **`validate(df, rules)`** *(per-engine)*  
  Returns a DataFrame with a `dq_status` column listing violations.  
- **`summarize(qc_df, rules, total_rows)`** *(per-engine)*  
  Consolidates violations into a summary report.

## ⚙️ Engines

Each engine implements the `validate()` + `summarize()` pair:

| Engine                | Module                                  | Status          |
|-----------------------|-----------------------------------------|-----------------|
| PySpark               | `sumeh.engine.pyspark_engine`           | ✅ Fully tested |
| Dask                  | `sumeh.engine.dask_engine`              | ✅ Fully tested |
| BigQuery (SQL)        | `sumeh.engine.bigquery_engine` *(stub)* | 🔧 Work in progress |

_Planned:_ native Pandas engine.

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
from sumeh.engine.dask_engine import validate, summarize
import dask.dataframe as dd

# 1) Load data
ddf = dd.read_csv("data.csv", assume_missing=True)

# 2) Cast types
ddf["join_date"] = dd.to_datetime(ddf["join_date"])

# 3) Run per-engine validation
qc_ddf = validate(ddf, rules)

# 4) Generate summary
total = ddf.shape[0].compute()
report = summarize(qc_ddf, rules, total).compute()
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

## 📂 Project Layout

```
sumeh/
├── __init__.py          # exposes report & version
├── core.py              # report implementation
├── engine/
│   ├── pyspark_engine.py
│   ├── dask_engine.py
│   └── bigquery_engine.py  # SQL‑based
└── services/
    ├── config.py        # rule loaders
    └── utils.py         # internal helpers
tests/
├── data/                # sample inputs
└── test_*.py            # unit tests
```

## 📈 Roadmap

- [ ] Native Pandas engine  
- [ ] Complete BigQuery engine  
- [ ] Read the Docs documentation  

## 🤝 Contributing

1. Fork & create a feature branch.  
2. Implement new checks or engines, following existing signatures.  
3. Add tests under `tests/`.  
4. Open a PR and ensure CI passes.

## 📜 License

Licensed under the [Apache License 2.0](LICENSE).  
```