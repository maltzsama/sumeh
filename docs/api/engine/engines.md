# Module `sumeh.engine` - Overview

::: sumeh.engine

This package provides engine-specific implementations for **data quality checks**, **schema validation**, and **result summarization** across multiple backends.

**📦 File Structure**

```
sumeh/engine/
├── __init__.py         # ⇨ engine registry and dynamic import logic
├── bigquery_engine.py  # ⇨ BigQuery schema introspection & validation
├── dask_engine.py      # ⇨ Dask DataFrame rule execution, validation & summarization
├── duckdb_engine.py    # ⇨ DuckDB SQL builder for rule validation & summarization
├── pandas_engine.py    # ⇨ Pandas DataFrame rule execution, validation & summarization
├── polars_engine.py    # ⇨ Polars DataFrame rule execution, validation & summarization
└── pyspark_engine.py   # ⇨ PySpark DataFrame rule execution, validation & summarization
```

- **`__init__.py`**  
  Detects the active engine based on the DataFrame type and dynamically dispatches to the appropriate module.

- **`bigquery_engine.py`**  
  Converts a BigQuery table schema into a unified format and compares it against an expected schema.

- **`dask_engine.py`**  
  Implements all data quality checks (completeness, uniqueness, value ranges, patterns, etc.) on Dask DataFrames, plus functions to aggregate violations and produce a summary report.

- **`duckdb_engine.py`**  
  Builds SQL expressions for each rule, executes them in DuckDB as UNION ALL queries, and returns both raw violations and an aggregated result, along with schema validation via PRAGMA introspection.

- **`pandas_engine.py`**  
  Executes the full suite of data quality checks directly against a Pandas DataFrame, annotating each violation in a `dq_status` column, and provides functions to aggregate raw results and generate a pass/fail summary. Also includes schema comparison utilities via Pandas dtypes.

- **`polars_engine.py`**  
  Mirrors the full suite of quality checks for Polars DataFrames, annotating violations in a `dq_status` column and providing summarization and schema-comparison utilities.

- **`pyspark_engine.py`**  
  Leverages PySpark SQL functions and window operations to apply the same rule set to Spark DataFrames, including logic for schema validation and summarization.
