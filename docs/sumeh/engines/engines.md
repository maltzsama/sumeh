# Engines Overview

Sumeh v2.0 supports **14 execution engines** across three tiers, enabling data quality validation at any scale—from local CSVs to unbounded streams and distributed Big Data workloads.

---

## What are Engines?

Engines are adapters that allow Sumeh to work with different DataFrame and SQL implementations. Each engine provides the exact same validation API (`validate`, `extract_schema`, `validate_schema`) but is optimized for different use cases and scales.

In v2.0, Sumeh uses a **namespace-first API**. You explicitly import the engine you want to use. This provides perfect IDE autocomplete and avoids hidden runtime routing.

```python
from sumeh import pandas, polars, duckdb
from sumeh.core.rules.rule_model import RuleDefinition
import pandas as pd

# Load rules (from CSV, DB, etc.)
rules = [RuleDefinition(field="age", check_type="is_positive")]

# Validate explicitly with the engine of your choice
df = pd.read_csv("data.csv")
report = pandas.validate(df, rules)

```

---

## Supported Engines

| Engine | Tier | Best For | Scale | Speed | Memory |
| --- | --- | --- | --- | --- | --- |
| **Pandas** | Batch | General purpose, prototyping | Small-Medium | Moderate | High |
| **Polars** | Batch | Fast analytics, large datasets | Medium-Large | Very Fast | Low |
| **Dask** | Batch | Distributed computing, clusters | Large-Huge | Parallel | Scalable |
| **PySpark** | Batch | Big data, Spark clusters | Huge | Parallel | Distributed |
| **DuckDB** | SQL | Analytical queries, OLAP | Medium-Large | Very Fast | Low |
| **BigQuery** | SQL | Cloud data warehouse | Huge | Serverless | Cloud |
| *(+8 Others)* | Various | Snowflake, Flink, Ray, etc. | Varies | Varies | Varies |

---

## Engine Selection Guide

### 🐼 Pandas

**Use when:**

* Dataset < 10GB
* Local development
* Quick prototyping
* Standard data science workflow

```python
import pandas as pd
from sumeh import pandas as sumeh_pandas

df = pd.read_csv("data.csv")
report = sumeh_pandas.validate(df, rules)

```

**Pros:** Simple, widely-used, great ecosystem

**Cons:** Single-threaded, memory-intensive

---

### 🐻‍❄️ Polars

**Use when:**

* Dataset 1GB - 100GB
* Need better performance than Pandas
* Memory efficiency is important
* Modern Python syntax

```python
import polars as pl
from sumeh import polars as sumeh_polars

df = pl.read_csv("data.csv")
report = sumeh_polars.validate(df, rules)

```

**Pros:** 10-100x faster than Pandas, lazy evaluation, low memory

**Cons:** Newer ecosystem, fewer integrations

---

### ⚡ Dask

**Use when:**

* Dataset > 100GB
* Multi-core processing needed
* Out-of-memory computation
* Scaling to multiple machines

```python
import dask.dataframe as dd
from sumeh import dask as sumeh_dask

df = dd.read_csv("data/*.csv")
report = sumeh_dask.validate(df, rules)

```

**Pros:** Scales to clusters, familiar Pandas API, parallel

**Cons:** Overhead for small data, complex debugging

---

### 🔥 PySpark

**Use when:**

* Dataset > 1TB
* Already using Spark infrastructure
* Complex distributed processing
* Databricks, EMR, or Dataproc

```python
from pyspark.sql import SparkSession
from sumeh import pyspark as sumeh_pyspark

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("data.csv", header=True)

# Fully lazy evaluation via fail_condition columns
report = sumeh_pyspark.validate(spark, df, rules)

```

**Pros:** Industry standard for big data, fault-tolerant, mature

**Cons:** JVM overhead, complex setup, verbose API

---

### 🦆 DuckDB

**Use when:**

* Analytical queries on large CSVs/Parquet
* Need SQL-like performance
* In-process OLAP
* Embedded analytics

```python
import duckdb
from sumeh import duckdb as sumeh_duckdb

conn = duckdb.connect()
report = sumeh_duckdb.validate(con=conn, df="data.parquet", rules=rules)

```

**Pros:** Extremely fast for analytics, SQL support, columnar

**Cons:** Less mature than Pandas, limited ML ecosystem

---

### ☁️ BigQuery

**Use when:**

* Data already in BigQuery
* Serverless compute needed
* Petabyte-scale datasets
* Google Cloud Platform

```python
from google.cloud import bigquery
from sumeh import bigquery as sumeh_bq

client = bigquery.Client()
# Pushes the AST-compiled validation SQL directly to BigQuery
report = sumeh_bq.validate(client=client, table="project.dataset.table", rules=rules)

```

**Pros:** Serverless, auto-scaling, SQL interface

**Cons:** Network latency, costs per query

---

## CLI Engine Selection

When using the CLI, Sumeh automatically handles the file loading and engine dispatching. Use the `--engine` flag to specify which engine to use:

```bash
# Use Pandas (default)
sumeh validate data.csv rules.csv

# Use Polars for better performance and memory management
sumeh validate data.csv rules.csv --engine polars

# Use Dask for out-of-core large files
sumeh validate data.csv rules.csv --engine dask

# Use DuckDB for SQL-speed processing
sumeh validate data.parquet rules.csv --engine duckdb

```

---

## Best Practices

### 1. Match Engine to Data Size

```python
# < 1GB → Pandas
# 1-100GB → Polars or DuckDB
# > 100GB → Dask or PySpark
# Petabyte → BigQuery / Snowflake

```

### 2. Use Lazy Evaluation & Bifurcation

All modern Batch engines in Sumeh v2.0 support single-pass bifurcation (`report.split()`). Take advantage of lazy execution where available:

```python
# Polars (Lazy)
df = pl.scan_csv("*.csv")  
df = df.filter(pl.col("age") > 18) 
report = sumeh_polars.validate(df, rules)  # Triggers execution
good_df, bad_df = report.split()

# Dask (Lazy)
df = dd.read_csv("*.csv")  
df = df[df.age > 18]  
report = sumeh_dask.validate(df, rules)  # Triggers execution

```

### 3. Optimize for Your Environment

* **Local laptop** → Pandas / Polars
* **Corporate server** → DuckDB / Dask
* **Cloud** → BigQuery / Snowflake / PySpark
* **Edge/IoT** → Polars (smallest footprint)

### 4. Consider Data Format

| Format | Best Engine |
| --- | --- |
| CSV | DuckDB, Polars |
| Parquet | DuckDB, Polars, PySpark |
| JSON | Pandas, Polars |
| Cloud Storage | BigQuery, PySpark, Athena |

---

## Troubleshooting

### Import Errors

**Problem:** `ModuleNotFoundError: No module named 'polars'`

**Solution:** Sumeh v2.0 uses optional dependencies to keep the core lightweight. Install the specific engine you need:

```bash
pip install sumeh[polars]
pip install sumeh[pyspark]

```

### Memory Errors

**Problem:** Pandas runs out of memory (OOM)

**Solutions:**

1. Switch to **Polars** (much lower memory footprint).
2. Use **Dask** (out-of-core processing via disk spilling).
3. Sample the data first.
4. Use a columnar format like Parquet instead of CSV.

```python
# Option 1: Polars
import polars as pl
from sumeh import polars as sumeh_pl
df = pl.read_csv("large.csv")
report = sumeh_pl.validate(df, rules)

# Option 2: Dask
import dask.dataframe as dd
from sumeh import dask as sumeh_dd
df = dd.read_csv("large.csv")
report = sumeh_dd.validate(df, rules)

```

### Slow Performance

**Problem:** Validation takes too long

**Solutions:**

1. Use a faster engine like **Polars** or **DuckDB**.
2. Parallelize with **Dask** or **Spark**.
3. Reduce data size (filter first).

---

## Migration Guide

Migrating between engines is trivial because the `validate` signature and `ValidationReport` output are identical across the board.

### Pandas → Polars

```python
# Before (Pandas)
import pandas as pd
from sumeh import pandas as sumeh_pd

df = pd.read_csv("data.csv")
report = sumeh_pd.validate(df, rules)

# After (Polars)
import polars as pl
from sumeh import polars as sumeh_pl

df = pl.read_csv("data.csv")
report = sumeh_pl.validate(df, rules)

```

### Pandas → DuckDB

```python
# Before (Pandas)
import pandas as pd
from sumeh import pandas as sumeh_pd
df = pd.read_parquet("data.parquet")
report = sumeh_pd.validate(df, rules)

# After (DuckDB) - No need to load the DataFrame into Python memory!
import duckdb
from sumeh import duckdb as sumeh_duckdb

conn = duckdb.connect()
report = sumeh_duckdb.validate(con=conn, df="data.parquet", rules=rules)

```
