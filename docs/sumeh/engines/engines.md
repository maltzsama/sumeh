# Engines Overview

Sumeh supports multiple DataFrame processing engines to validate data at any scale, from local CSVs to distributed big data workloads.

---

## What are Engines?

Engines are adapters that allow Sumeh to work with different DataFrame implementations. Each engine provides the same validation API but optimized for different use cases and scales.

```python
from sumeh import validate, summarize
from sumeh.core.config import get_rules_config

# Works with ANY engine!
rules = get_rules_config(source="rules.csv")
invalid_raw, invalid_agg = validate(df, rules)
summary = summarize(invalid_raw, rules, total_rows=len(df))
```

Sumeh **automatically detects** which engine your DataFrame is using and applies the appropriate validation logic.

---

## Supported Engines

| Engine | Best For | Scale | Speed | Memory |
|--------|----------|-------|-------|--------|
| [Pandas](pandas.md) | General purpose, prototyping | Small-Medium | Moderate | High |
| [Polars](polars.md) | Fast analytics, large datasets | Medium-Large | Very Fast | Low |
| [Dask](dask.md) | Distributed computing, clusters | Large-Huge | Parallel | Scalable |
| [PySpark](pyspark.md) | Big data, Spark clusters | Huge | Parallel | Distributed |
| [DuckDB](duckdb.md) | Analytical queries, OLAP | Medium-Large | Very Fast | Low |
| [BigQuery](bigquery.md) | Cloud data warehouse | Huge | Serverless | Cloud |

---

## Engine Selection Guide

### 🐼 Pandas
**Use when:**
- Dataset < 10GB
- Local development
- Quick prototyping
- Standard data science workflow

```python
import pandas as pd
from sumeh import validate

df = pd.read_csv("data.csv")
invalid_raw, invalid_agg = validate(df, rules)
```

**Pros:** Simple, widely-used, great ecosystem  
**Cons:** Single-threaded, memory-intensive

---

### 🐻‍❄️ Polars
**Use when:**
- Dataset 1GB - 100GB
- Need better performance than Pandas
- Memory efficiency is important
- Modern Python syntax

```python
import polars as pl
from sumeh import validate

df = pl.read_csv("data.csv")
invalid_raw, invalid_agg = validate(df, rules)
```

**Pros:** 10-100x faster than Pandas, lazy evaluation, low memory  
**Cons:** Newer ecosystem, fewer integrations

---

### ⚡ Dask
**Use when:**
- Dataset > 100GB
- Multi-core processing needed
- Out-of-memory computation
- Scaling to multiple machines

```python
import dask.dataframe as dd
from sumeh import validate

df = dd.read_csv("data/*.csv")
invalid_raw, invalid_agg = validate(df, rules)
```

**Pros:** Scales to clusters, familiar Pandas API, parallel  
**Cons:** Overhead for small data, complex debugging

---

### 🔥 PySpark
**Use when:**
- Dataset > 1TB
- Already using Spark infrastructure
- Complex distributed processing
- Databricks, EMR, or Dataproc

```python
from pyspark.sql import SparkSession
from sumeh import validate

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("data.csv", header=True)
invalid_raw, invalid_agg = validate(df, rules)
```

**Pros:** Industry standard for big data, fault-tolerant, mature  
**Cons:** JVM overhead, complex setup, verbose API

---

### 🦆 DuckDB
**Use when:**
- Analytical queries on large CSVs/Parquet
- Need SQL-like performance
- In-process OLAP
- Embedded analytics

```python
import duckdb
from sumeh import validate

conn = duckdb.connect()
df = conn.execute("SELECT * FROM 'data.parquet'").df()
invalid_raw, invalid_agg = validate(df, rules, conn=conn)
```

**Pros:** Extremely fast for analytics, SQL support, columnar  
**Cons:** Less mature than Pandas, limited ML ecosystem

---

### ☁️ BigQuery
**Use when:**
- Data already in BigQuery
- Serverless compute needed
- Petabyte-scale datasets
- Google Cloud Platform

```python
from google.cloud import bigquery
from sumeh import validate

client = bigquery.Client()
query = "SELECT * FROM `project.dataset.table` LIMIT 1000000"
df = client.query(query).to_dataframe()
invalid_raw, invalid_agg = validate(df, rules)
```

**Pros:** Serverless, auto-scaling, SQL interface  
**Cons:** Network latency, costs per query

---

## Engine Auto-Detection

Sumeh automatically detects your DataFrame engine:

```python
from sumeh.core import __detect_engine

# Returns engine name based on DataFrame type
engine = __detect_engine(df)
# → "pandas_engine" | "polars_engine" | "pyspark_engine" | ...
```

Detection logic:

```python
def __detect_engine(df):
    mod = df.__class__.__module__
    
    if mod.startswith("pyspark"):
        return "pyspark_engine"
    elif mod.startswith("dask"):
        return "dask_engine"
    elif mod.startswith("polars"):
        return "polars_engine"
    elif mod.startswith("pandas"):
        return "pandas_engine"
    elif mod.startswith("duckdb"):
        return "duckdb_engine"
    else:
        raise TypeError(f"Unsupported DataFrame type: {type(df)}")
```

---

## CLI Engine Selection

Use the `--engine` flag to specify which engine to use for loading data:

```bash
# Use Pandas (default)
sumeh validate data.csv rules.csv

# Use Polars for better performance
sumeh validate data.csv rules.csv --engine polars

# Use Dask for large files
sumeh validate data.csv rules.csv --engine dask
```

Available engines:
- `pandas` (default)
- `polars`
- `dask`

---

## Engine-Specific Features

### Context Parameters

Some engines require additional context:

```python
# DuckDB requires connection
import duckdb
conn = duckdb.connect()
invalid_raw, invalid_agg = validate(df, rules, conn=conn)

# PySpark can use custom executors
invalid_raw, invalid_agg = validate(
    df, 
    rules, 
    num_partitions=100
)
```

### Lazy Evaluation

Polars and Dask support lazy evaluation:

```python
import polars as pl

# Build computation graph (lazy)
df = pl.scan_csv("data.csv")
df_filtered = df.filter(pl.col("age") > 18)

# Execute validation (triggers computation)
invalid_raw, invalid_agg = validate(df_filtered, rules)
```

---

## Custom Engine Implementation

Want to add support for a new DataFrame library? Implement these functions:

```python
# sumeh/engines/myengine_engine.py

def validate(df, rules):
    """Run validation checks."""
    # Implementation here
    return invalid_raw, invalid_agg

def validate_schema(df, expected, **kwargs):
    """Validate schema matches expected."""
    # Implementation here
    return is_valid, errors

def summarize(df, rules, total_rows=None):
    """Summarize validation results."""
    # Implementation here
    return summary_df
```

See [Custom Engine Guide](custom-engine.md) for details.

---

## Best Practices

### 1. Match Engine to Data Size

```python
# < 1GB → Pandas
if data_size < 1_000_000_000:
    engine = "pandas"

# 1-100GB → Polars or DuckDB
elif data_size < 100_000_000_000:
    engine = "polars"  # or "duckdb"

# > 100GB → Dask or Spark
else:
    engine = "dask"  # or "pyspark"
```

### 2. Use Lazy Evaluation

```python
# Polars
df = pl.scan_csv("*.csv")  # Lazy
df = df.filter(...)  # Still lazy
result = validate(df, rules)  # Triggers execution

# Dask
df = dd.read_csv("*.csv")  # Lazy
df = df[df.age > 18]  # Still lazy
result = validate(df, rules)  # Triggers execution
```

### 3. Optimize for Your Environment

```python
# Local laptop → Pandas/Polars
# Corporate server → DuckDB/Dask
# Cloud → BigQuery/Spark
# Edge/IoT → Polars (smallest footprint)
```

### 4. Consider Data Format

| Format | Best Engine |
|--------|-------------|
| CSV | DuckDB, Polars |
| Parquet | DuckDB, Polars, Spark |
| JSON | Pandas, Polars |
| Database | Native connectors |
| Cloud Storage | BigQuery, Spark |

---

## Troubleshooting

### "Unsupported DataFrame type"

**Problem:** Engine not detected

**Solution:** Check if the DataFrame is from a supported library

```python
print(type(df))
print(df.__class__.__module__)

# Should be one of:
# - pandas.core.frame.DataFrame
# - polars.dataframe.frame.DataFrame
# - dask.dataframe.core.DataFrame
# - pyspark.sql.dataframe.DataFrame
```

### Memory Errors

**Problem:** Pandas runs out of memory

**Solutions:**
1. Switch to Polars (lower memory footprint)
2. Use Dask (out-of-core processing)
3. Sample the data first
4. Use columnar format (Parquet)

```python
# Option 1: Polars
import polars as pl
df = pl.read_csv("large.csv")

# Option 2: Dask
import dask.dataframe as dd
df = dd.read_csv("large.csv")

# Option 3: Sample
df = pd.read_csv("large.csv", nrows=100000)
```

### Slow Performance

**Problem:** Validation takes too long

**Solutions:**
1. Use faster engine (Polars, DuckDB)
2. Parallelize with Dask/Spark
3. Reduce data size (filter first)
4. Optimize rules (remove duplicates)

```python
# Use faster engine
import polars as pl
df = pl.read_csv("data.csv")

# Or parallelize
import dask.dataframe as dd
df = dd.read_csv("data.csv")
```

---

## Migration Guide

### Pandas → Polars

```python
# Before (Pandas)
import pandas as pd
df = pd.read_csv("data.csv")
df_clean = df[df.age > 18]

# After (Polars)
import polars as pl
df = pl.read_csv("data.csv")
df_clean = df.filter(pl.col("age") > 18)

# Validation works the same!
invalid_raw, invalid_agg = validate(df_clean, rules)
```

### Pandas → Dask

```python
# Before (Pandas)
import pandas as pd
df = pd.read_csv("data.csv")

# After (Dask)
import dask.dataframe as dd
df = dd.read_csv("data.csv")

# Validation works the same!
invalid_raw, invalid_agg = validate(df, rules)
```
