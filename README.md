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
| PySpark               | `sumeh.engines.pyspark_engine`           | ✅ Fully implemented |
| Dask                  | `sumeh.engines.dask_engine`              | ✅ Fully implemented |
| Polars                | `sumeh.engines.polars_engine`            | ✅ Fully implemented |
| DuckDB                | `sumeh.engines.duckdb_engine`            | ✅ Fully implemented |
| Pandas                | `sumeh.engines.pandas_engine`            | ✅ Fully implemented |
| BigQuery (SQL)        | `sumeh.engines.bigquery_engine`          | 🔧 Stub implementation |

## 🏗 Configuration Sources

**Load rules from CSV**

```python
from sumeh import get_rules_config

rules = get_rules_config("rules.csv", delimiter=";")
```

**Load rules from S3**
```python
from sumeh import get_rules_config
bucket_name = "<bucket>"
path = "<path>"
file_name = "<file_name>"

rules = get_rules_config(f"s3://{bucket_name}/{path}/{file_name}", delimiter=";")

```

**Load rules from MySQL**
```python
from sumeh import get_rules_config

host = "<host>"
port = "<port>" #optional
user = "<username>"
password = "<passwd>"
database = "<database>"
table = "<rules_table>"
query = "<select * from rules>" # optional

rules = get_rules_config(
    source="mysql", 
    host=host, 
    user=user, 
    password=password, 
    database=database, 
    table=table, 
    query=query
)

# or using Mysql Connector
import mysql.connector
conn = mysql.connector.connect(
    host=host,
    port=port,
    database=database,
    user=user,
    password=password
)

rules = get_rules_config(source="mysql", connection=conn, query=query)

```

**Load rules from Postgres**
```python
from sumeh import get_rules_config

host = "<host>"
port = "<port>" #optional
user = "<username>"
password = "<passwd>"
database = "<database>"
schema = "<public>"
table = "<rules_table>"
query = "<select * from rules>" # optional

rules_pgsql = get_rules_config(
    source="postgresql", 
    host=host, user=user, 
    password=password, 
    database=database, 
    schema=schema, 
    table=table, 
    query=query
)

# Or using the PostgreSQL Connector
import psycopg2

conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
)

rules_pgsql = get_rules_config(source="postgresql", connection=conn, query=query)

```

**Load rules from AWS Glue Data Catalog**
```python
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from sumeh import get_rules_config

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)


database_name = "<database>"
table_name = "<table>"

rules = get_rules_config(
    source="glue",
    glue_context=glue_context,
    database_name=database_name,
    table_name=table_name
)

job.commit()

```

**Load rules from Databricks Data Catalog**
```python
from sumeh import get_rules_config

catalog = "<catalog>"
database_name = "<database>"
table_name = "<table>"
query = "<query>" # OPTIONAL

rules = get_rules_config(
    "databricks", 
    spark=spark, 
    catalog="sumeh_demo", 
    schema="sample_data", 
    table="rules",
    query=query
)

```


## 🏃‍♂️ Typical Workflow

```python
from sumeh import report
from sumeh.engines.polars_engine import validate, summarize
import polars as pl

# 1) Load data
df = pl.read_csv("data.csv")

# 2) Run validation
result, result_raw = validate(df, rules)

# 3) Generate summary
total = df.height
report = summarize(result_raw, rules, total_rows=total)
print(report)
```

**Or simply:**

```python
from sumeh import report

report = report(df, rules)
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



## Schema Validation

Sumeh allows you to validate your DataFrame schemas against a schema registry stored in various data sources (BigQuery, MySQL, PostgreSQL, DuckDB, Databricks, Glue, CSV, S3).

### Step 1: Store Your Schema Registry

First, create a `schema_registry` table in your data source with the following structure:

| Column | Type | Description |
|--------|------|-------------|
| id | int | Auto-increment ID |
| environment | string | Environment (e.g., 'prod', 'staging', 'dev') |
| source_type | string | Source type (e.g., 'bigquery', 'mysql') |
| database_name | string | Database/project name |
| catalog_name | string | Catalog name (for Databricks) |
| schema_name | string | Schema name (for PostgreSQL) |
| table_name | string | Table name |
| field | string | Column name |
| data_type | string | Data type |
| nullable | boolean | Whether column can be null |
| max_length | int | Maximum length for strings |
| comment | string | Description/comment |
| created_at | datetime | Creation timestamp |
| updated_at | datetime | Last update timestamp |

### Step 2: Get Schema Configuration

Use `get_schema_config()` to retrieve the expected schema from your registry:

#### BigQuery

```python
from sumeh.core import get_schema_config

schema = get_schema_config(
    source="bigquery",
    project_id="my-project",
    dataset_id="my-dataset",
    table_id="users",
    environment="prod"  # optional, defaults to 'prod'
)
```

#### MySQL

```python
# Option 1: Create connection internally
schema = get_schema_config(
    source="mysql",
    host="localhost",
    user="root",
    password="secret",
    database="mydb",
    table="users",
    environment="prod"
)

# Option 2: Reuse existing connection
import mysql.connector
conn = mysql.connector.connect(host="localhost", user="root", password="secret", database="mydb")

schema = get_schema_config(
    source="mysql",
    conn=conn,
    table="users",
    environment="prod"
)
```

#### PostgreSQL

```python
# Option 1: Create connection internally
schema = get_schema_config(
    source="postgresql",
    host="localhost",
    user="postgres",
    password="secret",
    database="mydb",
    schema="public",
    table="users",
    environment="prod"
)

# Option 2: Reuse existing connection
import psycopg2
conn = psycopg2.connect(host="localhost", user="postgres", password="secret", dbname="mydb")

schema = get_schema_config(
    source="postgresql",
    conn=conn,
    schema="public",
    table="users",
    environment="prod"
)
```

#### DuckDB

```python
import duckdb

conn = duckdb.connect("my_database.db")

schema = get_schema_config(
    source="duckdb",
    conn=conn,
    table="users",
    environment="prod"
)
```

#### Databricks

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

schema = get_schema_config(
    source="databricks",
    spark=spark,
    catalog="main",
    schema="default",
    table="users",
    environment="prod"
)
```

#### AWS Glue

```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext

glueContext = GlueContext(SparkContext.getOrCreate())

schema = get_schema_config(
    source="glue",
    glue_context=glueContext,
    database_name="my_database",
    table_name="users",
    environment="prod"
)
```

#### CSV

```python
schema = get_schema_config(
    source="schema_registry.csv",
    table="users",
    environment="prod"
)
```

#### S3

```python
schema = get_schema_config(
    source="s3://my-bucket/path/schema_registry.csv",
    table="users",
    environment="prod"
)
```

### Step 3: Validate DataFrame Schema

Once you have the expected schema, validate your DataFrame against it:

```python
from sumeh.core import validate_schema

# Load your DataFrame (example with pandas)
import pandas as pd
df = pd.read_csv("users.csv")

# Validate
is_valid, errors = validate_schema(
    df_or_conn=df,
    expected=schema
)

if is_valid:
    print("✅ Schema is valid!")
else:
    print("❌ Schema validation failed:")
    for field, error in errors:
        print(f"  - {field}: {error}")
```

#### Example Output

```
❌ Schema validation failed:
  - email: missing
  - age: type mismatch (got 'object', expected 'int64')
  - created_at: nullable but expected non-nullable
  - extra_field: extra column
```

### Advanced: Custom Filters

You can add custom WHERE clauses to filter the schema registry:

```python
schema = get_schema_config(
    source="bigquery",
    project_id="my-project",
    dataset_id="my-dataset",
    table_id="users",
    environment="prod",
    query="source_type = 'bigquery' AND catalog_name IS NOT NULL"
)
```

**Note:** The `query` parameter adds additional filters to the base filter (`table_name` and `environment`).

### Supported Engines

Schema validation works with all supported DataFrame engines:
- Dask
- DuckDB
- Pandas
- Polars
- PySpark

**Important:** Make sure the `data_type` values in your `schema_registry` match the exact format returned by your DataFrame engine (e.g., `int64` for pandas, `string` for PySpark). Comparisons are case-insensitive.


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
