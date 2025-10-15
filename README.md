[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg?logo=python&logoColor=white)](https://www.python.org/downloads/)
[![Build Status](https://github.com/maltzsama/sumeh/workflows/Publish%20Python%20Package/badge.svg)](https://github.com/maltzsama/sumeh/actions)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg?logo=open-source-initiative&logoColor=white)](https://opensource.org/licenses/Apache-2.0)
[![Coverage](https://codecov.io/gh/maltzsama/sumeh/branch/main/graph/badge.svg)](https://codecov.io/gh/maltzsama/sumeh)
[![Downloads](https://img.shields.io/pypi/dm/sumeh?logo=pypi&logoColor=white)](https://pypi.org/project/sumeh/)
[![PyPI Version](https://img.shields.io/pypi/v/sumeh?color=blue&logo=pypi&logoColor=white)](https://pypi.org/project/sumeh/)
[![Version](https://img.shields.io/badge/version-1.2.0-blue.svg)](https://github.com/maltzsama/sumeh/releases)

# <h1 style="display: flex; align-items: center; gap: 0.5rem;"><img src="https://raw.githubusercontent.com/maltzsama/sumeh/refs/heads/main/docs/img/sumeh.svg" alt="Logo" style="height: 40px; width: auto; vertical-align: middle;" /> <span>Sumeh DQ</span> </h1>

Sumeh is a unified data quality validation framework supporting multiple backends (PySpark, Dask, Polars, DuckDB, Pandas, BigQuery) with centralized rule configuration and schema validation.

## 🚀 Installation

```bash
# Base installation
pip install sumeh

# With specific engine support
pip install sumeh[pyspark]     # PySpark support
pip install sumeh[aws]         # S3 + Pandas support
pip install sumeh[mysql]       # MySQL support
pip install sumeh[postgresql]  # PostgreSQL support
pip install sumeh[bigquery]    # BigQuery support
pip install sumeh[dashboard]   # Streamlit dashboard

# All extras
pip install sumeh[dev,aws,mysql,postgresql,bigquery,dashboard]
```

**Prerequisites:**  
- Python 3.10+  
- One or more of: `pyspark`, `dask[dataframe]`, `polars`, `duckdb`, `pandas`

## 🔍 Core API

Sumeh uses a **dispatcher pattern** for clean engine-specific access:

```python
from sumeh import validate, summarize, get_rules_config

# Load rules from various sources
rules = get_rules_config.csv("rules.csv")
rules = get_rules_config.s3("s3://bucket/rules.csv")
rules = get_rules_config.mysql(host="localhost", table="rules")

# Engine-specific validation
result = validate.pandas(df, rules)
result = validate.pyspark(spark, df, rules)
result = validate.duckdb(conn, df_rel, rules)

# Generate summary reports
summary = summarize.pandas(result, rules, total_rows=len(df))
```

## ⚙️ Supported Engines

All engines implement the same `validate()` + `summarize()` interface:

| Engine                | Module                                  | Status          | Streaming Support |
|-----------------------|-----------------------------------------|-----------------|-------------------|
| **Pandas**            | `sumeh.engines.pandas_engine`           | ✅ Fully implemented | ❌ Batch only |
| **PySpark**           | `sumeh.engines.pyspark_engine`          | ✅ Fully implemented | ✅ Structured Streaming |
| **Dask**              | `sumeh.engines.dask_engine`             | ✅ Fully implemented | ❌ Batch only |
| **Polars**            | `sumeh.engines.polars_engine`           | ✅ Fully implemented | ❌ Batch only |
| **DuckDB**            | `sumeh.engines.duckdb_engine`           | ✅ Fully implemented | ❌ Batch only |
| **BigQuery**          | `sumeh.engines.bigquery_engine`         | ✅ Fully implemented | ❌ Batch only |

## 🏗 Configuration Sources

Sumeh supports loading rules from multiple sources using the dispatcher pattern:

### CSV Files
```python
from sumeh import get_rules_config

# Local CSV
rules = get_rules_config.csv("rules.csv", delimiter=";")

# S3 CSV
rules = get_rules_config.s3("s3://bucket/path/rules.csv", delimiter=";")
```

### Database Sources
```python
# MySQL
rules = get_rules_config.mysql(
    host="localhost",
    user="root", 
    password="secret",
    database="mydb",
    table="rules"
)

# PostgreSQL
rules = get_rules_config.postgresql(
    host="localhost",
    user="postgres",
    password="secret", 
    database="mydb",
    schema="public",
    table="rules"
)

# BigQuery
rules = get_rules_config.bigquery(
    project_id="my-project",
    dataset_id="my-dataset", 
    table_id="rules"
)

# DuckDB
import duckdb
conn = duckdb.connect("my.db")
rules = get_rules_config.duckdb(conn=conn, table="rules")
```

### Cloud Data Catalogs
```python
# AWS Glue
from awsglue.context import GlueContext
rules = get_rules_config.glue(
    glue_context=glue_context,
    database_name="my_database",
    table_name="rules"
)

# Databricks Unity Catalog
rules = get_rules_config.databricks(
    spark=spark,
    catalog="main",
    schema="default", 
    table="rules"
)
```

### Using Existing Connections
```python
# Reuse MySQL connection
import mysql.connector
conn = mysql.connector.connect(host="localhost", user="root", password="secret")
rules = get_rules_config.mysql(conn=conn, query="SELECT * FROM rules WHERE active=1")

# Reuse PostgreSQL connection  
import psycopg2
conn = psycopg2.connect(host="localhost", user="postgres", password="secret")
rules = get_rules_config.postgresql(conn=conn, query="SELECT * FROM public.rules")
```

## 🏃 Typical Workflow

### Modern Dispatcher API (Recommended)
```python
from sumeh import validate, summarize, get_rules_config
import polars as pl

# 1) Load rules and data
rules = get_rules_config.csv("rules.csv")
df = pl.read_csv("data.csv")

# 2) Run validation (returns 3 DataFrames)
df_errors, violations, table_summary = validate.polars(df, rules)

# 3) Generate consolidated summary
summary = summarize.polars(
    validation_result=(df_errors, violations, table_summary),
    rules=rules,
    total_rows=len(df)
)
print(summary)
```

### Engine-Specific Examples
```python
# Pandas
df_errors, violations, table_sum = validate.pandas(df, rules)
summary = summarize.pandas((df_errors, violations, table_sum), rules, len(df))

# PySpark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df_errors, violations, table_sum = validate.pyspark(spark, df, rules)
summary = summarize.pyspark((df_errors, violations, table_sum), rules, df.count())

# DuckDB
import duckdb
conn = duckdb.connect()
df_rel = conn.sql("SELECT * FROM my_table")
df_errors, violations, table_sum = validate.duckdb(conn, df_rel, rules)
summary = summarize.duckdb((df_errors, violations, table_sum), rules, df_rel.count("*").fetchone()[0])
```

### Legacy API (Still Supported)
```python
from sumeh import report

# Simple one-liner using cuallee backend
report_df = report(df, rules, name="Quality Check")
```

## 📋 Rule Definition

Sumeh uses the `RuleDef` class for type-safe rule definitions with automatic validation:

### Basic Rule Structure
```python
from sumeh.core.rules import RuleDef

# Create rule programmatically
rule = RuleDef(
    field="customer_id",
    check_type="is_complete", 
    threshold=0.99,
    value=None,
    execute=True
)

# Or from dictionary
rule = RuleDef.from_dict({
    "field": "customer_id",
    "check_type": "is_complete",
    "threshold": 0.99,
    "value": None,
    "execute": True
})
```

### CSV Format Example
```csv
field,check_type,threshold,value,execute
customer_id,is_complete,0.99,,true
email,has_pattern,1.0,"^[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}$",true
age,is_between,1.0,"[18, 120]",true
status,is_contained_in,1.0,"['active', 'inactive', 'pending']",true
"[first_name,last_name]",are_complete,0.95,,true
```

### Advanced Features
- **Auto-parsing**: Values are automatically converted to correct types (int, float, list, date, regex)
- **Multi-column rules**: Use `[col1,col2]` syntax for composite validations
- **Metadata enrichment**: Category and level are auto-populated from rule registry
- **Engine compatibility**: Automatic validation against supported engines

## 📊 Supported Validation Rules

Sumeh supports **60+ validation rules** organized by level and category. All rules are defined in the [manifest.json](sumeh/core/rules/manifest.json) registry.

### Row-Level Validations

#### Completeness
| Rule | Description | Example |
|------|-------------|----------|
| `is_complete` | Column has no null values | `{"field": "email", "check_type": "is_complete"}` |
| `are_complete` | Multiple columns have no nulls | `{"field": "[name,email]", "check_type": "are_complete"}` |

#### Uniqueness  
| Rule | Description | Example |
|------|-------------|----------|
| `is_unique` | Column values are unique | `{"field": "user_id", "check_type": "is_unique"}` |
| `are_unique` | Column combination is unique | `{"field": "[email,phone]", "check_type": "are_unique"}` |
| `is_primary_key` | Alias for `is_unique` | `{"field": "id", "check_type": "is_primary_key"}` |

#### Comparison & Range
| Rule | Description | Example |
|------|-------------|----------|
| `is_between` | Value within range | `{"field": "age", "check_type": "is_between", "value": [18, 65]}` |
| `is_greater_than` | Value > threshold | `{"field": "score", "check_type": "is_greater_than", "value": 0}` |
| `is_positive` | Value > 0 | `{"field": "amount", "check_type": "is_positive"}` |
| `is_in_millions` | Value >= 1,000,000 | `{"field": "revenue", "check_type": "is_in_millions"}` |

#### Membership & Pattern
| Rule | Description | Example |
|------|-------------|----------|
| `is_contained_in` | Value in allowed list | `{"field": "status", "check_type": "is_contained_in", "value": ["active", "inactive"]}` |
| `has_pattern` | Matches regex pattern | `{"field": "email", "check_type": "has_pattern", "value": "^[\\w.-]+@[\\w.-]+\\.[a-zA-Z]{2,}$"}` |
| `is_legit` | Non-empty, non-whitespace | `{"field": "name", "check_type": "is_legit"}` |

#### Date Validations
| Rule | Description | Example |
|------|-------------|----------|
| `is_past_date` | Date before today | `{"field": "birth_date", "check_type": "is_past_date"}` |
| `is_future_date` | Date after today | `{"field": "expiry_date", "check_type": "is_future_date"}` |
| `is_date_between` | Date within range | `{"field": "created_at", "check_type": "is_date_between", "value": ["2023-01-01", "2023-12-31"]}` |
| `is_on_weekday` | Date falls on weekday | `{"field": "transaction_date", "check_type": "is_on_weekday"}` |
| `validate_date_format` | Matches date format | `{"field": "date_str", "check_type": "validate_date_format", "value": "%Y-%m-%d"}` |

#### SQL Custom Rules
| Rule | Description | Example |
|------|-------------|----------|
| `satisfies` | Custom SQL condition | `{"field": "*", "check_type": "satisfies", "value": "age >= 18 AND status = 'active'"}` |

### Table-Level Validations

#### Aggregation Checks
| Rule | Description | Example |
|------|-------------|----------|
| `has_min` | Minimum value check | `{"field": "price", "check_type": "has_min", "value": 0}` |
| `has_max` | Maximum value check | `{"field": "age", "check_type": "has_max", "value": 120}` |
| `has_cardinality` | Distinct count check | `{"field": "category", "check_type": "has_cardinality", "value": 10}` |
| `has_mean` | Average value check | `{"field": "rating", "check_type": "has_mean", "value": 3.5}` |
| `has_std` | Standard deviation check | `{"field": "scores", "check_type": "has_std", "value": 2.0}` |

#### Schema Validation
| Rule | Description | Example |
|------|-------------|----------|
| `validate_schema` | Schema structure check | `{"field": "*", "check_type": "validate_schema", "value": expected_schema}` |

### Engine Compatibility
All rules support all engines: **Pandas**, **PySpark**, **Dask**, **Polars**, **DuckDB**, **BigQuery**

## 🔍 Schema Validation

Sumeh provides comprehensive schema validation against registries stored in multiple data sources.

### Schema Registry Structure

Create a `schema_registry` table with this structure:

```sql
CREATE TABLE schema_registry (
    id INTEGER PRIMARY KEY,
    environment VARCHAR(50),     -- 'prod', 'staging', 'dev'
    source_type VARCHAR(50),     -- 'bigquery', 'mysql', etc.
    database_name VARCHAR(100),
    catalog_name VARCHAR(100),   -- For Databricks
    schema_name VARCHAR(100),    -- For PostgreSQL
    table_name VARCHAR(100),
    field VARCHAR(100),
    data_type VARCHAR(50),
    nullable BOOLEAN,
    max_length INTEGER,
    comment TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
```

### Get Schema Configuration

```python
from sumeh import get_schema_config

# From various sources
schema = get_schema_config.bigquery(
    project_id="my-project",
    dataset_id="my-dataset", 
    table_id="users",
    environment="prod"
)

schema = get_schema_config.mysql(
    host="localhost",
    user="root",
    password="secret",
    database="mydb",
    table="users"
)

schema = get_schema_config.csv(
    "schema_registry.csv",
    table="users"
)

schema = get_schema_config.s3(
    "s3://bucket/schema_registry.csv",
    table="users"
)
```

### Extract & Validate Schema

```python
from sumeh import extract_schema, validate_schema
import pandas as pd

# Extract actual schema from DataFrame
df = pd.read_csv("users.csv")
actual_schema = extract_schema.pandas(df)

# Get expected schema from registry
expected_schema = get_schema_config.csv("schema_registry.csv", table="users")

# Validate
is_valid, errors = validate_schema.pandas(df, expected_schema)

if is_valid:
    print("✅ Schema is valid!")
else:
    print("❌ Schema validation failed:")
    for field, error in errors:
        print(f"  - {field}: {error}")
```

### Engine Support

Schema validation works with all engines:
- `extract_schema.pandas(df)`
- `extract_schema.pyspark(df)`  
- `extract_schema.polars(df)`
- `extract_schema.duckdb(conn, relation)`
- `validate_schema.pandas(df, expected)`
- `validate_schema.pyspark(df, expected)`
- etc.

### Custom Filters

```python
# Add custom WHERE conditions
schema = get_schema_config.mysql(
    host="localhost",
    table="users",
    query="environment = 'prod' AND source_type = 'mysql'"
)
```

## 🛠️ Table Generators

Sumeh includes SQL DDL generators for creating `rules` and `schema_registry` tables across multiple database dialects:

### Generate DDL Statements

```python
from sumeh.generators import SQLGenerator

# Generate rules table for PostgreSQL
ddl = SQLGenerator.generate(table="rules", dialect="postgres", schema="public")
print(ddl)

# Generate schema_registry table for BigQuery
ddl = SQLGenerator.generate(
    table="schema_registry", 
    dialect="bigquery",
    schema="my_dataset",
    partition_by="DATE(created_at)",
    cluster_by=["table_name", "environment"]
)
print(ddl)

# Generate both tables for MySQL
ddl = SQLGenerator.generate(table="all", dialect="mysql", engine="InnoDB")
print(ddl)
```

### Supported Dialects

- **PostgreSQL** (`postgres`)
- **MySQL** (`mysql`) 
- **BigQuery** (`bigquery`)
- **Snowflake** (`snowflake`)
- **Redshift** (`redshift`)
- **Databricks** (`databricks`)
- **DuckDB** (`duckdb`)
- **SQLite** (`sqlite`)
- **Athena** (`athena`)
- **And more...**

### Dialect-Specific Features

```python
# BigQuery with partitioning and clustering
SQLGenerator.generate(
    table="rules",
    dialect="bigquery",
    partition_by="DATE(created_at)",
    cluster_by=["environment", "table_name"]
)

# Snowflake with clustering
SQLGenerator.generate(
    table="schema_registry",
    dialect="snowflake",
    cluster_by=["table_name", "field"]
)

# Redshift with distribution and sort keys
SQLGenerator.generate(
    table="rules",
    dialect="redshift",
    distkey="table_name",
    sortkey=["created_at", "environment"]
)
```

### Utility Functions

```python
# List available dialects
print(SQLGenerator.list_dialects())
# ['athena', 'bigquery', 'databricks', 'duckdb', 'mysql', ...]

# List available tables
print(SQLGenerator.list_tables())
# ['rules', 'schema_registry']

# Transpile SQL between dialects
sql = "SELECT * FROM users WHERE created_at >= CURRENT_DATE - 7"
transpiled = SQLGenerator.transpile(sql, "postgres", "bigquery")
print(transpiled)
```

## 📋 Table Schemas

### Rules Table Structure

```sql
CREATE TABLE rules (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    environment VARCHAR(50) NOT NULL,
    source_type VARCHAR(50) NOT NULL,
    database_name VARCHAR(255) NOT NULL,
    catalog_name VARCHAR(255),
    schema_name VARCHAR(255),
    table_name VARCHAR(255) NOT NULL,
    field VARCHAR(255) NOT NULL,
    level VARCHAR(100) NOT NULL,
    category VARCHAR(100) NOT NULL,
    check_type VARCHAR(100) NOT NULL,
    value TEXT,
    threshold FLOAT DEFAULT 1.0,
    execute BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP
);
```

### Schema Registry Table Structure

```sql
CREATE TABLE schema_registry (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    environment VARCHAR(50) NOT NULL,
    source_type VARCHAR(50) NOT NULL,
    database_name VARCHAR(255) NOT NULL,
    catalog_name VARCHAR(255),
    schema_name VARCHAR(255),
    table_name VARCHAR(255) NOT NULL,
    field VARCHAR(255) NOT NULL,
    data_type VARCHAR(100) NOT NULL,
    nullable BOOLEAN DEFAULT TRUE,
    max_length INTEGER,
    comment TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP
);
```

## 🏗️ Architecture

Sumeh follows a modular, dispatcher-based architecture:

```
sumeh/
├── cli/                    # Command-line interface
├── core/                   # Core framework
│   ├── rules/             # Rule definitions & registry
│   │   ├── manifest.json  # 60+ validation rules
│   │   ├── rule_model.py  # RuleDef class
│   │   └── regristry.py   # Rule registry
│   ├── __init__.py        # Dispatchers (validate, summarize, etc.)
│   ├── config.py          # Multi-source configuration
│   ├── report.py          # Legacy cuallee integration
│   └── utils.py           # Utilities
├── engines/               # Engine implementations
│   ├── pandas_engine.py   # Pandas backend
│   ├── pyspark_engine.py  # PySpark backend  
│   ├── dask_engine.py     # Dask backend
│   ├── polars_engine.py   # Polars backend
│   ├── duckdb_engine.py   # DuckDB backend
│   └── bigquery_engine.py # BigQuery backend (stub)
├── dash/                  # Streamlit dashboard
└── generators/            # SQL generation utilities
```

### Key Components

- **Dispatchers**: Clean API for engine-specific operations (`validate.pandas()`, `summarize.pyspark()`)
- **RuleDef**: Type-safe rule definitions with auto-validation
- **Rule Registry**: Centralized manifest of 60+ validation rules
- **Multi-source Config**: Load rules from CSV, S3, MySQL, PostgreSQL, BigQuery, etc.
- **Schema Validation**: Extract and validate DataFrame schemas
- **Engine Abstraction**: Consistent interface across all backends

## 🔍 Row-Level vs Table-Level Validations

Sumeh supports **two types** of validation rules:

### 🔸 **Row-Level Validations**
Validate individual rows and return violating records:

```python
# Examples of row-level rules
row_rules = [
    {"field": "email", "check_type": "is_complete", "level": "ROW"},
    {"field": "age", "check_type": "is_between", "value": "[18,120]", "level": "ROW"},
    {"field": "status", "check_type": "is_contained_in", "value": "['active','inactive']", "level": "ROW"}
]

# Returns: DataFrame with violating rows + dq_status column
df_errors, violations, _ = validate.pandas(df, row_rules)
```

#### 🌊 **Spark Structured Streaming Support**
Row-level validations are **fully compatible** with Spark Structured Streaming for real-time data quality:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sumeh import validate

spark = SparkSession.builder.getOrCreate()

# Create streaming DataFrame
streaming_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "events") \
    .load()

# Apply row-level validation to streaming data
row_rules = [
    {"field": "user_id", "check_type": "is_complete", "level": "ROW"},
    {"field": "event_type", "check_type": "is_contained_in", "value": "['click','view','purchase']", "level": "ROW"}
]

# Validate streaming data
validated_stream = validate.pyspark(spark, streaming_df, row_rules)

# Write violations to output sink
query = validated_stream[0] \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
```

> **Note**: Table-level validations require complete datasets and are not compatible with streaming.

### 🔸 **Table-Level Validations**  
Validate aggregate statistics and return summary metrics:

```python
# Examples of table-level rules
table_rules = [
    {"field": "salary", "check_type": "has_mean", "value": 50000, "level": "TABLE"},
    {"field": "department", "check_type": "has_cardinality", "value": 5, "level": "TABLE"},
    {"field": "score", "check_type": "has_max", "value": 100, "level": "TABLE"},
    {"field": "rating", "check_type": "has_std", "value": 2.0, "level": "TABLE"}
]

# Returns: Summary DataFrame with PASS/FAIL status
_, _, table_summary = validate.pandas(df, table_rules)
```

### 🔸 **Available Table-Level Rules**

| Rule | Description | Example |
|------|-------------|----------|
| `has_min` | Minimum value check | `{"field": "age", "check_type": "has_min", "value": 18}` |
| `has_max` | Maximum value check | `{"field": "score", "check_type": "has_max", "value": 100}` |
| `has_mean` | Average value check | `{"field": "salary", "check_type": "has_mean", "value": 50000}` |
| `has_std` | Standard deviation check | `{"field": "ratings", "check_type": "has_std", "value": 2.0}` |
| `has_sum` | Total sum check | `{"field": "revenue", "check_type": "has_sum", "value": 1000000}` |
| `has_cardinality` | Distinct count check | `{"field": "categories", "check_type": "has_cardinality", "value": 10}` |
| `has_entropy` | Data entropy check | `{"field": "distribution", "check_type": "has_entropy", "value": 2.5}` |
| `has_infogain` | Information gain check | `{"field": "features", "check_type": "has_infogain", "value": 0.8}` |

## 🚀 CLI Usage

Sumeh includes a powerful CLI built with **Typer** for validation workflows:

### Core Commands

```bash
# Install with CLI support
pip install sumeh

# Initialize new project
sumeh init my-project

# Validate data with rules
sumeh validate --data data.csv --rules rules.csv --engine pandas

# Get version and system info
sumeh info

# Manage rules
sumeh rules list                    # List available rules
sumeh rules info is_complete        # Get rule details
sumeh rules search "date"           # Search rules by keyword
sumeh rules template                # Generate rule template

# Schema operations
sumeh schema extract --data data.csv --output schema.json
sumeh schema validate --data data.csv --registry schema_registry.csv

# Generate SQL DDL for 17+ dialects
sumeh sql generate --table rules --dialect postgres
sumeh sql generate --table schema_registry --dialect bigquery
sumeh sql transpile --sql "SELECT * FROM users" --from postgres --to bigquery

# Web configuration UI
sumeh config --port 8080
```

### Available CLI Commands

| Command | Description | Example |
|---------|-------------|----------|
| `init` | Initialize new Sumeh project | `sumeh init my-project` |
| `validate` | Run data validation | `sumeh validate --data data.csv --rules rules.csv` |
| `info` | Show version and system info | `sumeh info` |
| `rules` | Manage validation rules | `sumeh rules list` |
| `schema` | Schema operations | `sumeh schema extract --data data.csv` |
| `sql` | Generate/transpile SQL | `sumeh sql generate --table rules --dialect mysql` |
| `config` | Launch web configuration UI | `sumeh config --port 8080` |

## 📊 Dashboard

Optional Streamlit dashboard for interactive validation:

```bash
# Install dashboard dependencies
pip install sumeh[dashboard]

# Launch dashboard
sumeh dashboard --port 8501
```

## 🔧 Advanced Usage

### Custom Rule Development
```python
from sumeh.core.rules import RuleDef, RuleRegistry

# Check available rules
print(RuleRegistry.list_rules())

# Get rule details
rule_info = RuleRegistry.get_rule("is_complete")
print(rule_info["description"])

# Check engine compatibility
print(RuleRegistry.is_rule_supported("has_pattern", "duckdb"))  # True
```

### Performance Optimization
```python
# Filter rules by engine compatibility
rules = get_rules_config.csv("rules.csv")
compatible_rules = [r for r in rules if r.is_supported_by_engine("polars")]

# Skip disabled rules
active_rules = [r for r in rules if r.execute]

# Level-specific validation
row_rules = [r for r in rules if r.is_applicable_for_level("ROW")]
table_rules = [r for r in rules if r.is_applicable_for_level("TABLE")]
```

### BigQuery Engine Features

The BigQuery engine is **fully implemented** with advanced SQL generation using **SQLGlot**:

```python
from sumeh import validate
from google.cloud import bigquery

# BigQuery validation with automatic SQL generation
client = bigquery.Client(project="my-project")
table_ref = "my-project.my_dataset.my_table"

# Supports all 60+ validation rules
rules = [
    {"field": "email", "check_type": "has_pattern", "value": r"^[\w.-]+@[\w.-]+\.[a-zA-Z]{2,}$"},
    {"field": "created_at", "check_type": "is_past_date"},
    {"field": "status", "check_type": "is_contained_in", "value": "['active','inactive']"},
    {"field": "revenue", "check_type": "has_mean", "value": 100000, "level": "TABLE"}
]

# Execute validation directly on BigQuery
df_errors, violations, table_summary = validate.bigquery(client, table_ref, rules)
```

## 📈 Roadmap

- ✅ **Dispatcher architecture**: Clean API with engine-specific dispatchers
- ✅ **60+ validation rules** across all engines
- ✅ **Multi-source configuration** (CSV, S3, MySQL, PostgreSQL, BigQuery, etc.)
- ✅ **Type-safe rule definitions** with auto-validation
- ✅ **Schema extraction & validation**
- ✅ **Complete BigQuery engine** implementation with SQLGlot
- ✅ **CLI with Typer**: 7 commands (validate, init, info, rules, schema, sql, config)
- ✅ **Row-level vs Table-level** validation distinction
- ✅ **SQL DDL generators** for 17+ dialects
- ✅ **Web configuration UI** for interactive setup
- 🔧 **Performance optimizations** & caching
- ✅ **Real-time streaming validation** (PySpark Structured Streaming)
- 🔧 **Plugin architecture** for custom engines

## 🤝 Contributing

1. **Fork** the repository
2. **Create** a feature branch: `git checkout -b feature/amazing-feature`
3. **Implement** following existing patterns:
   - Add rules to `manifest.json`
   - Implement in all engines
   - Add comprehensive tests
4. **Test**: `pytest tests/`
5. **Submit** a Pull Request

### Development Setup
```bash
git clone https://github.com/maltzsama/sumeh.git
cd sumeh
poetry install --with dev
poetry run pytest
```

## 📜 License

Licensed under the [Apache License 2.0](LICENSE).

---

**Built with ❤️ by the Sumeh team**