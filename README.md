[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg?logo=python&logoColor=white)](https://www.python.org/downloads/)
[![Build Status](https://github.com/maltzsama/sumeh/workflows/Publish%20Python%20Package/badge.svg)](https://github.com/maltzsama/sumeh/actions)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg?logo=open-source-initiative&logoColor=white)](https://opensource.org/licenses/Apache-2.0)
[![Coverage](https://codecov.io/gh/maltzsama/sumeh/branch/main/graph/badge.svg)](https://codecov.io/gh/maltzsama/sumeh)
[![Downloads](https://img.shields.io/pypi/dm/sumeh?logo=pypi&logoColor=white)](https://pypi.org/project/sumeh/)
[![PyPI Version](https://img.shields.io/pypi/v/sumeh?color=blue&logo=pypi&logoColor=white)](https://pypi.org/project/sumeh/)
[![Version](https://img.shields.io/badge/version-1.2.0-blue.svg)](https://github.com/maltzsama/sumeh/releases)

# <h1 style="display: flex; align-items: center; gap: 0.5rem;"><img src="https://raw.githubusercontent.com/maltzsama/sumeh/refs/heads/main/docs/img/sumeh.svg" alt="Logo" style="height: 40px; width: auto; vertical-align: middle;" /> <span>Sumeh DQ</span> </h1>

**Unified Data Quality Validation Framework**



*One API. Fifty-plus rules. Fourteen engines. Zero compromise.*

[Documentation](https://maltzsama.github.io/sumeh/) · [PyPI](https://pypi.org/project/sumeh/) · [Changelog](CHANGELOG.md)

</div>

---

## What is Sumeh?

Data quality validation is a solved problem — until you have to run the same checks on Pandas today, migrate to PySpark next quarter, and push results to BigQuery in production. Every engine has its own API, its own quirks, and its own way of breaking.

Sumeh provides a single, consistent interface that compiles to whatever engine is underneath. You define rules once. You run them everywhere.

```python
from sumeh import pandas, polars, duckdb, bigquery
from sumeh.core.rules.rule_model import RuleDefinition

rules = [
    RuleDefinition(field="user_id",  check_type="is_unique",      threshold=1.0),
    RuleDefinition(field="email",    check_type="is_complete",     threshold=1.0),
    RuleDefinition(field="email",    check_type="has_pattern",     value=r"^[\w.-]+@[\w.-]+\.[a-zA-Z]{2,}$"),
    RuleDefinition(field="age",      check_type="is_between",      min_value=18, max_value=120),
    RuleDefinition(field="status",   check_type="is_contained_in", allowed_values=["active","inactive","pending"]),
    RuleDefinition(field="revenue",  check_type="has_mean",        value=50_000.0, threshold=0.1),
]

# These are interchangeable — same rules, same report, different engine underneath
report = pandas.validate(df, rules)
report = polars.validate(df, rules)
report = duckdb.validate(con=con, df="orders", rules=rules)
report = bigquery.validate(client=bq_client, table="project.dataset.orders", rules=rules)

print(f"Pass rate: {report.pass_rate:.2%}")  # 83.33%
print(f"Failed:    {len(report.failed)} / {report.total_rules} rules")

good_df, bad_df = report.split()  # bifurcate clean and quarantine data
```

---

## What Changed in v2.0

v2.0 is a complete rewrite. The architecture is different, the API is different, and there is no dependency on cuallee.

| | v1.x | v2.0 |
|---|---|---|
| **API style** | `validate.pandas(df, rules)` | `from sumeh import pandas; pandas.validate(df, rules)` |
| **Return type** | `(df_errors, violations, table_summary)` tuple | `ValidationReport` object |
| **Rule class** | `RuleDef` | `RuleDefinition` |
| **Engines** | 6 | 14 |
| **SQL generation** | String concatenation | SQLGlot AST — zero injection risk |
| **PySpark bifurcation** | `.collect()` on driver | `fail_condition` Column expressions — never collects |
| **cuallee dependency** | Required | Removed |
| **Open SQL mode** | ❌ | ✅ Generate SQL without executing |
| **Profiler** | ❌ | ✅ Column-level statistics |
| **OpenMetadata exporter** | ❌ | ✅ Zero-SDK payload generation |

> **Migrating from v1.x?** See the [Migration Guide](#migrating-from-v1x) at the bottom.

---

## Table of Contents

- [ Sumeh DQ ](#-sumeh-dq-)
  - [What is Sumeh?](#what-is-sumeh)
  - [What Changed in v2.0](#what-changed-in-v20)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
  - [Quickstart](#quickstart)
  - [Engines](#engines)
    - [Batch DataFrame Engines](#batch-dataframe-engines)
    - [SQL Engines](#sql-engines)
    - [Streaming \& ML Engines](#streaming--ml-engines)
  - [Validation Rules](#validation-rules)
    - [Completeness](#completeness)
    - [Uniqueness](#uniqueness)
    - [Numeric \& Comparison](#numeric--comparison)
    - [Membership](#membership)
    - [Pattern](#pattern)
    - [Date](#date)
    - [Custom SQL](#custom-sql)
    - [Aggregations *(Table-level)*](#aggregations-table-level)
    - [Schema](#schema)
  - [Defining Rules](#defining-rules)
    - [Loading from CSV](#loading-from-csv)
  - [The ValidationReport](#the-validationreport)
  - [Bifurcation](#bifurcation)
  - [Open SQL Mode](#open-sql-mode)
  - [Schema Validation](#schema-validation)
    - [Schema Registry DDL](#schema-registry-ddl)
    - [Extract and Validate](#extract-and-validate)
  - [Data Profiling](#data-profiling)
  - [Rule Sources](#rule-sources)
    - [CSV and S3](#csv-and-s3)
    - [Databases](#databases)
    - [Cloud Catalogs](#cloud-catalogs)
    - [Reusing an existing connection](#reusing-an-existing-connection)
    - [Rules table DDL](#rules-table-ddl)
  - [OpenMetadata Integration](#openmetadata-integration)
  - [SQL DDL Generator](#sql-ddl-generator)
  - [CLI](#cli)
  - [Architecture](#architecture)
    - [Design Decisions](#design-decisions)
  - [Migrating from v1.x](#migrating-from-v1x)
    - [Import pattern](#import-pattern)
    - [Rule class](#rule-class)
    - [Working with results](#working-with-results)
    - [cuallee](#cuallee)
  - [Contributing](#contributing)
  - [License](#license)

---

## Installation

```bash
# Core (Pandas only)
pip install sumeh

# Engine-specific extras
pip install sumeh[pyspark]      # Apache Spark
pip install sumeh[polars]       # Polars
pip install sumeh[duckdb]       # DuckDB
pip install sumeh[bigquery]     # Google BigQuery
pip install sumeh[aws]          # S3 + Pandas
pip install sumeh[mysql]        # MySQL rule storage
pip install sumeh[postgresql]   # PostgreSQL rule storage
pip install sumeh[dashboard]    # Streamlit dashboard

# Everything
pip install sumeh[dev,aws,mysql,postgresql,bigquery,dashboard]
```

**Requirements:** Python 3.10+

---

## Quickstart

```python
from sumeh import pandas as sumeh_pandas
from sumeh.core.rules.rule_model import RuleDefinition
import pandas as pd

df = pd.read_csv("customers.csv")

rules = [
    RuleDefinition(field="customer_id", check_type="is_unique",      threshold=1.0),
    RuleDefinition(field="email",       check_type="is_complete",     threshold=1.0),
    RuleDefinition(field="age",         check_type="is_positive",     threshold=0.99),
    RuleDefinition(field="country",     check_type="is_contained_in", allowed_values=["BR","US","DE","FR"]),
    RuleDefinition(field="revenue",     check_type="has_mean",        value=3_500.0, threshold=0.15),
]

report = sumeh_pandas.validate(df, rules)

# Summary
print(f"Pass rate: {report.pass_rate:.2%}")
for r in report.failed:
    print(f"  ✗ [{r.check_type}] {r.field} — {r.message}")

# Annotated DataFrame (_dq_errors column added per row)
annotated = report.df

# Split into clean and quarantine
good_df, bad_df = report.split()
bad_df.to_parquet("quarantine/customers.parquet")
```

---

## Engines

Sumeh supports **fourteen engines** across three tiers. Every engine exposes the same `validate()` function and returns the same `ValidationReport`.

### Batch DataFrame Engines

| Engine | Import | Bifurcation | Notes |
|--------|--------|:-----------:|-------|
| **Pandas** | `from sumeh import pandas` | ✅ | Boolean mask bifurcation |
| **Polars** | `from sumeh import polars` | ✅ | Rust-powered; `list.len()` bifurcation |
| **PySpark** | `from sumeh import pyspark` | ✅ | `fail_condition` Column expressions — no `.collect()` |
| **Dask** | `from sumeh import dask` | ✅ | Out-of-core parallel computing |

### SQL Engines

All SQL engines share the `sql_core` compiler. Queries are built as SQLGlot AST and compiled to the target dialect at call time.

| Engine | Import | Bifurcation | Notes |
|--------|--------|:-----------:|-------|
| **DuckDB** | `from sumeh import duckdb` | ✅ | Embedded; in-process SQL bifurcation |
| **BigQuery** | `from sumeh import bigquery` | — | Pushes compiled SQL to BQ |
| **Snowflake** | `from sumeh import snowflake` | — | Aggregation mode |
| **Redshift** | `from sumeh import redshift` | — | Aggregation mode |
| **Athena** | `from sumeh import athena` | — | Serverless S3 queries |
| **Trino** | `from sumeh import trino` | — | Distributed SQL federation |
| **Apache Doris** | `from sumeh import doris` | — | Real-time OLAP |
| **Generic SQL** | `from sumeh import sql_core` | — | Query generation without execution |

### Streaming & ML Engines

| Engine | Import | Notes |
|--------|--------|-------|
| **PyFlink** | `from sumeh import pyflink` | Unbounded streams; row-level rules only |
| **Ray Data** | `from sumeh import ray_data` | ML/AI pipelines; GPU acceleration |

> **Streaming note:** Table-level aggregation rules (`has_mean`, `has_cardinality`, etc.) require a full dataset. They are not compatible with unbounded streaming sources.

---

## Validation Rules

Sumeh ships **50+ rules** in 7 categories. Every rule is defined in `sumeh/core/rules/manifest.json` and runs on every engine.

### Completeness

| Rule | Description |
|------|-------------|
| `is_complete` | No null values in column |
| `are_complete` | All specified columns are non-null |

### Uniqueness

| Rule | Description |
|------|-------------|
| `is_unique` | All values in column are distinct |
| `are_unique` | Combination of columns is globally unique |
| `is_primary_key` | Alias for `is_unique` |
| `is_composite_key` | Alias for `are_unique` |

### Numeric & Comparison

| Rule | Description |
|------|-------------|
| `is_positive` | Value > 0 |
| `is_negative` | Value < 0 |
| `is_equal` | Value == `value` |
| `is_equal_than` | Value == another column |
| `is_greater_than` | Value > `value` |
| `is_less_than` | Value < `value` |
| `is_greater_or_equal_than` | Value >= `value` |
| `is_less_or_equal_than` | Value <= `value` |
| `is_between` | `min_value` <= value <= `max_value` |
| `is_in_millions` | Value >= 1,000,000 |
| `is_in_billions` | Value >= 1,000,000,000 |

### Membership

| Rule | Description |
|------|-------------|
| `is_contained_in` / `is_in` | Value is in an allowed set |
| `not_contained_in` / `not_in` | Value is not in a forbidden set |

### Pattern

| Rule | Description |
|------|-------------|
| `has_pattern` | Value matches a regex |
| `is_legit` | Value is non-null and non-whitespace |

### Date

| Rule | Description |
|------|-------------|
| `is_today` | Date equals today |
| `is_yesterday` / `is_t_minus_1` | Date equals T-1 |
| `is_t_minus_2` | Date equals T-2 |
| `is_t_minus_3` | Date equals T-3 |
| `is_past_date` | Date is before today |
| `is_future_date` | Date is after today |
| `is_date_between` | Date within a range |
| `is_date_after` | Date after a reference |
| `is_date_before` | Date before a reference |
| `is_on_weekday` | Date falls on Mon–Fri |
| `is_on_weekend` | Date falls on Sat–Sun |
| `is_on_monday` … `is_on_sunday` | Date falls on a specific day of the week |
| `validate_date_format` | Date string matches expected format |
| `all_date_checks` | Runs the full date validation suite |

### Custom SQL

| Rule | Description |
|------|-------------|
| `satisfies` | Arbitrary SQL condition, e.g. `"age >= 18 AND status != 'banned'"` |

### Aggregations *(Table-level)*

These check a single metric across the full column. A rule passes when the measured value is within `threshold` percent of `value`.

| Rule | Metric |
|------|--------|
| `has_min` | Column minimum |
| `has_max` | Column maximum |
| `has_mean` | Column mean |
| `has_sum` | Column sum |
| `has_std` | Standard deviation |
| `has_cardinality` | Count of distinct values |
| `has_entropy` | Shannon entropy |
| `has_infogain` | Information gain |

### Schema

| Rule | Description |
|------|-------------|
| `validate_schema` | Validates DataFrame structure against a registered schema |

---

## Defining Rules

Rules are `RuleDefinition` dataclasses.

| Field | Type | Description |
|-------|------|-------------|
| `field` | `str \| list[str]` | Column(s) to validate. Use a list for multi-column rules. |
| `check_type` | `str` | Rule identifier from the manifest |
| `threshold` | `float` | For row-level rules: minimum pass rate (0.0–1.0). For aggregations: maximum allowed relative deviation from `value`. Default `1.0`. |
| `value` | `Any` | Expected value for aggregation and pattern rules |
| `min_value` / `max_value` | `Any` | Bounds for `is_between` and range rules |
| `allowed_values` | `list` | Allowed set for membership rules |
| `execute` | `bool` | `False` to skip without removing the rule |

```python
from sumeh.core.rules.rule_model import RuleDefinition

# Row-level: threshold = minimum pass rate across all rows
RuleDefinition(field="email",        check_type="is_complete",     threshold=1.0)
RuleDefinition(field=["name","dob"], check_type="are_complete",    threshold=0.95)
RuleDefinition(field="user_id",      check_type="is_unique",       threshold=1.0)
RuleDefinition(field=["id","date"],  check_type="are_unique",      threshold=1.0)
RuleDefinition(field="age",          check_type="is_positive",     threshold=0.99)
RuleDefinition(field="score",        check_type="is_between",      min_value=0, max_value=100)
RuleDefinition(field="status",       check_type="is_contained_in", allowed_values=["A","B","C"])
RuleDefinition(field="email",        check_type="has_pattern",     value=r"^[\w.-]+@[\w.-]+\.[a-zA-Z]{2,}$")
RuleDefinition(field="created_at",   check_type="is_past_date",    threshold=1.0)
RuleDefinition(field="*",            check_type="satisfies",       value="age >= 18 AND status != 'banned'")

# Table-level: threshold = allowed relative deviation from expected value
# threshold=0.1 → actual metric must be within ±10% of value
RuleDefinition(field="age",          check_type="has_mean",        value=35.0,    threshold=0.10)
RuleDefinition(field="salary",       check_type="has_min",         value=1_000.0, threshold=0.05)
RuleDefinition(field="category",     check_type="has_cardinality", value=5,       threshold=0.0)
```

### Loading from CSV

```csv
field,check_type,threshold,value,execute
customer_id,is_unique,1.0,,true
email,is_complete,1.0,,true
email,has_pattern,1.0,"^[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}$",true
age,is_between,1.0,"[18, 120]",true
status,is_contained_in,1.0,"['active','inactive','pending']",true
"[first_name,last_name]",are_complete,0.95,,true
salary,has_mean,0.1,50000,true
```

```python
from sumeh.config.csv import load_rules_csv
rules = load_rules_csv("rules.csv")
```

Values are automatically parsed to the correct Python type (int, float, list, regex, date range). Multi-column fields use the `"[col1,col2]"` notation.

---

## The ValidationReport

Every `validate()` call returns a `ValidationReport`. This is the core object of v2.0.

```python
report = pandas.validate(df, rules)

# Aggregate metrics
report.pass_rate           # float — fraction of rules that passed
report.total_rules         # int
report.passed              # list[ValidationResult]
report.failed              # list[ValidationResult]

# Per-rule results
for result in report.results:
    result.check_type      # "is_complete"
    result.field           # "email"
    result.status          # ValidationStatus.PASS | FAIL
    result.pass_rate       # 0.973 — 97.3% of rows passed
    result.actual_value    # measured metric
    result.expected_value  # expected metric
    result.message         # "27 null values found in 1000 rows"

# Annotated DataFrame: adds _dq_errors column to each row
annotated_df = report.df

# Lightweight summary dict for APIs and dashboards
metadata = report.summary()
# {
#   "pass_rate": 0.8333,
#   "total_rules": 6,
#   "passed": 5,
#   "failed": 1,
#   "results": [...]
# }
```

---

## Bifurcation

Bifurcation splits the validated dataset into **clean rows** and **quarantine rows** in a single pass. No double scanning, no extra joins.

```python
report = pandas.validate(df, rules)
good_df, bad_df = report.split()

# good_df — original columns, _dq_errors removed
# bad_df  — original columns + _dq_errors (list of failed rule names per row)

bad_df.to_parquet("quarantine/2024-06-01.parquet")
```

The same pattern works across all bifurcation-capable engines:

```python
# Polars
good_df, bad_df = polars.validate(df, rules).split()
# → pl.DataFrame, pl.DataFrame

# PySpark — fully lazy, no .collect(), no driver OOM
report = pyspark.validate(spark, df, rules)
good_df, bad_df = report.split()
good_df.write.parquet("s3://bucket/clean/")
bad_df.write.parquet("s3://bucket/quarantine/")

# Dask
good_df, bad_df = dask.validate(df, rules).split()
# → dask.DataFrame, dask.DataFrame

# DuckDB — bifurcation at the SQL layer
report = duckdb.validate(con=con, df="stg_orders", rules=rules, bifurcate=True)
good_rel, bad_rel = report.split()
# → DuckDBPyRelation, DuckDBPyRelation
good_rel.write_parquet("clean.parquet")
```

> Aggregation rules (`has_mean`, `has_cardinality`, etc.) are table-level checks — they are evaluated and reported, but they have no row-level counterpart and do not affect which rows end up in `bad_df`.

---

## Open SQL Mode

Generate the full validation SQL for any dialect without executing it. Useful for auditing, CI dry-runs, or submitting to an external scheduler.

```python
from sumeh import sql_core
from sumeh.core.rules.rule_model import RuleDefinition

rules = [
    RuleDefinition(field="user_id", check_type="is_unique",   threshold=1.0),
    RuleDefinition(field="email",   check_type="is_complete", threshold=1.0),
    RuleDefinition(field="age",     check_type="has_mean",    value=35.0, threshold=0.1),
]

sql = sql_core.get_validation_sql(
    table="bronze.stg_transactions",
    rules=rules,
    dialect="bigquery",  # "snowflake", "duckdb", "trino", "spark", "postgres", ...
)

print(sql)
# SELECT
#   CAST(COUNT(user_id) AS FLOAT64) / COUNT(*) AS is_unique__user_id,
#   CAST(COUNT(email) AS FLOAT64) / COUNT(*) AS is_complete__email,
#   AVG(age) AS has_mean__age
# FROM bronze.stg_transactions
```

All SQL is built through SQLGlot's AST — no string concatenation, no injection surface, full dialect normalization.

---

## Schema Validation

Validate the structure of a DataFrame against a schema stored in any supported backend.

### Schema Registry DDL

```sql
CREATE TABLE schema_registry (
    id            INTEGER PRIMARY KEY,
    environment   VARCHAR(50),     -- 'prod', 'staging', 'dev'
    source_type   VARCHAR(50),     -- 'bigquery', 'mysql', etc.
    database_name VARCHAR(100),
    catalog_name  VARCHAR(100),    -- Databricks Unity Catalog
    schema_name   VARCHAR(100),    -- PostgreSQL schema
    table_name    VARCHAR(100),
    field         VARCHAR(100),
    data_type     VARCHAR(50),
    nullable      BOOLEAN,
    max_length    INTEGER,
    comment       TEXT,
    created_at    TIMESTAMP,
    updated_at    TIMESTAMP
);
```

### Extract and Validate

```python
from sumeh import extract_schema, validate_schema, get_schema_config
import pandas as pd

df = pd.read_csv("users.csv")

# Extract what the DataFrame actually has
actual_schema = extract_schema.pandas(df)

# Load what it should have
expected_schema = get_schema_config.csv("schema_registry.csv", table="users")
# or: get_schema_config.bigquery(project_id=..., dataset_id=..., table_id="users")
# or: get_schema_config.postgresql(host=..., schema="public", table="users")

# Compare
is_valid, errors = validate_schema.pandas(df, expected_schema)

if not is_valid:
    for field, error in errors:
        print(f"  ✗ {field}: {error}")
```

Available for all engines: `extract_schema.{engine}()` and `validate_schema.{engine}()` for pandas, polars, pyspark, and duckdb.

---

## Data Profiling

Generate column-level statistics — without writing any validation rules.

```python
from sumeh.core.services.profiler import profile

stats = profile(df)

print(stats["table_stats"])
# { "total_rows": 10000, "columns_count": 12 }

for col, s in stats["column_profiles"].items():
    print(f"{col}: nulls={s['null_count']}, distinct={s['distinct_count']}, mean={s.get('mean')}")
```

The profiler output is directly consumable by the OpenMetadata exporter — see below.

---

## Rule Sources

### CSV and S3

```python
from sumeh.config.csv import load_rules_csv

rules = load_rules_csv("rules.csv", delimiter=";")
rules = load_rules_csv("s3://bucket/rules/prod.csv")
```

### Databases

```python
from sumeh import get_rules_config

# MySQL
rules = get_rules_config.mysql(
    host="localhost", user="root", password="secret",
    database="mydb", table="dq_rules"
)

# PostgreSQL
rules = get_rules_config.postgresql(
    host="localhost", user="postgres", password="secret",
    database="mydb", schema="public", table="dq_rules"
)

# BigQuery
rules = get_rules_config.bigquery(
    project_id="my-project", dataset_id="my-dataset", table_id="dq_rules"
)

# DuckDB
import duckdb
conn = duckdb.connect("warehouse.db")
rules = get_rules_config.duckdb(conn=conn, table="dq_rules")
```

### Cloud Catalogs

```python
# AWS Glue
rules = get_rules_config.glue(
    glue_context=glue_context,
    database_name="my_database",
    table_name="dq_rules"
)

# Databricks Unity Catalog
rules = get_rules_config.databricks(
    spark=spark, catalog="main", schema="default", table="dq_rules"
)
```

### Reusing an existing connection

```python
import psycopg2
conn = psycopg2.connect(host="localhost", user="postgres", password="secret")

rules = get_rules_config.postgresql(
    conn=conn,
    query="SELECT * FROM public.dq_rules WHERE execute = TRUE AND environment = 'prod'"
)
```

### Rules table DDL

```sql
CREATE TABLE dq_rules (
    id            INTEGER PRIMARY KEY,
    environment   VARCHAR(50)  NOT NULL,
    source_type   VARCHAR(50)  NOT NULL,
    database_name VARCHAR(255) NOT NULL,
    catalog_name  VARCHAR(255),
    schema_name   VARCHAR(255),
    table_name    VARCHAR(255) NOT NULL,
    field         VARCHAR(255) NOT NULL,
    level         VARCHAR(100) NOT NULL,   -- 'ROW' or 'TABLE'
    category      VARCHAR(100) NOT NULL,   -- 'completeness', 'uniqueness', ...
    check_type    VARCHAR(100) NOT NULL,
    value         TEXT,
    threshold     FLOAT        DEFAULT 1.0,
    execute       BOOLEAN      DEFAULT TRUE,
    created_at    TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP
);
```

Generate this DDL for any dialect with `sumeh sql generate --table rules --dialect <dialect>`.

---

## OpenMetadata Integration

Export validation results and profiling statistics to OpenMetadata **without the `openmetadata-ingestion` SDK**.

```python
from sumeh.exporters.openmetadata import OpenMetadataExport
from sumeh.core.services.profiler import profile
import requests

exporter = OpenMetadataExport(table_fqn="iceberg.bronze.stg_transactions")

# --- Validation payloads ---
payload = exporter.validation(report)

# payload["definitions"] → list of CreateTestCaseRequest dicts
for definition in payload["definitions"]:
    requests.post(
        f"{om_url}/api/v1/dataQuality/testCases",
        json=definition, headers=auth_headers
    )

# payload["results"] → list of TestCaseResult dicts
for result in payload["results"]:
    fqn = result["test_case_fqn"]
    requests.post(
        f"{om_url}/api/v1/dataQuality/testCases/{fqn}/testCaseResult",
        json=result["payload"], headers=auth_headers
    )

# --- Profiling payload ---
stats = profile(df)
profile_payload = exporter.profile(stats)
requests.put(
    f"{om_url}/api/v1/tables/{table_id}/tableProfile",
    json=profile_payload, headers=auth_headers
)
```

The exporter is pure Python with zero I/O. It generates dicts. You own the HTTP calls, the auth, and the retry logic. Nothing is ever sent without your explicit call.

---

## SQL DDL Generator

Generate `rules` and `schema_registry` table DDL for 17+ SQL dialects.

```python
from sumeh.generators import SQLGenerator

# PostgreSQL
print(SQLGenerator.generate(table="rules", dialect="postgres", schema="public"))

# BigQuery with partitioning and clustering
print(SQLGenerator.generate(
    table="schema_registry",
    dialect="bigquery",
    schema="my_dataset",
    partition_by="DATE(created_at)",
    cluster_by=["table_name", "environment"]
))

# Snowflake with clustering key
print(SQLGenerator.generate(
    table="rules",
    dialect="snowflake",
    cluster_by=["environment", "table_name"]
))

# Redshift with distribution and sort keys
print(SQLGenerator.generate(
    table="rules",
    dialect="redshift",
    distkey="table_name",
    sortkey=["created_at", "environment"]
))

# Transpile SQL between dialects
transpiled = SQLGenerator.transpile(
    "SELECT * FROM users WHERE created_at >= CURRENT_DATE - 7",
    from_dialect="postgres",
    to_dialect="bigquery"
)

# Introspect
print(SQLGenerator.list_dialects())  # ['athena', 'bigquery', 'databricks', 'duckdb', ...]
print(SQLGenerator.list_tables())    # ['rules', 'schema_registry']
```

---

## CLI

```bash
# Validate a file — defaults to Pandas engine
sumeh validate data.csv rules.csv

# Choose engine
sumeh validate data.parquet rules.csv --engine polars
sumeh validate data.csv rules.csv     --engine duckdb --format json

# Save clean and quarantine splits
sumeh validate data.csv rules.csv \
  --output     clean/data.csv     \
  --quarantine quarantine/data.csv

# CI/CD gate — exits with code 1 if any rule fails
sumeh validate data.csv rules.csv --fail-on-error

# Generate validation SQL without executing it
sumeh sql rules.csv --table bronze.orders --dialect bigquery
sumeh sql rules.csv --table bronze.orders --dialect snowflake

# Schema operations
sumeh schema extract  --data data.csv --output schema.json
sumeh schema validate --data data.csv --registry schema_registry.csv

# DDL generation
sumeh ddl generate --table rules           --dialect postgres
sumeh ddl generate --table schema_registry --dialect bigquery

# Rule introspection
sumeh rules list
sumeh rules info is_complete
sumeh rules search "date"
sumeh rules template

# Streamlit dashboard
sumeh dashboard --port 8501

# System info
sumeh info
```

---

## Architecture

```
sumeh/
│
├── core/
│   ├── base/
│   │   └── protocols.py        # IDataFrameValidator, IExporter — engine contracts
│   ├── models/
│   │   ├── validation.py       # ValidationReport, ValidationResult, ValidationStatus
│   │   └── metrics.py          # MetricResult
│   ├── rules/
│   │   ├── manifest.json       # 50+ rule definitions — single source of truth
│   │   └── rule_model.py       # RuleDefinition dataclass
│   ├── logic/
│   │   └── comparators.py      # Constraint classes per category
│   ├── services/
│   │   └── profiler/           # Column-level statistics
│   └── io.py                   # load_data / save_data helpers
│
├── engines/
│   ├── sql_core/               # Shared SQL compilation layer
│   │   ├── analyzers.py        # check_type → SQLGlot AST expression
│   │   ├── compiler.py         # Assembles SELECT from a rule list
│   │   ├── validator.py        # Maps SQL result row → ValidationResult
│   │   └── registry.py         # check_type → (Analyzer, Constraint)
│   │
│   ├── pandas/                 # Boolean mask bifurcation
│   ├── polars/                 # list.len() bifurcation
│   ├── pyspark/                # fail_condition Column expressions — no .collect()
│   ├── dask/                   # Out-of-core parallel
│   ├── duckdb/                 # sql_core + in-process SQL bifurcation
│   ├── bigquery/               # sql_core + BigQuery client
│   ├── snowflake/              # sql_core + Snowflake connector
│   ├── redshift/               # sql_core + Redshift
│   ├── athena/                 # sql_core + Athena
│   ├── trino/                  # sql_core + Trino
│   ├── doris/                  # sql_core + Apache Doris
│   ├── pyflink/                # PyFlink streaming UDF engine
│   └── ray_data/               # Ray Data ML engine
│
├── config/                     # Rule loading backends
├── exporters/
│   └── openmetadata.py         # Zero-SDK OpenMetadata payload generator
├── generators/
│   ├── ddl.py                  # SQL DDL for 17+ dialects
│   └── transpiler.py           # SQLGlot-based dialect transpiler
└── cli/
    └── commands/               # validate, sql, ddl, schema, rules
```

### Design Decisions

**Namespace-first API.** `from sumeh import pandas; pandas.validate(df, rules)` — not `validate(df, rules, engine="pandas")`. The engine is resolved at import time. Errors are immediate and specific. IDEs understand the full call signature. There is no internal string dispatcher routing at runtime.

**Analyzer / Constraint separation.** An `Analyzer` knows how to measure a metric (compute the null rate as a SQLGlot expression, or as a vectorized Pandas operation). A `Constraint` knows how to decide whether that metric satisfies the rule. Both are small, independently testable, and replaceable. Adding a new check type means writing one of each.

**SQLGlot AST for all SQL.** No SQL string concatenation anywhere in the codebase. Every expression is a typed SQLGlot AST node compiled to the target dialect at call time. This eliminates SQL injection, escape sequence bugs, and the silent drift that comes from dialect-specific string templates.

**Fail-condition pattern for PySpark.** Analyzers return `Column` expressions applied lazily across the cluster. `.collect()` is never called — not even for sampling. Calling `.collect()` on a dataset of any real scale is a driver OOM waiting to happen. The full validation result is computed in a single distributed aggregation pass.

**Single-pass bifurcation.** Validation and the good/bad row split happen in one scan. The `_dq_errors` array column is populated per row during the same pass that computes per-rule metrics. The dataset is never read twice.

---

## Migrating from v1.x

### Import pattern

```python
# v1.x
from sumeh import validate, summarize
df_errors, violations, table_summary = validate.pandas(df, rules)

# v2.0
from sumeh import pandas
report = pandas.validate(df, rules)
```

### Rule class

```python
# v1.x
from sumeh.core.rules import RuleDef
rule = RuleDef(field="email", check_type="is_complete", threshold=0.99)

# v2.0
from sumeh.core.rules.rule_model import RuleDefinition
rule = RuleDefinition(field="email", check_type="is_complete", threshold=0.99)
```

### Working with results

```python
# v1.x — unpack 3-tuple
df_errors, violations, table_summary = validate.pandas(df, rules)
summary = summarize.pandas((df_errors, violations, table_summary), rules, len(df))

# v2.0 — everything lives on the report
report   = pandas.validate(df, rules)
summary  = report.summary()          # dict
good_df, bad_df = report.split()     # bifurcation
annotated = report.df                # DataFrame with _dq_errors column
```

### cuallee

The `cuallee` backend has been removed. v2.0 has its own validation engine built from scratch.

---

## Contributing

```bash
git clone https://github.com/maltzsama/sumeh.git
cd sumeh
git checkout develop
poetry install --with dev

# All tests
poetry run pytest

# Engine-specific
poetry run pytest tests/engines/test_pandas.py  -v
poetry run pytest tests/engines/test_polars.py  -v
poetry run pytest tests/engines/test_duckdb.py  -v
poetry run pytest tests/engines/test_pyspark.py -v
```

To add a new rule:

1. Add the definition to `sumeh/core/rules/manifest.json`
2. Implement an `Analyzer` in the target engine's `analyzers.py`
3. Register it in the engine's `registry.py`
4. Write tests

Releases are automated via semantic-release. Merging to `main` with a conventional commit triggers versioning, changelog generation, and PyPI publishing via Trusted Publishers.

---

## License

Licensed under the [Apache License 2.0](LICENSE).

---

<div align="center">
Built by <a href="https://github.com/maltzsama">@maltzsama</a>
</div>