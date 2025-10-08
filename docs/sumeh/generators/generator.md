# SQL Dialects Usage Guide

Complete reference for generating DDL across all supported databases.

---

## Overview

Sumeh supports 10+ SQL dialects for generating `rules` and `schema_registry` tables.

```bash
# List all available dialects
sumeh sql --list-dialects

# Output:
# Supported SQL dialects:
#   - athena
#   - bigquery
#   - databricks
#   - duckdb
#   - mysql
#   - postgres
#   - redshift
#   - snowflake
#   - sqlite
```

---

## PostgreSQL / PostgreSQL

**Best for:** Traditional relational databases, ACID compliance

```bash
# Basic table
sumeh sql --table rules --dialect postgres

# With schema
sumeh sql --table rules --dialect postgres --schema public

# Both tables
sumeh sql --table all --dialect postgres --schema public

# Save to file
sumeh sql --table all --dialect postgres \
  --schema public \
  --output postgres_ddl.sql
```

**Example output:**

```sql
CREATE TABLE IF NOT EXISTS public.rules (
    id SERIAL PRIMARY KEY,
    environment VARCHAR(50),
    table_name VARCHAR(255),
    field VARCHAR(255),
    check_type VARCHAR(100),
    value TEXT,
    threshold DOUBLE PRECISION,
    execute BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## MySQL

**Best for:** Web applications, high-read workloads

```bash
# Basic table
sumeh sql --table rules --dialect mysql

# With schema and engine
sumeh sql --table rules --dialect mysql \
  --schema mydb \
  --engine InnoDB

# Both tables with custom engine
sumeh sql --table all --dialect mysql \
  --schema dq \
  --engine InnoDB
```

**Example output:**

```sql
CREATE TABLE IF NOT EXISTS mydb.rules (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    environment VARCHAR(50),
    table_name VARCHAR(255),
    field VARCHAR(255),
    check_type VARCHAR(100),
    value TEXT,
    threshold DOUBLE,
    execute BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

---

## BigQuery

**Best for:** Data warehousing, serverless analytics, petabyte-scale

```bash
# Basic table (requires dataset)
sumeh sql --table rules --dialect bigquery --schema mydataset

# With partitioning
sumeh sql --table rules --dialect bigquery \
  --schema dq \
  --partition-by "DATE(created_at)"

# With clustering
sumeh sql --table rules --dialect bigquery \
  --schema dq \
  --cluster-by table_name environment

# Full optimization
sumeh sql --table all --dialect bigquery \
  --schema prod_dq \
  --partition-by "DATE(created_at)" \
  --cluster-by table_name check_type
```

**Example output:**

```sql
CREATE TABLE `mydataset.rules` (
  id INT64,
  environment STRING,
  table_name STRING,
  field STRING,
  check_type STRING,
  value STRING,
  threshold FLOAT64,
  execute BOOL,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
PARTITION BY DATE(created_at)
CLUSTER BY table_name, environment;
```

---

## Databricks

**Best for:** Lakehouse, Delta Lake, Spark workloads

```bash
# Basic table
sumeh sql --table rules --dialect databricks

# With schema
sumeh sql --table rules --dialect databricks --schema default

# With Unity Catalog
sumeh sql --table rules --dialect databricks \
  --catalog prod \
  --schema dq

# With partitioning
sumeh sql --table rules --dialect databricks \
  --schema default \
  --partition-by environment

# With clustering (Z-ordering)
sumeh sql --table rules --dialect databricks \
  --schema default \
  --cluster-by table_name

# External table
sumeh sql --table rules --dialect databricks \
  --schema default \
  --location "s3://my-bucket/rules/"

# Full features
sumeh sql --table all --dialect databricks \
  --catalog prod \
  --schema dq \
  --partition-by environment \
  --cluster-by "table_name, check_type" \
  --location "s3://prod-data/dq/"
```

**Example output:**

```sql
CREATE TABLE IF NOT EXISTS `prod`.`dq`.`rules` (
  `id` BIGINT NOT NULL,
  `environment` STRING,
  `table_name` STRING,
  `field` STRING,
  `check_type` STRING,
  `value` STRING,
  `threshold` DOUBLE,
  `execute` BOOLEAN,
  `created_at` TIMESTAMP,
  `updated_at` TIMESTAMP
)
USING DELTA
PARTITIONED BY (environment)
CLUSTER BY (table_name, check_type)
LOCATION 's3://prod-data/dq/';
```

---

## DuckDB

**Best for:** Embedded analytics, OLAP queries, fast CSV/Parquet processing

```bash
# Basic table
sumeh sql --table rules --dialect duckdb

# With schema
sumeh sql --table rules --dialect duckdb --schema main

# Both tables
sumeh sql --table all --dialect duckdb --schema main

# Save and execute
sumeh sql --table all --dialect duckdb \
  --schema main \
  --output duckdb_schema.sql

duckdb mydb.duckdb < duckdb_schema.sql
```

**Example output:**

```sql
CREATE TABLE IF NOT EXISTS main.rules (
    id INTEGER PRIMARY KEY,
    environment VARCHAR,
    table_name VARCHAR,
    field VARCHAR,
    check_type VARCHAR,
    value VARCHAR,
    threshold DOUBLE,
    execute BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## Athena

**Best for:** Serverless queries on S3, Presto/Trino SQL

```bash
# Basic external table (requires location)
sumeh sql --table rules --dialect athena \
  --schema default \
  --location "s3://my-bucket/rules/" \
  --format PARQUET

# With partitioning
sumeh sql --table rules --dialect athena \
  --schema dq \
  --location "s3://data/rules/" \
  --format PARQUET \
  --partition-by environment

# Both tables with ORC format
sumeh sql --table all --dialect athena \
  --schema prod_dq \
  --location "s3://prod/dq/" \
  --format ORC
```

**Example output:**

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS default.rules (
    id BIGINT,
    environment STRING,
    table_name STRING,
    field STRING,
    check_type STRING,
    value STRING,
    threshold DOUBLE,
    execute BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://my-bucket/rules/';
```

---

## Snowflake

**Best for:** Cloud data warehouse, separation of compute/storage

```bash
# Basic table
sumeh sql --table rules --dialect snowflake

# With schema
sumeh sql --table rules --dialect snowflake --schema PUBLIC

# With database and schema
sumeh sql --table rules --dialect snowflake \
  --database PROD_DB \
  --schema DQ

# Both tables
sumeh sql --table all --dialect snowflake \
  --database ANALYTICS \
  --schema DATA_QUALITY
```

**Example output:**

```sql
CREATE TABLE IF NOT EXISTS PROD_DB.DQ.RULES (
    ID NUMBER AUTOINCREMENT PRIMARY KEY,
    ENVIRONMENT VARCHAR(50),
    TABLE_NAME VARCHAR(255),
    FIELD VARCHAR(255),
    CHECK_TYPE VARCHAR(100),
    VALUE TEXT,
    THRESHOLD FLOAT,
    EXECUTE BOOLEAN DEFAULT TRUE,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

---

## Redshift

**Best for:** Data warehousing, columnar storage, AWS ecosystem

```bash
# Basic table
sumeh sql --table rules --dialect redshift

# With schema
sumeh sql --table rules --dialect redshift --schema public

# With distribution key
sumeh sql --table rules --dialect redshift \
  --schema dq \
  --distkey table_name

# With sort key
sumeh sql --table rules --dialect redshift \
  --schema dq \
  --sortkey created_at

# With both optimizations
sumeh sql --table all --dialect redshift \
  --schema prod_dq \
  --distkey table_name \
  --sortkey created_at updated_at
```

**Example output:**

```sql
CREATE TABLE IF NOT EXISTS prod_dq.rules (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    environment VARCHAR(50),
    table_name VARCHAR(255),
    field VARCHAR(255),
    check_type VARCHAR(100),
    value VARCHAR(MAX),
    threshold DOUBLE PRECISION,
    execute BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT GETDATE(),
    updated_at TIMESTAMP DEFAULT GETDATE()
)
DISTKEY(table_name)
SORTKEY(created_at, updated_at);
```

---

## SQLite

**Best for:** Embedded databases, local development, testing

```bash
# Basic table
sumeh sql --table rules --dialect sqlite

# Both tables
sumeh sql --table all --dialect sqlite

# Save and execute
sumeh sql --table all --dialect sqlite --output schema.sql
sqlite3 mydb.sqlite < schema.sql
```

**Example output:**

```sql
CREATE TABLE IF NOT EXISTS rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    environment TEXT,
    table_name TEXT,
    field TEXT,
    check_type TEXT,
    value TEXT,
    threshold REAL,
    execute INTEGER DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);
```

---

## Comparison Table

| Dialect | Auto-increment | Schema Support | Partitioning | Clustering | External Tables |
|---------|----------------|----------------|--------------|------------|-----------------|
| PostgreSQL | SERIAL | ✅ | ❌ | ❌ | ❌ |
| MySQL | AUTO_INCREMENT | ✅ | ❌ | ❌ | ❌ |
| BigQuery | ❌ | ✅ | ✅ | ✅ | ❌ |
| Databricks | ❌ | ✅ | ✅ | ✅ | ✅ |
| DuckDB | INTEGER | ✅ | ❌ | ❌ | ❌ |
| Athena | ❌ | ✅ | ✅ | ❌ | ✅ |
| Snowflake | AUTOINCREMENT | ✅ | ❌ | ❌ | ✅ |
| Redshift | IDENTITY | ✅ | ❌ | ✅ (SORTKEY) | ✅ |
| SQLite | AUTOINCREMENT | ❌ | ❌ | ❌ | ❌ |

---

## Common Workflows

### 1. Local Development

```bash
# SQLite for quick testing
sumeh sql --table all --dialect sqlite --output local_schema.sql
sqlite3 dev.sqlite < local_schema.sql
```

### 2. Cloud Data Warehouse

```bash
# BigQuery
sumeh sql --table all --dialect bigquery \
  --schema dq \
  --partition-by "DATE(created_at)" \
  --cluster-by table_name environment \
  --output bigquery_schema.sql

# Execute in BigQuery console or bq CLI
bq query < bigquery_schema.sql
```

### 3. Lakehouse Platform

```bash
# Databricks
sumeh sql --table all --dialect databricks \
  --catalog prod \
  --schema data_quality \
  --partition-by environment \
  --cluster-by table_name \
  --output databricks_schema.sql

# Execute in Databricks SQL
```

### 4. Multi-Environment Setup

```bash
# Development (SQLite)
sumeh sql --table all --dialect sqlite > dev_schema.sql

# Staging (PostgreSQL)
sumeh sql --table all --dialect postgres --schema staging_dq > staging_schema.sql

# Production (BigQuery)
sumeh sql --table all --dialect bigquery \
  --schema prod_dq \
  --partition-by "DATE(created_at)" \
  --cluster-by table_name \
  > prod_schema.sql
```

::: sumeh.generators.generator
