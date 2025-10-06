# SQL DDL Generation

Automated SQL DDL generation for sumeh's data quality tables across 8 database platforms.

---

## Overview

Sumeh generates CREATE TABLE statements for two core tables:
- **rules** - Data quality validation rules
- **schema_registry** - Table schema metadata and expected definitions

Supports: PostgreSQL, MySQL, BigQuery, DuckDB, Athena, SQLite, Snowflake, and Redshift.

---

## Quick Start

```bash
# List available options
sumeh-sql --list-dialects
sumeh-sql --list-tables

# Generate DDL
sumeh-sql --table rules --dialect postgres
sumeh-sql --table schema_registry --dialect mysql --schema mydb
sumeh-sql --table all --dialect bigquery --schema mydataset

# Save to file
sumeh-sql --table rules --dialect postgres --output setup.sql
```

---

## Command Reference

```bash
sumeh-sql --table <TABLE> --dialect <DIALECT> [OPTIONS]

Required:
  --table, -t          rules | schema_registry | all
  --dialect, -d        postgres | mysql | bigquery | duckdb | athena | 
                       sqlite | snowflake | redshift

Common Options:
  --schema, -s         Schema/dataset/database name
  --output, -o         Output file (default: stdout)

Platform-Specific:
  --partition-by       BigQuery: Partition expression
  --cluster-by         BigQuery: Clustering columns (space-separated)
  --format             Athena: PARQUET | ORC | JSON | CSV | AVRO
  --location           Athena: S3 path for external table
  --engine             MySQL: InnoDB | MyISAM
  --distkey            Redshift: Distribution key column
  --sortkey            Redshift: Sort key columns (space-separated)
```

---

## Table Schemas

### Rules Table

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Primary key, auto-increment |
| `environment` | VARCHAR(50) | prod, staging, dev |
| `source_type` | VARCHAR(50) | Source system type |
| `database_name` | VARCHAR(255) | Database name |
| `catalog_name` | VARCHAR(255) | Catalog name (nullable) |
| `schema_name` | VARCHAR(255) | Schema name (nullable) |
| `table_name` | VARCHAR(255) | Target table |
| `field` | VARCHAR(255) | Column to validate |
| `check_type` | VARCHAR(100) | Validation type (is_complete, is_unique, etc.) |
| `value` | TEXT | Expected value/threshold (nullable) |
| `threshold` | FLOAT | Validation threshold 0.0-1.0 (default: 1.0) |
| `execute` | BOOLEAN | Execute this rule (default: true) |
| `created_at` | TIMESTAMP | Creation time |
| `updated_at` | TIMESTAMP | Last update (nullable) |

### Schema Registry Table

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Primary key, auto-increment |
| `environment` | VARCHAR(50) | prod, staging, dev |
| `source_type` | VARCHAR(50) | Source system type |
| `database_name` | VARCHAR(255) | Database name |
| `catalog_name` | VARCHAR(255) | Catalog name (nullable) |
| `schema_name` | VARCHAR(255) | Schema name (nullable) |
| `table_name` | VARCHAR(255) | Target table |
| `field` | VARCHAR(255) | Column name |
| `data_type` | VARCHAR(100) | Expected data type |
| `nullable` | BOOLEAN | Allows NULL |
| `max_length` | INTEGER | Max string length (nullable) |
| `comment` | TEXT | Documentation (nullable) |
| `created_at` | TIMESTAMP | Creation time |
| `updated_at` | TIMESTAMP | Last update (nullable) |

---

## Platform Examples

### PostgreSQL

```bash
sumeh-sql --table rules --dialect postgres --schema public
```

```sql
CREATE TABLE public.rules (
  id SERIAL NOT NULL,
  environment VARCHAR(50) NOT NULL,
  table_name VARCHAR(255) NOT NULL,
  field VARCHAR(255) NOT NULL,
  check_type VARCHAR(100) NOT NULL,
  threshold DOUBLE PRECISION DEFAULT 1.0,
  execute BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
);
```

### MySQL

```bash
sumeh-sql --table rules --dialect mysql --schema mydb --engine InnoDB
```

```sql
CREATE TABLE mydb.rules (
  id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  environment VARCHAR(50) NOT NULL,
  table_name VARCHAR(255) NOT NULL,
  threshold DOUBLE DEFAULT 1.0,
  execute BOOLEAN DEFAULT 1,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

### BigQuery

```bash
sumeh-sql --table rules --dialect bigquery --schema mydataset \
  --partition-by "DATE(created_at)" --cluster-by table_name environment
```

```sql
CREATE TABLE `mydataset.rules` (
  id INT64 NOT NULL,
  environment STRING NOT NULL,
  table_name STRING NOT NULL,
  threshold FLOAT64,
  execute BOOL,
  created_at TIMESTAMP
)
PARTITION BY DATE(created_at)
CLUSTER BY table_name, environment;
```

**Note:** BigQuery doesn't support AUTO_INCREMENT or DEFAULT values in DDL. Handle in application code.

### Athena

```bash
sumeh-sql --table rules --dialect athena --schema my_database \
  --format PARQUET --location "s3://bucket/rules/"
```

```sql
CREATE EXTERNAL TABLE my_database.rules (
  id INT,
  environment STRING,
  table_name STRING,
  threshold DOUBLE,
  created_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://bucket/rules/';
```

**Note:** No constraints supported. Add partitions with `MSCK REPAIR TABLE` or Glue Crawler.

### DuckDB / SQLite

```bash
# DuckDB - analytics workloads
sumeh-sql --table rules --dialect duckdb

# SQLite - testing/development
sumeh-sql --table rules --dialect sqlite
```

Both produce standard SQL with full constraint support.

### Snowflake

```bash
sumeh-sql --table rules --dialect snowflake --schema DQ_SCHEMA
```

```sql
CREATE TABLE DQ_SCHEMA.rules (
  id NUMBER(38,0) AUTOINCREMENT PRIMARY KEY NOT NULL,
  threshold FLOAT DEFAULT 1.0,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### Redshift

```bash
sumeh-sql --table rules --dialect redshift --schema public \
  --distkey table_name --sortkey created_at table_name
```

```sql
CREATE TABLE public.rules (
  id INTEGER IDENTITY(1,1) NOT NULL,
  threshold DOUBLE PRECISION DEFAULT 1.0,
  created_at TIMESTAMP DEFAULT SYSDATE,
  PRIMARY KEY (id)
)
DISTKEY(table_name) SORTKEY(created_at, table_name);
```

---

## Common Workflows

### Initial Setup

```bash
# PostgreSQL production
sumeh-sql --table all --dialect postgres --schema public > setup.sql
psql -d prod_db -f setup.sql

# MySQL with proper encoding
sumeh-sql --table all --dialect mysql --schema dq > setup.sql
mysql -u user -p dq_db < setup.sql

# BigQuery with optimization
sumeh-sql --table all --dialect bigquery --schema dq_dataset \
  --partition-by "DATE(created_at)" \
  --cluster-by environment table_name > setup.sql
bq query < setup.sql
```

### Multi-Environment

```bash
# Generate for each environment
for env in prod staging dev; do
  sumeh-sql --table all --dialect postgres --schema ${env} > ${env}_setup.sql
done

# Or use SQLite for local dev
sumeh-sql --table all --dialect sqlite > local_dev.sql
sqlite3 dev.db < local_dev.sql
```

### Data Lake (Athena + S3)

```bash
# Create external tables
sumeh-sql --table rules --dialect athena \
  --schema dq_catalog \
  --format PARQUET \
  --location "s3://datalake/quality/rules/"

sumeh-sql --table schema_registry --dialect athena \
  --schema dq_catalog \
  --format PARQUET \
  --location "s3://datalake/quality/schema_registry/"
```

### Testing

```bash
# DuckDB for analytics testing
sumeh-sql --table all --dialect duckdb > test.sql
duckdb test.db < test.sql

# SQLite for unit tests
sumeh-sql --table all --dialect sqlite > tests/fixtures/schema.sql
```

---

## Python API

```python
from sumeh.services.sql import SQLGenerator

# Generate DDL
ddl = SQLGenerator.generate(
    table="rules",
    dialect="postgres",
    schema="public"
)

# With platform-specific options
ddl = SQLGenerator.generate(
    table="rules",
    dialect="bigquery",
    schema="mydataset",
    partition_by="DATE(created_at)",
    cluster_by=["table_name", "environment"]
)

# Save to file
with open("setup.sql", "w") as f:
    f.write(ddl)

# List options
print(SQLGenerator.list_dialects())
print(SQLGenerator.list_tables())
```

---

## Best Practices

### Indexing

Add indexes after table creation:

```sql
-- PostgreSQL/MySQL
CREATE INDEX idx_rules_table ON rules(table_name, environment);
CREATE INDEX idx_rules_execute ON rules(execute) WHERE execute = true;

-- BigQuery: Use --cluster-by during generation
-- Redshift: Use --sortkey during generation
```

### Version Control

```bash
# Track DDL in git
sumeh-sql --table all --dialect postgres --schema public > ddl/v0.4.0_schema.sql
git add ddl/
git commit -m "feat: Add v0.4.0 schema DDL"
```

### Schema Organization

```sql
-- PostgreSQL: Use schemas
CREATE SCHEMA data_quality;
sumeh-sql --table all --dialect postgres --schema data_quality

-- MySQL: Use databases
CREATE DATABASE dq_config;
sumeh-sql --table all --dialect mysql --schema dq_config

-- BigQuery: Use datasets
bq mk --dataset mydataset
sumeh-sql --table all --dialect bigquery --schema mydataset
```

---

## Type Mappings

| Generic | Postgres | MySQL | BigQuery | Athena | SQLite | Snowflake | Redshift |
|---------|----------|-------|----------|--------|--------|-----------|----------|
| integer | INTEGER | INT | INT64 | INT | INTEGER | NUMBER(38,0) | INTEGER |
| varchar(n) | VARCHAR(n) | VARCHAR(n) | STRING | STRING | TEXT | VARCHAR(n) | VARCHAR(n) |
| text | TEXT | TEXT | STRING | STRING | TEXT | TEXT | VARCHAR(65535) |
| float | DOUBLE PRECISION | DOUBLE | FLOAT64 | DOUBLE | REAL | FLOAT | DOUBLE PRECISION |
| boolean | BOOLEAN | BOOLEAN | BOOL | BOOLEAN | INTEGER | BOOLEAN | BOOLEAN |
| timestamp | TIMESTAMP | TIMESTAMP | TIMESTAMP | TIMESTAMP | TEXT | TIMESTAMP_NTZ | TIMESTAMP |

---

## Troubleshooting

**BigQuery: Missing IDs**
- BigQuery doesn't support AUTO_INCREMENT
- Generate IDs in application code or use `GENERATE_UUID()`

**Athena: Partitions not found**
- Run `MSCK REPAIR TABLE my_database.rules`
- Or configure AWS Glue Crawler

**MySQL: Character encoding issues**
- Always use: `--engine InnoDB` with UTF8MB4 (default)

**Reserved keywords**
- Quote table names: `"schema_registry"` (PostgreSQL/MySQL) or `` `schema_registry` `` (BigQuery)

---

## Contributing

Add support for new databases by extending `BaseDialect` in `sumeh/services/sql/dialects/`.

See existing implementations:
- `sumeh/services/sql/dialects/postgres.py`
- `sumeh/services/sql/dialects/mysql.py`
- etc.

Add tests in `tests/test_sql_generator.py` and submit a PR.