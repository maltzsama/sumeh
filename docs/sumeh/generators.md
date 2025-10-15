# SQL Generator

Sumeh's SQL Generator provides cross-dialect DDL generation for creating `rules` and `schema_registry` tables across 17+ database systems using SQLGlot transpilation.

## Overview

The SQLGenerator class automatically generates CREATE TABLE statements optimized for each database dialect, handling differences in:

- **Data types** (VARCHAR vs STRING vs TEXT)
- **Auto-increment** (SERIAL vs AUTO_INCREMENT vs IDENTITY)
- **Constraints** and indexing
- **Dialect-specific optimizations** (partitioning, clustering, distribution keys)

## Supported Dialects

| Dialect | Status | Auto-increment | Schema Support | Advanced Features |
|---------|--------|----------------|----------------|-----------------|
| **PostgreSQL** | ✅ Full | SERIAL | ✅ | Indexes, constraints |
| **MySQL** | ✅ Full | AUTO_INCREMENT | ✅ | Engine selection, charset |
| **BigQuery** | ✅ Full | ❌ | ✅ | Partitioning, clustering |
| **Snowflake** | ✅ Full | AUTOINCREMENT | ✅ | Clustering keys |
| **Databricks** | ✅ Full | ❌ | ✅ | Delta Lake, Z-ordering |
| **Redshift** | ✅ Full | IDENTITY | ✅ | DISTKEY, SORTKEY |
| **DuckDB** | ✅ Full | INTEGER | ✅ | Fast analytics |
| **SQLite** | ✅ Full | AUTOINCREMENT | ❌ | Embedded use |
| **Athena** | ✅ Full | ❌ | ✅ | External tables, S3 |
| **Oracle** | ✅ Full | SEQUENCE | ✅ | Enterprise features |
| **Teradata** | ✅ Full | IDENTITY | ✅ | MPP architecture |
| **Trino/Presto** | ✅ Full | ❌ | ✅ | Federated queries |
| **ClickHouse** | ✅ Full | ❌ | ✅ | Columnar OLAP |
| **Hive** | ✅ Full | ❌ | ✅ | Hadoop ecosystem |
| **Spark SQL** | ✅ Full | ❌ | ✅ | Distributed processing |
| **TSQL** | ✅ Full | IDENTITY | ✅ | SQL Server/Azure SQL |
| **Apache Drill** | ✅ Basic | ❌ | ✅ | Schema-free queries |

## Table Schemas

### Rules Table
Stores data quality validation rules with metadata:

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

### Schema Registry Table
Stores expected schema definitions for validation:

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

## Usage Examples

### Basic Generation
```python
from sumeh.generators import SQLGenerator

# Generate rules table for PostgreSQL
ddl = SQLGenerator.generate(table="rules", dialect="postgres")
print(ddl)

# Generate both tables for MySQL
ddl = SQLGenerator.generate(table="all", dialect="mysql", schema="dq")
print(ddl)
```

### Advanced Features

#### BigQuery with Optimization
```python
# Partitioned and clustered table
ddl = SQLGenerator.generate(
    table="rules",
    dialect="bigquery",
    schema="prod_dq",
    partition_by="DATE(created_at)",
    cluster_by=["table_name", "environment"]
)
```

#### Redshift with Distribution
```python
# Optimized for analytics workload
ddl = SQLGenerator.generate(
    table="rules",
    dialect="redshift",
    schema="analytics",
    distkey="table_name",
    sortkey=["created_at", "environment"]
)
```

#### Databricks Delta Lake
```python
# Unity Catalog with Delta optimizations
ddl = SQLGenerator.generate(
    table="schema_registry",
    dialect="databricks",
    schema="main.dq",
    partition_by="environment",
    cluster_by=["table_name", "field"],
    location="s3://data-lake/schema-registry/"
)
```

## Dialect-Specific Features

### Cloud Data Warehouses
- **BigQuery**: Partitioning, clustering, nested/repeated fields
- **Snowflake**: Clustering keys, time travel, zero-copy cloning
- **Redshift**: Distribution keys, sort keys, columnar compression
- **Databricks**: Delta Lake, Z-ordering, Unity Catalog integration

### Traditional Databases
- **PostgreSQL**: Advanced indexing, constraints, extensions
- **MySQL**: Storage engines (InnoDB, MyISAM), charset selection
- **Oracle**: Sequences, tablespaces, advanced partitioning
- **SQL Server**: Identity columns, filegroups, compression

### Analytics Engines
- **DuckDB**: Columnar storage, vectorized execution
- **ClickHouse**: MergeTree engines, materialized views
- **Athena**: External tables, Parquet/ORC optimization
- **Presto/Trino**: Federated queries, connector-specific optimizations

## CLI Integration

Generate DDL directly from command line:

```bash
# List supported dialects
sumeh sql --list-dialects

# Generate PostgreSQL DDL
sumeh sql --table rules --dialect postgres --schema public

# Generate all tables for BigQuery
sumeh sql --table all --dialect bigquery --schema prod_dq

# Save to file
sumeh sql --table rules --dialect mysql --output schema.sql
```

## API Reference

::: sumeh.generators.SQLGenerator