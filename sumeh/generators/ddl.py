"""
SQL DDL generator using SQLGlot for cross-dialect support.

Generates CREATE TABLE statements for Sumeh config tables.
"""

from typing import List, Optional

import sqlglot


class SQLGenerator:
    """Generates DDL statements for sumeh tables using SQLGlot."""

    TABLE_SCHEMAS = {
        "rules": {
            "id": "INT PRIMARY KEY AUTO_INCREMENT",
            "environment": "VARCHAR(50) NOT NULL",
            "source_type": "VARCHAR(50) NOT NULL",
            "database_name": "VARCHAR(255) NOT NULL",
            "catalog_name": "VARCHAR(255)",
            "schema_name": "VARCHAR(255)",
            "table_name": "VARCHAR(255) NOT NULL",
            "field": "VARCHAR(255) NOT NULL",
            "level": "VARCHAR(100) NOT NULL",
            "category": "VARCHAR(100) NOT NULL",
            "check_type": "VARCHAR(100) NOT NULL",
            "value": "TEXT",
            "threshold": "FLOAT DEFAULT 1.0",
            "execute": "BOOLEAN DEFAULT TRUE",
            "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "updated_at": "TIMESTAMP",
        },
        "schema_registry": {
            "id": "INT PRIMARY KEY AUTO_INCREMENT",
            "environment": "VARCHAR(50) NOT NULL",
            "source_type": "VARCHAR(50) NOT NULL",
            "database_name": "VARCHAR(255) NOT NULL",
            "catalog_name": "VARCHAR(255)",
            "schema_name": "VARCHAR(255)",
            "table_name": "VARCHAR(255) NOT NULL",
            "field": "VARCHAR(255) NOT NULL",
            "data_type": "VARCHAR(100) NOT NULL",
            "nullable": "BOOLEAN DEFAULT TRUE",
            "max_length": "INT",
            "comment": "TEXT",
            "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "updated_at": "TIMESTAMP",
        },
    }

    SUPPORTED_DIALECTS = [
        "athena",
        "bigquery",
        "clickhouse",
        "databricks",
        "duckdb",
        "hive",
        "mysql",
        "oracle",
        "postgres",
        "presto",
        "redshift",
        "snowflake",
        "spark",
        "sqlite",
        "teradata",
        "trino",
        "tsql",
    ]

    @classmethod
    def generate(
        cls, table: str, dialect: str, schema: Optional[str] = None, **kwargs
    ) -> str:
        """
        Generate DDL for a specific table and dialect.

        Args:
            table: 'rules', 'schema_registry', or 'all'
            dialect: SQL dialect name
            schema: Optional schema/dataset name
            **kwargs: Dialect-specific options

        Returns:
            DDL statement(s)
        """
        dialect_lower = dialect.lower()
        dialect_map = {"postgresql": "postgres", "mssql": "tsql", "sqlserver": "tsql"}
        dialect_lower = dialect_map.get(dialect_lower, dialect_lower)

        if dialect_lower not in cls.SUPPORTED_DIALECTS:
            available = ", ".join(sorted(cls.SUPPORTED_DIALECTS))
            raise ValueError(f"Unknown dialect '{dialect}'. Available: {available}")

        tables = list(cls.TABLE_SCHEMAS.keys()) if table == "all" else [table]
        if table != "all" and table not in cls.TABLE_SCHEMAS:
            available = ", ".join(sorted(cls.TABLE_SCHEMAS.keys()))
            raise ValueError(f"Unknown table '{table}'. Available: {available}, all")

        results = [
            cls._generate_table_ddl(tbl, schema, dialect_lower, **kwargs)
            for tbl in tables
        ]
        return "\n\n".join(results)

    @classmethod
    def _generate_table_ddl(
        cls,
        table_name: str,
        schema_name: Optional[str] = None,
        dialect: str = "postgres",
        **kwargs,
    ) -> str:
        """Generate DDL for a single table."""
        columns = cls.TABLE_SCHEMAS[table_name]
        column_defs = [f"{col_name} {col_def}" for col_name, col_def in columns.items()]

        full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
        col_separator = ",\n    "
        base_ddl = f"CREATE TABLE {full_table_name} (\n    {col_separator.join(column_defs)}\n)"

        try:
            parsed = sqlglot.parse_one(base_ddl, read="postgres")
            transpiled = parsed.sql(dialect=dialect, pretty=True)
            return cls._apply_dialect_customizations(
                transpiled, dialect, table_name, schema_name, **kwargs
            )
        except Exception:
            return base_ddl

    @classmethod
    def _apply_dialect_customizations(
        cls,
        ddl: str,
        dialect: str,
        table_name: str,
        schema_name: Optional[str] = None,
        **kwargs,
    ) -> str:
        """Apply dialect-specific customizations."""
        options = []

        if dialect == "bigquery":
            if kwargs.get("partition_by"):
                options.append(f"PARTITION BY {kwargs['partition_by']}")
            if kwargs.get("cluster_by"):
                options.append(f"CLUSTER BY {', '.join(kwargs['cluster_by'])}")

        elif dialect == "snowflake" and kwargs.get("cluster_by"):
            options.append(f"CLUSTER BY ({', '.join(kwargs['cluster_by'])})")

        elif dialect == "redshift":
            if kwargs.get("distkey"):
                options.append(f"DISTKEY({kwargs['distkey']})")
            if kwargs.get("sortkey"):
                options.append(f"SORTKEY({', '.join(kwargs['sortkey'])})")

        elif dialect == "athena":
            if kwargs.get("location"):
                options.append(f"LOCATION '{kwargs['location']}'")
            if kwargs.get("format"):
                options.append(f"STORED AS {kwargs['format']}")

        elif dialect == "mysql" and kwargs.get("engine"):
            options.append(f"ENGINE={kwargs['engine']}")

        return ddl + ("\n" + "\n".join(options) if options else "")

    @classmethod
    def list_dialects(cls) -> List[str]:
        """Return list of supported SQL dialects."""
        return sorted(cls.SUPPORTED_DIALECTS)

    @classmethod
    def list_tables(cls) -> List[str]:
        """Return list of available tables."""
        return sorted(cls.TABLE_SCHEMAS.keys())
