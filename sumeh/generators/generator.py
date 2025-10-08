"""Core SQL DDL generator for sumeh tables."""

from typing import Dict, Any, List
from .dialects import (
    PostgresDialect,
    MySQLDialect,
    BigQueryDialect,
    DuckDBDialect,
    AthenaDialect,
    SQLiteDialect,
    SnowflakeDialect,
    RedshiftDialect,
    DatabricksDialect,
)


class SQLGenerator:
    """Generates DDL statements for sumeh tables across different SQL dialects."""

    DIALECTS = {
        "postgres": PostgresDialect,
        "postgresql": PostgresDialect,
        "mysql": MySQLDialect,
        "bigquery": BigQueryDialect,
        "duckdb": DuckDBDialect,
        "athena": AthenaDialect,
        "sqlite": SQLiteDialect,
        "snowflake": SnowflakeDialect,
        "redshift": RedshiftDialect,
        "databricks": DatabricksDialect,
    }

    TABLE_SCHEMAS = {
        "rules": [
            {
                "name": "id",
                "type": "integer",
                "primary_key": True,
                "auto_increment": True,
            },
            {"name": "environment", "type": "varchar", "length": 50},
            {"name": "source_type", "type": "varchar", "length": 50},
            {"name": "database_name", "type": "varchar", "length": 255},
            {
                "name": "catalog_name",
                "type": "varchar",
                "length": 255,
                "nullable": True,
            },
            {"name": "schema_name", "type": "varchar", "length": 255, "nullable": True},
            {"name": "table_name", "type": "varchar", "length": 255},
            {"name": "field", "type": "varchar", "length": 255},
            {"name": "check_type", "type": "varchar", "length": 100},
            {"name": "value", "type": "text", "nullable": True},
            {"name": "threshold", "type": "float", "default": 1.0},
            {"name": "execute", "type": "boolean", "default": True},
            {"name": "created_at", "type": "timestamp", "default": "current_timestamp"},
            {"name": "updated_at", "type": "timestamp", "nullable": True},
        ],
        "schema_registry": [
            {
                "name": "id",
                "type": "integer",
                "primary_key": True,
                "auto_increment": True,
            },
            {"name": "environment", "type": "varchar", "length": 50},
            {"name": "source_type", "type": "varchar", "length": 50},
            {"name": "database_name", "type": "varchar", "length": 255},
            {
                "name": "catalog_name",
                "type": "varchar",
                "length": 255,
                "nullable": True,
            },
            {"name": "schema_name", "type": "varchar", "length": 255, "nullable": True},
            {"name": "table_name", "type": "varchar", "length": 255},
            {"name": "field", "type": "varchar", "length": 255},
            {"name": "data_type", "type": "varchar", "length": 100},
            {"name": "nullable", "type": "boolean", "default": True},
            {"name": "max_length", "type": "integer", "nullable": True},
            {"name": "comment", "type": "text", "nullable": True},
            {"name": "created_at", "type": "timestamp", "default": "current_timestamp"},
            {"name": "updated_at", "type": "timestamp", "nullable": True},
        ],
    }

    @classmethod
    def generate(cls, table: str, dialect: str, schema: str = None, **kwargs) -> str:
        """
        Generate DDL for a specific table and dialect.

        Args:
            table: Table name ('rules', 'schema_registry', or 'all')
            dialect: SQL dialect (postgres, mysql, bigquery, etc.)
            schema: Optional schema/dataset name
            **kwargs: Additional dialect-specific options

        Returns:
            DDL statement(s) as string

        Raises:
            ValueError: If table or dialect is invalid
        """
        dialect_lower = dialect.lower()

        if dialect_lower not in cls.DIALECTS:
            available = ", ".join(sorted(cls.DIALECTS.keys()))
            raise ValueError(f"Unknown dialect '{dialect}'. Available: {available}")

        if table == "all":
            tables = list(cls.TABLE_SCHEMAS.keys())
        elif table in cls.TABLE_SCHEMAS:
            tables = [table]
        else:
            available = ", ".join(sorted(cls.TABLE_SCHEMAS.keys()))
            raise ValueError(f"Unknown table '{table}'. Available: {available}, all")

        dialect_class = cls.DIALECTS[dialect_lower]()

        results = []
        for tbl in tables:
            ddl = dialect_class.generate_ddl(
                table_name=tbl, columns=cls.TABLE_SCHEMAS[tbl], schema=schema, **kwargs
            )
            results.append(ddl)

        return "\n\n".join(results)

    @classmethod
    def list_dialects(cls) -> List[str]:
        """Return list of supported SQL dialects."""
        return sorted(cls.DIALECTS.keys())

    @classmethod
    def list_tables(cls) -> List[str]:
        """Return list of available tables."""
        return sorted(cls.TABLE_SCHEMAS.keys())
