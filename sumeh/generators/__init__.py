# sumeh/generators/__init__.py
"""SQL DDL generator using SQLGlot for cross-dialect support."""

from typing import List
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
    def generate(cls, table: str, dialect: str, schema: str = None, **kwargs) -> str:
        """
        Generate DDL for a specific table and dialect using SQLGlot.

        Args:
            table: Table name ('rules', 'schema_registry', or 'all')
            dialect: SQL dialect (postgres, bigquery, snowflake, etc.)
            schema: Optional schema/dataset name
            **kwargs: Additional dialect-specific options

        Returns:
            DDL statement(s) as string

        Raises:
            ValueError: If table or dialect is invalid
        """
        dialect_lower = dialect.lower()

        # Normalize dialect names
        dialect_map = {
            "postgresql": "postgres",
            "mssql": "tsql",
            "sqlserver": "tsql",
        }
        dialect_lower = dialect_map.get(dialect_lower, dialect_lower)

        if dialect_lower not in cls.SUPPORTED_DIALECTS:
            available = ", ".join(sorted(cls.SUPPORTED_DIALECTS))
            raise ValueError(f"Unknown dialect '{dialect}'. Available: {available}")

        # Select tables
        if table == "all":
            tables = list(cls.TABLE_SCHEMAS.keys())
        elif table in cls.TABLE_SCHEMAS:
            tables = [table]
        else:
            available = ", ".join(sorted(cls.TABLE_SCHEMAS.keys()))
            raise ValueError(f"Unknown table '{table}'. Available: {available}, all")

        results = []
        for tbl in tables:
            ddl = cls._generate_table_ddl(
                table_name=tbl, schema_name=schema, dialect=dialect_lower, **kwargs
            )
            results.append(ddl)

        newline = "\n\n"
        return newline.join(results)

    @classmethod
    def _generate_table_ddl(
        cls,
        table_name: str,
        schema_name: str = None,
        dialect: str = "postgres",
        **kwargs,
    ) -> str:
        """Generate DDL for a single table."""
        columns = cls.TABLE_SCHEMAS[table_name]

        # Build column definitions
        column_defs = []
        for col_name, col_def in columns.items():
            column_defs.append(f"{col_name} {col_def}")

        # Build base CREATE TABLE in standard SQL
        full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name

        newline = "\n"
        indent = "    "
        col_separator = f",{newline}{indent}"

        base_ddl = (
            f"CREATE TABLE {full_table_name} ({newline}"
            f"{indent}{col_separator.join(column_defs)}{newline}"
            f")"
        )

        # Parse and transpile using SQLGlot
        try:
            parsed = sqlglot.parse_one(base_ddl, read="postgres")
            transpiled = parsed.sql(dialect=dialect, pretty=True)

            # Apply dialect-specific customizations
            transpiled = cls._apply_dialect_customizations(
                transpiled, dialect, table_name, schema_name, **kwargs
            )

            return transpiled

        except Exception as e:
            # Fallback to base DDL if transpilation fails
            return base_ddl

    @classmethod
    def _apply_dialect_customizations(
        cls, ddl: str, dialect: str, table_name: str, schema_name: str = None, **kwargs
    ) -> str:
        """Apply dialect-specific customizations."""

        newline = "\n"

        # BigQuery customizations
        if dialect == "bigquery":
            options = []

            if kwargs.get("partition_by"):
                partition_expr = kwargs["partition_by"]
                options.append(f"PARTITION BY {partition_expr}")

            if kwargs.get("cluster_by"):
                cluster_cols = ", ".join(kwargs["cluster_by"])
                options.append(f"CLUSTER BY {cluster_cols}")

            if options:
                ddl = ddl + newline + newline.join(options)

        # Snowflake customizations
        elif dialect == "snowflake":
            if kwargs.get("cluster_by"):
                cluster_cols = ", ".join(kwargs["cluster_by"])
                ddl = ddl + f"{newline}CLUSTER BY ({cluster_cols})"

        # Redshift customizations
        elif dialect == "redshift":
            options = []

            if kwargs.get("distkey"):
                distkey = kwargs["distkey"]
                options.append(f"DISTKEY({distkey})")

            if kwargs.get("sortkey"):
                sort_cols = ", ".join(kwargs["sortkey"])
                options.append(f"SORTKEY({sort_cols})")

            if options:
                ddl = ddl + newline + newline.join(options)

        # Athena customizations
        elif dialect == "athena":
            options = []

            if kwargs.get("location"):
                location = kwargs["location"]
                options.append(f"LOCATION '{location}'")

            if kwargs.get("format"):
                format_type = kwargs["format"]
                options.append(f"STORED AS {format_type}")

            if options:
                ddl = ddl + newline + newline.join(options)

        # MySQL customizations
        elif dialect == "mysql":
            if kwargs.get("engine"):
                engine = kwargs["engine"]
                ddl = ddl + f"{newline}ENGINE={engine}"

        return ddl

    @classmethod
    def list_dialects(cls) -> List[str]:
        """Return list of supported SQL dialects."""
        return sorted(cls.SUPPORTED_DIALECTS)

    @classmethod
    def list_tables(cls) -> List[str]:
        """Return list of available tables."""
        return sorted(cls.TABLE_SCHEMAS.keys())

    @classmethod
    def transpile(cls, sql: str, from_dialect: str, to_dialect: str) -> str:
        """
        Transpile SQL from one dialect to another.

        Args:
            sql: SQL statement to transpile
            from_dialect: Source dialect
            to_dialect: Target dialect

        Returns:
            Transpiled SQL statement
        """
        parsed = sqlglot.parse_one(sql, read=from_dialect)
        return parsed.sql(dialect=to_dialect, pretty=True)
