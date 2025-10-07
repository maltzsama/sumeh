"""Databricks dialect for DDL generation."""

from typing import Dict, Any
from .base import BaseDialect


class DatabricksDialect(BaseDialect):
    """
    Databricks-specific DDL generation with Delta Lake support.

    Features:
        - Delta Lake format (USING DELTA)
        - Unity Catalog support (catalog.schema.table)
        - Partitioning (PARTITIONED BY)
        - Clustering/Z-Ordering (CLUSTER BY)
        - External tables (LOCATION)
        - Table properties (TBLPROPERTIES)

    Examples:
        >>> from sumeh.generators import SQLGenerator
        >>>
        >>> # Basic table
        >>> ddl = SQLGenerator.generate("rules", "databricks")
        >>>
        >>> # With Unity Catalog
        >>> ddl = SQLGenerator.generate(
        ...     "rules",
        ...     "databricks",
        ...     catalog="prod",
        ...     schema="dq"
        ... )
        >>>
        >>> # With partitioning and clustering
        >>> ddl = SQLGenerator.generate(
        ...     "rules",
        ...     "databricks",
        ...     schema="default",
        ...     partition_by="environment",
        ...     cluster_by=["table_name", "check_type"]
        ... )
        >>>
        >>> # External table
        >>> ddl = SQLGenerator.generate(
        ...     "rules",
        ...     "databricks",
        ...     schema="default",
        ...     location="s3://my-bucket/rules/"
        ... )
    """

    def map_type(self, col_def: Dict[str, Any]) -> str:
        """
        Map generic type to Databricks/Spark SQL type.

        Type mappings:
            - integer → BIGINT
            - varchar → STRING
            - text → STRING
            - float → DOUBLE
            - boolean → BOOLEAN
            - timestamp → TIMESTAMP
            - date → DATE

        Args:
            col_def: Column definition with 'type' key

        Returns:
            Databricks SQL type as string

        Examples:
            >>> dialect = DatabricksDialect()
            >>> dialect.map_type({"type": "integer"})
            'BIGINT'
            >>> dialect.map_type({"type": "varchar"})
            'STRING'
        """
        col_type = col_def["type"].lower()

        type_mapping = {
            "integer": "BIGINT",
            "varchar": "STRING",
            "text": "STRING",
            "float": "DOUBLE",
            "boolean": "BOOLEAN",
            "timestamp": "TIMESTAMP",
            "date": "DATE",
        }

        return type_mapping.get(col_type, "STRING")

    def format_default(self, col_def: Dict[str, Any]) -> str:
        """
        Format default value for Databricks.

        Databricks supports DEFAULT clause since DBR 8.0+.

        Args:
            col_def: Column definition with optional 'default' key

        Returns:
            Formatted DEFAULT clause or empty string

        Examples:
            >>> dialect = DatabricksDialect()
            >>> dialect.format_default({"default": "active"})
            "DEFAULT 'active'"
            >>> dialect.format_default({"default": True})
            'DEFAULT TRUE'
            >>> dialect.format_default({"default": 0})
            'DEFAULT 0'
        """
        if "default" not in col_def:
            return ""

        default_val = col_def["default"]

        if default_val is None:
            return ""
        elif isinstance(default_val, str):
            return f"DEFAULT '{default_val}'"
        elif isinstance(default_val, bool):
            return f"DEFAULT {str(default_val).upper()}"
        else:
            return f"DEFAULT {default_val}"

    def _build_column_definition(self, col: Dict[str, Any]) -> str:
        """
        Build column definition for Databricks.

        Includes: name, type, nullability, default, and comment.

        Args:
            col: Column definition dictionary

        Returns:
            Formatted column definition

        Examples:
            >>> dialect = DatabricksDialect()
            >>> col = {
            ...     "name": "id",
            ...     "type": "integer",
            ...     "nullable": False,
            ...     "comment": "Primary key"
            ... }
            >>> dialect._build_column_definition(col)
            "`id` BIGINT NOT NULL COMMENT 'Primary key'"
        """
        parts = [f"`{col['name']}`", self.map_type(col)]

        # Nullable
        if not col.get("nullable", True) or col.get("primary_key"):
            parts.append("NOT NULL")

        # Default value
        default = self.format_default(col)
        if default:
            parts.append(default)

        # Comment
        if col.get("comment"):
            parts.append(f"COMMENT '{col['comment']}'")

        return " ".join(parts)

    def generate_ddl(
            self, table_name: str, columns, schema: str = None, **kwargs
    ) -> str:
        """
        Generate Databricks DDL with Delta Lake features.

        Supports Unity Catalog, partitioning, clustering, and external tables.

        Args:
            table_name: Name of the table
            columns: List of column definitions
            schema: Schema name (optional)
            **kwargs: Additional options:
                - catalog (str): Unity Catalog name
                - partition_by (str|list): Column(s) to partition by
                - cluster_by (str|list): Column(s) to cluster by (Z-ordering)
                - location (str): S3/DBFS path for external table
                - table_comment (str): Table description
                - properties (dict): Custom table properties

        Returns:
            Complete CREATE TABLE DDL statement

        Examples:
            >>> dialect = DatabricksDialect()
            >>> columns = [
            ...     {"name": "id", "type": "integer", "nullable": False},
            ...     {"name": "name", "type": "varchar", "nullable": True}
            ... ]
            >>>
            >>> # Basic table
            >>> ddl = dialect.generate_ddl("users", columns)
            >>> print(ddl)
            CREATE TABLE IF NOT EXISTS `users` (
              `id` BIGINT NOT NULL,
              `name` STRING
            )
            USING DELTA;
            >>>
            >>> # With Unity Catalog and partitioning
            >>> ddl = dialect.generate_ddl(
            ...     "users",
            ...     columns,
            ...     schema="analytics",
            ...     catalog="prod",
            ...     partition_by="created_date",
            ...     cluster_by=["user_id", "status"]
            ... )
            >>> print(ddl)
            CREATE TABLE IF NOT EXISTS `prod`.`analytics`.`users` (
              `id` BIGINT NOT NULL,
              `name` STRING
            )
            USING DELTA
            PARTITIONED BY (created_date)
            CLUSTER BY (user_id, status);
            >>>
            >>> # External table
            >>> ddl = dialect.generate_ddl(
            ...     "users",
            ...     columns,
            ...     schema="default",
            ...     location="s3://my-bucket/users/",
            ...     table_comment="User data table"
            ... )
        """
        # Build full table name with catalog support
        catalog = kwargs.get("catalog")
        if catalog and schema:
            full_table_name = f"`{catalog}`.`{schema}`.`{table_name}`"
        elif schema:
            full_table_name = f"`{schema}`.`{table_name}`"
        else:
            full_table_name = f"`{table_name}`"

        # Build column definitions
        col_definitions = []
        for col in columns:
            col_def = self._build_column_definition(col)
            col_definitions.append(col_def)

        columns_sql = ",\n  ".join(col_definitions)

        # Start DDL
        ddl = f"CREATE TABLE IF NOT EXISTS {full_table_name} (\n  {columns_sql}\n)"

        # Delta Lake format (default in Databricks)
        ddl += "\nUSING DELTA"

        # Partitioning
        if "partition_by" in kwargs:
            partition = kwargs["partition_by"]
            if isinstance(partition, list):
                partition_cols = ", ".join(partition)
            else:
                partition_cols = partition
            ddl += f"\nPARTITIONED BY ({partition_cols})"

        # Clustering (Z-Ordering)
        if "cluster_by" in kwargs:
            cluster = kwargs["cluster_by"]
            if isinstance(cluster, list):
                cluster_cols = ", ".join(cluster)
            else:
                cluster_cols = cluster
            ddl += f"\nCLUSTER BY ({cluster_cols})"

        # External table location
        if "location" in kwargs:
            ddl += f"\nLOCATION '{kwargs['location']}'"

        # Table comment
        if "table_comment" in kwargs:
            ddl += f"\nCOMMENT '{kwargs['table_comment']}'"

        # Table properties (optional)
        if "properties" in kwargs:
            props = kwargs["properties"]
            if isinstance(props, dict):
                props_str = ", ".join([f"'{k}' = '{v}'" for k, v in props.items()])
                ddl += f"\nTBLPROPERTIES ({props_str})"

        ddl += ";"

        return ddl