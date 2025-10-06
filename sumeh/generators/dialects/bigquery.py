"""BigQuery dialect for DDL generation."""

from typing import Dict, Any
from .base import BaseDialect


class BigQueryDialect(BaseDialect):
    """BigQuery-specific DDL generation."""

    def map_type(self, col_def: Dict[str, Any]) -> str:
        """Map generic type to BigQuery type."""
        col_type = col_def["type"].lower()

        type_mapping = {
            "integer": "INT64",
            "varchar": "STRING",
            "text": "STRING",
            "float": "FLOAT64",
            "boolean": "BOOL",
            "timestamp": "TIMESTAMP",
            "date": "DATE",
        }

        return type_mapping.get(col_type, "STRING")

    def format_default(self, col_def: Dict[str, Any]) -> str:
        """Format default value for BigQuery."""
        # BigQuery doesn't support DEFAULT in the same way
        # This is handled via INSERT statements or views
        return ""

    def _build_column_definition(self, col: Dict[str, Any]) -> str:
        """Build column definition for BigQuery."""
        parts = [col["name"], self.map_type(col)]

        # BigQuery uses different nullability syntax
        if col.get("nullable", True) and not col.get("primary_key"):
            # Nullable is default, no need to specify
            pass
        else:
            parts.append("NOT NULL")

        # BigQuery doesn't support auto_increment
        # Use GENERATE_UUID() or row_number in queries

        return " ".join(parts)

    def generate_ddl(
        self, table_name: str, columns, schema: str = None, **kwargs
    ) -> str:
        """Override to handle BigQuery syntax."""
        full_table_name = f"`{schema}.{table_name}`" if schema else f"`{table_name}`"

        col_definitions = []
        for col in columns:
            # Skip auto_increment and primary_key - not supported in BigQuery DDL
            col_def = self._build_column_definition(col)
            col_definitions.append(col_def)

        columns_sql = ",\n  ".join(col_definitions)

        ddl = f"CREATE TABLE {full_table_name} (\n  {columns_sql}\n)"

        # Add partitioning/clustering if specified
        if "partition_by" in kwargs:
            ddl += f"\nPARTITION BY {kwargs['partition_by']}"

        if "cluster_by" in kwargs:
            cluster_cols = ", ".join(kwargs["cluster_by"])
            ddl += f"\nCLUSTER BY {cluster_cols}"

        ddl += ";"

        return ddl
