"""AWS Athena dialect for DDL generation."""

from typing import Dict, Any
from .base import BaseDialect


class AthenaDialect(BaseDialect):
    """AWS Athena SQL dialect for DDL generation."""

    def map_type(self, col_def: Dict[str, Any]) -> str:
        """Map generic type to Athena type."""
        col_type = col_def["type"].lower()

        type_mapping = {
            "integer": "INT",
            "varchar": "STRING",  # Athena uses STRING, not VARCHAR
            "text": "STRING",
            "float": "DOUBLE",
            "boolean": "BOOLEAN",
            "timestamp": "TIMESTAMP",
            "date": "DATE",
        }

        return type_mapping.get(col_type, "STRING")

    def format_default(self, col_def: Dict[str, Any]) -> str:
        return ""

    def _build_column_definition(self, col: Dict[str, Any]) -> str:
        """Build column definition for Athena."""
        # Athena doesn't support NOT NULL, PRIMARY KEY, AUTO_INCREMENT, or DEFAULT
        return f"{col['name']} {self.map_type(col)}"

    def generate_ddl(
        self, table_name: str, columns, schema: str = None, **kwargs
    ) -> str:
        """Override to handle Athena-specific syntax."""
        full_table_name = f"{schema}.{table_name}" if schema else table_name

        col_definitions = [self._build_column_definition(col) for col in columns]
        columns_sql = ",\n  ".join(col_definitions)

        ddl = f"CREATE EXTERNAL TABLE {full_table_name} (\n  {columns_sql}\n)"

        # Add storage format
        format_type = kwargs.get("format", "PARQUET")
        ddl += f"\nSTORED AS {format_type}"

        # Add location if provided
        if "location" in kwargs:
            ddl += f"\nLOCATION '{kwargs['location']}'"

        # Add table properties if provided
        if "tblproperties" in kwargs:
            props = ", ".join(
                [f"'{k}'='{v}'" for k, v in kwargs["tblproperties"].items()]
            )
            ddl += f"\nTBLPROPERTIES ({props})"

        ddl += ";"

        return ddl
