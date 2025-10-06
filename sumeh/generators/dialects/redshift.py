"""AWS Redshift dialect for DDL generation."""

from typing import Dict, Any
from .base import BaseDialect


class RedshiftDialect(BaseDialect):
    """AWS Redshift-specific DDL generation."""

    def map_type(self, col_def: Dict[str, Any]) -> str:
        """Map generic type to Redshift type."""
        col_type = col_def["type"].lower()

        type_mapping = {
            "integer": "INTEGER",
            "varchar": f"VARCHAR({col_def.get('length', 255)})",
            "text": "VARCHAR(65535)",  # Redshift max VARCHAR
            "float": "DOUBLE PRECISION",
            "boolean": "BOOLEAN",
            "timestamp": "TIMESTAMP",
            "date": "DATE",
        }

        return type_mapping.get(col_type, "VARCHAR(255)")

    def format_default(self, col_def: Dict[str, Any]) -> str:
        """Format default value for Redshift."""
        default = col_def["default"]

        if default == "current_timestamp":
            return "DEFAULT SYSDATE"
        elif isinstance(default, bool):
            return f"DEFAULT {str(default).upper()}"
        elif isinstance(default, (int, float)):
            return f"DEFAULT {default}"
        else:
            return f"DEFAULT '{default}'"

    def _build_column_definition(self, col: Dict[str, Any]) -> str:
        """Build column definition for Redshift."""
        parts = [col["name"], self.map_type(col)]

        # Redshift uses IDENTITY for auto_increment
        if col.get("auto_increment"):
            parts.append("IDENTITY(1,1)")

        if not col.get("nullable", False) or col.get("primary_key"):
            parts.append("NOT NULL")

        if "default" in col and not col.get("auto_increment"):
            parts.append(self.format_default(col))

        return " ".join(parts)

    def generate_ddl(
        self, table_name: str, columns, schema: str = None, **kwargs
    ) -> str:
        """Override to add Redshift-specific options."""
        ddl = super().generate_ddl(table_name, columns, schema, **kwargs)

        # Add distribution and sort keys if provided
        additions = []

        if "distkey" in kwargs:
            additions.append(f"DISTKEY({kwargs['distkey']})")

        if "sortkey" in kwargs:
            sort_cols = (
                ", ".join(kwargs["sortkey"])
                if isinstance(kwargs["sortkey"], list)
                else kwargs["sortkey"]
            )
            additions.append(f"SORTKEY({sort_cols})")

        if additions:
            ddl = ddl.replace(");", f")\n{' '.join(additions)};")

        return ddl
