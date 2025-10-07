"""Snowflake dialect for DDL generation."""

from typing import Dict, Any
from .base import BaseDialect


class SnowflakeDialect(BaseDialect):
    """Snowflake-specific DDL generation."""

    def map_type(self, col_def: Dict[str, Any]) -> str:
        """Map generic type to Snowflake type."""
        col_type = col_def["type"].lower()

        type_mapping = {
            "integer": "NUMBER(38,0)",
            "varchar": f"VARCHAR({col_def.get('length', 255)})",
            "text": "TEXT",
            "float": "FLOAT",
            "boolean": "BOOLEAN",
            "timestamp": "TIMESTAMP_NTZ",  # No timezone
            "date": "DATE",
        }

        return type_mapping.get(col_type, "VARCHAR")

    def format_default(self, col_def: Dict[str, Any]) -> str:
        """Format default value for Snowflake."""
        default = col_def["default"]

        if default == "current_timestamp":
            return "DEFAULT CURRENT_TIMESTAMP()"
        elif isinstance(default, bool):
            return f"DEFAULT {str(default).upper()}"
        elif isinstance(default, (int, float)):
            return f"DEFAULT {default}"
        else:
            return f"DEFAULT '{default}'"

    def _build_column_definition(self, col: Dict[str, Any]) -> str:
        """Build column definition for Snowflake."""
        parts = [col["name"], self.map_type(col)]

        # Snowflake auto_increment uses AUTOINCREMENT
        if col.get("auto_increment"):
            parts.append("AUTOINCREMENT")

        if not col.get("nullable", False) or col.get("primary_key"):
            parts.append("NOT NULL")

        if col.get("primary_key"):
            parts.append("PRIMARY KEY")

        if "default" in col and not col.get("auto_increment"):
            parts.append(self.format_default(col))

        return " ".join(parts)
