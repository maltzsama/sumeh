"""DuckDB dialect for DDL generation."""

from typing import Dict, Any
from .base import BaseDialect


class DuckDBDialect(BaseDialect):
    """DuckDB-specific DDL generation."""

    def map_type(self, col_def: Dict[str, Any]) -> str:
        """Map generic type to DuckDB type."""
        col_type = col_def["type"].lower()

        type_mapping = {
            "integer": "INTEGER",
            "varchar": (
                f"VARCHAR({col_def.get('length', 255)})"
                if col_def.get("length")
                else "VARCHAR"
            ),
            "text": "TEXT",
            "float": "DOUBLE",
            "boolean": "BOOLEAN",
            "timestamp": "TIMESTAMP",
            "date": "DATE",
        }

        return type_mapping.get(col_type, "VARCHAR")

    def format_default(self, col_def: Dict[str, Any]) -> str:
        """Format default value for DuckDB."""
        default = col_def["default"]

        if default == "current_timestamp":
            return "DEFAULT CURRENT_TIMESTAMP"
        elif isinstance(default, bool):
            return f"DEFAULT {str(default).upper()}"
        elif isinstance(default, (int, float)):
            return f"DEFAULT {default}"
        else:
            return f"DEFAULT '{default}'"

    def map_type(self, col_def: Dict[str, Any]) -> str:
        """Override to handle auto_increment."""
        col_type = col_def["type"].lower()

        # DuckDB auto_increment
        if col_def.get("auto_increment") and col_type == "integer":
            return "INTEGER"

        type_mapping = {
            "integer": "INTEGER",
            "varchar": (
                f"VARCHAR({col_def.get('length', 255)})"
                if col_def.get("length")
                else "VARCHAR"
            ),
            "text": "TEXT",
            "float": "DOUBLE",
            "boolean": "BOOLEAN",
            "timestamp": "TIMESTAMP",
            "date": "DATE",
        }

        return type_mapping.get(col_type, "VARCHAR")

    def _build_column_definition(self, col: Dict[str, Any]) -> str:
        """Build column definition with DuckDB-specific PRIMARY KEY handling."""
        parts = [col["name"], self.map_type(col)]

        if col.get("primary_key"):
            parts.append("PRIMARY KEY")

        if not col.get("nullable", False) and not col.get("primary_key"):
            parts.append("NOT NULL")

        if "default" in col and col["default"] != "current_timestamp":
            parts.append(self.format_default(col))
        elif "default" in col:
            parts.append(self.format_default(col))

        return " ".join(parts)
