"""PostgreSQL dialect for DDL generation."""

from typing import Dict, Any
from .base import BaseDialect


class PostgresDialect(BaseDialect):
    """PostgreSQL-specific DDL generation."""

    def map_type(self, col_def: Dict[str, Any]) -> str:
        """Map generic type to PostgreSQL type."""
        col_type = col_def["type"].lower()

        type_mapping = {
            "integer": "INTEGER",
            "varchar": f"VARCHAR({col_def.get('length', 255)})",
            "text": "TEXT",
            "float": "DOUBLE PRECISION",
            "boolean": "BOOLEAN",
            "timestamp": "TIMESTAMP",
            "date": "DATE",
        }

        return type_mapping.get(col_type, "TEXT")

    def format_default(self, col_def: Dict[str, Any]) -> str:
        """Format default value for PostgreSQL."""
        default = col_def["default"]

        if default == "current_timestamp":
            return "DEFAULT CURRENT_TIMESTAMP"
        elif isinstance(default, bool):
            return f"DEFAULT {str(default).upper()}"
        elif isinstance(default, (int, float)):
            return f"DEFAULT {default}"
        else:
            return f"DEFAULT '{default}'"

    def _auto_increment_keyword(self) -> str:
        """PostgreSQL uses SERIAL for auto increment."""
        return ""  # SERIAL handled in map_type

    def map_type(self, col_def: Dict[str, Any]) -> str:
        """Override to handle SERIAL for auto_increment."""
        col_type = col_def["type"].lower()

        if col_def.get("auto_increment") and col_type == "integer":
            return "SERIAL"

        type_mapping = {
            "integer": "INTEGER",
            "varchar": f"VARCHAR({col_def.get('length', 255)})",
            "text": "TEXT",
            "float": "DOUBLE PRECISION",
            "boolean": "BOOLEAN",
            "timestamp": "TIMESTAMP",
            "date": "DATE",
        }

        return type_mapping.get(col_type, "TEXT")
