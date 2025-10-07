"""SQLite dialect for DDL generation."""

from typing import Dict, Any
from .base import BaseDialect


class SQLiteDialect(BaseDialect):
    """SQLite-specific DDL generation."""

    def map_type(self, col_def: Dict[str, Any]) -> str:
        """Map generic type to SQLite type."""
        col_type = col_def["type"].lower()

        type_mapping = {
            "integer": "INTEGER",
            "varchar": "TEXT",  # SQLite uses TEXT for all strings
            "text": "TEXT",
            "float": "REAL",
            "boolean": "INTEGER",  # SQLite stores boolean as 0/1
            "timestamp": "TEXT",  # Store as ISO8601 strings
            "date": "TEXT",
        }

        return type_mapping.get(col_type, "TEXT")

    def format_default(self, col_def: Dict[str, Any]) -> str:
        """Format default value for SQLite."""
        default = col_def["default"]

        if default == "current_timestamp":
            return "DEFAULT CURRENT_TIMESTAMP"
        elif isinstance(default, bool):
            return f"DEFAULT {1 if default else 0}"
        elif isinstance(default, (int, float)):
            return f"DEFAULT {default}"
        else:
            return f"DEFAULT '{default}'"

    def _inline_primary_key(self) -> bool:
        """SQLite uses inline PRIMARY KEY."""
        return True

    def _auto_increment_keyword(self) -> str:
        """SQLite uses AUTOINCREMENT."""
        return "AUTOINCREMENT"
