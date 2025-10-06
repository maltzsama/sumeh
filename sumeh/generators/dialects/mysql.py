"""MySQL dialect for DDL generation."""

from typing import Dict, Any
from .base import BaseDialect


class MySQLDialect(BaseDialect):
    """MySQL-specific DDL generation."""

    def map_type(self, col_def: Dict[str, Any]) -> str:
        """Map generic type to MySQL type."""
        col_type = col_def["type"].lower()

        type_mapping = {
            "integer": "INT",
            "varchar": f"VARCHAR({col_def.get('length', 255)})",
            "text": "TEXT",
            "float": "DOUBLE",
            "boolean": "BOOLEAN",
            "timestamp": "TIMESTAMP",
            "date": "DATE",
        }

        return type_mapping.get(col_type, "TEXT")

    def format_default(self, col_def: Dict[str, Any]) -> str:
        """Format default value for MySQL."""
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
        """MySQL can use inline PRIMARY KEY."""
        return True

    def _add_table_options(self, ddl: str, **kwargs) -> str:
        """Add MySQL-specific table options."""
        engine = kwargs.get("engine", "InnoDB")
        charset = kwargs.get("charset", "utf8mb4")
        collate = kwargs.get("collate", "utf8mb4_unicode_ci")

        options = f" ENGINE={engine} DEFAULT CHARSET={charset} COLLATE={collate}"
        return ddl.replace(");", f"){options};")
