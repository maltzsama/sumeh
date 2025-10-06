"""Base dialect class for SQL DDL generation."""

from typing import List, Dict, Any
from abc import ABC, abstractmethod


class BaseDialect(ABC):
    """Abstract base class for SQL dialect implementations."""

    @abstractmethod
    def map_type(self, col_def: Dict[str, Any]) -> str:
        """Map generic type to dialect-specific type."""
        pass

    @abstractmethod
    def format_default(self, col_def: Dict[str, Any]) -> str:
        """Format default value for dialect."""
        pass

    def generate_ddl(
        self,
        table_name: str,
        columns: List[Dict[str, Any]],
        schema: str = None,
        **kwargs,
    ) -> str:
        """
        Generate complete DDL statement.

        Args:
            table_name: Name of the table
            columns: List of column definitions
            schema: Optional schema/dataset name
            **kwargs: Dialect-specific options

        Returns:
            Complete DDL statement
        """
        full_table_name = f"{schema}.{table_name}" if schema else table_name

        col_definitions = []
        primary_keys = []

        for col in columns:
            col_def = self._build_column_definition(col)
            col_definitions.append(col_def)

            if col.get("primary_key"):
                primary_keys.append(col["name"])

        # Add primary key constraint if needed
        if primary_keys and not self._inline_primary_key():
            pk_constraint = f"PRIMARY KEY ({', '.join(primary_keys)})"
            col_definitions.append(pk_constraint)

        columns_sql = ",\n  ".join(col_definitions)

        ddl = f"CREATE TABLE {full_table_name} (\n  {columns_sql}\n);"

        return self._add_table_options(ddl, **kwargs)

    def _build_column_definition(self, col: Dict[str, Any]) -> str:
        """Build a single column definition."""
        parts = [col["name"], self.map_type(col)]

        # Nullability
        if not col.get("nullable", False) or col.get("primary_key"):
            parts.append("NOT NULL")

        # Primary key (inline for some dialects)
        if col.get("primary_key") and self._inline_primary_key():
            parts.append("PRIMARY KEY")

        # Auto increment
        if col.get("auto_increment"):
            parts.append(self._auto_increment_keyword())

        # Default value
        if "default" in col:
            parts.append(self.format_default(col))

        return " ".join(parts)

    def _inline_primary_key(self) -> bool:
        """Whether this dialect uses inline PRIMARY KEY."""
        return False

    def _auto_increment_keyword(self) -> str:
        """Auto increment keyword for this dialect."""
        return "AUTO_INCREMENT"

    def _add_table_options(self, ddl: str, **kwargs) -> str:
        """Add dialect-specific table options."""
        return ddl
