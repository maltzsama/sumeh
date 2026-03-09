"""
PyFlink Table wrapper with DQ capabilities.

Wraps PyFlink Table to add split() method for bifurcation.
"""

from typing import Tuple, Any


class ValidatedFlinkTable:
    """
    Wrapper around PyFlink Table with data quality methods.

    Example:
        >>> validated = ValidatedFlinkTable(table)
        >>> good_table, bad_table = validated.split()
        >>> good_table.execute_insert("clean_topic")
        >>> bad_table.execute_insert("dlq_topic")
    """

    def __init__(self, table: Any):
        """
        Wrap a PyFlink Table with _dq_errors column.

        Args:
            table: PyFlink Table with _dq_errors column
        """
        self._table = table

    def split(self, error_column: str = "_dq_errors") -> Tuple[Any, Any]:
        """
        Split Flink Table into good and bad rows.

        Args:
            error_column: Name of error column (default: _dq_errors)

        Returns:
            (good_table, bad_table) tuple where:
              - good_table: PyFlink Table without errors (no error column)
              - bad_table: PyFlink Table with errors (WITH error column)

        Example:
            >>> good, bad = validated.split()
            >>> good.execute_insert("clean_sink")
            >>> bad.execute_insert("quarantine_sink")
        """
        # Good: no errors (empty array)
        good_table = self._table.filter(f"{error_column} = ARRAY[]")

        # Remove error column from good table
        columns = [
            c for c in self._table.get_schema().get_field_names() if c != error_column
        ]
        good_table = good_table.select(",".join(columns))

        # Bad: has errors (non-empty array)
        bad_table = self._table.filter(f"{error_column} != ARRAY[]")

        return good_table, bad_table

    def to_native(self) -> Any:
        """Return the underlying PyFlink Table."""
        return self._table

    # ========================================================================
    # Proxy common PyFlink Table methods
    # ========================================================================

    def filter(self, condition: str):
        """Proxy to PyFlink filter"""
        return ValidatedFlinkTable(self._table.filter(condition))

    def select(self, *fields):
        """Proxy to PyFlink select"""
        return ValidatedFlinkTable(self._table.select(*fields))

    def where(self, condition: str):
        """Proxy to PyFlink where (alias for filter)"""
        return ValidatedFlinkTable(self._table.where(condition))

    def execute_insert(self, table_path: str, overwrite: bool = False):
        """Proxy to PyFlink execute_insert"""
        return self._table.execute_insert(table_path, overwrite)

    def execute(self):
        """Proxy to PyFlink execute"""
        return self._table.execute()

    def get_schema(self):
        """Proxy to PyFlink get_schema"""
        return self._table.get_schema()

    def print_schema(self):
        """Proxy to PyFlink print_schema"""
        return self._table.print_schema()

    def __repr__(self):
        """Show table info"""
        return f"ValidatedFlinkTable({self._table})"

    def __str__(self):
        """Show underlying table str"""
        return str(self._table)
