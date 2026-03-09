"""CLI commands."""

from sumeh.cli.commands.ddl import ddl
from sumeh.cli.commands.sql import sql
from sumeh.cli.commands.validate import validate

__all__ = ["validate", "sql", "ddl"]
