"""CLI commands."""

from sumeh.cli.commands.validate import validate
from sumeh.cli.commands.sql import sql
from sumeh.cli.commands.ddl import ddl

__all__ = ["validate", "sql", "ddl"]
