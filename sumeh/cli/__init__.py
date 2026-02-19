"""
Sumeh CLI - Command-line interface.

Clean, modular structure:
    - commands/validate.py  - Validate data against rules
    - commands/sql.py       - Generate SQL validation query
    - commands/ddl.py       - Generate DDL for config tables
    - utils.py              - Helper functions

Usage:
    sumeh validate data.csv rules.csv
    sumeh sql rules.csv --dialect bigquery
    sumeh ddl all --dialect postgres
"""
import typer

from sumeh.cli.commands import validate, sql, ddl

# Create app
app = typer.Typer(
    name="sumeh",
    help="Sumeh Data Quality Framework",
    no_args_is_help=True,
)

# Register commands
app.command()(validate)
app.command()(sql)
app.command()(ddl)


def main():
    """CLI entry point."""
    app()


if __name__ == "__main__":
    main()