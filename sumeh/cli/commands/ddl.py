"""DDL command - Generate DDL for config tables."""

from pathlib import Path
from typing import Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.syntax import Syntax

console = Console()


def ddl(
    table: str = typer.Argument(..., help="Table name (rules, schema_registry, all)"),
    dialect: str = typer.Option("postgres", "--dialect", "-d", help="SQL dialect"),
    schema: Optional[str] = typer.Option(None, "--schema", "-s", help="Schema name"),
    output: Optional[Path] = typer.Option(None, "--output", "-o", help="Output file"),
):
    """
    Generate DDL for config tables.

    Examples:
        sumeh ddl rules --dialect bigquery
        sumeh ddl all -d postgres -s dq_framework -o tables.sql
    """
    try:
        from sumeh.generators import SQLGenerator

        console.print(f"🔨 Generating DDL for [cyan]{table}[/cyan] ({dialect})...")

        schema_value: str = schema if schema is not None else "public"
        
        ddl_sql = SQLGenerator.generate(table, dialect, schema_value)


        if output:
            output.write_text(ddl_sql)
            console.print(f"💾 Saved: [cyan]{output}[/cyan]")
        else:
            rprint("\n[bold]Generated DDL:[/bold]\n")
            syntax = Syntax(ddl_sql, "sql", theme="monokai")
            console.print(syntax)

    except Exception as e:
        console.print(f"[red]❌ Error: {e}[/red]")
        raise typer.Exit(code=1)
