"""SQL command - Generate SQL validation query."""

from pathlib import Path
from typing import Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.syntax import Syntax

console = Console()


def sql(
    rules_file: Path = typer.Argument(..., exists=True, help="Rules file (CSV)"),
    table: str = typer.Option("users", "--table", "-t", help="Table name"),
    dialect: str = typer.Option("postgres", "--dialect", "-d", help="SQL dialect"),
    output: Optional[Path] = typer.Option(
        None, "--output", "-o", help="Output SQL file"
    ),
):
    """
    Generate SQL validation query from rules.

    Examples:
        sumeh sql rules.csv --dialect bigquery
        sumeh sql rules.csv -t users -d snowflake -o validation.sql
    """
    try:
        from sumeh.config.csv import load_rules_csv
        from sumeh.engines.sql_core.compiler import compile_rules_to_sql

        # Load rules
        console.print(f"📋 Loading rules: [cyan]{rules_file}[/cyan]")
        rules = load_rules_csv(rules_file)
        console.print(f"✓ Loaded {len(rules)} rules")

        # Generate SQL
        console.print(f"🔨 Generating SQL ({dialect})...")
        sql_query, _ = compile_rules_to_sql(rules, table, dialect=dialect)

        # Output
        if output:
            output.write_text(sql_query)
            console.print(f"💾 Saved: [cyan]{output}[/cyan]")
        else:
            rprint("\n[bold]Generated SQL:[/bold]\n")
            syntax = Syntax(sql_query, "sql", theme="monokai")
            console.print(syntax)

    except Exception as e:
        console.print(f"[red]❌ Error: {e}[/red]")
        raise typer.Exit(code=1)
