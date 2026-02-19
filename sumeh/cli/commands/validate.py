"""Validate command - Validate data against rules."""

import typer
from typing import Optional
from pathlib import Path
from enum import Enum
from rich.console import Console

from sumeh.cli.utils import print_summary, print_table

console = Console()


class Engine(str, Enum):
    """Supported DataFrame engines."""

    pandas = "pandas"
    polars = "polars"
    dask = "dask"
    pyspark = "pyspark"
    duckdb = "duckdb"


class OutputFormat(str, Enum):
    """Output formats for reports."""

    json = "json"
    table = "table"
    summary = "summary"


def validate(
    data_file: Path = typer.Argument(
        ..., exists=True, help="Data file (CSV, Parquet, JSON)"
    ),
    rules_file: Path = typer.Argument(..., exists=True, help="Rules file (CSV)"),
    engine: Engine = typer.Option(
        Engine.pandas, "--engine", "-e", help="DataFrame engine"
    ),
    output: Optional[Path] = typer.Option(
        None, "--output", "-o", help="Output file (good data)"
    ),
    quarantine: Optional[Path] = typer.Option(
        None, "--quarantine", "-q", help="Quarantine file (bad data)"
    ),
    format: OutputFormat = typer.Option(
        OutputFormat.summary, "--format", "-f", help="Report format"
    ),
    fail_on_error: bool = typer.Option(
        False, "--fail-on-error", help="Exit with error code if validation fails"
    ),
):
    """
    Validate data file against quality rules.

    Examples:
        sumeh validate data.csv rules.csv
        sumeh validate data.parquet rules.csv --engine polars
        sumeh validate data.csv rules.csv -o clean.csv -q quarantine.csv
    """
    try:
        # Import modules
        from sumeh.core.io import load_data, save_data
        from sumeh.config.csv import load_rules_csv
        import sumeh

        # Load data
        console.print(f"📂 Loading data: [cyan]{data_file}[/cyan]")
        df = load_data(data_file, engine=engine.value)
        console.print(f"✓ Loaded {len(df):,} rows")

        # Load rules
        console.print(f"📋 Loading rules: [cyan]{rules_file}[/cyan]")
        rules = load_rules_csv(rules_file)
        console.print(f"✓ Loaded {len(rules)} rules")

        # Get engine module
        engine_module = getattr(sumeh, engine.value)

        # Validate
        console.print(f"🔍 Running validation...")
        report = engine_module.validate(df, rules)

        # Print report
        if format == OutputFormat.summary:
            print_summary(report)
        elif format == OutputFormat.table:
            print_table(report)
        elif format == OutputFormat.json:
            import json
            from rich import print as rprint

            rprint(json.dumps(report.summary(), indent=2))

        # Save outputs
        if output or quarantine:
            good_df, bad_df = report.split()

            if output:
                console.print(f"💾 Saving clean data: [cyan]{output}[/cyan]")
                save_data(good_df, output, engine=engine.value)
                console.print(f"✓ Saved {len(good_df):,} rows")

            if quarantine:
                console.print(f"💾 Saving quarantine: [cyan]{quarantine}[/cyan]")
                save_data(bad_df, quarantine, engine=engine.value)
                console.print(f"✓ Saved {len(bad_df):,} rows")

        # Exit code
        if fail_on_error and report.pass_rate < 1.0:
            raise typer.Exit(code=1)

    except Exception as e:
        console.print(f"[red]❌ Error: {e}[/red]")
        raise typer.Exit(code=1)
