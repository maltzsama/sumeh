# sumeh/cli/__init__.py
"""
Sumeh CLI - Command-line interface for quick file-based operations.

For programmatic use (including database sources), use the Python API directly.
"""

import typer
from typing import Optional
from pathlib import Path
from enum import Enum
from rich.console import Console
from rich.table import Table

app = typer.Typer(
    name="sumeh",
    help="Sumeh Data Quality Framework CLI",
    no_args_is_help=True,
    rich_markup_mode="rich",
)

rules_app = typer.Typer(help="Manage quality rules")
schema_app = typer.Typer(help="Schema operations")

app.add_typer(rules_app, name="rules")
app.add_typer(schema_app, name="schema")

console = Console()


class Engine(str, Enum):
    pandas = "pandas"
    polars = "polars"
    dask = "dask"
    pyspark = "pyspark"
    duckdb = "duckdb"


class OutputFormat(str, Enum):
    json = "json"
    csv = "csv"
    html = "html"
    markdown = "markdown"


# ========================================
# MAIN COMMANDS
# ========================================


@app.command()
def validate(
    data_file: Path = typer.Argument(
        ..., exists=True, help="Data file (CSV, Parquet, JSON, Excel)"
    ),
    rules_file: Path = typer.Argument(..., exists=True, help="Rules file (CSV, JSON)"),
    output: Optional[Path] = typer.Option(
        None, "--output", "-o", help="Output file path"
    ),
    format: OutputFormat = typer.Option(
        OutputFormat.json, "--format", "-f", help="Output format"
    ),
    engine: Engine = typer.Option(
        Engine.pandas, "--engine", "-e", help="DataFrame engine to use"
    ),
    dashboard: bool = typer.Option(
        False, "--dashboard", "-d", help="Launch interactive dashboard"
    ),
    fail_on_error: bool = typer.Option(
        False, "--fail-on-error", help="Exit with code 1 if checks fail"
    ),
    verbose: int = typer.Option(
        0, "--verbose", "-v", count=True, help="Increase verbosity"
    ),
):
    """
    Validate data file against quality rules.

    [bold]Examples:[/bold]
        sumeh validate data.csv rules.csv
        sumeh validate data.parquet rules.csv --engine polars
        sumeh validate data.csv rules.csv --dashboard
        sumeh validate data.csv rules.csv -o results.json --fail-on-error
    """
    from sumeh.core.io import load_data
    from sumeh import get_rules_config, validate as validate_fn, report

    try:
        # Load data
        if verbose:
            console.print(f"📂 Loading data from: [cyan]{data_file}[/cyan]")

        df = load_data(str(data_file), engine=engine.value)

        if verbose:
            console.print(f"✓ Loaded {len(df)} rows")

        # Load rules
        if verbose:
            console.print(f"📋 Loading rules from: [cyan]{rules_file}[/cyan]")

        # Auto-detect format
        if str(rules_file).endswith(".csv"):
            rules = get_rules_config.csv(str(rules_file))
        elif str(rules_file).endswith(".json"):
            rules = get_rules_config.json(str(rules_file))
        else:
            rules = get_rules_config.csv(str(rules_file))  # Default to CSV

        if verbose:
            console.print(f"✓ Loaded {len(rules)} rules")

        # Validate
        if verbose:
            console.print("🔍 Running validation...")

        # Use the report function for complete results
        results = report(df=df, rules=rules)

        # Count failures
        failed_checks = (
            (results["status"] == "FAIL").sum()
            if hasattr(results, "__getitem__")
            else 0
        )

        # Output
        if dashboard:
            console.print("\n🚀 Launching dashboard...")
            # TODO: Implement dashboard launch
            console.print("[yellow]Dashboard not yet implemented[/yellow]")
        elif output:
            # Save results
            if format == OutputFormat.json:
                import json

                output.write_text(
                    json.dumps(results.to_dict(orient="records"), indent=2, default=str)
                )
            elif format == OutputFormat.csv:
                results.to_csv(output, index=False)
            elif format == OutputFormat.html:
                output.write_text(results.to_html(index=False))
            else:
                output.write_text(str(results))

            console.print(f"✓ Results saved to [cyan]{output}[/cyan]")
        else:
            # Print to console
            from rich.table import Table

            table = Table(title="Validation Results")
            for col in results.columns:
                table.add_column(col)

            for _, row in results.head(20).iterrows():
                table.add_row(*[str(v) for v in row])

            console.print(table)

            if len(results) > 20:
                console.print(f"\n[dim]... and {len(results) - 20} more rows[/dim]")

        # Status message
        if failed_checks > 0:
            console.print(f"\n[red]⚠️  {failed_checks} check(s) failed[/red]")
            if fail_on_error:
                raise typer.Exit(1)
        else:
            console.print("\n[green]✓ All checks passed![/green]")

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        if verbose >= 2:
            console.print_exception()
        raise typer.Exit(1)


@app.command()
def sql(
    table: Optional[str] = typer.Option(None, "--table", "-t", help="Table name"),
    dialect: Optional[str] = typer.Option(None, "--dialect", "-d", help="SQL dialect"),
    schema: Optional[str] = typer.Option(
        None, "--schema", "-s", help="Schema/dataset name"
    ),
    output: Optional[Path] = typer.Option(None, "--output", "-o", help="Output file"),
    list_dialects: bool = typer.Option(
        False, "--list-dialects", help="List supported dialects"
    ),
    list_tables: bool = typer.Option(
        False, "--list-tables", help="List available tables"
    ),
):
    """
    Generate SQL DDL for Sumeh system tables.

    [bold]Examples:[/bold]
        sumeh sql --table rules --dialect postgres
        sumeh sql --table all --dialect bigquery --schema mydataset
        sumeh sql --list-dialects
    """
    from sumeh.generators import SQLGenerator

    if list_dialects:
        console.print("Supported SQL dialects:", style="bold")
        for d in SQLGenerator.list_dialects():
            console.print(f"  • {d}")
        return

    if list_tables:
        console.print("Available tables:", style="bold")
        for t in SQLGenerator.list_tables():
            console.print(f"  • {t}")
        console.print("  • all (generates all tables)")
        return

    if not table or not dialect:
        console.print("[red]Error:[/red] Both --table and --dialect are required")
        raise typer.Exit(1)

    try:
        ddl = SQLGenerator.generate(table=table, dialect=dialect, schema=schema)

        if output:
            output.write_text(ddl)
            console.print(f"✓ DDL saved to [cyan]{output}[/cyan]")
        else:
            console.print(ddl)

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)


@app.command()
def init(
    path: Path = typer.Argument(Path("."), help="Project directory"),
):
    """
    Initialize a new Sumeh project with example files.

    Creates:
        - rules.csv (example rules)
        - data/ (directory for data files)
        - README.md (usage guide)

    [bold]Example:[/bold]
        sumeh init
        sumeh init my-project
    """
    try:
        path.mkdir(parents=True, exist_ok=True)

        # Create rules.csv template
        rules_file = path / "rules.csv"
        rules_content = """field,check_type,value,threshold,level
id,is_unique,,1.0,ROW
name,is_complete,,0.95,ROW
age,is_positive,,1.0,ROW
email,has_pattern,^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$,0.9,ROW
salary,has_min,30000,1.0,TABLE
"""
        rules_file.write_text(rules_content)

        # Create data directory
        data_dir = path / "data"
        data_dir.mkdir(exist_ok=True)

        # Create README
        readme_file = path / "README.md"
        readme_content = """# Sumeh Data Quality Project

## Quick Start

1. Add your data files to `data/`
2. Edit `rules.csv` to define quality rules
3. Run validation:
```bash
sumeh validate data/your_file.csv rules.csv
```

## Documentation

See: https://github.com/your-org/sumeh
"""
        readme_file.write_text(readme_content)

        console.print(f"[green]✓[/green] Project initialized at [cyan]{path}[/cyan]")
        console.print("\nCreated files:")
        console.print("  • rules.csv (example rules)")
        console.print("  • data/ (directory for data files)")
        console.print("  • README.md (usage guide)")

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)


@app.command()
def config():
    """
    Launch configuration web interface.

    Opens a simple web UI for exploring rules and generating configurations.
    """
    from .config import serve_index

    serve_index()


@app.command()
def info():
    """Show Sumeh version and available engines."""
    import sumeh

    table = Table(title="Sumeh Installation Info")
    table.add_column("Component", style="cyan")
    table.add_column("Status", style="green")

    table.add_row("Version", sumeh.__version__)

    # Check engines
    engines = []
    for engine_name in ["pandas", "polars", "dask", "pyspark", "duckdb"]:
        try:
            mod = __import__(engine_name)
            engines.append(f"{engine_name} {mod.__version__}")
        except ImportError:
            pass

    table.add_row(
        "Engines", ", ".join(engines) if engines else "[yellow]None installed[/yellow]"
    )

    console.print(table)


# ========================================
# RULES SUB-COMMANDS
# ========================================


@rules_app.command("list")
def rules_list(
    category: Optional[str] = typer.Option(
        None, "--category", "-c", help="Filter by category"
    ),
    level: Optional[str] = typer.Option(
        None, "--level", "-l", help="Filter by level (ROW/TABLE)"
    ),
    engine: Optional[str] = typer.Option(
        None, "--engine", "-e", help="Filter by engine support"
    ),
):
    """List all available quality rules."""
    from sumeh.core.rules.regristry import RuleRegistry

    rules = RuleRegistry.list_rules()

    # Filter
    if category or level or engine:
        filtered = []
        for rule_name in rules:
            rule_def = RuleRegistry.get_rule(rule_name)
            if category and rule_def.get("category") != category:
                continue
            if level and rule_def.get("level") != level:
                continue
            if engine and engine not in rule_def.get("engines", []):
                continue
            filtered.append(rule_name)
        rules = filtered

    table = Table(title=f"Available Rules ({len(rules)})")
    table.add_column("Rule", style="cyan", no_wrap=True)
    table.add_column("Category", style="green")
    table.add_column("Level", style="yellow")

    for rule_name in sorted(rules):
        rule_def = RuleRegistry.get_rule(rule_name)
        table.add_row(
            rule_name,
            rule_def.get("category", "unknown"),
            rule_def.get("level", "unknown"),
        )

    console.print(table)


@rules_app.command("info")
def rules_info(
    rule_name: str = typer.Argument(..., help="Rule name"),
):
    """Show detailed information about a specific rule."""
    from sumeh.core.rules.regristry import RuleRegistry
    from rich.panel import Panel

    rule_def = RuleRegistry.get_rule(rule_name)

    if not rule_def:
        console.print(f"[red]Error:[/red] Rule '{rule_name}' not found")
        console.print("\nUse [cyan]sumeh rules list[/cyan] to see available rules")
        raise typer.Exit(1)

    info = f"""[bold cyan]{rule_name}[/bold cyan]

[bold]Description:[/bold]
{rule_def.get('description', 'No description available')}

[bold]Category:[/bold] {rule_def.get('category', 'unknown')}
[bold]Level:[/bold] {rule_def.get('level', 'unknown')}
[bold]Supported Engines:[/bold] {', '.join(rule_def.get('engines', []))}
"""

    console.print(Panel(info.strip(), title="Rule Information"))


@rules_app.command("search")
def rules_search(
    keyword: str = typer.Argument(..., help="Search keyword"),
):
    """Search for rules by keyword."""
    from sumeh.core.rules.regristry import RuleRegistry

    all_rules = RuleRegistry.list_rules()
    matches = []

    keyword_lower = keyword.lower()

    for rule_name in all_rules:
        rule_def = RuleRegistry.get_rule(rule_name)
        if (
            keyword_lower in rule_name.lower()
            or keyword_lower in rule_def.get("description", "").lower()
            or keyword_lower in rule_def.get("category", "").lower()
        ):
            matches.append(rule_name)

    if matches:
        console.print(
            f"Found [green]{len(matches)}[/green] rule(s) matching '[cyan]{keyword}[/cyan]':"
        )
        for rule in sorted(matches):
            console.print(f"  • {rule}")
    else:
        console.print(f"No rules found matching '[cyan]{keyword}[/cyan]'")


@rules_app.command("template")
def rules_template(
    output: Path = typer.Option("rules.csv", "--output", "-o", help="Output file path"),
    example: bool = typer.Option(
        False, "--example", "-e", help="Include example rules"
    ),
):
    """Generate a rules configuration template CSV."""

    if example:
        template = """field,check_type,value,threshold,level,category
id,is_unique,,1.0,ROW,uniqueness
name,is_complete,,0.95,ROW,completeness
age,is_positive,,1.0,ROW,validity
age,is_between,"[0,120]",1.0,ROW,validity
email,has_pattern,^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$,0.9,ROW,validity
salary,has_min,30000,1.0,TABLE,consistency
salary,has_max,500000,1.0,TABLE,consistency
department,is_contained_in,"[HR,IT,Finance,Sales]",1.0,ROW,validity
"""
    else:
        template = """field,check_type,value,threshold,level,category
"""

    output.write_text(template)
    console.print(f"✓ Template saved to [cyan]{output}[/cyan]")

    if not example:
        console.print("\n[dim]Tip: Use --example to generate with sample rules[/dim]")


# ========================================
# SCHEMA SUB-COMMANDS
# ========================================


@schema_app.command("extract")
def schema_extract(
    data_file: Path = typer.Argument(..., exists=True, help="Data file"),
    output: Optional[Path] = typer.Option(None, "--output", "-o", help="Output file"),
):
    """Extract schema from data file."""
    from sumeh.core.io import load_data
    import json

    try:
        df = load_data(str(data_file))

        # Extract schema (implementation depends on your extract_schema function)
        schema = []
        for col in df.columns:
            schema.append(
                {
                    "field": col,
                    "data_type": str(df[col].dtype),
                    "nullable": df[col].isnull().any(),
                }
            )

        if output:
            output.write_text(json.dumps(schema, indent=2))
            console.print(f"✓ Schema saved to [cyan]{output}[/cyan]")
        else:
            console.print(json.dumps(schema, indent=2))

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)


@schema_app.command("validate")
def schema_validate(
    data_file: Path = typer.Argument(..., exists=True, help="Data file"),
    schema_file: Path = typer.Argument(
        ..., exists=True, help="Schema definition (JSON)"
    ),
):
    """Validate data file against schema definition."""
    from sumeh.core.io import load_data
    from sumeh.core.utils import __compare_schemas as compare_schemas
    import json

    try:
        df = load_data(str(data_file))

        # Extract actual schema
        actual = []
        for col in df.columns:
            actual.append(
                {
                    "field": col,
                    "data_type": str(df[col].dtype),
                    "nullable": df[col].isnull().any(),
                }
            )

        # Load expected schema
        expected = json.loads(schema_file.read_text())

        # Compare
        valid, errors = compare_schemas(actual, expected)

        if valid:
            console.print("[green]✓[/green] Schema validation passed")
        else:
            console.print(
                f"[red]✗[/red] Schema validation failed with {len(errors)} error(s):"
            )
            for error in errors:
                console.print(f"  • {error}")
            raise typer.Exit(1)

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
