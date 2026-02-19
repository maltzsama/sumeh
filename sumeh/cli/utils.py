"""CLI utility functions."""
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


def print_summary(report):
    """
    Print summary report with Panel.
    
    Args:
        report: ValidationReport instance
    """
    # Header
    header = f"""
[bold]Validation Summary[/bold]

Engine:        {report.engine}
Total Rows:    {report.total_rows:,}
Execution:     {report.execution_time_ms:.0f}ms
Rules:         {len(report.results)}
Passed:        [green]{len(report.passed)}[/green]
Failed:        [red]{len(report.failed)}[/red]
Pass Rate:     {report.pass_rate:.1%}
"""
    console.print(Panel(header, border_style="cyan"))
    
    # Failed rules
    if report.failed:
        console.print("\n[bold red]❌ Failed Rules:[/bold red]\n")
        for result in report.failed:
            console.print(
                f"  • {result.check_type} on [cyan]{result.field}[/cyan]: {result.message}"
            )


def print_table(report):
    """
    Print detailed table of validation results.
    
    Args:
        report: ValidationReport instance
    """
    table = Table(title="Validation Results")
    
    table.add_column("Check", style="cyan")
    table.add_column("Field", style="magenta")
    table.add_column("Status")
    table.add_column("Pass Rate", justify="right")
    table.add_column("Message")
    
    for result in report.results:
        status = "✅" if result.status.value == "PASS" else "❌"
        pass_rate = f"{result.pass_rate:.1%}" if result.pass_rate is not None else "-"
        
        table.add_row(
            result.check_type,
            result.field,
            status,
            pass_rate,
            result.message or ""
        )
    
    console.print(table)