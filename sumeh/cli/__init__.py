"""CLI commands for sumeh."""

import os
import webbrowser
import argparse
import sys
from http.server import SimpleHTTPRequestHandler
from socketserver import TCPServer


def serve_index():
    """
    Serves the index.html file for initial configuration and opens it in a browser.

    This function determines the path to the 'index.html' file, changes the
    working directory to the appropriate location, and starts a simple HTTP server
    to serve the file. It also automatically opens the served page in the default
    web browser.

    The server runs on localhost at port 8000. The process continues until
    interrupted by the user (via a KeyboardInterrupt), at which point the server
    shuts down.

    Raises:
        KeyboardInterrupt: If the server is manually interrupted by the user.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(base_dir, "dash")

    os.chdir(html_path)

    port = 8000
    url = f"http://localhost:{port}"

    with TCPServer(("localhost", port), SimpleHTTPRequestHandler) as httpd:
        print(f"Serving index.html at {url}")
        webbrowser.open(url)

        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down server.")
            httpd.server_close()


def generate_sql():
    """
    Generate SQL DDL statements for sumeh tables.

    Supports multiple SQL dialects and can generate DDL for individual tables
    or all tables at once.
    """
    from sumeh.generators import SQLGenerator

    parser = argparse.ArgumentParser(
        description="Generate SQL DDL for sumeh tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  sumeh-sql --table rules --dialect postgres
  sumeh-sql --table schema_registry --dialect mysql --schema myschema
  sumeh-sql --table all --dialect bigquery --schema mydataset
  sumeh-sql --list-dialects
  sumeh-sql --list-tables

Supported dialects:
  postgres, mysql, bigquery, duckdb, athena, sqlite, snowflake, redshift

Supported tables:
  rules, schema_registry, all
        """,
    )

    parser.add_argument(
        "--table",
        "-t",
        choices=["rules", "schema_registry", "all"],
        help="Table to generate DDL for (use 'all' for all tables)",
    )

    parser.add_argument(
        "--dialect",
        "-d",
        help="SQL dialect (postgres, mysql, bigquery, duckdb, athena, sqlite, snowflake, redshift)",
    )

    parser.add_argument(
        "--schema", "-s", help="Schema/dataset name (optional, depends on dialect)"
    )

    parser.add_argument(
        "--output", "-o", help="Output file path (if not specified, prints to stdout)"
    )

    parser.add_argument(
        "--list-dialects", action="store_true", help="List all supported SQL dialects"
    )

    parser.add_argument(
        "--list-tables", action="store_true", help="List all available tables"
    )

    # BigQuery-specific options
    parser.add_argument(
        "--partition-by",
        help="BigQuery: Partition by expression (e.g., 'DATE(created_at)')",
    )

    parser.add_argument(
        "--cluster-by",
        nargs="+",
        help="BigQuery: Cluster by columns (e.g., table_name environment)",
    )

    # Athena-specific options
    parser.add_argument(
        "--format", help="Athena: Storage format (PARQUET, ORC, JSON, etc.)"
    )

    parser.add_argument("--location", help="Athena: S3 location for external table")

    # MySQL-specific options
    parser.add_argument("--engine", help="MySQL: Storage engine (InnoDB, MyISAM, etc.)")

    # Redshift-specific options
    parser.add_argument("--distkey", help="Redshift: Distribution key column")

    parser.add_argument("--sortkey", nargs="+", help="Redshift: Sort key columns")

    args = parser.parse_args()

    # Handle list commands
    if args.list_dialects:
        print("Supported SQL dialects:")
        for dialect in SQLGenerator.list_dialects():
            print(f"  - {dialect}")
        return

    if args.list_tables:
        print("Available tables:")
        for table in SQLGenerator.list_tables():
            print(f"  - {table}")
        print("  - all (generates DDL for all tables)")
        return

    # Validate required arguments
    if not args.table:
        parser.error("--table is required (or use --list-dialects / --list-tables)")

    if not args.dialect:
        parser.error("--dialect is required")

    # Prepare kwargs for dialect-specific options
    kwargs = {}

    if args.partition_by:
        kwargs["partition_by"] = args.partition_by

    if args.cluster_by:
        kwargs["cluster_by"] = args.cluster_by

    if args.format:
        kwargs["format"] = args.format

    if args.location:
        kwargs["location"] = args.location

    if args.engine:
        kwargs["engine"] = args.engine

    if args.distkey:
        kwargs["distkey"] = args.distkey

    if args.sortkey:
        kwargs["sortkey"] = args.sortkey

    try:
        # Generate DDL
        ddl = SQLGenerator.generate(
            table=args.table, dialect=args.dialect, schema=args.schema, **kwargs
        )

        # Output
        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(ddl)
            print(f"DDL written to {args.output}")
        else:
            print(ddl)

    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


def run_validation():
    """Execute validation command."""
    from sumeh.core.io import load_data, save_results
    from sumeh.core import get_rules_config
    from sumeh.core.validation import validate
    from sumeh.core.summarize import summarize
    from sumeh.core.report import generate_markdown_report
    import json

    parser = argparse.ArgumentParser(
        description="Validate data against quality rules",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  sumeh validate data.csv rules.csv
  sumeh validate data.parquet rules.csv --output results.json
  sumeh validate data.csv rules.csv --format html --output report.html
  sumeh validate data.csv rules.csv --dashboard
  sumeh validate data.csv rules.csv --engine polars --verbose
        """
    )

    parser.add_argument(
        "data_source",
        help="Path to data file (CSV, Parquet, JSON, Excel)"
    )

    parser.add_argument(
        "rules_source",
        help="Path to rules file (CSV)"
    )

    parser.add_argument(
        "--output", "-o",
        help="Output file path (default: print to stdout)"
    )

    parser.add_argument(
        "--format", "-f",
        choices=["json", "csv", "html", "markdown"],
        default="json",
        help="Output format (default: json)"
    )

    parser.add_argument(
        "--engine",
        choices=["pandas", "polars", "dask"],
        default="pandas",
        help="DataFrame engine to use (default: pandas)"
    )

    parser.add_argument(
        "--dashboard",
        action="store_true",
        help="Launch interactive Streamlit dashboard"
    )

    parser.add_argument(
        "--fail-on-error",
        action="store_true",
        help="Exit with code 1 if any check fails"
    )

    parser.add_argument(
        "--verbose", "-v",
        action="count",
        default=0,
        help="Increase verbosity (use -vv for more)"
    )

    args = parser.parse_args()

    try:
        # 1. Load data
        if args.verbose:
            print(f"📂 Loading data from: {args.data_source}")

        df = load_data(args.data_source, engine=args.engine)

        if args.verbose:
            print(f"✓ Loaded {len(df)} rows")

        # 2. Load rules
        if args.verbose:
            print(f"📋 Loading rules from: {args.rules_source}")

        rules = get_rules_config(source=args.rules_source)

        if args.verbose:
            print(f"✓ Loaded {len(rules)} rules")

        # 3. Run validation
        if args.verbose:
            print("🔍 Running validation...")

        invalid_raw, invalid_agg = validate(df, rules)

        # 4. Generate summary
        summary = summarize(invalid_raw, rules, total_rows=len(df))

        # 5. Build results
        results = {
            "summary": summary,
            "metadata": {
                "data_source": args.data_source,
                "rules_source": args.rules_source,
                "total_rows": len(df),
                "engine": args.engine
            }
        }

        # 6. Output results (if not launching dashboard)
        if not args.dashboard:
            if args.output:
                save_results(results, args.output, args.format)
                print(f"✓ Results saved to: {args.output}")
            else:
                # Print to stdout
                if args.format == "json":
                    # Convert DataFrame to dict for JSON serialization
                    summary_dict = summary.to_dict(orient="records") if hasattr(summary, 'to_dict') else summary
                    output = {
                        "summary": summary_dict,
                        "metadata": results["metadata"]
                    }
                    print(json.dumps(output, indent=2, default=str))
                elif args.format == "markdown":
                    print(generate_markdown_report(results))
                else:
                    print(summary)

        # 7. Check if failed
        import pandas as pd
        if isinstance(summary, pd.DataFrame):
            failed_checks = (summary["status"] == "FAIL").sum()
        else:
            failed_checks = 0

        if failed_checks > 0 and not args.dashboard:
            print(f"\n⚠️  {failed_checks} check(s) failed", file=sys.stderr)

            if args.fail_on_error:
                sys.exit(1)
        elif not args.dashboard:
            print("\n✓ All checks passed!")

        # 8. Launch dashboard if requested
        if args.dashboard:
            try:
                from sumeh.dash.app import launch_dashboard

                # Pass results to dashboard
                import streamlit.web.cli as stcli
                import tempfile
                from pathlib import Path

                # Save results to temp file
                temp_file = tempfile.NamedTemporaryFile(
                    mode='w',
                    suffix='.json',
                    delete=False
                )

                # Convert DataFrame to dict for JSON
                summary_dict = summary.to_dict(orient="records") if hasattr(summary, 'to_dict') else summary
                json.dump({
                    "summary": summary_dict,
                    "metadata": results["metadata"]
                }, temp_file, default=str)
                temp_file.close()

                # Launch Streamlit with temp file as argument
                dash_path = Path(__file__).parent.parent / "dash" / "app.py"

                print(f"\n🚀 Launching dashboard...")
                print(f"   Dashboard will open in your browser")
                print(f"   Press Ctrl+C to stop\n")

                sys.argv = ["streamlit", "run", str(dash_path), "--", temp_file.name]
                sys.exit(stcli.main())

            except ImportError:
                print("\n⚠️  Dashboard requires streamlit. Install with:", file=sys.stderr)
                print("   poetry install --with dashboard", file=sys.stderr)
                print("   or: pip install streamlit plotly", file=sys.stderr)
                sys.exit(1)

    except FileNotFoundError as e:
        print(f"❌ File not found: {e}", file=sys.stderr)
        sys.exit(1)

    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        if args.verbose >= 2:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Sumeh CLI tools",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    subparsers.add_parser("config", help="Launch configuration web interface")
    subparsers.add_parser("sql", help="Generate SQL DDL", add_help=False)
    subparsers.add_parser("validate", help="Validate data", add_help=False)

    args, remaining = parser.parse_known_args()

    if args.command == "config":
        serve_index()
    elif args.command == "sql":
        sys.argv = ["sumeh-sql"] + remaining
        generate_sql()
    elif args.command == "validate":
        sys.argv = ["sumeh-validate"] + remaining
        run_validation()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
