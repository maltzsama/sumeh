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
    html_path = os.path.join(base_dir, "services")

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
    from sumeh.services.sql import SQLGenerator

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
        """
    )

    parser.add_argument(
        "--table",
        "-t",
        choices=["rules", "schema_registry", "all"],
        help="Table to generate DDL for (use 'all' for all tables)"
    )

    parser.add_argument(
        "--dialect",
        "-d",
        help="SQL dialect (postgres, mysql, bigquery, duckdb, athena, sqlite, snowflake, redshift)"
    )

    parser.add_argument(
        "--schema",
        "-s",
        help="Schema/dataset name (optional, depends on dialect)"
    )

    parser.add_argument(
        "--output",
        "-o",
        help="Output file path (if not specified, prints to stdout)"
    )

    parser.add_argument(
        "--list-dialects",
        action="store_true",
        help="List all supported SQL dialects"
    )

    parser.add_argument(
        "--list-tables",
        action="store_true",
        help="List all available tables"
    )

    # BigQuery-specific options
    parser.add_argument(
        "--partition-by",
        help="BigQuery: Partition by expression (e.g., 'DATE(created_at)')"
    )

    parser.add_argument(
        "--cluster-by",
        nargs="+",
        help="BigQuery: Cluster by columns (e.g., table_name environment)"
    )

    # Athena-specific options
    parser.add_argument(
        "--format",
        help="Athena: Storage format (PARQUET, ORC, JSON, etc.)"
    )

    parser.add_argument(
        "--location",
        help="Athena: S3 location for external table"
    )

    # MySQL-specific options
    parser.add_argument(
        "--engine",
        help="MySQL: Storage engine (InnoDB, MyISAM, etc.)"
    )

    # Redshift-specific options
    parser.add_argument(
        "--distkey",
        help="Redshift: Distribution key column"
    )

    parser.add_argument(
        "--sortkey",
        nargs="+",
        help="Redshift: Sort key columns"
    )

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
            table=args.table,
            dialect=args.dialect,
            schema=args.schema,
            **kwargs
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


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Sumeh CLI tools",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Config command (existing)
    subparsers.add_parser(
        "config",
        help="Launch configuration web interface"
    )

    # SQL command (new)
    subparsers.add_parser(
        "sql",
        help="Generate SQL DDL for sumeh tables",
        add_help=False  # We'll handle help in generate_sql()
    )

    args, remaining = parser.parse_known_args()

    if args.command == "config":
        serve_index()
    elif args.command == "sql":
        # Pass remaining args to generate_sql
        sys.argv = ["sumeh-sql"] + remaining
        generate_sql()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()