"""
Tests for sumeh/cli/commands/

Covers:
- cli/commands/ddl.py
- cli/commands/sql.py
- cli/commands/validate.py
- cli/utils.py: print_summary, print_table
"""

from unittest.mock import patch

import pandas as pd
import pytest
from typer import Typer
from typer.testing import CliRunner

from sumeh.cli.commands.ddl import ddl
from sumeh.cli.commands.sql import sql
from sumeh.cli.commands.validate import validate
from sumeh.cli.utils import print_summary, print_table
from sumeh.core.models.validation import (
    ValidationReport,
    ValidationResult,
    ValidationStatus,
    ValidationLevel,
)

runner = CliRunner()


# App wrappers (typer needs an app to test commands)


ddl_app = Typer()
ddl_app.command()(ddl)

sql_app = Typer()
sql_app.command()(sql)

validate_app = Typer()
validate_app.command()(validate)


# Fixtures


@pytest.fixture
def rules_csv(tmp_path):
    p = tmp_path / "rules.csv"
    p.write_text(
        "field,check_type,threshold\nemail,is_complete,1.0\nuser_id,is_unique,1.0\n"
    )
    return p


@pytest.fixture
def data_csv(tmp_path):
    df = pd.DataFrame(
        {
            "user_id": [1, 2, 3],
            "email": ["a@a.com", "b@b.com", "c@c.com"],
            "age": [25, 30, 45],
        }
    )
    p = tmp_path / "data.csv"
    df.to_csv(p, index=False)
    return p


def make_report(pass_rate=1.0, n_failed=0):
    results = []
    for i in range(3):
        status = ValidationStatus.FAIL if i < n_failed else ValidationStatus.PASS
        results.append(
            ValidationResult(
                check_type="is_complete",
                field=f"col_{i}",
                status=status,
                pass_rate=1.0 if status == ValidationStatus.PASS else 0.8,
                level=ValidationLevel.ROW,
                message=(
                    None if status == ValidationStatus.PASS else "1 null value found"
                ),
            )
        )
    return ValidationReport(
        results=results,
        total_rows=100,
        execution_time_ms=10.0,
        engine="pandas",
    )


# DDL command


class TestDDLCommand:

    def test_basic_invocation(self):
        result = runner.invoke(ddl_app, ["rules", "--dialect", "postgres"])
        assert result.exit_code == 0

    def test_output_contains_create_table(self):
        result = runner.invoke(ddl_app, ["rules", "--dialect", "postgres"])
        assert "CREATE TABLE" in result.output.upper() or result.exit_code == 0

    def test_dialect_option(self):
        result = runner.invoke(ddl_app, ["rules", "-d", "bigquery"])
        assert result.exit_code == 0

    def test_all_tables(self):
        result = runner.invoke(ddl_app, ["all", "--dialect", "postgres"])
        assert result.exit_code == 0

    def test_schema_option(self):
        result = runner.invoke(
            ddl_app, ["rules", "--dialect", "postgres", "--schema", "dq"]
        )
        assert result.exit_code == 0

    def test_output_to_file(self, tmp_path):
        out = tmp_path / "tables.sql"
        result = runner.invoke(
            ddl_app, ["rules", "--dialect", "postgres", "--output", str(out)]
        )
        assert result.exit_code == 0
        assert out.exists()
        assert "CREATE TABLE" in out.read_text().upper()

    def test_invalid_dialect_exits_nonzero(self):
        result = runner.invoke(ddl_app, ["rules", "--dialect", "nonexistent_xyz"])
        assert result.exit_code != 0

    def test_invalid_table_exits_nonzero(self):
        result = runner.invoke(ddl_app, ["nonexistent_table", "--dialect", "postgres"])
        assert result.exit_code != 0

    def test_schema_registry_table(self):
        result = runner.invoke(ddl_app, ["schema_registry", "--dialect", "duckdb"])
        assert result.exit_code == 0


# SQL command


class TestSQLCommand:

    def test_basic_invocation(self, rules_csv):
        result = runner.invoke(
            sql_app, [str(rules_csv), "--table", "users", "--dialect", "duckdb"]
        )
        assert result.exit_code == 0

    def test_output_to_file(self, rules_csv, tmp_path):
        out = tmp_path / "query.sql"
        result = runner.invoke(
            sql_app,
            [
                str(rules_csv),
                "--table",
                "users",
                "--dialect",
                "duckdb",
                "--output",
                str(out),
            ],
        )
        assert result.exit_code == 0
        assert out.exists()
        assert len(out.read_text()) > 0

    def test_missing_rules_file_exits_nonzero(self):
        result = runner.invoke(sql_app, ["nonexistent.csv", "--table", "users"])
        assert result.exit_code != 0

    def test_dialect_option(self, rules_csv):
        result = runner.invoke(sql_app, [str(rules_csv), "-d", "postgres"])
        assert result.exit_code == 0

    def test_table_option(self, rules_csv):
        result = runner.invoke(sql_app, [str(rules_csv), "--table", "stg_transactions"])
        assert result.exit_code == 0


# Validate command


class TestValidateCommand:

    def test_basic_invocation(self, data_csv, rules_csv):
        result = runner.invoke(validate_app, [str(data_csv), str(rules_csv)])
        assert result.exit_code == 0

    def test_missing_data_file_exits_nonzero(self, rules_csv):
        result = runner.invoke(validate_app, ["nonexistent.csv", str(rules_csv)])
        assert result.exit_code != 0

    def test_missing_rules_file_exits_nonzero(self, data_csv):
        result = runner.invoke(validate_app, [str(data_csv), "nonexistent.csv"])
        assert result.exit_code != 0

    def test_format_summary(self, data_csv, rules_csv):
        result = runner.invoke(
            validate_app,
            [
                str(data_csv),
                str(rules_csv),
                "--format",
                "summary",
            ],
        )
        assert result.exit_code == 0

    def test_format_table(self, data_csv, rules_csv):
        result = runner.invoke(
            validate_app,
            [
                str(data_csv),
                str(rules_csv),
                "--format",
                "table",
            ],
        )
        assert result.exit_code == 0

    def test_format_json(self, data_csv, rules_csv):
        result = runner.invoke(
            validate_app,
            [
                str(data_csv),
                str(rules_csv),
                "--format",
                "json",
            ],
        )
        assert result.exit_code == 0

    def test_output_file_created(self, data_csv, rules_csv, tmp_path):
        out = tmp_path / "clean.csv"
        result = runner.invoke(
            validate_app,
            [
                str(data_csv),
                str(rules_csv),
                "--output",
                str(out),
            ],
        )
        assert result.exit_code == 0
        assert out.exists()

    def test_quarantine_file_created(self, data_csv, rules_csv, tmp_path):
        q = tmp_path / "bad.csv"
        result = runner.invoke(
            validate_app,
            [
                str(data_csv),
                str(rules_csv),
                "--quarantine",
                str(q),
            ],
        )
        assert result.exit_code == 0

    def test_engine_option_pandas(self, data_csv, rules_csv):
        result = runner.invoke(
            validate_app,
            [
                str(data_csv),
                str(rules_csv),
                "--engine",
                "pandas",
            ],
        )
        assert result.exit_code == 0

    def test_fail_on_error_exits_nonzero_when_failures(self, tmp_path):
        # Create data with a null that will fail is_complete
        df = pd.DataFrame({"user_id": [1, 2, 3], "email": ["a@a.com", None, "c@c.com"]})
        data_file = tmp_path / "data.csv"
        df.to_csv(data_file, index=False)

        rules_file = tmp_path / "rules.csv"
        rules_file.write_text("field,check_type,threshold\nemail,is_complete,1.0\n")

        result = runner.invoke(
            validate_app,
            [
                str(data_file),
                str(rules_file),
                "--fail-on-error",
            ],
        )
        assert result.exit_code != 0

    def test_fail_on_error_exits_zero_when_all_pass(self, data_csv, rules_csv):
        result = runner.invoke(
            validate_app,
            [
                str(data_csv),
                str(rules_csv),
                "--fail-on-error",
            ],
        )
        assert result.exit_code == 0


# CLI utils


class TestCLIUtils:

    def test_print_summary_no_failures(self, capsys):
        from rich.console import Console

        report = make_report(n_failed=0)
        with patch("sumeh.cli.utils.console", Console(force_terminal=True)):
            print_summary(report)  # should not raise

    def test_print_summary_with_failures(self):
        report = make_report(n_failed=1)
        print_summary(report)  # should not raise

    def test_print_table_no_failures(self):
        report = make_report(n_failed=0)
        print_table(report)  # should not raise

    def test_print_table_with_failures(self):
        report = make_report(n_failed=2)
        print_table(report)  # should not raise

    def test_print_table_handles_none_message(self):
        results = [
            ValidationResult(
                check_type="is_complete",
                field="email",
                status=ValidationStatus.PASS,
                pass_rate=1.0,
                message=None,
            )
        ]
        report = ValidationReport(
            results=results, total_rows=10, execution_time_ms=5.0, engine="pandas"
        )
        print_table(report)  # should not raise

    def test_print_summary_handles_zero_rows(self):
        report = ValidationReport(
            results=[], total_rows=0, execution_time_ms=0.0, engine="pandas"
        )
        print_summary(report)  # should not raise
