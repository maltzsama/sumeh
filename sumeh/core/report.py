"""Report generation functions."""

from typing import Dict, Any
import pandas as pd
from datetime import datetime


def generate_html_report(results: Dict[str, Any]) -> str:
    """
    Generate HTML report from validation results.

    Args:
        results: Dictionary containing validation results

    Returns:
        HTML string
    """
    summary = results.get("summary")

    if not isinstance(summary, pd.DataFrame):
        summary = pd.DataFrame(summary)

    total_checks = len(summary)
    passed = (summary["status"] == "PASS").sum()
    failed = (summary["status"] == "FAIL").sum()
    pass_rate = (passed / total_checks * 100) if total_checks > 0 else 0

    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Data Quality Report</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            max-width: 1200px;
            margin: 40px auto;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
        }}
        .header h1 {{ margin: 0 0 10px 0; }}
        .stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: white;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .stat-card h3 {{
            margin: 0 0 10px 0;
            color: #666;
            font-size: 14px;
            text-transform: uppercase;
        }}
        .stat-card .value {{
            font-size: 32px;
            font-weight: bold;
            color: #333;
        }}
        .passed {{ color: #4caf50; }}
        .failed {{ color: #f44336; }}
        table {{
            width: 100%;
            background: white;
            border-collapse: collapse;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        th {{
            background-color: #667eea;
            color: white;
            padding: 15px;
            text-align: left;
        }}
        td {{
            padding: 12px 15px;
            border-bottom: 1px solid #ddd;
        }}
        tr:last-child td {{ border-bottom: none; }}
        tr:hover {{ background-color: #f5f5f5; }}
        .badge {{
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: bold;
        }}
        .badge-pass {{
            background-color: #e8f5e9;
            color: #2e7d32;
        }}
        .badge-fail {{
            background-color: #ffebee;
            color: #c62828;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Data Quality Validation Report</h1>
        <p>Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
    </div>
    
    <div class="stats">
        <div class="stat-card">
            <h3>Total Checks</h3>
            <div class="value">{total_checks}</div>
        </div>
        <div class="stat-card">
            <h3>Passed</h3>
            <div class="value passed">{passed}</div>
        </div>
        <div class="stat-card">
            <h3>Failed</h3>
            <div class="value failed">{failed}</div>
        </div>
        <div class="stat-card">
            <h3>Pass Rate</h3>
            <div class="value">{pass_rate:.1f}%</div>
        </div>
    </div>
    
    <table>
        <thead>
            <tr>
                <th>Column</th>
                <th>Rule</th>
                <th>Pass Rate</th>
                <th>Violations</th>
                <th>Status</th>
            </tr>
        </thead>
        <tbody>
"""

    for _, row in summary.iterrows():
        status_class = "badge-pass" if row["status"] == "PASS" else "badge-fail"
        pass_rate_val = row.get("pass_rate", 0) * 100
        violations = row.get("violations", 0)
        rows_count = row.get("rows", 0)

        html += f"""
            <tr>
                <td><strong>{row.get('column', 'N/A')}</strong></td>
                <td>{row.get('rule', 'N/A')}</td>
                <td>{pass_rate_val:.1f}%</td>
                <td>{violations}/{rows_count}</td>
                <td><span class="badge {status_class}">{row['status']}</span></td>
            </tr>
"""

    html += """
        </tbody>
    </table>
</body>
</html>
"""

    return html


def generate_markdown_report(results: Dict[str, Any]) -> str:
    """Generate Markdown report from validation results."""
    summary = results.get("summary")

    if not isinstance(summary, pd.DataFrame):
        summary = pd.DataFrame(summary)

    total_checks = len(summary)
    passed = (summary["status"] == "PASS").sum()
    failed = (summary["status"] == "FAIL").sum()
    pass_rate = (passed / total_checks * 100) if total_checks > 0 else 0

    md = f"""# Data Quality Validation Report

**Generated:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Summary

- **Total Checks:** {total_checks}
- **✅ Passed:** {passed}
- **❌ Failed:** {failed}
- **Pass Rate:** {pass_rate:.1f}%

## Results

| Column | Rule | Pass Rate | Violations | Status |
|--------|------|-----------|------------|--------|
"""

    for _, row in summary.iterrows():
        pass_rate_val = row.get("pass_rate", 0) * 100
        violations = row.get("violations", 0)
        rows_count = row.get("rows", 0)
        status_icon = "✅" if row["status"] == "PASS" else "❌"

        md += f"| {row.get('column', 'N/A')} | {row.get('rule', 'N/A')} | {pass_rate_val:.1f}% | {violations}/{rows_count} | {status_icon} {row['status']} |\n"

    return md
