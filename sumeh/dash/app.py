"""Sumeh Data Quality Dashboard - The Ultimate Validation Experience"""

import streamlit as st
import pandas as pd

import sys
import json
import tempfile

from datetime import datetime
from typing import Dict, Any, Tuple, Sequence, Optional
import plotly.express as px
import plotly.graph_objects as go


def _setup_awesome_style(theme: str = "light"):
    st.set_page_config(
        page_title="🚀 Sumeh - Data Quality Intelligence",
        page_icon="🔍",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    if theme == "dark":
        bg_color = "#121212"
        text_color = "#f1f1f1"
        card_shadow = "0 10px 30px rgba(255,255,255,0.1)"
    else:
        bg_color = "#ffffff"
        text_color = "#222222"
        card_shadow = "0 10px 30px rgba(0,0,0,0.2)"

    st.markdown(
        f"""
    <style>
    body {{
        background-color: {bg_color};
        color: {text_color};
    }}

    .main-header {{
        font-size: 3.5rem !important;
        font-weight: 900 !important;
        background: linear-gradient(45deg, #FF6B6B, #4ECDC4, #45B7D1);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 2rem;
    }}

    .metric-card {{
        border-radius: 15px;
        padding: 1.5rem;
        color: white;
        box-shadow: {card_shadow};
    }}

    .success-card {{
        background: linear-gradient(135deg, #56ab2f 0%, #a8e6cf 100%);
    }}

    .warning-card {{
        background: linear-gradient(135deg, #f46b45 0%, #eea849 100%);
    }}

    .danger-card {{
        background: linear-gradient(135deg, #ff416c 0%, #ff4b2b 100%);
    }}

    .export-section {{
        background: #f8f9fa;
        border-radius: 10px;
        padding: 1.5rem;
        border-left: 5px solid #4ECDC4;
    }}
    </style>
    """,
        unsafe_allow_html=True,
    )


def _render_hero_header(metadata: Dict[str, Any]) -> None:
    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        st.markdown(
            '<h1 class="main-header">🚀 SUMEH DATA QUALITY</h1>', unsafe_allow_html=True
        )

        st.markdown(
            f"""
        <div style="text-align: center; margin-bottom: 2rem;">
            <span style="background: #4ECDC4; color: white; padding: 0.5rem 1rem; border-radius: 20px; margin: 0 0.5rem;">
                🔍 {metadata.get('engine', 'pandas').upper()}
            </span>
            <span style="background: #45B7D1; color: white; padding: 0.5rem 1rem; border-radius: 20px; margin: 0 0.5rem;">
                📊 {metadata.get('total_rows', 0):,} ROWS
            </span>
            <span style="background: #96CEB4; color: white; padding: 0.5rem 1rem; border-radius: 20px; margin: 0 0.5rem;">
                ⚡ {datetime.now().strftime('%Y-%m-%d %H:%M')}
            </span>
        </div>
        """,
            unsafe_allow_html=True,
        )


def _render_kpi_cards(summary: pd.DataFrame) -> None:
    total_checks = len(summary)
    passed = (summary["status"] == "PASS").sum()
    failed = (summary["status"] == "FAIL").sum()

    pass_rate = (passed / total_checks * 100) if total_checks > 0 else 0
    risk_score = (failed / total_checks * 100) if total_checks > 0 else 0

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(
            f"""
        <div class="metric-card">
            <h3 style="color: white; margin: 0;">📋 TOTAL</h3>
            <h1 style="color: white; margin: 0; font-size: 2.5rem;">{total_checks}</h1>
            <p style="color: white; margin: 0;">Quality Checks</p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown(
            f"""
        <div class="metric-card success-card">
            <h3 style="color: white; margin: 0;">✅ PASSED</h3>
            <h1 style="color: white; margin: 0; font-size: 2.5rem;">{passed}</h1>
            <p style="color: white; margin: 0;">{pass_rate:.1f}% Success</p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    with col3:
        st.markdown(
            f"""
        <div class="metric-card danger-card">
            <h3 style="color: white; margin: 0;">❌ FAILED</h3>
            <h1 style="color: white; margin: 0; font-size: 2.5rem;">{failed}</h1>
            <p style="color: white; margin: 0;">{risk_score:.1f}% Risk</p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    with col4:
        avg_pass_rate = (
            summary["pass_rate"].mean() * 100 if "pass_rate" in summary.columns else 0
        )
        st.markdown(
            f"""
        <div class="metric-card" style="background: linear-gradient(135deg, #a8e6cf 0%, #56ab2f 100%);">
            <h3 style="color: white; margin: 0;">📈 QUALITY</h3>
            <h1 style="color: white; margin: 0; font-size: 2.5rem;">{avg_pass_rate:.1f}%</h1>
            <p style="color: white; margin: 0;">Avg Pass Rate</p>
        </div>
        """,
            unsafe_allow_html=True,
        )


def _render_quality_bars(summary: pd.DataFrame) -> Optional[go.Figure]:
    if "column" not in summary.columns or "pass_rate" not in summary.columns:
        return

    col_quality = summary.groupby("column")["pass_rate"].mean().sort_values()
    fig = go.Figure(
        go.Bar(
            x=col_quality.values * 100,
            y=col_quality.index,
            orientation="h",
            marker=dict(
                color=col_quality.values,
                colorscale=[[0, "red"], [0.5, "yellow"], [1, "green"]],
                line=dict(color="rgba(0,0,0,0)", width=1),
            ),
            text=[f"{v*100:.1f}%" for v in col_quality.values],
            textposition="inside",
        )
    )

    fig.update_layout(
        title="🔝 Column Pass Rate",
        xaxis_title="Pass Rate (%)",
        yaxis_title="Column",
        yaxis=dict(autorange="reversed"),
        height=450,
        margin=dict(l=50, r=20, t=60, b=50),
        showlegend=False,
    )
    return fig


def _render_trend_analysis(summary: pd.DataFrame) -> Optional[px.imshow]:
    if "column" not in summary.columns or "pass_rate" not in summary.columns:
        return

    quality_pivot = (
        summary.pivot_table(
            values="pass_rate", index="column", columns="rule", aggfunc="mean"
        ).fillna(0)
        * 100
    )

    fig = px.imshow(
        quality_pivot,
        aspect="auto",
        color_continuous_scale="RdYlGn",
        title="🔥 Quality Heatmap - Columns vs Rules",
    )

    fig.update_layout(height=400)
    return fig


def _render_sidebar_filters(
    summary: pd.DataFrame,
) -> Tuple[Sequence[str], Sequence[str], Sequence[str], Optional[str], float]:

    with st.sidebar:
        st.markdown("## 🎛️ CONTROL PANEL")

        status_options = summary["status"].unique().tolist()
        selected_status = st.multiselect(
            "**FILTER BY STATUS**",
            options=status_options,
            default=status_options,
            help="Select status types to display",
        )

        column_options = summary["column"].unique().tolist()
        selected_columns = st.multiselect(
            "**FILTER BY COLUMN**",
            options=column_options,
            default=column_options,
            help="Select specific columns",
        )

        rule_options = summary["rule"].unique().tolist()
        selected_rules = st.multiselect(
            "**FILTER BY RULE**",
            options=rule_options,
            default=rule_options,
            help="Select validation rules",
        )

        search_term = st.text_input(
            "**🔍 SMART SEARCH**", "", placeholder="Search columns, rules, patterns..."
        )

        min_quality = st.slider(
            "**MINIMUM PASS RATE**",
            min_value=0.0,
            max_value=1.0,
            value=0.0,
            step=0.1,
            help="Filter by minimum pass rate threshold",
        )

        st.markdown("---")
        st.markdown("### 📊 QUICK ACTIONS")

        if st.button("🔄 EXPORT FULL REPORT", width="content"):
            st.session_state.export_full = True

        if st.button("📧 GENERATE SUMMARY", width="content"):
            st.session_state.generate_summary = True

        return (
            selected_status,
            selected_columns,
            selected_rules,
            search_term,
            min_quality,
        )


def _render_dashboard(results: Dict[str, Any]) -> None:
    summary_data = results.get("summary", [])
    metadata = results.get("metadata", {})

    if isinstance(summary_data, list):
        summary = pd.DataFrame(summary_data)
    else:
        summary = summary_data

    if summary.empty:
        st.error("🚫 No validation results to display")
        return

    theme = st.session_state.get("theme", "light")
    _setup_awesome_style(theme)

    _render_hero_header(metadata)

    _render_kpi_cards(summary)

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        fig_radar = _render_quality_bars(summary)
        if fig_radar:
            st.plotly_chart(fig_radar, width="content")

    with col2:
        fig_trend = _render_trend_analysis(summary)
        if fig_trend:
            st.plotly_chart(fig_trend, width="content")

    selected_status, selected_columns, selected_rules, search_term, min_quality = (
        _render_sidebar_filters(summary)
    )

    filtered_data = summary[
        summary["status"].isin(selected_status)
        & summary["column"].isin(selected_columns)
        & summary["rule"].isin(selected_rules)
        & (summary["pass_rate"] >= min_quality)
    ]

    if search_term:
        filtered_data = filtered_data[
            filtered_data["column"]
            .astype(str)
            .str.contains(search_term, case=False, na=False)
            | filtered_data["rule"]
            .astype(str)
            .str.contains(search_term, case=False, na=False)
        ]

    st.markdown("## 📋 DETAILED VALIDATION RESULTS")

    st.info(
        f"**Showing {len(filtered_data)} of {len(summary)} checks** "
        f"({len(filtered_data) / len(summary) * 100:.1f}% of total)"
    )

    st.dataframe(
        filtered_data,
        width="content",
        hide_index=True,
        column_config={
            "status": st.column_config.TextColumn(
                "Status", width="small", help="Validation status"
            ),
            "pass_rate": st.column_config.ProgressColumn(
                "Pass Rate",
                format="%.1f%%",
                min_value=0,
                max_value=1,
                help="Percentage of passing validations",
            ),
            "violations": st.column_config.NumberColumn(
                "Violations", format="%d", help="Number of rule violations"
            ),
            "column": st.column_config.TextColumn(
                "Column", width="medium", help="Data column being validated"
            ),
            "rule": st.column_config.TextColumn(
                "Rule", width="large", help="Validation rule applied"
            ),
        },
    )

    _render_export_section(filtered_data, metadata)


def _render_export_section(summary: pd.DataFrame, metadata: Dict[str, Any]) -> None:
    st.markdown("---")
    st.markdown("## 💾 EXPORT & SHARE")

    col1, col2, col3, col4 = st.columns(4)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    with col1:
        csv_data = summary.to_csv(index=False)
        st.download_button(
            label="📥 CSV REPORT",
            data=csv_data,
            file_name=f"sumeh_report_{timestamp}.csv",
            mime="text/csv",
            width="content",
        )

    with col2:
        json_data = summary.to_json(orient="records", indent=2)
        st.download_button(
            label="📥 JSON DATA",
            data=json_data,
            file_name=f"sumeh_data_{timestamp}.json",
            mime="application/json",
            width="content",
        )

    with col3:
        exec_summary = f"""
# Sumeh Data Quality Report
**Generated:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
**Data Source:** {metadata.get('data_source', 'N/A')}
**Total Rows:** {metadata.get('total_rows', 0):,}
**Total Checks:** {len(summary)}

## Executive Summary
- ✅ **Passed:** {(summary['status'] == 'PASS').sum()} checks
- ❌ **Failed:** {(summary['status'] == 'FAIL').sum()} checks  
- 📊 **Overall Quality:** {summary['pass_rate'].mean() * 100:.1f}%

## Top Issues
"""

        failed_checks = summary[summary["status"] == "FAIL"]
        if not failed_checks.empty:
            for _, check in failed_checks.head(5).iterrows():
                exec_summary += f"- **{check['column']}** - {check['rule']} ({check['pass_rate'] * 100:.1f}%)\n"

        st.download_button(
            label="📄 EXEC SUMMARY",
            data=exec_summary,
            file_name=f"sumeh_exec_summary_{timestamp}.md",
            mime="text/markdown",
            width="content",
        )

    with col4:
        if st.button("🔄 NEW VALIDATION", width="content"):
            st.info(
                "Return to CLI and run: `sumeh validate data.csv rules.csv --dashboard`"
            )


def launch_dashboard(validation_results=None, summary=None, metadata=None) -> None:
    """
    Launch the ultimate Sumeh dashboard!

    Args:
        validation_results: Results from validate() function
        rules_config: Rules configuration
        summary: Validation summary
        metadata: Additional metadata
    """
    if validation_results is not None:
        results = {
            "summary": summary if summary is not None else [],
            "metadata": metadata if metadata is not None else {},
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(results, f, default=str)
            temp_file = f.name

        sys.argv = [sys.argv[0], temp_file] if len(sys.argv) == 1 else sys.argv

    main()


def main() -> None:
    try:
        if len(sys.argv) > 1:
            results_file = sys.argv[1]
            try:
                with open(results_file, "r") as f:
                    results = json.load(f)
                _render_dashboard(results)
            except Exception as e:
                st.error(f"❌ Error loading results: {e}")
                _show_usage()
        else:
            _show_usage()
    except Exception as e:
        st.error(f"💥 Dashboard crashed: {e}")
        st.info("Please report this issue to the Sumeh team!")


def _show_usage() -> None:
    st.markdown(
        """
    <div style="text-align: center; padding: 4rem 2rem;">
        <h1 style="font-size: 4rem; margin-bottom: 1rem;">🚀</h1>
        <h1>Welcome to Sumeh Dashboard!</h1>
        <p style="font-size: 1.2rem; color: #666; margin-bottom: 2rem;">
            The ultimate data quality validation experience
        </p>
    </div>
    """,
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 🎯 Quick Start")
        st.code("sumeh validate data.csv rules.csv --dashboard", language="bash")

        st.markdown(
            """
        **Supported Formats:**
        - 📁 CSV, Parquet, JSON, Excel
        - 🗄️ PostgreSQL, MySQL, SQLite
        - ⚡ Pandas, Polars, Dask
        """
        )

    with col2:
        st.markdown("### 🛠️ Advanced Usage")
        st.code(
            """
# With custom output
sumeh validate data.parquet rules.csv \
  --output results.json \
  --format json \
  --dashboard

# With different engine
sumeh validate data.csv rules.csv \
  --engine polars \
  --verbose \
  --dashboard
        """.strip(),
            language="bash",
        )

    st.markdown("---")
    st.markdown("### 📚 Learn More")

    col3, col4, col5 = st.columns(3)

    with col3:
        st.markdown("**📖 Documentation**  \n" "Complete guides and examples")

    with col4:
        st.markdown("**🐛 Issue Tracking**  \n" "Report bugs and request features")

    with col5:
        st.markdown("**💡 Examples**  \n" "Real-world use cases and patterns")


if __name__ == "__main__":
    main()
