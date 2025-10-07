import streamlit as st
import pandas as pd
import sys
import json
from pathlib import Path


def main():
    """Main dashboard entry point."""
    st.set_page_config(
        page_title="Sumeh - Data Quality Report",
        page_icon="🔍",
        layout="wide"
    )

    # Check if results file was passed as argument
    if len(sys.argv) > 1:
        results_file = sys.argv[1]
        try:
            with open(results_file, 'r') as f:
                results = json.load(f)
            _render_dashboard(results)
        except Exception as e:
            st.error(f"Error loading results: {e}")
            _show_usage()
    else:
        _show_usage()


def _show_usage():
    """Show usage instructions."""
    st.error("This dashboard should be launched via `sumeh validate --dashboard`")

    st.code("sumeh validate data.csv rules.csv --dashboard", language="bash")

    st.info("""
    **How to use:**
    
    1. Run validation with the `--dashboard` flag
    2. The dashboard will open automatically with your results
    """)


def _render_dashboard(results: dict):
    """Render the dashboard UI."""

    summary_data = results.get("summary", [])
    metadata = results.get("metadata", {})

    # Convert to DataFrame if it's a list
    if isinstance(summary_data, list):
        summary = pd.DataFrame(summary_data)
    else:
        summary = summary_data

    if summary.empty:
        st.warning("No validation results to display")
        return

    # Header
    st.title("🔍 Data Quality Validation Report")

    from datetime import datetime
    st.markdown(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Metadata
    with st.expander("📊 Dataset Information", expanded=True):
        col1, col2, col3 = st.columns(3)
        col1.metric("Data Source", metadata.get("data_source", "N/A"))
        col2.metric("Total Rows", f"{metadata.get('total_rows', 0):,}")
        col3.metric("Engine", metadata.get("engine", "pandas"))

    st.divider()

    # Summary metrics
    total_checks = len(summary)
    passed = (summary["status"] == "PASS").sum()
    failed = (summary["status"] == "FAIL").sum()
    pass_rate = (passed / total_checks * 100) if total_checks > 0 else 0

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Total Checks", total_checks)
    col2.metric("✅ Passed", passed, delta=f"{pass_rate:.1f}%")
    col3.metric("❌ Failed", failed, delta=f"-{100-pass_rate:.1f}%")
    col4.metric("Pass Rate", f"{pass_rate:.1f}%")

    # Status indicator
    if failed > 0:
        st.error(f"⚠️ {failed} check(s) failed")
    else:
        st.success("✅ All checks passed!")

    st.divider()

    # Visualizations
    try:
        import plotly.express as px
        import plotly.graph_objects as go

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Status Distribution")

            status_counts = summary["status"].value_counts()
            fig_pie = px.pie(
                values=status_counts.values,
                names=status_counts.index,
                color=status_counts.index,
                color_discrete_map={"PASS": "#4caf50", "FAIL": "#f44336"},
                hole=0.4
            )
            fig_pie.update_layout(showlegend=True, height=300)
            st.plotly_chart(fig_pie, use_container_width=True)

        with col2:
            st.subheader("Pass Rate by Check")

            summary_sorted = summary.sort_values("pass_rate")
            fig_bar = px.bar(
                summary_sorted,
                x="pass_rate",
                y="column",
                orientation="h",
                color="status",
                color_discrete_map={"PASS": "#4caf50", "FAIL": "#f44336"},
                labels={"pass_rate": "Pass Rate", "column": "Column"}
            )
            fig_bar.update_layout(showlegend=False, height=300)
            st.plotly_chart(fig_bar, use_container_width=True)

    except ImportError:
        st.warning("Install plotly for charts: `pip install plotly`")

    st.divider()

    # Filters
    st.subheader("📋 Detailed Results")

    col1, col2 = st.columns(2)
    with col1:
        status_filter = st.multiselect(
            "Filter by Status",
            options=summary["status"].unique().tolist(),
            default=summary["status"].unique().tolist()
        )

    with col2:
        search = st.text_input("Search column/rule", "")

    # Apply filters
    filtered = summary[summary["status"].isin(status_filter)]

    if search:
        filtered = filtered[
            filtered["column"].astype(str).str.contains(search, case=False, na=False) |
            filtered["rule"].astype(str).str.contains(search, case=False, na=False)
        ]

    # Results table
    st.dataframe(
        filtered,
        use_container_width=True,
        hide_index=True,
        column_config={
            "status": st.column_config.TextColumn("Status", width="small"),
            "pass_rate": st.column_config.ProgressColumn(
                "Pass Rate",
                format="%.1f%%",
                min_value=0,
                max_value=1,
            ),
            "violations": st.column_config.NumberColumn("Violations", format="%d"),
        }
    )

    # Failed checks detail
    if failed > 0:
        st.divider()
        st.subheader("❌ Failed Checks Detail")

        failed_df = summary[summary["status"] == "FAIL"]

        for idx, row in failed_df.iterrows():
            with st.expander(f"🔴 {row['column']} - {row['rule']}"):
                col1, col2, col3 = st.columns(3)
                col1.metric("Pass Rate", f"{row.get('pass_rate', 0)*100:.1f}%")
                col2.metric("Violations", f"{row.get('violations', 0):,}")
                col3.metric("Total Rows", f"{row.get('rows', 0):,}")

                st.markdown(f"**Threshold:** {row.get('pass_threshold', 1.0)*100:.1f}%")

    # Download options
    st.divider()
    st.subheader("💾 Export Results")

    col1, col2 = st.columns(2)

    with col1:
        csv = summary.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"validation_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

    with col2:
        json_str = summary.to_json(orient="records", indent=2)
        st.download_button(
            label="📥 Download JSON",
            data=json_str,
            file_name=f"validation_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json"
        )


if __name__ == "__main__":
    main()