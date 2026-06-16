# Quickstart 🚀

A concise guide to getting started with Sumeh’s v2.0 unified data quality framework.

## 1. Installation 💻

Install Sumeh via pip. The core package includes the Pandas engine. You can add extra engines as needed:

```bash
# Core only (Pandas)
pip install sumeh

# With specific engines
pip install sumeh[polars,duckdb]
pip install sumeh[pyspark]

```

## 2. Defining Rules ⚙️

Rules in v2.0 are simply lists of `RuleDefinition` dataclasses. You can define them directly in code, or load them from any source (CSV, databases) using your standard tools like Pandas or DuckDB.

```python
from sumeh.core.rules.rule_definition import RuleDefinition
import pandas as pd

# Define directly
rules = [
    RuleDefinition(field="user_id", check_type="is_unique", threshold=1.0),
    RuleDefinition(field="email", check_type="is_complete", threshold=1.0),
    RuleDefinition(field="age", check_type="has_mean", value=35.0, threshold=0.1)
]

# OR load from a CSV using standard Pandas
df_rules = pd.read_csv("rules.csv")
rules_from_csv = [
    RuleDefinition(**row)
    for row in df_rules.to_dict(orient="records")
    if row.get("execute", True)
]

```

## 3. Data Validation 🔍

Sumeh v2.0 uses a **namespace-first API**. You explicitly import the engine you want to use. This provides perfect IDE autocomplete and avoids hidden runtime routing.

```python
from sumeh import pandas as sumeh_pandas
import pandas as pd

df = pd.read_csv("customers.csv")

# Validate returns a comprehensive ValidationReport
report = sumeh_pandas.validate(df, rules)

```

## 4. Working with the Report 📊

The `ValidationReport` object contains all your metrics, statuses, and allows for single-pass data bifurcation.

```python
# 1. High-level metrics
print(f"Pass rate: {report.pass_rate:.2%}")
print(f"Passed: {len(report.passed)} | Failed: {len(report.failed)}")

# 2. Detailed results
for result in report.failed:
    print(f"✗ [{result.check_type}] on {result.field}: {result.message}")

# 3. Summary Dictionary (great for APIs)
summary_dict = report.summary()

# 4. Bifurcation (Split clean and bad data)
good_df, bad_df = report.split()
bad_df.to_parquet("quarantine.parquet")

```
