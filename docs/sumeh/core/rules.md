# Rules & Definitions

In Sumeh v2.0, the validation engine is completely decoupled from the storage layer. Rules are defined using a strictly typed dataclass. 

You extract your rule configurations using your standard data tools (Pandas, DuckDB, etc.) and map them directly into `RuleDefinition` objects.

::: sumeh.core.rules.rule_model