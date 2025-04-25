
# Module `sumeh.services` - Overview
::: sumeh.services

## Configuration and Utilities Services Package – Overview

This package consolidates all the logic needed to **load**, **parse**, and **infer** both Data Quality rules (configurations) and schema metadata from multiple sources—while also providing a lightweight web interface for initial interaction.

**📦 File Structure**

```
sumeh/services/
├── config.py       # ⇨ loading and parsing of configurations and schemas
├── utils.py        # ⇨ generic helper functions (conversion, comparison, extraction)
└── index.html      # ⇨ static interface for configuration via browser
```