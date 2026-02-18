"""
Rule Registry - loads and queries the manifest.json.

Simple flat structure lookup.
"""

import json
from pathlib import Path
from typing import Optional, List


class RuleRegistry:
    """Registry of all validation rules from manifest.json"""

    _manifest = None

    @classmethod
    def _ensure_loaded(cls):
        """Load manifest.json if not already loaded."""
        if cls._manifest is not None:
            return

        manifest_path = Path(__file__).parent / "manifest.json"
        with open(manifest_path, "r") as f:
            data = json.load(f)
            cls._manifest = {rule["check_type"]: rule for rule in data["rules"]}

    @classmethod
    def get_rule(cls, check_type: str) -> Optional[dict]:
        """
        Get rule definition by check_type.

        Args:
            check_type: Rule name (e.g., "is_complete")

        Returns:
            Rule dict or None if not found
        """
        cls._ensure_loaded()
        return cls._manifest.get(check_type)

    @classmethod
    def list_rules(cls) -> List[str]:
        """List all available rule check_types."""
        cls._ensure_loaded()
        return list(cls._manifest.keys())

    @classmethod
    def is_rule_supported(cls, check_type: str, engine: str) -> bool:
        """Check if rule is supported by engine."""
        rule = cls.get_rule(check_type)
        if not rule:
            return False
        return engine in rule.get("engines", [])
