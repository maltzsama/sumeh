import json
from pathlib import Path
from typing import List, Optional, Dict


class RuleRegistry:
    """Central registry for rule definitions loaded from manifest.json"""

    _manifest = None

    @classmethod
    def _ensure_loaded(cls):
        if cls._manifest is None:
            path = Path(__file__).parent / "manifest.json"
            with open(path, "r", encoding="utf-8") as f:
                cls._manifest = json.load(f)["validation_framework"]

    @classmethod
    def get_version(cls) -> str:
        cls._ensure_loaded()
        return cls._manifest["version"]

    @classmethod
    def get_engines(cls) -> List[str]:
        cls._ensure_loaded()
        return cls._manifest["engines_supported"]

    @classmethod
    def list_levels(cls) -> List[str]:
        cls._ensure_loaded()
        return list(cls._manifest["levels"].keys())

    @classmethod
    def list_categories(cls, level: str) -> List[str]:
        cls._ensure_loaded()
        return list(cls._manifest["levels"][level]["categories"].keys())

    @classmethod
    def list_rules(
        cls, level: str = None, category: str = None, engine: str = None
    ) -> List[str]:
        cls._ensure_loaded()
        results = []

        for lvl_name, lvl_data in cls._manifest["levels"].items():
            if level and lvl_name != level:
                continue
            for cat_name, cat_data in lvl_data["categories"].items():
                if category and cat_name != category:
                    continue
                for rule_name, rule_data in cat_data["rules"].items():
                    if engine and engine not in rule_data["engines"]:
                        continue
                    results.append(rule_name)
        return sorted(results)

    @classmethod
    def get_rule(cls, rule_name: str) -> Optional[Dict]:
        """Retrieve a rule definition by name, searching all levels/categories."""
        cls._ensure_loaded()
        for lvl in cls._manifest["levels"].values():
            for cat in lvl["categories"].values():
                if rule_name in cat["rules"]:
                    return cat["rules"][rule_name]
        return None

    @classmethod
    def is_rule_supported(cls, rule_name: str, engine: str) -> bool:
        """Check if a rule is supported by specific engine."""
        rule_data = cls.get_rule(rule_name)
        return rule_data and engine in rule_data.get("engines", [])
