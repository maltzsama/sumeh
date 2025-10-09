# sumeh/core/rule.py

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, List, Union, Optional
from dateutil import parser
from .regristry import RuleRegistry


@dataclass
class RuleDef:
    """
    Data quality rule with automatic validation and metadata enrichment.

    Validates rule types against RuleRegistry (manifest.json) and automatically
    populates category and level metadata.

    Attributes:
        field: Column name(s) to validate (string or list of strings)
        check_type: Validation rule type (e.g., 'is_complete', 'is_unique')
        value: Threshold or comparison value for the rule
        threshold: Pass rate threshold (0.0-1.0, default 1.0 = 100%)
        execute: Whether rule should be executed (default True)
        category: Rule category (auto-populated from manifest)
        level: Validation level 'ROW' or 'TABLE' (auto-populated from manifest)
        engine: Target engine name (optional, for validation)
        created_at: Rule creation timestamp
        updated_at: Rule update timestamp
    """

    field: Union[str, List[str]]
    check_type: str
    value: Any = None
    threshold: float = 1.0
    execute: bool = True
    category: Optional[str] = None
    level: Optional[str] = None
    engine: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None

    def __post_init__(self):
        """Validates rule and enriches with metadata from RuleRegistry."""

        rule_def = RuleRegistry.get_rule(self.check_type)
        if rule_def is None:
            available = RuleRegistry.list_rules()
            raise ValueError(
                f"Invalid rule type '{self.check_type}'. "
                f"Available rules: {', '.join(available[:10])}... ({len(available)} total)"
            )

        if self.engine and not RuleRegistry.is_rule_supported(
            self.check_type, self.engine
        ):
            supported = rule_def.get("engines", [])
            raise ValueError(
                f"Rule '{self.check_type}' not supported by engine '{self.engine}'. "
                f"Supported: {', '.join(supported)}"
            )

        if self.category is None or self.level is None:
            self._enrich_from_manifest()

    def _enrich_from_manifest(self):

        RuleRegistry._ensure_loaded()

        for level_key, level_data in RuleRegistry._manifest["levels"].items():
            for cat_key, cat_data in level_data["categories"].items():
                if self.check_type in cat_data["rules"]:
                    if self.level is None:
                        self.level = level_key.replace("_level", "").upper()
                    if self.category is None:
                        self.category = cat_key
                    return

    @classmethod
    def from_dict(cls, data: dict, engine: Optional[str] = None) -> "Rule":
        """
        Creates Rule from dictionary with automatic parsing and validation.

        Args:
            data: Dictionary containing rule configuration
            engine: Optional engine name for compatibility validation

        Returns:
            Validated Rule instance with enriched metadata

        Raises:
            ValueError: If rule type is invalid or unsupported by engine
        """
        field = data.get("field", "")
        if isinstance(field, str) and field.startswith("[") and field.endswith("]"):

            field = [item.strip().strip("'\"") for item in field.strip("[]").split(",")]

        value = data.get("value")
        if isinstance(value, str) and value.upper() in ("NULL", ""):
            value = None

        try:
            threshold = float(data.get("threshold", 1.0))
        except (ValueError, TypeError):
            threshold = 1.0

        execute = data.get("execute", True)
        if isinstance(execute, str):
            execute = execute.lower() in ["true", "1", "yes", "y", "t"]

        created_at = cls._parse_timestamp(
            data.get("created_at"), default=datetime.now()
        )
        updated_at = cls._parse_timestamp(data.get("updated_at"))

        return cls(
            field=field,
            check_type=data.get("check_type"),
            value=value,
            threshold=threshold,
            execute=execute,
            category=data.get("category"),
            level=data.get("level"),
            engine=engine or data.get("engine"),
            created_at=created_at,
            updated_at=updated_at,
        )

    @staticmethod
    def _parse_timestamp(
        value: Any, default: Optional[datetime] = None
    ) -> Optional[datetime]:
        """Parse timestamp from various formats."""
        if value is None:
            return default
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return parser.parse(value)
            except Exception:
                return default
        return default

    def to_dict(self) -> dict:
        """
        Converts Rule to dictionary with formatted timestamps.

        Returns:
            Dictionary representation of the rule
        """
        result = asdict(self)

        if self.created_at:
            result["created_at"] = self.created_at.isoformat()
        if self.updated_at:
            result["updated_at"] = self.updated_at.isoformat()

        return result

    def get_description(self) -> str:
        """Returns rule description from manifest."""
        rule_def = RuleRegistry.get_rule(self.check_type)
        return rule_def.get("description", "") if rule_def else ""

    def get_supported_engines(self) -> List[str]:
        """Returns list of engines that support this rule."""
        rule_def = RuleRegistry.get_rule(self.check_type)
        return rule_def.get("engines", []) if rule_def else []

    def __repr__(self) -> str:
        field_str = (
            self.field if isinstance(self.field, str) else f"[{','.join(self.field)}]"
        )
        return (
            f"Rule(field={field_str}, check={self.check_type}, "
            f"level={self.level}, category={self.category})"
        )
