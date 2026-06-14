"""
Rule definition model with metadata preservation.

Combines original RuleDef features with metadata preservation.
"""

import ast
import re
from dataclasses import dataclass, field as dataclass_field
from datetime import datetime, date
from typing import Any, List, Union, Optional

from dateutil import parser

from .registry import RuleRegistry


@dataclass
class RuleDefinition:
    """
    Data quality rule with validation and metadata preservation.

    Features:
        - Validates against RuleRegistry (manifest.json)
        - Auto-enriches category/level from manifest
        - Intelligent parsing of field/value
        - Engine compatibility validation
        - Preserves extra columns in metadata dict

    Attributes:
        field: Column name(s) to validate
        check_type: Validation rule type
        value: Threshold or comparison value
        threshold: Pass rate threshold (0.0-1.0)
        execute: Whether rule should be executed
        category: Rule category (auto-populated)
        level: Validation level (auto-populated)
        engine: Target engine (optional)
        created_at: Rule creation timestamp
        updated_at: Rule update timestamp
        metadata: Extra fields from source (PRESERVED!)
    """

    field: Union[str, List[str]]
    check_type: str
    value: Any = None
    threshold: float = 1.0
    level: str = "ROW"
    category: str = "unknown"
    execute: bool = True
    updated_at: Optional[Any] = None
    metadata: dict[str, Any] = dataclass_field(default_factory=dict)

    def __post_init__(self):
        """Validates rule and enriches with metadata from RuleRegistry."""
        # Initialize metadata if None
        if self.metadata is None:
            self.metadata = {}

        self.field = self._parse_field(self.field)

        rule_def = RuleRegistry.get_rule(self.check_type)
        if rule_def is None:
            available = RuleRegistry.list_rules()
            raise ValueError(
                f"Invalid rule type '{self.check_type}'. "
                f"Available rules: {', '.join(available[:10])}... ({len(available)} total)"
            )

        self.level = rule_def.get("level", "ROW")
        self.category = rule_def.get("category", "unknown")

    def _enrich_from_manifest(self):
        """Enrich rule with category and level from manifest."""
        rule_def = RuleRegistry.get_rule(self.check_type)
        if rule_def:
            if self.level is None:
                self.level = rule_def.get("level", "ROW")
            if self.category is None:
                self.category = rule_def.get("category", "unknown")

    @classmethod
    def from_dict(cls, data: dict, engine: Optional[str] = None) -> "RuleDefinition":
        """
        Creates RuleDef from dictionary with metadata preservation.

        Args:
            data: Dictionary containing rule configuration
            engine: Optional engine name for compatibility validation

        Returns:
            Validated RuleDef instance with preserved metadata
        """
        # Known Sumeh fields
        known_fields = {
            "field",
            "check_type",
            "value",
            "threshold",
            "level",
            "category",
            "execute",
            "updated_at",
        }

        # Parse field (handles multiple formats)
        field = cls._parse_field(data.get("field", ""))

        # Parse value (auto-detects type)
        value = cls._parse_value(data.get("value"))

        # Parse threshold
        try:
            threshold = float(data.get("threshold", 1.0))
        except (ValueError, TypeError):
            threshold = 1.0

        # Parse execute
        execute = data.get("execute", True)
        if isinstance(execute, str):
            execute = execute.lower() in ["true", "1", "yes", "y", "t"]

        # Parse timestamps
        updated_at = cls._parse_timestamp(data.get("updated_at"))

        # Extract metadata (extra fields)
        metadata = {}
        for key, val in data.items():
            if key not in known_fields:
                metadata[key] = val

        check_type = data.get("check_type")
        if not check_type:
            raise ValueError("Missing required field: check_type")

        category = data.get("category", "unknown")

        level = data.get("level", "ROW")

        return cls(
            field=field,
            check_type=check_type,
            value=value,
            threshold=threshold,
            execute=execute,
            category=category,
            level=level,
            updated_at=updated_at,
            metadata=metadata,  # ✅ Preserved!
        )

    @staticmethod
    def _parse_field(field_input: Any) -> Union[str, List[str]]:
        """Parse field input into string or list of strings."""
        if isinstance(field_input, list):
            return field_input if len(field_input) > 1 else field_input[0]

        if not isinstance(field_input, str):
            return str(field_input)

        field_str = field_input.strip()

        # Remove outer quotes
        if (field_str.startswith('"') and field_str.endswith('"')) or (
            field_str.startswith("'") and field_str.endswith("'")
        ):
            field_str = field_str[1:-1].strip()

        if not field_str:
            return ""

        # Handle list notation: "[col1, col2]"
        if field_str.startswith("[") and field_str.endswith("]"):
            inner = field_str[1:-1].strip()
            if not inner:
                return ""

            try:
                result = ast.literal_eval(field_str)
                if isinstance(result, list):
                    return result if len(result) > 1 else result[0]
            except (ValueError, SyntaxError):
                pass

            if "," in inner:
                items = [
                    item.strip(" \"'") for item in inner.split(",") if item.strip()
                ]
                return items if len(items) > 1 else (items[0] if items else "")
            else:
                return inner.strip(" \"'")

        # Handle comma-separated: "col1, col2"
        elif "," in field_str:
            items = [
                item.strip(" \"'") for item in field_str.split(",") if item.strip()
            ]
            return items if len(items) > 1 else (items[0] if items else "")

        return field_str.strip(" \"'")

    @staticmethod
    def _parse_value(value_input: Any) -> Any:
        """Parse value input into appropriate type."""
        if value_input is None:
            return None
        if isinstance(value_input, str) and value_input.upper() in ("NULL", ""):
            return None

        if isinstance(value_input, (int, float, bool, date, datetime)):
            return value_input

        if isinstance(value_input, list):
            return RuleDefinition._parse_value_list(value_input)

        if isinstance(value_input, str):
            value_str = value_input.strip()

            # Handle list notation
            if value_str.startswith("[") and value_str.endswith("]"):
                try:
                    result = ast.literal_eval(value_str)
                    if isinstance(result, list):
                        return RuleDefinition._parse_value_list(result)
                except (ValueError, SyntaxError):
                    inner = value_str[1:-1].strip()
                    if inner:
                        items = [item.strip(" \"'") for item in inner.split(",")]
                        return RuleDefinition._parse_value_list(items)

            # Try date parsing
            try:
                if re.match(r"^\d{4}-\d{2}-\d{2}$", value_str):
                    return datetime.strptime(value_str, "%Y-%m-%d").date()
                elif re.match(r"^\d{2}/\d{2}/\d{4}$", value_str):
                    return datetime.strptime(value_str, "%d/%m/%Y").date()
            except ValueError:
                pass

            # Try numeric parsing
            try:
                if "." in value_str:
                    return float(value_str)
                return int(value_str)
            except ValueError:
                pass

            return value_str

        return value_input

    @staticmethod
    def _parse_value_list(items: List[Any]) -> List[Union[int, float, str]]:
        """Parse list items into appropriate types."""
        if not items:
            return []

        parsed: List[Union[int, float, str]] = []
        for item in items:
            if isinstance(item, str):
                item = item.strip(" \"'")
                try:
                    if "." in item:
                        parsed.append(float(item))
                    else:
                        parsed.append(int(item))
                except ValueError:
                    parsed.append(item)
            else:
                parsed.append(item)

        return parsed

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
        Converts RuleDef to dictionary with metadata flattened.

        Returns:
            Dictionary with all fields + metadata
        """
        result = {
            "field": self.field,
            "check_type": self.check_type,
            "value": self.value,
            "threshold": self.threshold,
            "level": self.level,
            "category": self.category,
            "execute": self.execute,
            "updated_at": (
                self.updated_at.isoformat()
                if isinstance(self.updated_at, datetime)
                else self.updated_at
            ),
        }

        # Flatten metadata
        if self.metadata:
            result.update(self.metadata)

        # Format date values
        if isinstance(self.value, date) and not isinstance(self.value, datetime):
            result["value"] = self.value.isoformat()

        return result

    def get_description(self) -> str:
        """Returns rule description from manifest."""
        rule_def = RuleRegistry.get_rule(self.check_type)
        return rule_def.get("description", "") if rule_def else ""

    def get_supported_engines(self) -> List[str]:
        """Returns list of engines that support this rule."""
        rule_def = RuleRegistry.get_rule(self.check_type)
        return rule_def.get("engines", []) if rule_def else []

    def is_supported_by_engine(self, engine: str) -> bool:
        """Check if this rule is supported by the given engine."""
        return RuleRegistry.is_rule_supported(self.check_type, engine)

    def is_applicable_for_level(self, target_level: str) -> bool:
        """Check if this rule matches the target level."""
        if self.level is None:
            return True
        rule_level = self.level.upper().replace("_LEVEL", "")
        target_level = target_level.upper().replace("_LEVEL", "")
        return rule_level == target_level

    def get_skip_reason(self, target_level: str, engine: str) -> Optional[str]:
        """Returns reason why rule should be skipped, or None if applicable."""
        if not self.execute:
            return "execute=False"

        if not self.is_applicable_for_level(target_level):
            return f"Wrong level: expected '{target_level}', got '{self.level}'"

        if not self.is_supported_by_engine(engine):
            supported = self.get_supported_engines()
            return (
                f"Engine '{engine}' not supported (available: {', '.join(supported)})"
            )

        return None

    def __repr__(self) -> str:
        field_str = (
            self.field if isinstance(self.field, str) else f"[{','.join(self.field)}]"
        )
        meta_str = f", +{len(self.metadata)} meta" if self.metadata else ""
        return (
            f"RuleDef(field={field_str}, check={self.check_type}, "
            f"level={self.level}, category={self.category}{meta_str})"
        )
