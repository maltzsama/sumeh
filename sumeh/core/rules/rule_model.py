import ast
import re
import warnings
from dataclasses import dataclass, asdict
from datetime import datetime, date
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
        value: Threshold or comparison value for the rule (auto-parsed to correct type)
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
    created_at: datetime = None
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
            warnings.warn(
                f"Rule '{self.check_type}' not supported by engine '{self.engine}'. "
                f"Supported: {', '.join(supported)}. Rule will be skipped during validation."
            )
            self.execute = False

        if self.category is None or self.level is None:
            self._enrich_from_manifest()

    def _enrich_from_manifest(self):
        """Enrich rule with category and level from manifest."""
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
    def from_dict(cls, data: dict, engine: Optional[str] = None) -> "RuleDef":
        """
        Creates RuleDef from dictionary with automatic parsing and validation.

        Args:
            data: Dictionary containing rule configuration
            engine: Optional engine name for compatibility validation

        Returns:
            Validated RuleDef instance with enriched metadata

        Raises:
            ValueError: If rule type is invalid or unsupported by engine
        """
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
        created_at = cls._parse_timestamp(
            data.get("created_at"), default=datetime.utcnow()
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
    def _parse_field(field_input: Any) -> Union[str, List[str]]:
        """
        Parse field input into string or list of strings.

        Handles formats:
            - "column_name"
            - "[col1, col2]"
            - "col1, col2"
            - ['col1', 'col2']

        Returns:
            String for single column, List[str] for multiple columns
        """
        # Already a list
        if isinstance(field_input, list):
            return field_input if len(field_input) > 1 else field_input[0]

        # Not a string
        if not isinstance(field_input, str):
            return str(field_input)

        field_str = field_input.strip()

        # Remove outer quotes
        if (field_str.startswith('"') and field_str.endswith('"')) or (
            field_str.startswith("'") and field_str.endswith("'")
        ):
            field_str = field_str[1:-1].strip()

        # Handle empty
        if not field_str:
            return ""

        # Handle list notation: "[col1, col2]"
        if field_str.startswith("[") and field_str.endswith("]"):
            inner = field_str[1:-1].strip()
            if not inner:
                return ""

            # Try AST parse first (safest)
            try:
                result = ast.literal_eval(field_str)
                if isinstance(result, list):
                    return result if len(result) > 1 else result[0]
            except (ValueError, SyntaxError):
                pass

            # Fallback: manual split
            if "," in inner:
                items = [
                    item.strip(" \"'") for item in inner.split(",") if item.strip()
                ]
                return items if len(items) > 1 else (items[0] if items else "")
            else:
                return inner.strip(" \"'")

        # Handle comma-separated without brackets: "col1, col2"
        elif "," in field_str:
            items = [
                item.strip(" \"'") for item in field_str.split(",") if item.strip()
            ]
            return items if len(items) > 1 else (items[0] if items else "")

        # Single column
        return field_str.strip(" \"'")

    @staticmethod
    def _parse_value(value_input: Any) -> Any:
        """
        Parse value input into appropriate type.

        Auto-detects and converts to:
            - None (for NULL/"")
            - date/datetime
            - list (int/float/str)
            - float
            - int
            - string (including regex)

        Returns:
            Parsed value in appropriate type
        """
        # Handle None/NULL/empty
        if value_input is None:
            return None
        if isinstance(value_input, str) and value_input.upper() in ("NULL", ""):
            return None

        # Already correct type
        if isinstance(value_input, (int, float, bool, date, datetime)):
            return value_input

        # Already a list
        if isinstance(value_input, list):
            return RuleDef._parse_value_list(value_input)

        # String parsing
        if isinstance(value_input, str):
            value_str = value_input.strip()

            # Handle list notation: "[1, 2, 3]" or "[a, b, c]"
            if value_str.startswith("[") and value_str.endswith("]"):
                try:
                    # Try AST parse (safest)
                    result = ast.literal_eval(value_str)
                    if isinstance(result, list):
                        return RuleDef._parse_value_list(result)
                except (ValueError, SyntaxError):
                    # Fallback: manual split
                    inner = value_str[1:-1].strip()
                    if inner:
                        items = [item.strip(" \"'") for item in inner.split(",")]
                        return RuleDef._parse_value_list(items)

            # Try date parsing (YYYY-MM-DD or DD/MM/YYYY)
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

            # Return as string (could be regex or text)
            return value_str

        # Fallback: return as-is
        return value_input

    @staticmethod
    def _parse_value_list(items: List[Any]) -> List[Union[int, float, str]]:
        """
        Parse list items into appropriate types.

        Returns:
            List with items converted to int/float/str
        """
        if not items:
            return []

        parsed = []
        for item in items:
            if isinstance(item, str):
                item = item.strip(" \"'")
                # Try numeric
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
        Converts RuleDef to dictionary with formatted timestamps.

        Returns:
            Dictionary representation of the rule
        """
        result = asdict(self)

        # Format timestamps
        if self.created_at:
            result["created_at"] = self.created_at.isoformat()
        if self.updated_at:
            result["updated_at"] = self.updated_at.isoformat()

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

    def __repr__(self) -> str:
        field_str = (
            self.field if isinstance(self.field, str) else f"[{','.join(self.field)}]"
        )
        return (
            f"RuleDef(field={field_str}, check={self.check_type}, "
            f"level={self.level}, category={self.category})"
        )

    def is_supported_by_engine(self, engine: str) -> bool:
        """Check if this rule is supported by the given engine."""
        return RuleRegistry.is_rule_supported(self.check_type, engine)

    def is_applicable_for_level(self, target_level: str) -> bool:
        """Check if this rule matches the target level."""
        if self.level is None:
            return True  # Se não tem level definido, assume que é aplicável
        # Normaliza: "ROW" ou "row_level" -> "ROW"
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
