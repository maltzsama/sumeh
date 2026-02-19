"""
Schema Definition Models.
Formalizes the structural contract (Data Contract) for datasets.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any


@dataclass
class ColumnDef:
    """Column definition with type, nullability, and metadata."""

    name: str
    expected_type: str
    is_optional: bool = False
    nullable: bool = True
    element_type: Optional[str] = None  # For arrays: List[Int64], array<string>
    require_comment: bool = False
    expected_comment: Optional[str] = None
    fields: Optional[List["ColumnDef"]] = None  # For nested structs

    @classmethod
    def from_dict(cls, name: str, props: Any) -> "ColumnDef":
        """Create ColumnDef from dictionary specification."""
        if isinstance(props, str):
            return cls(name=name, expected_type=props)

        nested_fields = None
        if "fields" in props:
            nested_fields = [cls.from_dict(k, v) for k, v in props["fields"].items()]

        return cls(
            name=name,
            expected_type=props.get("type", "string"),
            is_optional=props.get("is_optional", False),
            nullable=props.get("nullable", True),
            element_type=props.get("element_type"),
            require_comment=props.get("require_comment", False),
            expected_comment=props.get("expected_comment"),
            fields=nested_fields,
        )


@dataclass
class SchemaDef:
    """Schema definition with column specifications."""

    columns: List[ColumnDef]
    strict_columns: bool = False

    @classmethod
    def from_dict(cls, data: Dict[str, Any], strict: bool = False) -> "SchemaDef":
        """Create SchemaDef from dictionary specification."""
        cols = [ColumnDef.from_dict(k, v) for k, v in data.items()]
        return cls(columns=cols, strict_columns=strict)


@dataclass
class SchemaReport:
    """Schema validation report with detailed error tracking."""

    passed: bool
    missing_cols: List[str] = field(default_factory=list)
    type_errors: Dict[str, str] = field(default_factory=dict)
    metadata_errors: Dict[str, str] = field(default_factory=dict)
    extra_cols: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert report to dictionary."""
        return {
            "passed": self.passed,
            "missing_columns": self.missing_cols,
            "type_errors": self.type_errors,
            "metadata_errors": self.metadata_errors,
            "extra_columns": self.extra_cols,
            "total_issues": len(self.missing_cols)
            + len(self.type_errors)
            + len(self.metadata_errors)
            + len(self.extra_cols),
        }

    def __repr__(self) -> str:
        """Human-readable representation."""
        status = "✓ PASSED" if self.passed else "✗ FAILED"
        issues = (
            len(self.missing_cols)
            + len(self.type_errors)
            + len(self.metadata_errors)
            + len(self.extra_cols)
        )
        return f"SchemaReport({status}, {issues} issues)"

    def __bool__(self) -> bool:
        """Boolean conversion for easy checking: if report: ..."""
        return self.passed
