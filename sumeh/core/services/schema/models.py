"""
Schema Definition Models.
Formalizes the structural contract (Data Contract) for datasets.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any


@dataclass
class ColumnDef:
    """
    ColumnDef class documentation.

    A class representing a column definition with comprehensive type and metadata information.

    Attributes:
        name (str): The name of the column.
        expected_type (str): The expected data type of the column (e.g., 'string', 'int', 'double').
        is_optional (bool): Whether the column is optional. Defaults to False.
        nullable (bool): Whether the column can contain null values. Defaults to True.
        element_type (Optional[str]): The element type for array columns (e.g., 'List[Int64]', 'array<string>'). Defaults to None.
        require_comment (bool): Whether the column requires a comment/description. Defaults to False.
        expected_comment (Optional[str]): The expected comment or description for the column. Defaults to None.
        fields (Optional[List[ColumnDef]]): A list of nested ColumnDef objects for struct/nested columns. Defaults to None.

    Methods:
        from_dict(cls, name: str, props: Any) -> ColumnDef:
            Create a ColumnDef instance from a dictionary specification.
            
            Args:
                name (str): The column name.
                props (Any): A dictionary or string containing column properties. If a string is provided,
                            it is treated as the expected_type. For dictionaries, supports keys:
                            - 'type': The expected data type (default: 'string')
                            - 'is_optional': Whether the column is optional
                            - 'nullable': Whether the column is nullable
                            - 'element_type': The element type for arrays
                            - 'require_comment': Whether a comment is required
                            - 'expected_comment': The expected comment text
                            - 'fields': A dictionary of nested column definitions
            
            Returns:
                ColumnDef: A new ColumnDef instance initialized with the provided properties.
    """
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
    """
    Schema definition model for data validation and column specifications.

    This class represents a schema composed of multiple column definitions,
    providing a way to specify and validate the structure of data.

    Attributes:
        columns: List of column definitions that compose the schema.
        strict_columns: Boolean flag indicating whether to enforce strict column
                       validation. When True, only columns defined in the schema
                       are allowed. Defaults to False.

    Methods:
        from_dict: Class method to instantiate a SchemaDef from a dictionary
                  representation where keys are column names and values are
                  column specifications.
    """

    columns: List[ColumnDef]
    strict_columns: bool = False

    @classmethod
    def from_dict(cls, data: Dict[str, Any], strict: bool = False) -> "SchemaDef":
        """Create SchemaDef from dictionary specification."""
        cols = [ColumnDef.from_dict(k, v) for k, v in data.items()]
        return cls(columns=cols, strict_columns=strict)


@dataclass
class SchemaReport:
    """
    Schema validation report with detailed error tracking.

    Attributes:
        passed (bool): Whether the schema validation passed without errors.
        missing_cols (List[str]): List of column names that are missing from the data.
        type_errors (Dict[str, str]): Mapping of column names to their type error descriptions.
        metadata_errors (Dict[str, str]): Mapping of column names to their metadata error descriptions.
        extra_cols (List[str]): List of column names that are not defined in the schema.

    Methods:
        to_dict() -> Dict[str, Any]:
            Convert the report to a dictionary representation including all validation details
            and a total issue count.
        
        __repr__() -> str:
            Return a human-readable string representation showing validation status and issue count.
        
        __bool__() -> bool:
            Allow boolean conversion of the report for convenient conditional checks.
    """

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
