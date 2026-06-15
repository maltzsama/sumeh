"""
OpenMetadata Export - "The Variable-Only Mode".
Translates Sumeh results into OpenMetadata schema.
Zero I/O. Zero Side Effects.
"""

from datetime import datetime
from typing import Any, Dict, List

from sumeh.core.base.protocols import IExporter


class OpenMetadataExport(IExporter):
    """
    Metadata exporter for the OpenMetadata ecosystem.
    Generates ready-to-use dictionaries (payloads) for external consumption.
    """

    def __init__(self, table_fqn: str):
        self.table_fqn = table_fqn

    def profile(self, profile_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Returns a formatted dictionary for the /tableProfile endpoint.
        """
        ts = int(datetime.utcnow().timestamp() * 1000)
        table_stats = profile_data.get("table_stats", {})
        col_stats = profile_data.get("column_profiles", {})

        return {
            "timestamp": ts,
            "rowCount": table_stats.get("total_rows"),
            "columnCount": table_stats.get("columns_count"),
            "columnProfile": [
                {
                    "name": col,
                    "timestamp": ts,
                    "nullCount": s.get("null_count"),
                    "distinctCount": s.get("distinct_count"),
                    "min": str(s.get("min")) if s.get("min") is not None else None,
                    "max": str(s.get("max")) if s.get("max") is not None else None,
                    "mean": s.get("mean"),
                    "sum": s.get("sum"),
                }
                for col, s in col_stats.items()
            ],
        }

    def validation(self, report: Any) -> Dict[str, List[Dict[str, Any]]]:
        """
        THE MASTER STROKE: Returns a comprehensive package with definitions and results.
        Makes life easier for developers who want to iterate over everything at once.
        """
        return {
            "definitions": self.test_definitions(report),
            "results": self.test_results(report),
        }

    def test_definitions(self, report: Any) -> List[Dict[str, Any]]:
        """
        Generates Test Case definitions (CreateTestCaseRequest).
        """
        return [
            {
                "name": self._test_name(res),
                "description": f"Sumeh DQ: {res.check_type}",
                "testDefinition": self._map_test_definition(res.check_type),
                "entityLink": self._entity_link(res),
                "testSuite": f"{self.table_fqn}.testSuite",
            }
            for res in report.results
        ]

    def test_results(self, report: Any) -> List[Dict[str, Any]]:
        """
        Generates execution results (TestCaseResult).
        """
        return [
            {
                "test_case_fqn": f"{self.table_fqn}.{self._test_name(res)}",
                "payload": {
                    "timestamp": int(datetime.utcnow().timestamp() * 1000),
                    "testCaseStatus": (
                        "Success" if res.status.value == "PASS" else "Failed"
                    ),
                    "result": f"Sumeh Pass Rate: {getattr(res, 'pass_rate', 0.0):.2%}",
                },
            }
            for res in report.results
        ]

    def _test_name(self, res: Any) -> str:
        field = getattr(res, "field", None)
        return f"sumeh_{res.check_type}_{field or 'table'}"

    def _entity_link(self, res: Any) -> str:
        field = getattr(res, "field", None)
        if field and getattr(res.level, "value", "") == "ROW":
            return f"<#E::table::{self.table_fqn}::columns::{field}>"
        return f"<#E::table::{self.table_fqn}>"

    def _map_test_definition(self, check_type: str) -> str:
        """
        Maps Sumeh check types to OpenMetadata test definitions.
        This saves developers from having to guess API endpoint names.
        """
        mapping = {
            "is_complete": "columnValuesToBeNotNull",
            "is_positive": "columnValuesToBeAtLeast",
            "is_contained_in": "columnValuesToBeInSet",
            "is_past_date": "columnValuesToBeBetween",
        }
        return mapping.get(check_type, "columnValuesToBeNotNull")
