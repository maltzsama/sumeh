"""
OpenMetadata Sink for Sumeh.
Exports validation results and statistical profiles to OpenMetadata catalog.
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from sumeh.core.services.sink.model import SinkResult
from sumeh.core.base.protocols import SinkProtocol

logger = logging.getLogger(__name__)


class OpenMetadataSink(SinkProtocol):
    """
    OpenMetadata catalog sink.

    Exports Sumeh validation and profiling results to OpenMetadata Data Quality
    and Profiler endpoints.

    Args:
        api_url: OpenMetadata API base URL (e.g., 'http://localhost:8585')
        token: JWT authentication token
        timeout: Request timeout in seconds (default: 30)
        max_retries: Max retry attempts for failed requests (default: 3)

    Example:
        >>> sink = OpenMetadataSink(
        ...     api_url="http://localhost:8585",
        ...     token="eyJhbGc..."
        ... )
        >>> result = sink.send_profile(profile, table_fqn="default.public.users")
        >>> if result:
        ...     print(f"✓ Sent {result.records_sent} columns")
    """

    def __init__(
        self, api_url: str, token: str, timeout: int = 30, max_retries: int = 3
    ):
        self.api_url = api_url.rstrip("/")
        self.token = token
        self.timeout = timeout

        # Setup session with retry strategy
        self._session = self._create_session(max_retries)

    def _create_session(self, max_retries: int) -> requests.Session:
        """Create session with retry strategy and connection pooling."""
        session = requests.Session()

        # Retry strategy
        retry_strategy = Retry(
            total=max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["GET", "PUT", "POST"],
            backoff_factor=1,  # 1s, 2s, 4s, 8s...
        )

        adapter = HTTPAdapter(
            max_retries=retry_strategy, pool_connections=10, pool_maxsize=10
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Headers
        session.headers.update(
            {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            }
        )

        return session

    @property
    def name(self) -> str:
        """Sink identifier."""
        return "openmetadata"

    def send_validation(self, report: Any, **kwargs) -> SinkResult:
        """
        Export validation report to OpenMetadata Data Quality.

        Args:
            report: ValidationReport from engine.validate()
            **kwargs: Must include 'table_fqn' (e.g., 'default.public.users')

        Returns:
            SinkResult with operation status

        OpenMetadata mapping:
            - Each validation rule → TestCase result
            - POST to /api/v1/testCases/name/{fqn}/testCaseResult
        """
        table_fqn = kwargs.get("table_fqn")
        if not table_fqn:
            return SinkResult(
                success=False,
                sink_name=self.name,
                duration_ms=0.0,
                records_sent=0,
                timestamp=datetime.utcnow(),
                error="Missing required parameter: table_fqn",
            )

        start_time = datetime.utcnow()
        timestamp_ms = int(start_time.timestamp() * 1000)

        sent_count = 0
        errors = []

        # Send each validation result as a test case
        for result in report.results:
            test_case_fqn = f"{table_fqn}.{result.field}_{result.check_type}"

            payload = {
                "timestamp": timestamp_ms,
                "testCaseStatus": "Success" if result.status == "PASS" else "Failed",
                "result": result.message or "",
                "testResultValue": [
                    {"name": "actual_value", "value": str(result.actual_value)}
                ],
            }

            url = f"{self.api_url}/api/v1/testCases/name/{test_case_fqn}/testCaseResult"

            try:
                response = self._session.put(url, json=payload, timeout=self.timeout)
                response.raise_for_status()
                sent_count += 1

            except requests.exceptions.RequestException as e:
                error_msg = f"TestCase {test_case_fqn}: {str(e)}"
                logger.warning(error_msg)
                errors.append(error_msg)

        duration_ms = (datetime.utcnow() - start_time).total_seconds() * 1000

        # Success if at least some results were sent
        success = sent_count > 0
        error = "; ".join(errors) if errors else None

        return SinkResult(
            success=success,
            sink_name=self.name,
            duration_ms=duration_ms,
            records_sent=sent_count,
            timestamp=datetime.utcnow(),
            error=error,
        )

    def send_profile(self, profile: Dict[str, Any], **kwargs) -> SinkResult:
        """
        Export statistical profile to OpenMetadata Profiler.

        Args:
            profile: Profile dict from DataProfiler.profile()
            **kwargs: Must include 'table_fqn'

        Returns:
            SinkResult with operation status

        OpenMetadata mapping:
            - Table stats + all column stats in single request
            - PUT to /api/v1/tables/{id}/tableProfile
        """
        table_fqn = kwargs.get("table_fqn")
        if not table_fqn:
            return SinkResult(
                success=False,
                sink_name=self.name,
                duration_ms=0.0,
                records_sent=0,
                timestamp=datetime.utcnow(),
                error="Missing required parameter: table_fqn",
            )

        start_time = datetime.utcnow()

        # Resolve FQN to table ID
        table_id = self._resolve_table_id(table_fqn)
        if not table_id:
            return SinkResult(
                success=False,
                sink_name=self.name,
                duration_ms=0.0,
                records_sent=0,
                timestamp=datetime.utcnow(),
                error=f"Could not resolve table FQN: {table_fqn}",
            )

        # Build OpenMetadata profile payload
        payload = self._build_profile_payload(profile)

        # Send profile
        url = f"{self.api_url}/api/v1/tables/{table_id}/tableProfile"

        try:
            response = self._session.put(url, json=payload, timeout=self.timeout)
            response.raise_for_status()

            duration_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
            column_count = len(profile.get("column_profiles", {}))

            return SinkResult(
                success=True,
                sink_name=self.name,
                duration_ms=duration_ms,
                records_sent=column_count,
                timestamp=datetime.utcnow(),
            )

        except requests.exceptions.RequestException as e:
            duration_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
            return SinkResult(
                success=False,
                sink_name=self.name,
                duration_ms=duration_ms,
                records_sent=0,
                timestamp=datetime.utcnow(),
                error=str(e),
            )

    def _resolve_table_id(self, table_fqn: str) -> Optional[str]:
        """
        Resolve table FQN to UUID.

        Args:
            table_fqn: Fully qualified table name (e.g., 'default.public.users')

        Returns:
            Table UUID or None if not found
        """
        url = f"{self.api_url}/api/v1/tables/name/{table_fqn}"

        try:
            response = self._session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json().get("id")

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to resolve table FQN {table_fqn}: {e}")
            return None

    def _build_profile_payload(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build OpenMetadata profile payload from Sumeh profile.

        Args:
            profile: Sumeh profile dict

        Returns:
            OpenMetadata-compatible payload
        """
        table_stats = profile.get("table_stats", {})
        column_profiles = profile.get("column_profiles", {})

        # Build column profiles
        columns = []
        for col_name, col_stats in column_profiles.items():
            col_payload = {
                "name": col_name,
                "valuesCount": table_stats.get("total_rows", 0),
                "nullCount": col_stats.get("null_count", 0),
                "distinctCount": col_stats.get("distinct_count", 0),
            }

            # Add numeric stats if present
            if "min" in col_stats:
                col_payload["min"] = col_stats["min"]
            if "max" in col_stats:
                col_payload["max"] = col_stats["max"]
            if "mean" in col_stats:
                col_payload["mean"] = col_stats["mean"]
            if "std_dev" in col_stats:
                col_payload["stddev"] = col_stats["std_dev"]

            columns.append(col_payload)

        return {
            "timestamp": int(datetime.utcnow().timestamp() * 1000),
            "columnCount": table_stats.get("columns_count", len(columns)),
            "rowCount": table_stats.get("total_rows", 0),
            "columnProfile": columns,
        }

    def __del__(self):
        """Close session on cleanup."""
        if hasattr(self, "_session"):
            self._session.close()
