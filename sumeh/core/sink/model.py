from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class SinkResult:
    """
    Structured response for metadata export operations.
    Allows the orchestrator to monitor health and performance.
    """
    success: bool
    sink_name: str
    duration_ms: float
    records_sent: int
    timestamp: datetime
    error: Optional[str] = None

    def __bool__(self) -> bool:
        """Allow direct boolean check: if result: ..."""
        return self.success
    
    def __repr__(self) -> str:
        """Human-readable representation."""
        status = "✓" if self.success else "✗"
        msg = f"{status} {self.sink_name} ({self.duration_ms:.1f}ms, {self.records_sent} records)"
        if self.error:
            msg += f" - Error: {self.error}"
        return msg