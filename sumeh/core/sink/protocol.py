"""
Sumeh Sink Protocol.
Contract for exporting validation and profiling results to external catalogs.
"""
from typing import Protocol, Any, Dict, runtime_checkable
from sumeh.core.sink.model import SinkResult



@runtime_checkable
class SinkProtocol(Protocol):
    """
    Base protocol for all Sumeh sinks.
    
    Implementations export Sumeh results to:
    - OpenMetadata (Data Quality + Profiler)
    - DataHub (Assertions + Dataset Stats)
    - Great Expectations (Validation Results)
    - Custom catalogs (S3, databases, etc)
    """
    
    @property
    def name(self) -> str:
        """
        Sink identifier.
        
        Examples: 'openmetadata', 'datahub', 's3', 'postgres'
        """
        ...
    
    def send_validation(self, report: Any, **kwargs) -> SinkResult:
        """
        Export validation report to external catalog.
        
        Args:
            report: ValidationReport from engine.validate()
            **kwargs: Sink-specific params (table_fqn, connection_id, etc)
            
        Returns:
            SinkResult with operation status
            
        Maps to:
            - OpenMetadata: POST /dataQuality/testCases
            - DataHub: POST /assertions
            - GX: validation_result.json
        """
        ...
    
    def send_profile(self, profile: Dict[str, Any], **kwargs) -> SinkResult:
        """
        Export statistical profile to external catalog.
        
        Args:
            profile: Profile dict from DataProfiler.profile()
            **kwargs: Sink-specific params
            
        Returns:
            SinkResult with operation status
            
        Maps to:
            - OpenMetadata: POST /tableProfile + /columnProfile
            - DataHub: POST /datasets/{urn}/profile
            - S3: profile.json upload
        """
        ...