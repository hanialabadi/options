from dataclasses import dataclass, field
from typing import Dict, Any, Optional

@dataclass(frozen=True)
class ChartStateResult:
    state: str
    raw_metrics: Dict[str, Any] = field(default_factory=dict)
    resolution_reason: str = "RESOLVED"
    data_complete: bool = True
