from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

@dataclass(frozen=True)
class PosSample:
    "One telemetry point for a PIG"
    dt: datetime
    gc: Optional[int]  # Global Channel
    kp: Optional[float]  # Kilometer Point

@dataclass(frozen=True)
class POI:
    """ POI metadata"""
    tag: str
    valve_type: Optional[str]
    global_channel: Optional[int]
    kp: Optional[float] 
    legacy_route: str
    
@dataclass(frozen=True)
class GapPoint:
    """Gap boundary in a legacy route"""
    legacy_route: str
    kind: str  # 'start' or 'end'
    kp: float

@dataclass
class PigState:
    """Persisted per pig_id to make decisions consistent over runs"""

    # Sticky route: keep chosen Legacy Route untill Completed
    locked_legacy_route: Optional[str] = None

    # 30 min update cadence
    first_notif_at: Optional[datetime] = None
    last_notif_at: Optional[datetime] = None

    # Pre-POI de-duplication (store the last tag we fired for)
    fired_pre30_for_tag: Optional[str] = None
    fired_pre15_for_tag: Optional[str] = None

    # Transitions / history (used later for speed window selection)
    last_event: Optional[str] = None
    last_event_dt: Optional[datetime] = None
    moving_started_at: Optional[datetime] = None

 