from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, Set

@dataclass
class PosSample:
    '''One telemetry position sample.'''
    dt: datetime
    gc: Optional[int]
    kp: Optional[float]

@dataclass
class POI:
    tag: str
    valve_type: str
    global_channel: Optional[int]
    kp: Optional[float]
    legacy_route: str

@dataclass
class GapPoint:
    '''Start and end of a gap in coverage.'''
    legacy_route: str
    kind: str  # 'start' or 'end'
    kp: float

@dataclass
class PigState:
    '''State of a pig at a given time.'''
    legacy_route: Optional[str] = None

    last_pos_m: Optional[float] = None
    last_dt: Optional[datetime] = None

    first_notif_at: Optional[datetime] = None
    last_notif_at: Optional[datetime] = None

    # Notifications fired for tags to avoid duplicates
    fired_pre15_for_tag: Set[str] = None
    fired_pre30_for_tag: Set[str] = None 

    def __post_init__(self):
        if self.fired_pre15_for_tag is None:
            self.fired_pre15_for_tag = set()
        if self.fired_pre30_for_tag is None:
            self.fired_pre30_for_tag = set()

if __name__ == "__main__":
    # Example usage
    from datetime import datetime, timezone, timedelta

    MST = timezone(timedelta(hours=-7), "MST")

    s = PosSample(
        dt=datetime(2024, 1, 1, 12, 0, 0, tzinfo=MST),
        gc=123,
        kp=55
    )
    print(s)