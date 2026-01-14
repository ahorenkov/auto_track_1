from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class PosSample:
    """One telemetry point for a PIG."""
    dt: datetime
    gc: Optional[int] = None   # Global Channel
    kp: Optional[float] = None # Kilometer Point


@dataclass(frozen=True)
class POI:
    """Point of Interest (valve) metadata."""
    tag: str
    valve_type: str
    global_channel: Optional[int]
    kp: Optional[float]
    legacy_route: str


@dataclass(frozen=True)
class GapPoint:
    """Gap boundary in a legacy route."""
    legacy_route: str
    kind: str  # "start" | "end"
    kp: float


@dataclass
class PigState:
    """Persisted per pig_id to make decisions consistent across runs."""

    # Sticky route: keep chosen legacy route until Completed
    locked_legacy_route: Optional[str] = None

    # 30-min update cadence
    first_notif_at: Optional[datetime] = None
    last_notif_at: Optional[datetime] = None

    # pre-POI de-duplication
    fired_pre30_for_tag: Optional[str] = None
    fired_pre15_for_tag: Optional[str] = None

    # --- Variant A: transitions for speed window selection ---
    last_event: Optional[str] = None
    last_event_dt: Optional[datetime] = None
    moving_started_at: Optional[datetime] = None
