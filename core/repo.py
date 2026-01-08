import csv
import os
from datetime import datetime
from typing import List, Dict, Any, Optional

from core.models import POI, GapPoint, PosSample, PigState
from core.state import InMemoryStateStore

def _pick(row:dict, keys:List[str]) -> dict:
    """Try keys in order and return a cleaned non-empty string.
    Return "" if nothing found."""
    for k in keys:
        if k in row and (row.get(k) or "").strip():
            return row.get(k).strip()
    return ""

class CsvRepo:
    """CSV implementation (metadata in CSV files + in-memory demo telemetry)."""
    def __init__(self, root_dir: str = ".") -> None:
        self.root_dir = root_dir

        #metadate
        self._gc_to_kp: Dict[int, float] = {}
        self._pois: List[POI] = []
        self._gaps: List[GapPoint] = []

        #state (in-memory for now)
        self._state = InMemoryStateStore()

        #demo telemetry
        self._telemetry: Dict[str, List[PosSample]] = {}
        self._load_all()

# ---------- public API (used by Engine) ----------
def get_gc_to_kp_map(self) -> Dict[int, float]:
    return self._gc_to_kp

def get_pois(self) -> List[POI]:
    return self._pois

def get_gaps(self) -> List[GapPoint]:
    return self._gaps


def get_recent_positions(self, pig_id: str, since_dt: datetime) -> List[PosSample]:
    samples = self._telemetry.get(pig_id, [])
    return [s for s in samples if s.dt >= since_dt]

def get_state(self, pig_id: str) -> PigState:
    return self._state.get(pig_id)

def save_state(self, pig_id: str, state: PigState) -> None:
    self._state.upsert(pig_id, state)

# ---------- demo helpers ----------

def set_demo_telemetry(self, pig_id: str, samples: List[PosSample]) -> None:
    # telemtry has to be ordered by dt
    self._telemetry[pig_id] = sorted(samples, key=lambda s: s.dt)

# ---------- csv loading ----------
def _load_all(self) -> None:
    self._gc_to_kp = self._load_gc_to_kp(os.path.join(self.root_dir, "GCtoKP.csv"))
    self._pois = self._load_pois(os.path.join(self.root_dir, "POI.csv"))
    self._gaps = self._load_gaps(os.path.join(self.root_dir, "GAP.csv"))

@staticmethod
def _load_gc_to_kp(path: str) -> Dict[int, float]:
    if not os.path.exists(path):
        return {}
    
    m: Dict[int, float] = {}
    with open(path, "r", newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            gc_s = _pick(row, ["GC", "Global Channel", "GlobalChannel"])
            kp_s = _pick(row, ["KP", "Kilometer Post", "KilometerPost"])

            if not gc_s or not kp_s:
                continue
            try:
                m[int(float(gc_s))] = float(kp_s)
            except Exception:
            # skip bad rows
                continue
    return m

@staticmethod
def _load_pois(path: str) -> List[POI]:
    if not os.path.exists(path):
        return []
    
    out: List[POI] = []
    with open(path, "r", newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            tag = _pick(row, ["Tag", "Valve Tag", "tag", "valve tag", "ValveTag", "Valve_Tag"])
            if not tag:
                continue

            legacy = _pick(row, ["Legacy Route Name", "LegacyRouteName", "Legacy_Route", "LegacyRoute", "route"])
            valve_type = _pick(row, ["Type", "Valve Type", "valve type", "ValveType", "Valve_Type"])

            gc_s = _pick(row, ["GC", "Global Channel", "GlobalChannel"])
            kp_s = _pick(row, ["KP", "Kilometer Post", "KilometerPost"])

            gc: Optional[int] = None
            kp: Optional[float] = None

            try:
                if gc_s:
                    gc = int(float(gc_s))
            except Exception:
                    gc = None
            try:
                if kp_s:
                    kp = float(kp_s)
            except Exception:
                kp = None

            out.append(POI(tag=tag, valve_type=valve_type, gc=gc, kp=kp, legacy_route_name=legacy))

    return out

@staticmethod
def _load_gaps(path: str) -> List[GapPoint]:
    """gap.csv format (flexible headers)
    legacy route name, gap start/end, kp
    """

    if not os.path.exists(path):
        return []

    out: List[GapPoint] = []
    with open(path, "r", newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            legacy = _pick(row, ["Legacy Route Name", "LegacyRouteName", "Legacy_Route", "LegacyRoute", "route"])
            kind_row = _pick(row, ["Gap Start/End", "GapStartEnd", "Gap_Start_End", "gap start/end", "type", "Kind", "kind"])
            kp_s = _pick(row, ["KP", "Kilometer Post", "KilometerPost"])

            if not kp_s:
                continue

            try:
                kp = float(kp_s)
            except Exception:
                continue

            kind = "start" if "start" in kind_row else ("end" if "end" in kind_row else "")
            if not kind:
                continue

            out.append(GapPoint(legacy_route_name=legacy, kind=kind, kp=kp))

    return out



                
