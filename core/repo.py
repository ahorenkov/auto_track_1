from typing import Dict, List, Optional
from datetime import datetime

from core.models import PosSample, POI, GapPoint, PigState
from core.state import InMemoryStateStore

import csv
from pathlib import Path

class CsvRepo:
    ''' Demo repo based on CSV files and in-memory telemetry'''

    def __init__(self, base_dir: str = ".") -> None:
        self._state_store = InMemoryStateStore()
        self._states: Dict[str, PigState] = {}


        base = Path(base_dir)

        # metadata
        self._gc_to_kp:  Dict[int, float] = _load_gc_to_kp(str(base / "GCtoKP.csv"))
        self._pois: List[POI] = _load_pois(str(base / "Pig Tracking POI Valves Locations - Monitoring Team_updated(Nov11).csv"))
        self._gaps: List[GapPoint] = _load_gaps(str(base / "GAP.csv"))

        # demo telemetry    
        self._telemetry: Dict[str, List[PosSample]] = {}

    def get_state(self, pig_id:str) -> Optional[PigState]:
        return self._states.get(pig_id)
    
    def save_state(self, pig_id: str, state: PigState) -> None:
        self._states[pig_id] = state
    
    def set_demo_telemetry(self, pig_id: str, samples: List[PosSample]) -> None:
        self._telemetry[pig_id] = samples

    def get_recent_positions(self, pig_id: str, since: datetime) -> List[PosSample]:
        samples = self._telemetry.get(pig_id, [])
        return [s for s in samples if s.dt >= since]
    
    def get_pois(self) -> List[POI]:
        return self._pois
    
    def get_gaps(self) -> List[GapPoint]:
        return self._gaps
    
    def get_gc_to_kp(self) -> Dict[int, float]:
        return self._gc_to_kp
    

# Helpers

def _pick(row: dict, keys: List[str]) -> Optional[str]:
    for k in keys:
        v = row.get(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return None

def _to_int(s: Optional[str]) -> Optional[int]:
    if s is None:
        return None
    s = s.strip()
    if s == "":
        return None
    try:
        return int(s)
    except Exception:
        return None

def _to_float(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    s = s.strip()
    if s == "":
        return None
    try:
        return float(s)
    except Exception:
        return None
    

def _load_gc_to_kp(path: str) -> Dict[int, float]:

    mapping: Dict[int, float] = {}

    
    if not Path(path).exists():
        return mapping
    
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            gc_raw = _pick(row, ["Global Channel", "GC", "gc"])
            kp_raw = _pick(row, ["KP", "kp"])

            gc = _to_int(gc_raw)
            kp = _to_float(kp_raw)

            if gc is None or kp is None:
                continue

            mapping[gc] = kp
        
    return mapping

def _load_pois(path: str) -> List[POI]:
    pois: List[POI] = []

    if not Path(path).exists():
        return pois
    
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tag = _pick(row, ["Valve Tag", "valve tag", "valve_tag", "Tag", "tag"])
            legacy = _pick(row, ["Legacy Route Name", "Legacy_Route_Name", "route", "legacy_route"])
            valve_type = _pick(row, ["Valve Type", "valve_type", "valve type"])

            if tag is None or legacy is None:
                continue

            gc_raw = _pick(row, ["GlobalChannel", "GC", "gc"])
            kp_raw = _pick(row, ["KP", "kp"])

            gc = _to_int(gc_raw)
            kp = _to_float(kp_raw)

            vt = (valve_type or "").strip()

            pois.append(
                POI(
                    tag=tag.strip(),
                    valve_type=vt,
                    global_channel=gc,
                    kp=kp,
                    legacy_route=legacy.strip()
                )
            )

    return pois

def _load_gaps(path:str) -> List[GapPoint]:
    gaps: List[GapPoint] = []

    if not Path(path).exists():
        return gaps
    
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            legacy = _pick(row, ["Legacy Route Name", "Legacy_Route_Name", "route", "legacy_route"])
            kp_raw = _pick(row, ["KP", "kp"])
            kind = _pick(row, ["Kind", "kind"])

            if legacy is None or kind is None or kp_raw is None:
                continue

            kind_norm = kind.strip().lower()
            if kind_norm not in ("start", "end"):
                continue
            
            kp = _to_float(kp_raw)
            if kp is None:
                continue

            gaps.append(
                GapPoint(
                    legacy_route=legacy.strip(),
                    kind=kind_norm,
                    kp=kp
                )
            )

    return gaps

