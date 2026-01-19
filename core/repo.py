from __future__ import annotations

import csv
import os
from datetime import datetime
from typing import Dict, List, Protocol, Optional
from dataclasses import asdict

from core import state
from core.models import POI, GapPoint, PosSample, PigState
from core.state import InMemoryStateStore
import psycopg
import json


class TelemetryRepo(Protocol):
    """Repository interface. CSV and Postgres must implement these methods."""

    def get_gc_to_kp(self) -> Dict[int, float]: ...
    def get_pois(self) -> List[POI]: ...
    def get_gaps(self) -> List[GapPoint]: ...

    def get_recent_positions(self, pig_id: str, since_dt: datetime) -> List[PosSample]: ...

    def get_state(self, pig_id: str) -> PigState: ...
    def save_state(self, pig_id: str, state: PigState) -> None: ...


def _pick(row: dict, keys: List[str]) -> str:
    for k in keys:
        if k in row and (row.get(k) or "").strip():
            return (row.get(k) or "").strip()
    return ""


class CsvRepo:
    """CSV implementation:
    - POI.csv
    - GCtoKP.csv
    - gap.csv  (legacy route name, gap start/gap end, kp)
    All are expected to be in root_dir.
    Telemetry is demo-only (set_demo_telemetry)."""

    def __init__(self, root_dir: str = ".") -> None:
        self.root_dir = root_dir
        self._gc_to_kp: Dict[int, float] = {}
        self._pois: List[POI] = []
        self._gaps: List[GapPoint] = []
        self._state = InMemoryStateStore()
        self._telemetry: Dict[str, List[PosSample]] = {}
        self._load_all()

    def _load_all(self) -> None:
        self._gc_to_kp = self._load_gc_to_kp(os.path.join(self.root_dir, "GCtoKP.csv"))
        self._pois = self._load_pois(os.path.join(self.root_dir, "POI.csv"))
        self._gaps = self._load_gaps(os.path.join(self.root_dir, "gap.csv"))

    def get_gc_to_kp(self) -> Dict[int, float]:
        return self._gc_to_kp

    def get_pois(self) -> List[POI]:
        return self._pois

    def get_gaps(self) -> List[GapPoint]:
        return self._gaps

    # --- demo telemetry (replace with Postgres later) ---
    def set_demo_telemetry(self, pig_id: str, samples: List[PosSample]) -> None:
        self._telemetry[pig_id] = sorted(samples, key=lambda s: s.dt)

    def get_recent_positions(self, pig_id: str, since_dt: datetime) -> List[PosSample]:
        samples = self._telemetry.get(pig_id, [])
        return [s for s in samples if s.dt >= since_dt]

    def get_state(self, pig_id: str) -> PigState:
        return self._state.get(pig_id)

    def save_state(self, pig_id: str, state: PigState) -> None:
        self._state.upsert(pig_id, state)

    # ---- CSV loaders ----
    @staticmethod
    def _load_gc_to_kp(path: str) -> Dict[int, float]:
        if not os.path.exists(path):
            return {}
        m: Dict[int, float] = {}
        with open(path, "r", newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                gc_s = _pick(row, ["Global Channel", "GC"])
                kp_s = _pick(row, ["KP", "matched_kp", "kp"])
                if not gc_s or not kp_s:
                    continue
                try:
                    m[int(float(gc_s))] = float(kp_s)
                except Exception:
                    continue
        return m

    @staticmethod
    def _load_pois(path: str) -> List[POI]:
        if not os.path.exists(path):
            return []
        out: List[POI] = []
        with open(path, "r", newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                tag = _pick(row, ["Valve Tag", "Tag"])
                if not tag:
                    continue
                legacy_row = _pick(row, ["Legacy Route Name", "Legacy Route", "Legacy"]) or "Unknown"
                legacy = _norm_legacy(legacy_row)
                vt = _pick(row, ["Valve Type", "Type"])
                gc_s = _pick(row, ["Global Channel", "GC"])
                kp_s = _pick(row, ["KP", "matched_kp", "kp"])

                gc = None
                kp = None
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

                out.append(POI(tag=tag, valve_type=vt, global_channel=gc, kp=kp, legacy_route=legacy))
        return out

    @staticmethod
    def _load_gaps(path: str) -> List[GapPoint]:
        """gap.csv format: legacy route name, gap start/gap end, kp."""
        if not os.path.exists(path):
            return []
        out: List[GapPoint] = []
        with open(path, "r", newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                legacy_row = _pick(row, ["Legacy Route Name", "Legacy Route", "legacy_route", "route"]) or "Unknown"
                legacy = _norm_legacy(legacy_row)
                kind_raw = _pick(row, ["Gap", "Gap Type", "gap", "kind"]).strip().lower()
                kp_s = _pick(row, ["KP", "kp"])
                if not kp_s:
                    continue
                try:
                    kp = float(kp_s)
                except Exception:
                    continue

                kind = "start" if "start" in kind_raw else ("end" if "end" in kind_raw else "")
                if not kind:
                    continue

                out.append(GapPoint(legacy_route=legacy, kind=kind, kp=kp))
        return out


class PostgresRepo:
    
    def __init__(self, dsn: str, root_dir: str=".") -> None:
        self.dsn = dsn
        self._csv = CsvRepo(root_dir=root_dir)

    def get_gc_to_kp(self) -> Dict[int, float]:
        return self._csv.get_gc_to_kp()

    def get_pois(self) -> List[POI]:
        return self._csv.get_pois()
    
    def get_gaps(self) -> List[GapPoint]:
        return self._csv.get_gaps()
    
    def get_recent_positions(self, pig_id: str, since_dt: datetime) -> List[PosSample]:
        sql = """
        SELECT ts, gc, kp
        FROM pig_positions
        WHERE pig_id = %s AND ts >= %s
        ORDER BY ts ASC
        """
        out: List[PosSample] = []
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (pig_id, since_dt))
                rows = cur.fetchall()
                for ts, gc, kp in rows:
                    out.append(PosSample(dt=ts, gc=gc, kp=kp))
        return out

    def get_state(self, pig_id: str) -> PigState:
        sql = "SELECT state_json FROM pig_state WHERE pig_id = %s"
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (pig_id,))
                row = cur.fetchone()
        if not row:
            return PigState()
        
        data = row[0]
        st = PigState()
        for k, v in (data or {}).items():
            if not hasattr(st, k):
                continue
            if k.endswith("_at") and v is not None:
                setattr(st, k, _parse_dt(v))
            else:
                setattr(st, k, v)
        return st


    def save_state(self, pig_id: str, state: PigState) -> None:
        sql = """
        INSERT INTO pig_state (pig_id, state_json, updated_at)
        VALUES (%s, %s::jsonb, NOW())
        ON CONFLICT (pig_id) 
        DO UPDATE set state_json = EXCLUDED.state_json, updated_at = NOW()
        """
        payload = json.dumps(asdict(state), default=str)
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (pig_id, payload))
            conn.commit()

def _parse_dt(v):
    if isinstance(v, str):
        return datetime.fromisoformat(v)
    return v

def _norm_legacy(s: str) -> str:
    return (s or "").strip().casefold() or "unknown"