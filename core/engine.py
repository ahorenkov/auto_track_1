from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

from core.models import PosSample, POI, GapPoint, PigState
from core.repo import TelemetryRepo


@dataclass(frozen=True)
class EngineConfig:
    meters_per_channel: int = 25
    poi_tol_meters: int = 50

    # Moving/Stopped: look back last 5 minutes
    stopped_window_sec: int = 300

    # Pre-POI notification time match: ±60 seconds around ETA-15/ETA-30
    prepoi_time_window_sec: int = 60

    eps_kp: float = 1e-3

    # --- Speed/ETA windows ---
    speed_window_sec: int = 1500          # long window: 25 minutes
    speed_short_window_sec: int = 300     # short window right after restart: 5 minutes
    moving_boost_sec: int = 600           # use short window for first 10 minutes of movement
    min_speed_dt_sec: int = 120           # require at least 2 minutes between ref and cur

    # Fetch more history than 25 minutes so we can find ref <= (now-25m)
    speed_search_sec: int = 2100          # 35 minutes


def _pos_m(sample: PosSample, gc_to_kp: Dict[int, float], meters_per_channel: int) -> Optional[float]:
    """Convert sample to meters along line: prefer KP, else GC->KP, else GC*meters_per_channel."""
    if sample.kp is not None:
        return sample.kp * 1000.0
    if sample.gc is not None:
        kp = gc_to_kp.get(sample.gc)
        if kp is not None:
            return kp * 1000.0
        return sample.gc * meters_per_channel
    return None

def _poi_pos_m_(
        poi:POI,
        gc_to_kp: Dict[int, float],
        meters_per_channel: int
    ) -> Optional[float]:
    if poi.kp is not None:
        return poi.kp * 1000.0
    if poi.global_channel is not None:
        kp = gc_to_kp.get(int(poi.global_channel))
        if kp is not None:
            return kp * 1000.0
        return int(poi.global_channel) * meters_per_channel
    return None


def _current_sample(samples: List[PosSample]) -> Optional[PosSample]:
    """Return the newest sample by dt (or None if empty)."""
    if not samples:
        return None
    return max(samples, key=lambda s: s.dt)


def pick_ref_sample_at_or_before(samples: List[PosSample], target_dt: datetime) -> Optional[PosSample]:
    """Prefer dt <= target_dt and closest to it. Fallback: closest by absolute time distance."""
    if not samples:
        return None

    older_or_equal = [s for s in samples if s.dt <= target_dt]
    if older_or_equal:
        return min(older_or_equal, key=lambda s: (target_dt - s.dt).total_seconds())

    return min(samples, key=lambda s: abs((s.dt - target_dt).total_seconds()))


def speed_mps_by_ref(cur: PosSample, ref: PosSample, gc_to_kp: Dict[int, float], cfg: EngineConfig) -> float:
    dt_s = (cur.dt - ref.dt).total_seconds()
    if dt_s <= 0:
        return 0.0
    p_cur = _pos_m(cur, gc_to_kp, cfg.meters_per_channel)
    p_ref = _pos_m(ref, gc_to_kp, cfg.meters_per_channel)
    if p_cur is None or p_ref is None:
        return 0.0
    return abs(p_cur - p_ref) / dt_s


def _build_routes(pois: List[POI]) -> Dict[str, List[POI]]:
    routes: Dict[str, List[POI]] = {}
    for p in pois:
        routes.setdefault(p.legacy_route or "Unknown", []).append(p)

    def sort_key(p: POI) -> Tuple[int, float, int, int, str]:
        kp_missing = 1 if p.kp is None else 0
        kp_val = float("inf") if p.kp is None else float(p.kp)
        gc_missing = 1 if p.global_channel is None else 0
        gc_val = 10**12 if p.global_channel is None else int(p.global_channel)
        return (kp_missing, kp_val, gc_missing, gc_val, p.tag)

    for name in list(routes.keys()):
        routes[name] = sorted(routes[name], key=sort_key)
    return routes


def _route_range_m(route: List[POI], gc_to_kp: Dict[int, float], cfg: EngineConfig) -> Tuple[Optional[float], Optional[float]]:
    vals = []
    for p in route:
        pm = _poi_pos_m_(p, gc_to_kp, cfg.meters_per_channel)
        if pm is not None:
            vals.append(pm)

    if not vals:
        return (None, None)
    return (min(vals), max(vals))


def pick_legacy_route(
    state: PigState,
    routes: Dict[str, List[POI]],
    cur: PosSample,
    gc_to_kp: Dict[int, float],
    cfg: EngineConfig,
    pig_event: str,
) -> str:
    """Sticky legacy route until Completed."""
    if state.locked_legacy_route and state.locked_legacy_route != "Unknown" and pig_event != "Completed":
        return state.locked_legacy_route

    cur_m = _pos_m(cur, gc_to_kp, cfg.meters_per_channel)
    if cur_m is None:
        return "Unknown"

    tol_m = float(cfg.poi_tol_meters)
    candidates: List[Tuple[float, str]] = []

    for name, route in routes.items():
        rmin_m, rmax_m = _route_range_m(route, gc_to_kp, cfg)
        if rmin_m is None or rmax_m is None:
            continue
        if (rmin_m- tol_m) <= cur_m <= (rmax_m + tol_m):
            candidates.append((rmax_m - rmin_m, name))
            

    picked = min(candidates)[1] if candidates else "Unknown"
    if picked != "Unknown":
        state.locked_legacy_route = picked
    return picked

def pick_legacy_route_by_nearest_poi(
    state: PigState,
    pois: List[POI],
    cur: PosSample,
    gc_to_kp: Dict[int, float],
    cfg: EngineConfig,
    pig_event: str,
) -> str:
    # sticky until Completed
    if state.locked_legacy_route and state.locked_legacy_route != "Unknown" and pig_event != "Completed":
        return state.locked_legacy_route

    cur_m = _pos_m(cur, gc_to_kp, cfg.meters_per_channel)
    if cur_m is None:
        return "Unknown"

    best: Optional[Tuple[float, str]] = None  # (distance_m, legacy_route)

    for p in pois:
        pm = _poi_pos_m_(p, gc_to_kp, cfg.meters_per_channel)
        if pm is None:
            continue

        dist = abs(cur_m - pm)
        if best is None or dist < best[0]:
            best = (dist, p.legacy_route or "Unknown")

    if best is None:
        picked = "Unknown"
    else:
        picked = best[1]

    if picked != "Unknown":
        state.locked_legacy_route = picked
    return picked


def find_prev_next_end(route: List[POI], cur: PosSample, gc_to_kp: Dict[int, float], cfg: EngineConfig) -> Tuple[Optional[POI], Optional[POI], Optional[POI]]:
    if not route:
        return (None, None, None)

    cur_m = _pos_m(cur, gc_to_kp, cfg.meters_per_channel)
    if cur_m is None:
        return (None, None, route[-1])

    def poi_m(p: POI) -> Optional[float]:
        return _poi_pos_m_(p, gc_to_kp, cfg.meters_per_channel)

    prev = None
    nextp = None
    for p in route:
        pm = _poi_pos_m_(p, gc_to_kp, cfg.meters_per_channel)
        if pm is None:
            continue
        if pm < cur_m - cfg.poi_tol_meters:
            prev = p
        elif abs(pm - cur_m) <= cfg.poi_tol_meters:
            prev = p
        elif pm > cur_m + cfg.poi_tol_meters:
            nextp = p
            break
    return (prev, nextp, route[-1])


def _is_close_to_poi(cur: PosSample, poi: POI, gc_to_kp: Dict[int, float], cfg: EngineConfig) -> bool:
    cur_m = _pos_m(cur, gc_to_kp, cfg.meters_per_channel)
    if cur_m is None:
        return False
    if poi.kp is not None:
        trg_m = poi.kp * 1000.0
    elif poi.global_channel is not None:
        trg_m = _poi_pos_m_(poi, gc_to_kp, cfg.meters_per_channel)
        if trg_m is None:
            return False
    else:
        return False
    return abs(cur_m - trg_m) <= cfg.poi_tol_meters


def _is_close_to_gap(cur: PosSample, gap: GapPoint, gc_to_kp: Dict[int, float], cfg: EngineConfig) -> bool:
    cur_m = _pos_m(cur, gc_to_kp, cfg.meters_per_channel)
    if cur_m is None:
        return False
    gap_m = gap.kp * 1000.0
    return abs(cur_m - gap_m) <= cfg.poi_tol_meters


def infer_pig_event(
    recent_samples: List[PosSample],
    route_end_poi: Optional[POI],
    gc_to_kp: Dict[int, float],
    cfg: EngineConfig,
) -> str:
    """Moving/Stopped based on last 5 minutes; Completed if near last POI."""
    cur = _current_sample(recent_samples)
    if cur is None:
        return "Not Detected"

    if route_end_poi and _is_close_to_poi(cur, route_end_poi, gc_to_kp, cfg):
        return "Completed"

    vals = [v for v in (_pos_m(s, gc_to_kp, cfg.meters_per_channel) for s in recent_samples) if v is not None]
    if len(vals) < 2:
        return "Not Detected"

    span = max(vals) - min(vals)

    return "Stopped" if span <= cfg.poi_tol_meters else "Moving"


def eta_from_to(cur: PosSample, target: POI, speed: float, gc_to_kp: Dict[int, float], cfg: EngineConfig) -> Optional[datetime]:
    if speed <= 0:
        return None
    cur_m = _pos_m(cur, gc_to_kp, cfg.meters_per_channel)
    if cur_m is None:
        return None

    if target.kp is not None:
        trg_m = target.kp * 1000.0
    elif target.global_channel is not None:
        trg_m = _poi_pos_m_(target, gc_to_kp, cfg.meters_per_channel)
        if trg_m is None:
            return None
    else:
        return None

    dist_m = trg_m - cur_m
    if dist_m < 0:
        return None
    return cur.dt + timedelta(seconds=(dist_m / speed))


def infer_notification_type(
    state: PigState,
    pig_event: str,
    cur: PosSample,
    legacy_route: str,
    route: List[POI],
    next_poi: Optional[POI],
    end_poi: Optional[POI],
    gaps: List[GapPoint],
    eta_next: Optional[datetime],
    gc_to_kp: Dict[int, float],
    cfg: EngineConfig,
) -> str:
    """Priority: Completion > POI Passage > Gap > pre-POI > 30-min update."""
    now = cur.dt

    # 1) Completion
    if pig_event == "Completed":
        return "Run Completion"
    if end_poi and _is_close_to_poi(cur, end_poi, gc_to_kp, cfg):
        return "Run Completion"

    # 2) POI Passage
    for p in route:
        if _is_close_to_poi(cur, p, gc_to_kp, cfg):
            return "POI Passage"

    # 3) Gap Start/End
    for g in gaps:
        if g.legacy_route != legacy_route:
            continue
        if _is_close_to_gap(cur, g, gc_to_kp, cfg):
            return "Gap Start" if g.kind == "start" else "Gap End"

    # 4) pre-POI
    if eta_next and next_poi:
        t15 = eta_next - timedelta(minutes=15)
        t30 = eta_next - timedelta(minutes=30)
        win = cfg.prepoi_time_window_sec

        if abs((now - t15).total_seconds()) <= win:
            if state.fired_pre15_for_tag != next_poi.tag:
                state.fired_pre15_for_tag = next_poi.tag
                return "15 Min Upstream - Station"

        if abs((now - t30).total_seconds()) <= win:
            if state.fired_pre30_for_tag != next_poi.tag:
                state.fired_pre30_for_tag = next_poi.tag
                return "30 Min Upstream - Station"

    # 5) 30-min update
    if state.first_notif_at is None:
        state.first_notif_at = now
        state.last_notif_at = now
        return "30 Min Update"

    if state.last_notif_at is None:
        state.last_notif_at = now
        return "30 Min Update"

    if (now - state.last_notif_at) >= timedelta(minutes=30):
        state.last_notif_at = now
        return "30 Min Update"

    return ""


def build_payload(
    pig_id: str,
    tool_type: str,
    pig_event: str,
    notif_type: str,
    speed_mps: float,
    prev_poi: Optional[POI],
    next_poi: Optional[POI],
    eta_next: Optional[datetime],
    eta_end: Optional[datetime],
    legacy_route: str,
    current_gc: Optional[int],
    current_kp: Optional[float],
    time: datetime,
) -> Dict[str, Any]:
    return {
        "Pig ID": pig_id,
        "Tool Type": tool_type,
        "Pig Event": pig_event,
        "Notification Type": notif_type,
        "Speed": f"{speed_mps:.2f}",
        "Previous Valve Type": (prev_poi.valve_type if prev_poi else ""),
        "Previous Valve Tag": (prev_poi.tag if prev_poi else ""),
        "Next Valve Type": (next_poi.valve_type if next_poi else ""),
        "Next Valve Tag": (next_poi.tag if next_poi else ""),
        "ETA to the Next Valve": eta_next.strftime("%d-%m-%y %H%M%S") if eta_next else "",
        "ETA to the End": eta_end.strftime("%d-%m-%y %H%M%S") if eta_end else "",
        "Legacy Route": legacy_route,
        "Current Global Channel": str(current_gc) if current_gc is not None else "",
        "Current KP": f"{current_kp:.3f}" if current_kp is not None else "",
        "Timestamp": time.strftime("%d-%m-%y %H%M%S"),
    }

def pick_legacy_route_smart(
        state: PigState,
        routes: Dict[str, List[POI]],
        pois: List[POI],
        cur: PosSample,
        gc_to_kp: Dict[int, float],
        cfg: EngineConfig,
        pig_event: str,
    ) -> str:
    legacy = pick_legacy_route(
        state=state,
        routes=routes,
        cur=cur,
        gc_to_kp=gc_to_kp,
        cfg=cfg,
        pig_event=pig_event,
    )
    if legacy != "Unknown":
        return legacy
    
    # fallback
    return pick_legacy_route_by_nearest_poi(
        state=state,
        pois=pois,
        cur=cur,
        gc_to_kp=gc_to_kp,
        cfg=cfg,
        pig_event=pig_event,
    )

class Engine:
    def __init__(self, repo: TelemetryRepo, cfg: Optional[EngineConfig] = None) -> None:
        self.repo = repo
        self.cfg = cfg or EngineConfig()

        # Load once at startup
        self._gc_to_kp = self.repo.get_gc_to_kp()
        self._pois = self.repo.get_pois()
        self._gaps = self.repo.get_gaps()
        self._routes = _build_routes(self._pois)
        


    def process_pig(self, pig_id: str, tool_type: str, now: datetime) -> Dict[str, Any]:
        cfg = self.cfg
        # use cache metadata
        gc_to_kp = self._gc_to_kp
        gaps = self._gaps
        routes = self._routes

        state = self.repo.get_state(pig_id)

        default_tool_type = "Cleaning Tool"
        effective_tool = (state.locked_tool_type or (tool_type.strip() if tool_type else "") or default_tool_type)

        # 1) last 5 minutes -> Moving/Stopped
        since_move = now - timedelta(seconds=cfg.stopped_window_sec)
        recent = self.repo.get_recent_positions(pig_id, since_dt=since_move)

        # 2) longer history -> speed/ETA (need ref around now-25m)
        since_speed = now - timedelta(seconds=cfg.speed_search_sec)
        speed_samples = self.repo.get_recent_positions(pig_id, since_dt=since_speed)

        cur = _current_sample(speed_samples) or _current_sample(recent)
        if cur is None:
            return build_payload(
                pig_id=pig_id,
                tool_type=effective_tool,
                pig_event="Not Detected",
                notif_type="",
                speed_mps=0.0,
                prev_poi=None,
                next_poi=None,
                eta_next=None,
                eta_end=None,
                legacy_route=state.locked_legacy_route or "Unknown",
                current_gc=None,
                current_kp=None,
                time=now,
            )

        # Route pick (sticky)
        legacy = pick_legacy_route_smart(
            state=state, 
            routes=routes,
            pois=self._pois,
            cur=cur,
            gc_to_kp=gc_to_kp,
            cfg=cfg, 
            pig_event="Moving"
            ) # Not real "Moving" event
        route = routes.get(legacy, [])
        if legacy == "Unknown":
            print(f"[WARN] legacy route Unknown; cur_m may be outside all route ranges. cur.gc={cur.gc} cur.kp={cur.kp}")

        if legacy != "Unknown" and not route:
            print(f"[WARN] legacy '{legacy}' not found in routes (routes keys mismatch?)")
        prev_poi, next_poi, end_poi = find_prev_next_end(route, cur, gc_to_kp, cfg)

        raw_event = infer_pig_event(recent, end_poi, gc_to_kp, cfg)
        pig_event = raw_event

        # Variant A: transitions (Stopped -> Moving)
        prev_event = state.last_event
        if prev_event == "Stopped" and pig_event == "Moving":
            pig_event = "Resumption"
            state.moving_started_at = cur.dt
        elif raw_event == "Moving":
            pig_event = "Moving"
            
        if pig_event in ("Stopped", "Completed"):
            state.moving_started_at = None
        state.last_event = raw_event
        state.last_event_dt = cur.dt

        # Re-pick route with real event
        legacy = pick_legacy_route_smart(
            state=state,
            routes=routes,
            pois=self._pois,
            cur=cur, 
            gc_to_kp=gc_to_kp, 
            cfg=cfg, 
            pig_event=pig_event)
        route = routes.get(legacy, [])
        if legacy == "Unknown":
            print(f"[WARN] legacy route Unknown; cur_m may be outside all route ranges. cur.gc={cur.gc} cur.kp={cur.kp}")

        if legacy != "Unknown" and not route:
            print(f"[WARN] legacy '{legacy}' not found in routes (routes keys mismatch?)")
        prev_poi, next_poi, end_poi = find_prev_next_end(route, cur, gc_to_kp, cfg)

        # Speed/ETA
        if pig_event == "Stopped":
            spd = 0.0
            eta_next = None
            eta_end = None
        else:
            use_short = False
            if state.moving_started_at is not None:
                if (cur.dt - state.moving_started_at).total_seconds() < cfg.moving_boost_sec:
                    use_short = True

            window_sec = cfg.speed_short_window_sec if use_short else cfg.speed_window_sec
            target_dt = cur.dt - timedelta(seconds=window_sec)

            pool = speed_samples
            if use_short and state.moving_started_at is not None:
                filtered = [s for s in speed_samples if s.dt >= state.moving_started_at]
                if filtered:
                    pool = filtered

            ref = pick_ref_sample_at_or_before(pool, target_dt)

            spd = 0.0
            if ref is not None:
                if (cur.dt - ref.dt).total_seconds() >= cfg.min_speed_dt_sec:
                    spd = speed_mps_by_ref(cur, ref, gc_to_kp, cfg)

            eta_next = eta_from_to(cur, next_poi, spd, gc_to_kp, cfg) if (next_poi and spd > 0) else None
            eta_end = eta_from_to(cur, end_poi, spd, gc_to_kp, cfg) if (end_poi and spd > 0) else None

        notif = infer_notification_type(
            state=state,
            pig_event=pig_event,
            cur=cur,
            legacy_route=legacy,
            route=route,
            next_poi=next_poi,
            end_poi=end_poi,
            gaps=gaps,
            eta_next=eta_next,
            gc_to_kp=gc_to_kp,
            cfg=cfg,
        )

        if pig_event == "Completed":
            state.locked_legacy_route = None
            state.moving_started_at = None
            state.locked_tool_type = None
        print("legacy=", legacy, "cur.gc=", cur.gc, "cur.kp=", cur.kp)
        print("prev=", prev_poi.tag if prev_poi else None, "next=", next_poi.tag if next_poi else None)
        self.repo.save_state(pig_id, state)

        return build_payload(
            pig_id=pig_id,
            tool_type=effective_tool,
            pig_event=pig_event,
            notif_type=notif,
            speed_mps=spd,
            prev_poi=prev_poi,
            next_poi=next_poi,
            eta_next=eta_next,
            eta_end=eta_end,
            legacy_route=legacy,
            current_gc=cur.gc,
            current_kp=cur.kp,
            time=cur.dt,
        )
