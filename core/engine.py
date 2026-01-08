from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
from core.models import PigState, PosSample, PigState, POI
from core.repo import CsvRepo

PIG_EVENT_NOT_DETECTED = "Not Detected"
PIG_EVENT_MOVING = "Moving"
PIG_EVENT_STOPPED = "Stopped"
PIG_EVENT_COMPLETED = "Completed"

NOTIF_NONE = ""
NOTIF_RUN_COMPLETION = "Run Completion"
NOTIF_POI_PASSAGE = "POI Passage"
NOTIF_PRE_15 = "15 Minutes before POI"
NOTIF_PRE_30 = "30 Minutes before POI"
NOTIF_30_MIN_UPDATE = "30 Minute Update"

@dataclass
class EngineConfig:
    """Configuration for the engine."""
    meters_per_channel: float = 25.0
    speed_min_mps: float = 0.01
    max_ref_age_minutes: int = 35
    poi_tol_meters: float = 50.0
    
    stop_window_seconds: int = 120
    stop_max_move_meters: float = 50.0
    
class Engine:
    def __init__(self, repo:object, cfg: Optional[EngineConfig] = None) -> None:
        self.repo = repo
        self.cfg = cfg or EngineConfig()

    def process_pig(self, pig_id: str, tool_type: str, now: datetime) -> dict:

        telemetry = self.repo.get_telemetry(pig_id)
        if not telemetry:
            return {}
        
        cur = telemetry[-1]
        effective_now = now or cur.dt

        state: PigState = self.repo.get_state(pig_id) or PigState()
        print(f'[DEBUG] state BEFORE for {pig_id}: {state}')

        # temporarily imitate a state change
        if state.first_notif_at is None:
            state.first_notif_at = effective_now

        looback: timedelta = timedelta(minutes=self.cfg.max_ref_age_minutes)
        since = effective_now - looback

        samples: List[PosSample] = self.repo.get_recent_positions(
            pig_id=pig_id,
            since=since
        )

        if not samples:
            return {
                "Pig ID": pig_id,
                "Tool Type": tool_type,
                "Now": effective_now.isoformat(),
                "Position m:": None,
                "Speed mps:": None
            }
        
        cur = pick_current_sample(samples)
        gc_to_kp = self.repo.get_gc_to_kp()
        
        cur_pos_m = pos_m(cur, gc_to_kp, self.cfg.meters_per_channel)
        ref = pick_ref_sample_at_or_before(samples, cur.dt)

        speed_mps = speed_mps_by_ref(
            cur=cur,
            ref=ref,
            gc_to_kp=gc_to_kp,
            speed_min_mps=self.cfg.speed_min_mps,
            meters_per_channel=self.cfg.meters_per_channel,
        )

        pois = self.repo.get_pois()
        routes = build_routes(pois)

        route = pick_legacy_route(
            state=state,
            routes=routes,
            cur_pos_m=cur_pos_m,
            gc_to_kp=gc_to_kp,
            meters_per_channel=self.cfg.meters_per_channel,
        )

        state.locked_legacy_route = route

        prev_poi = next_poi = end_poi = None

        if route is not None and route in routes:
            prev_poi, next_poi, end_poi = find_prev_next_end_poi(
                route_pois=routes[route],
                cur_pos_m=cur_pos_m,
                gc_to_kp=gc_to_kp,
                meters_per_channel=self.cfg.meters_per_channel,
            )

        next_poi_pos_m = None
        end_poi_pos_m = None

        if next_poi:
            next_poi_pos_m = pos_m(
                PosSample(dt=effective_now, gc=next_poi.global_channel, kp=next_poi.kp),
                gc_to_kp,
                self.cfg.meters_per_channel,
            )
        if end_poi:
            end_poi_pos_m = pos_m(
                PosSample(dt=effective_now, gc=end_poi.global_channel, kp=end_poi.kp),
                gc_to_kp,
                self.cfg.meters_per_channel,
            )   

        eta_to_next_sec = eta_seconds(cur_pos_m, next_poi_pos_m, speed_mps)
        eta_to_end_sec = eta_seconds(cur_pos_m, end_poi_pos_m, speed_mps)

        dist_to_next_m = distance_to_poi_m(cur_pos_m, next_poi, gc_to_kp, self.cfg.meters_per_channel)
        near_next_poi = is_near_poi(dist_to_next_m, self.cfg.poi_tol_meters)

        poi_passed = False
        if next_poi_pos_m is not None:
            poi_passed = is_passed_poi(state, cur_pos_m, next_poi_pos_m)

        stop_since = effective_now - timedelta(seconds=self.cfg.stop_window_seconds)
        recent_for_stop = self.repo.get_recent_positions(pig_id, since=stop_since)

        pig_event = infer_pig_event(
            recent_samples=recent_for_stop,
            cur_pos_m=cur_pos_m,
            end_poi_pos_m=end_poi_pos_m,
            gc_to_kp=gc_to_kp,
            cfg=self.cfg,
        )   

        notification_type = infer_notification_type(
            pig_event=pig_event,
            poi_passed=poi_passed,
            next_poi=next_poi,
            eta_to_next_sec=eta_to_next_sec,
            now=effective_now,
            state=state,
        )
        self.repo.save_state(pig_id, state)
        print(f'[DEBUG] state AFTER for {pig_id}: {state}')


        return {
            "Pig ID": pig_id,
            "Tool Type": tool_type,
            "Now": effective_now.isoformat(),
            "Sample_time": cur.dt.isoformat(),
            "Position m:": cur_pos_m,
            "Speed mps:": speed_mps,
            "Legacy Route": route,
            "Previous POI": prev_poi.tag if prev_poi else None,
            "Next POI": next_poi.tag if next_poi else None,
            "End POI": end_poi.tag if end_poi else None,
            "ETA to Next POI (sec)": eta_to_next_sec,
            "ETA to End POI (sec)": eta_to_end_sec,
            "Near Next POI": near_next_poi,
            "Passed Next POI": poi_passed,
            "Pig Event": pig_event,
            "Notification Type": notification_type,
        }
        

def pos_m(
        sample: PosSample,
        gc_to_kp: Dict[int, float],
        meters_per_channel: float,
    ) -> Optional[float]:

    if sample.kp is not None:
        return float(sample.kp) * 1000.0
    
    if sample.gc is None:
        return None
    
    if sample.gc in gc_to_kp:
        return float(gc_to_kp[sample.gc]) * 1000.0
    
    return float(sample.gc) * float(meters_per_channel)

def pick_current_sample(samples: List[PosSample]) -> Optional[PosSample]:
    if not samples:
        return None
    
    # pick the latest sample
    samples_sorted = sorted(samples, key=lambda s: s.dt)

    return samples_sorted[-1]

def pick_ref_sample_at_or_before(samples: List[PosSample],target_dt: datetime) -> Optional[PosSample]:
    if not samples:
        return None
    
    samples_sorted = sorted(samples, key=lambda s: s.dt)
    best: Optional[PosSample] = None
    for s in samples_sorted:
        if s.dt <= target_dt:
            best = s
        else:
            break

    return best

def speed_mps_by_ref(cur: PosSample, ref: PosSample, gc_to_kp: Dict[int, float], meters_per_channel: float, speed_min_mps:float,) -> Optional[float]:
    
    cur_m = pos_m(cur, gc_to_kp, meters_per_channel)
    ref_m = pos_m(ref, gc_to_kp, meters_per_channel)

    if cur_m is None or ref_m is None:
        return None
    
    dt_sec = (cur.dt - ref.dt).total_seconds()
    if dt_sec <= 0:
        return None
    
    dist_m = cur_m - ref_m
    speed = dist_m / dt_sec

    if abs(speed) < speed_min_mps:
        return 0.0
    
    return speed

def build_routes(pois: List[POI]) -> Dict[str, List[POI]]:
    routes: Dict[str, List[POI]] = {}

    for p in pois:
        r = p.legacy_route
        if r not in routes:
            routes[r] = []
        routes[r].append(p)

    for r, items in routes.items():
        items.sort(key=lambda x: (x.kp if x.kp is not None else float('inf'),
                                  x.global_channel if x.global_channel is not None else 10**12))
        
    return routes

def route_range_m(route_pois: List[POI], gc_to_kp: Dict[int, float], meters_per_channel: float) -> Optional[Tuple[float, float]]:
    
    min_m: Optional[float] = None
    max_m: Optional[float] = None

    for p in route_pois:
        fake_sample = PosSample(dt=datetime.now(), gc=p.global_channel, kp=p.kp)
        m = pos_m(fake_sample, gc_to_kp, meters_per_channel)
        if m is None:
            continue
        if min_m is None or m < min_m:
            min_m = m
        if max_m is None or m > max_m:
            max_m = m
        
    if min_m is None or max_m is None:
        return None
    
    return (min_m, max_m)

def pick_legacy_route(
        state: PigState,
        routes: Dict[str, List[POI]],
        cur_pos_m: Optional[float],
        gc_to_kp: Dict[int, float],
        meters_per_channel: float,
        ) -> Optional[str]:
    
    if state.locked_legacy_route is not None:
        if state.locked_legacy_route in routes:
            return state.locked_legacy_route
        
    if cur_pos_m is None:
        return None
    
    best_route: Optional[str] = None
    best_score: Optional[float] = None

    for r, r_pois in routes.items():
        rr = route_range_m(r_pois, gc_to_kp, meters_per_channel)
        if rr is None:
            continue
        r_min, r_max = rr
        if not (r_min <= cur_pos_m <= r_max):
            continue

        center = (r_min + r_max) / 2.0
        score = abs(cur_pos_m - center)

        if best_score is None or score < best_score:
            best_score = score
            best_route = r
        
    return best_route
    
def find_prev_next_end_poi(
        route_pois: List[POI],
        cur_pos_m: Optional[float],
        gc_to_kp: Dict[int, float],
        meters_per_channel: float,
    ) -> Tuple[Optional[POI], Optional[POI], Optional[POI]]:
    if not route_pois:
        return (None, None, None)
    
    end_poi = route_pois[-1]

    if cur_pos_m is None:
        return (None, None, end_poi)
    
    prev_poi: Optional[POI] = None
    next_poi: Optional[POI] = None

    for p in route_pois:
        fake = PosSample(dt=datetime.now(), gc=p.global_channel, kp=p.kp)
        pm = pos_m(fake, gc_to_kp, meters_per_channel)
        if pm is None:
            continue

        if pm <= cur_pos_m:
            prev_poi = p
        elif pm > cur_pos_m and next_poi is None:
            next_poi = p
            break

    return (prev_poi, next_poi, end_poi)

def eta_seconds(
        cur_pos_m: Optional[float],
        target_pos_m: Optional[float],
        speed_mps: Optional[float],
) -> Optional[float]:
    
    if cur_pos_m is None:
        return None
    
    if target_pos_m is None:
        return None 
    
    if speed_mps <= 0.0:   
        return None
    
    distance = cur_pos_m - target_pos_m
    if distance < 0.0:
        return None
    return distance / speed_mps

def distance_to_poi_m(
        cur_pos_m: Optional[float],
        poi: Optional[POI],
        gc_to_kp: Dict[int, float],
        meters_per_channel: float,
) -> Optional[float]:
    
    if cur_pos_m is None or poi is None:
        return None
    
    fake = PosSample(
        dt=datetime.now(),
        gc=poi.global_channel,
        kp=poi.kp)
    poi_pos_m = pos_m(fake, gc_to_kp, meters_per_channel)
    if poi_pos_m is None:
        return None
    return poi_pos_m - cur_pos_m

def is_near_poi(
        dist_m: Optional[float],
        tol_m: float,
) -> bool:
    
    if dist_m is None:
        return False
    
    return abs(dist_m) <= tol_m

def is_passed_poi(
        state: PigState,
        cur_pos_m: Optional[float],
        poi_pos_m: Optional[float],
) -> bool:
  
  if state.last_pos_m is None:
      return False
  if cur_pos_m is None or poi_pos_m is None:
      return False
  
  # Check if an object was before the POI and now is at or beyond it
  was_ahead = state.last_pos_m < poi_pos_m
  now_behind = cur_pos_m >= poi_pos_m

  return was_ahead and now_behind

def infer_moving_or_stopped(
        recent_samples: List[PosSample],
        gc_to_kp: Dict[int, float],
        meters_per_channel: float,
        stop_max_move_meters: float,
) -> str:
    
    if len(recent_samples) < 2:
        return PIG_EVENT_NOT_DETECTED
    
    positions = []

    for s in recent_samples:
        m = pos_m(s, gc_to_kp, meters_per_channel)
        if m is not None:
            positions.append((s.dt, m))
    
    if len(positions) < 2:
        return PIG_EVENT_NOT_DETECTED
    
    min_m = min(positions)
    max_m = max(positions)

    if max_m - min_m <= stop_max_move_meters:
        return PIG_EVENT_STOPPED
    
    return PIG_EVENT_MOVING

def infer_completed(
    cur_pos_m: Optional[float],
    end_poi_pos_m: Optional[float],
    tol_m: float,
) -> bool:
    
    if cur_pos_m is None or end_poi_pos_m is None:
        return False
    
    return abs(cur_pos_m - end_poi_pos_m) <= tol_m

def infer_pig_event(
        recent_samples: List[PosSample],
        cur_pos_m: Optional[float],
        end_poi_pos_m: Optional[float],
        gc_to_kp: Dict[int, float],
        cfg: EngineConfig,
) -> str:

    if not recent_samples:
        return PIG_EVENT_NOT_DETECTED

    if infer_completed(cur_pos_m, end_poi_pos_m, cfg.poi_tol_meters):
        return PIG_EVENT_COMPLETED

    return infer_moving_or_stopped(recent_samples, gc_to_kp, cfg.meters_per_channel, cfg.stop_max_move_meters)

def infer_notification_type(
        *,
        pig_event: str,
        poi_passed: bool,
        next_poi: Optional[POI],
        eta_to_next_sec: Optional[float],
        now: datetime,
        state: PigState,
) -> str:
    
    if pig_event == PIG_EVENT_COMPLETED:
        return NOTIF_RUN_COMPLETION
    if poi_passed:
        return NOTIF_POI_PASSAGE
    if state.fired_pre30_for_tag != next_poi.tag:
        state.fired_pre30_for_tag = next_poi.tag
        return NOTIF_PRE_30
    if state.fired_pre15_for_tag != next_poi.tag:
        state.fired_pre15_for_tag = next_poi.tag
        return NOTIF_PRE_15
    if state.first_notif_at in None:
        state.first_notif_at = now
        state.last_notif_at = now
        return NOTIF_30_MIN_UPDATE
    
    if state.last_notif_at is not None:
        delta = (now - state.last_notif_at).total_seconds()
        if delta >= 30 * 60:
            state.last_notif_at = now
            return NOTIF_30_MIN_UPDATE
    
    return NOTIF_NONE

