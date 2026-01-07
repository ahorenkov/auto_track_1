from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
from core.state import InMemoryStateStore
from core.models import PigState, PosSample, PigState, POI
from core.repo import CsvRepo

@dataclass
class EngineConfig:
    """Configuration for the engine."""
    meters_per_channel: float = 25.0
    speed_min_mps: float = 0.01
    max_ref_age_minutes: int = 35
    
class Engine:
    def __init__(self, repo:object, cfg: Optional[EngineConfig] = None) -> None:
        self.repo = repo
        self.cfg = cfg or EngineConfig()
        self.state_store = InMemoryStateStore()

    def process_pig(self, pig_id: str, tool_type: str, now: datetime) -> dict:

        state: PigState = self.state_store.get(pig_id)

        # temporarily imitate a state change
        if state.first_notif_at is None:
            state.first_notif_at = now

        looback: timedelta = timedelta(minutes=self.cfg.max_ref_age_minutes)
        since = now - looback

        samples: List[PosSample] = self.repo.get_recent_positions(
            pig_id=pig_id,
            since=since
        )

        if not samples:
            return {
                "Pig ID": pig_id,
                "Tool Type": tool_type,
                "Now": now.isoformat(),
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

        state.legacy_route = route
        prev_poi = next_poi = end_poi = None

        if route is not None and route in routes:
            prev_poi, next_poi, end_poi = find_prev_next_end_poi(
                route_pois=routes[route],
                cur_pos_m=cur_pos_m,
                gc_to_kp=gc_to_kp,
                meters_per_channel=self.cfg.meters_per_channel,
            )

        state.last_dt = cur.dt
        state.last_pos_m = cur_pos_m
        self.repo.save_state(pig_id, state)
        # self.state_store.save(pig_id, state)

        return {
            "Pig ID": pig_id,
            "Tool Type": tool_type,
            "Now": now.isoformat(),
            "Sample_time": cur.dt.isoformat(),
            "Position m:": cur_pos_m,
            "Speed mps:": speed_mps,
            "Legacy Route": route,
            "Previous POI": prev_poi.tag if prev_poi else None,
            "Next POI": next_poi.tag if next_poi else None,
            "End POI": end_poi.tag if end_poi else None,
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
    
    if state.legacy_route is not None:
        if state.legacy_route in routes:
            return state.legacy_route
        
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



