from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
from core.models import PigState, PosSample, PigState, POI, GapPoint
from core.repo import CsvRepo


NOTIF_COMPLETION = "Completed"
NOTIF_POI_PASSAGE = "POI Passage"
NOTIF_PRE_15 = "15 Min Notification"
NOTIF_PRE_30 = "30 Min Notification"
NOTIF_30MIN_UPDATE = "30 Min Update"

NOTIF_GAP_START = "Gap Start"
NOTIF_GAP_END = "Gap End"


@dataclass
class EngineConfig:
    """Configuration for the engine."""
    meters_per_channel: float = 25.0
    speed_min_mps: float = 0.01
    max_ref_age_minutes: int = 35
    poi_tol_meters: float = 50.0
    meters_per_channel: float = 25.0
    
    # stop detection parameters
    stop_window_seconds: int = 120
    stop_span_tol_m: float = 50.0

    # speed/eta
    speed_search_sec: int = 25 * 60 # 25 minutes back
    speed_window_sec: int = 25 * 60 # try to use 25 minutes delta
    speed_short_window_sec: int = 5 * 60 # 5 minutes right after start
    moving_boost_sec: int = 25 * 60 # first 25 minutes after moving start
    

    
class Engine:
    def __init__(self, repo:object, cfg: Optional[EngineConfig] = None) -> None:
        self.repo = repo
        self.cfg = cfg or EngineConfig()

    def _kp_from_gc(self, gc:int) -> Optional[float]:
        """Conver GC to KP using mapping loaded by repo.
        Returns None if mapping doens't have this GC."""

        gc_to_kp = self.repo.get_gc_to_kp()

        return gc_to_kp.get(gc)
    
    def _build_routes(self, pois: List[POI]) -> Dict[str, List[POI]]:
        """Group POIs by legacy route and sort by postision(KP preffered)."""

        routes: Dict[str, List[POI]] = {}

        for p in pois:
            routes.setdefault(p.legacy_route, []).append(p)

            def poi_sort_key(p:POI) -> float:
                # prefer KP if available; otherwise approximate GC
                if p.kp is not None:
                    return float(p.kp)
                if p.global_channel is not None:
                    return float(p.global_channel) / 1000.0
                return 0.0
            
            for route_name in routes:
                routes[route_name].sort(key=poi_sort_key)

            return routes
        
    def _pick_legacy_route(self, routes: Dict[str, List[POI]], state: PigState) -> str:
        """Sticky route selection:
        if state.locked_legacy_route exists and still present -> use it
        else pick the longest route (most POIs) and lock it
        """
        if state.locked_legacy_route and state.locked_legacy_route in routes:
            return state.locked_legacy_route
        
        if not routes:
            return "unknown"
        
        # pick route with most POIs
        chosen = max(routes.keys(), key=lambda r: len(routes[r]))
        state.locked_legacy_route = chosen
        return chosen
    
    def _poi_pos_m(self, poi: POI) -> Optional[float]:
        """convert POI to position in meters.
        if kp exists -> kp * 1000
        else if gc exists -> try gc -> kp then kp*1000, elso GC * meters_per_channel
        """
        if poi.kp is not None:
            return float(poi.kp) * 1000.0
        
        if poi.global_channel is not None:
            kp = self._kp_from_gc(int(poi.global_channel))
            if kp is not None:
                return float(kp) * 1000.0
            return float(poi.global_channel) * float(self.cfg.meters_per_channel)
        
        return None
    
    def _find_prev_next_end(self, route_pois: List[POI], cur_pos_m: float) -> Tuple[Optional[POI], Optional[POI], Optional[POI]]:
        """Given sorted POIs and current position in meters:
        prev_poi: last poi with pos <= cur_pos_m
        next_poi: first poi with pos > cur_pos_m
        end_poi: last poi in the route
        """
        if not route_pois:
            return None, None, None
        
        # compute positions once
        items: List[Tuple[POI, float]] = []
        for p in route_pois:
            pm = self._poi_pos_m(p)
            if pm is None:
                continue
            items.append((p, pm))
        
        if not items:
            return None, None, None
        
        end_poi = items[-1][0]
        prev_poi: Optional[POI] = None
        next_poi: Optional[POI] = None

        for p, pm in items:
            if pm <= cur_pos_m:
                prev_poi = p
            else:
                next_poi = p
                break
        
        return prev_poi, next_poi, end_poi
    
    def _positions_span_m(self, samples: List[PosSample]) -> Optional[float]:
        """Return (max_pos_m - min_pos_m) for the given samples.
        Ignores samples with unknown position.
        Returns None if no valid positions."""

        positions: List[float] = []
        for s in samples:
            pm = self._pos_m(s)
            if pm is not None:
                positions.append(pm)
        if not positions:
            return None
        
        return max(positions) - min(positions)
    
    def _infer_pig_event(self, pig_id: str, cur: PosSample, cur_pos_m: float, end_poi: Optional[POI]) -> str:
        """Decide pig event based on telemetry time and recent motion
        Priority:
        1. Completed if clost to the end POI
        2. Stopped if span in last stop_window_seconds is within stop_span_tol_m
        3. Moving otherwise
        """

        # 1. Completed
        if end_poi is not None:
            end_m = self._poi_pos_m(end_poi)
            if end_m is not None and abs(cur_pos_m - end_m) <= float(self.cfg.poi_tol_meters):
                return "Completed"
        # 2. Stopped
        since_dt = cur.dt - timedelta(seconds=int(self.cfg.stop_window_seconds))
        recent = self.repo.get_recent_positions(pig_id, since_dt)
        span = self._positions_span_m(recent)

        if span is not None and span <= float(self.cfg.stop_span_tol_m):
            return "Stopped"
        # 3. Moving

        return "Moving"
    
    def _update_event_state(self, state: PigState, new_event: str, event_dt) -> None:
        """update PigState transitions:
        last event/last event dt always reflect latest event decision
        moving started_at is set when Stopped -> Moving transition occurs"""
        
        prev_event = state.last_event

        # transtion decision: event changed
        if prev_event != new_event:
            # if we start moving after being stopped , mark moving started at
            # Implement RESUMPTION EVENT in the future
            if prev_event == "Stopped" and new_event == "Moving":
                state.moving_started_at = event_dt
        
        state.last_event = new_event
        state.last_event_dt = event_dt

    def _select_speed_window_sec(self, state: PigState, cur_dt) -> int:
        """Choose speed window:
        if we started moving recently (within moving_boost_sec) -> use short window
        else use regular window
        """
        if state.moving_started_at is None:
            return int(self.cfg.speed_window_sec)
        
        # how long pig has been moving since last start
        age_sec = (cur_dt - state.moving_started_at).total_seconds()
        if age_sec <= float(self.cfg.moving_boost_sec):
            return int(self.cfg.speed_short_window_sec)
        
        return int(self.cfg.speed_window_sec)
    
    def _pick_reference_sample(self, pig_id: str, cur_dt, target_dt) -> Optional[PosSample]:
        """Return sample closest to target_dt among recent positions.
        Uses repo.get_recent_positions(pig_id, since_dt) to limit data.
        """
        # ask repo for enough history
        since_dt = target_dt - timedelta(seconds=int(self.cfg.speed_search_sec))
        recent = self.repo.get_recent_positions(pig_id, since_dt)
        if not recent:
            return None
        
        # choose sample with minimal abs(timediff)
        best: Optional[PosSample] = None
        best_abs: Optional[float] = None
        for s in recent:
            diff = abs((s.dt - target_dt).total_seconds())
            if best_abs is None or diff < best_abs:
                best = s
                best_abs = diff

        return best
    
    def _calc_speed_mps(self, cur: PosSample, cur_pos_m: float, ref: PosSample) -> Optional[float]:
        """ speed = delta_pos / delta_time
        returns None if dt invalid or ref position unknown
        """
        ref_pos_m = self._pos_m(ref)
        if ref_pos_m is None:
            return None
        
        dt_sec = (cur.dt - ref.dt).total_seconds()
        if dt_sec <= 0:
            return None
        
        return (cur_pos_m - ref_pos_m) / dt_sec
    
    def _eta_second(self, cur_pos_m: float, target_pos_m: float, speed_mps: float) -> Optional[float]:
        """ETA in seconds to reach target position.
        returns None if speed invalid or taget behind current position
        """
        if speed_mps is None or speed_mps <= 0.0:
            return None
        
        dist_m = target_pos_m - cur_pos_m
        if dist_m < 0.0:
            return None
        
        return int(dist_m / speed_mps)
    
    def _find_poi_passage(self, route_pois: List[POI], cur_pos_m: float) -> Optional[POI]:
        """Return POI if current position is within poi_tol_meters
        Picks the closest  POI (minimal abs distance)
        """
        tol = float(self.cfg.poi_tol_meters)
        best_poi: Optional[POI] = None
        best_abs: Optional[float] = None

        for p in route_pois:
            pm = self._poi_pos_m(p)
            if pm is None:
                continue

            d = abs(cur_pos_m - pm)
            if d <= tol:
                if best_abs is None or d < best_abs:
                    best_abs = d
                    best_poi = p
            
        return best_poi
    
    def _infer_30min_update(self, state: PigState, cur_dt) -> bool:
        """Returns True if 30-min update notification should be sent.
        Rule:
        On first ever run -> True (and set first/last)
        then every >= 30 minutes since last_notif_at"""

        if state.first_notif_at is None:
            state.first_notif_at = cur_dt
            state.last_notif_at = cur_dt
            return True
        
        if state.last_notif_at is None:
            state.last_notif_at = cur_dt
            return True
        
        delta_sec = (cur_dt - state.last_notif_at).total_seconds()
        if delta_sec >= 30 * 60:
            state.last_notif_at = cur_dt
            return True
        
        return False
    
    def _infer_pre_poi_notification(self, state: PigState, next_poi: Optional[POI], eta_next_sec: Optional[float]) -> str:
        """Decid pre-POI notification type: based on ETA to next POI
        if ETA within 30 minutes +/- 60s and not fired for this tag -> "30 Min Notification"
        if ETA within 15 minutes +/- 60s and not fired for this tag -> "15 Min Notification"
        Uses state.fired_pre30_for_tag / fired_pre15_for_tag for dedup
        """
        if next_poi is None or eta_next_sec is None:
            return ""
        
        tag = next_poi.tag
        tol = 60 # seconds tolerance

        # 30 Min Notification
        if (30 * 60 - tol) <= eta_next_sec <= (30 * 60 + tol):
            if state.fired_pre30_for_tag != tag:
                state.fired_pre30_for_tag = tag
                return NOTIF_PRE_30
            
        # 15 Min Notification
        if (15 * 60 - tol) <= eta_next_sec <= (15 * 60 + tol):
            if state.fired_pre15_for_tag != tag:
                state.fired_pre15_for_tag = tag
                return NOTIF_PRE_15
            
        return ""
    
    def _reset_on_completion(self, state: PigState) -> None:
        """When PIG copletes a run, reset sticky route and pre-POI dedup"""
        state.locked_legacy_route = None
        state.fired_pre15_for_tag = None
        state.fired_pre30_for_tag = None
        state.moving_started_at = None
        state.last_gap_fired = None

    def _build_gaps_by_route(self, gaps: List[GapPoint]) -> Dict[str, List[GapPoint]]:
        """ Group GapPoints by legacy route and sort by postision(KP preffered)."""

        by_route: Dict[str, List[GapPoint]] = {}

        for g in gaps:
            by_route.setdefault(g.legacy_route, []).append(g)

        for r in by_route:
            by_route[r].sort(key=lambda x: float(x.kp))

        return by_route
    

    def _gap_pos_m(seld, kp: float) -> float:
        """Convert GapPoint KP to position in meters."""
        return float(kp) * 1000.0
    
    def _infer_gap_notification(self, state: PigState, route_name: str, cur_pos_m: float) -> str:
        """Decide gap bounary notification if we are close to a gap point.
        Uses state.last_gap_fired for deduplication."""

        gaps = self.repo.get_gaps()
        if not gaps:
            return ""

        by_route = self._build_gaps_by_route(gaps)
        route_gaps = by_route.get(route_name, [])
        if not route_gaps:
            return ""
        
        tol = float(self.cfg.poi_tol_meters)

        # find the closest gap point within tolerance
        best_key: Optional[str] = None
        best_kind: Optional[str] = None
        best_abs: Optional[float] = None

        for g in route_gaps:
            gm = self._gap_pos_m(g.kp)
            d = abs(cur_pos_m - gm)
            if d <= tol:
                key = f'{route_name}:{g.kind}:{g.kp}'
                if best_abs is None or d < best_abs:
                    best_abs = d
                    best_key = key
                    best_kind = g.kind
        if best_key is None or best_kind is None:
            return ""
        
        # deduplication
        if state.last_gap_fired == best_key:
            return ""
        
        state.last_gap_fired = best_key
        if best_kind == 'start':
            return NOTIF_GAP_START
        elif best_kind == 'end':
            return NOTIF_GAP_END
        
        return ""
    
    def _build_payload(
            self,
            pig_id: str,
            tool_type: str,
            route_name: str,
            cur: PosSample,
            cur_pos_m: Optional[float],
            prev_poi: Optional[POI],
            next_poi: Optional[POI],
            end_poi: Optional[POI],
            pig_event: str,
            speed_mps: Optional[float],
            eta_next: Optional[int],
            eta_end: Optional[int],
            notification_type: str,
            notif_poi: Optional[POI],
    ) -> dict:
        """Build final output dict (payload)"""

        return {
            "Pig ID": pig_id,
            "Tool Type": tool_type,
            "Legacy Route": route_name,
            "Position m:": round(cur_pos_m, 1) if cur_pos_m is not None else None,
            "Prev POI": prev_poi.tag if prev_poi else "",
            "Next POI": next_poi.tag if next_poi else "",  
            "End POI": end_poi.tag if end_poi else "",
            "Pig Event": pig_event,
            "Speed mps:": round(speed_mps, 2) if speed_mps is not None else None,
            "ETA to Next POI (sec)": eta_next,
            "ETA to End POI (sec)": eta_end,
            "Notification Type": notification_type,
            "Notification Tag": (notif_poi.tag if notif_poi else None),
            "Notification Valve Type": (notif_poi.valve_type if notif_poi else None),
        }
    

    def process_pig(self, pig_id: str, tool_type: str, now: datetime) -> dict:

        telemetry = self.repo.get_telemetry(pig_id)
        if not telemetry:
            return {}
        
        cur = telemetry[-1]
        effective_now = now or cur.dt

        state: PigState = self.repo.get_state(pig_id) or PigState()
        print(f'[DEBUG] state BEFORE for {pig_id}: {state}')

        pois = self.repo.get_pois()
        routes = self._build_routes(pois)
        route_name = self._pick_legacy_route(routes, state)
        route_pois = routes.get(route_name, [])

        cur_pos_m = self._pos_m(cur)
        if cur_pos_m is None:
            # position unknown -> still save state and return minimal payload
            self.repo.save_state(pig_id, state)
            return {
                "Pig ID": pig_id,
                "Tool Type": tool_type,
                "Legacy Route": route_name,
                "Position m:": None,
                "Prev POI": "",
                "Next POI": "",
                "End POI": "",
                "Notification Type": ""
            }
        prev_poi, next_poi, end_poi = self._find_prev_next_end(route_pois, cur_pos_m)
        pig_event = self._infer_pig_event(pig_id, cur, cur_pos_m, end_poi)
        self._update_event_state(state, pig_event, cur.dt)

        # speed window based on moving_started_at
        window_sec = self._select_speed_window_sec(state, cur.dt)
        target_dt = cur.dt - timedelta(seconds=window_sec) 

        ref = self._pick_reference_sample(pig_id, cur.dt, target_dt)
        speed_mps: Optional[float] = None
        if ref is not None:
            speed_mps = self._calc_speed_mps(cur, cur_pos_m, ref)

        eta_next: Optional[int] = None
        if next_poi is not None:
            next_m = self._poi_pos_m(next_poi)
            if next_m is not None:
                eta_next = self._eta_second(cur_pos_m, next_m, speed_mps)
        eta_end: Optional[int] = None
        if end_poi is not None:
            end_m = self._poi_pos_m(end_poi)
            if end_m is not None:
                eta_end = self._eta_second(cur_pos_m, end_m, speed_mps)
        
        notification_type = ""
        notif_poi: Optional[POI] = None

        # 1. Completion notification
        if pig_event == "Completed":
            notification_type = NOTIF_COMPLETION
            self._reset_on_completion(state)

        else:
            # 2. POI passage notification
            passed = self._find_poi_passage(route_pois, cur_pos_m)
            if passed is not None:
                notification_type = NOTIF_POI_PASSAGE
                notif_poi = passed
            else:
                # 3. Gap boundary notification
                gap_notif = self._infer_gap_notification(state, route_name, cur_pos_m)
                if gap_notif:
                    notification_type = gap_notif
                else:
                    # 4. Pre-POI notification
                    pre = self._infer_pre_poi_notification(state, next_poi, eta_next)
                    if pre:
                        notification_type = pre
                    else:
                        # 5. 30-min update notification
                        if self._infer_30min_update(state, cur.dt):
                            notification_type = NOTIF_30MIN_UPDATE


        self.repo.save_state(pig_id, state)



        return self._build_payload(
            pig_id=pig_id,
            tool_type=tool_type,
            route_name=route_name,
            cur=cur,
            cur_pos_m=cur_pos_m,
            prev_poi=prev_poi,
            next_poi=next_poi,
            end_poi=end_poi,
            pig_event=pig_event,
            speed_mps=speed_mps,
            eta_next=eta_next,
            eta_end=eta_end,
            notification_type=notification_type,
            notif_poi=notif_poi,
        )
    
    

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
        
        


        return {
            # "Pig ID": pig_id,
            # "Tool Type": tool_type,
            # "Now": effective_now.isoformat(),
            # "Sample_time": cur.dt.isoformat(),
            # "Position m:": cur_pos_m,
            # "Speed mps:": speed_mps,
            # "Legacy Route": route,
            # "Previous POI": prev_poi.tag if prev_poi else None,
            # "Next POI": next_poi.tag if next_poi else None,
            # "End POI": end_poi.tag if end_poi else None,
            # "ETA to Next POI (sec)": eta_to_next_sec,
            # "ETA to End POI (sec)": eta_to_end_sec,
            # "Near Next POI": near_next_poi,
            # "Passed Next POI": poi_passed,
            # "Pig Event": pig_event,
            # "Notification Type": notification_type,
        }
        

def _pos_m(self, sample: PosSample) -> Optional[float]:
    """Convert a PosSample to position in meters.
    Priority:
    1. If KP is provided -> meters = KP * 1000
    2. Else if GC is provided and mapping exists in GC-to-KP map -> meters = mapped KP * 1000
    3. Else if GC is provided but no mapping -> meters = GC * (meters_per_channel)
    4. Else -> None
    """

    if sample.kp is not None:
        return float(sample.kp) * 1000.0
    
    if sample.gc is not None:
        kp = self._kp_from_gc(int(sample.gc))
        if kp is not None:
            return float(kp) * 1000.0
        return float(sample.gc) * float(self.cfg.meters_per_channel)
    
    return None

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
    
    cur_m = _pos_m(cur, gc_to_kp, meters_per_channel)
    ref_m = _pos_m(ref, gc_to_kp, meters_per_channel)

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

    

def route_range_m(route_pois: List[POI], gc_to_kp: Dict[int, float], meters_per_channel: float) -> Optional[Tuple[float, float]]:
    
    min_m: Optional[float] = None
    max_m: Optional[float] = None

    for p in route_pois:
        fake_sample = PosSample(dt=datetime.now(), gc=p.global_channel, kp=p.kp)
        m = _pos_m(fake_sample)
        if m is None:
            continue
        if min_m is None or m < min_m:
            min_m = m
        if max_m is None or m > max_m:
            max_m = m
        
    if min_m is None or max_m is None:
        return None
    
    return (min_m, max_m)

    
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
        pm = _pos_m(fake)
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
    poi_pos_m = _pos_m(fake)
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




