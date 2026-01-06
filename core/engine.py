from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Dict, List
from core.state import InMemoryStateStore
from core.models import PigState, PosSample


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

        self.state_store.save(pig_id, state)

        return {
            "Pig ID": pig_id,
            "Tool Type": tool_type,
            "Now": now.isoformat(),
            "First Notification At": (
                state.first_notif_at.isoformat()
                if state.first_notif_at
                else None
            ),
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

