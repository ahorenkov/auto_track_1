import json
import os
from datetime import datetime, timezone, timedelta
from core.engine import Engine, EngineConfig, pick_current_sample, pick_ref_sample_at_or_before, speed_mps_by_ref
from core.state import InMemoryStateStore, PigState
from core.repo import CsvRepo
from core.models import PigState, PosSample

MST = timezone(timedelta(hours=-7), "MST")

def main() -> None:
    repo = CsvRepo(".") 
    engine = Engine(repo)

    cfg = EngineConfig(meters_per_channel=25.0, speed_min_mps=0.01, max_ref_age_minutes=35)

    gc_to_kp = {}
    samples = [
        PosSample(dt=datetime(2025, 12, 25, 8, 0, tzinfo=MST), gc=None, kp=10.0),
        PosSample(dt=datetime(2025, 12, 25, 8, 10, tzinfo=MST), gc=None, kp=11.0),
    ]
    s = repo.get_state("pig_1")
    s.locked_legacy_route = "route_1"
    repo.save_state("pig_1", s)

    s2 = repo.get_state("pig_1")
    print(s2.locked_legacy_route)
    
    print("gc to kp rows: ", len(repo._load_gc_to_kp(os.path.join(repo.root_dir, "GCtoKP.csv"))))
    print("POIs: ", len(repo._load_pois(os.path.join(repo.root_dir, "Pig Tracking POI Valves Locations - Monitoring Team_updated(Nov11).csv"))))
    print("Gaps: ", len(repo._load_gaps(os.path.join(repo.root_dir, "GAP.csv"))))

def dt(hh, mm, ss=0):
    return datetime(2026, 1, 8, hh, mm, ss, tzinfo=MST)


if __name__ == "__main__":
    main()