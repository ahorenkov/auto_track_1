import json
from datetime import datetime, timezone, timedelta
from core.engine import Engine, EngineConfig, pick_current_sample, pick_ref_sample_at_or_before, speed_mps_by_ref
from core.state import InMemoryStateStore
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

    cur = pick_current_sample(samples)
    ref = pick_ref_sample_at_or_before(samples, datetime(2025, 12, 25, 8, 0, tzinfo=MST))
    speed = speed_mps_by_ref(cur, ref, gc_to_kp, cfg.meters_per_channel, cfg.speed_min_mps)
    
    print(f"Current sample: {cur}")
    print(f"Reference sample: {ref}")
    print(f"Computed speed (mps): {speed}")

    for minute in (0, 5, 10):
        
        payload = engine.process_pig(
            pig_id="PIG_001",
            tool_type="Tool A",
            now=datetime(2025, 12, 25, 8, minute, tzinfo=MST),
        )

        print(f'iter minute={minute}')
        print(json.dumps(payload, ensure_ascii=False, indent=2))

    print("gc to kp rows: ", len(repo.get_gc_to_kp()))
    print("POIs: ", len(repo.get_pois()))
    print("Gaps: ", len(repo.get_gaps()))

if __name__ == "__main__":
    main()