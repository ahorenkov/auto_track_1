import json
from datetime import datetime, timezone, timedelta
from core.engine import Engine, EngineConfig
from core.state import InMemoryStateStore
from core.repo import CsvRepo

MST = timezone(timedelta(hours=-7), "MST")

def main() -> None:
    repo = CsvRepo(".") 
    engine = Engine(repo)

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