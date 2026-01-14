from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from core.engine import Engine, EngineConfig
from core.repo import CsvRepo
from core.models import PosSample


print( 'CLI Demo Running...' )
MST = timezone(timedelta(hours=-7), name="MST")


def main() -> None:
    repo = CsvRepo(root_dir=".")  # expects POI.csv, GCtoKP.csv, gap.csv in current folder

    pig_id = "PIG_001"
    tool_type = "UnknownTool"
    now = datetime(2025, 12, 23, 8, 10, 0, tzinfo=MST)

    # Demo telemetry: include enough history (35 min) for long-window speed ref.
    repo.set_demo_telemetry(
        pig_id,
        [
            PosSample(dt=now - timedelta(minutes=35), gc=11900),
            PosSample(dt=now - timedelta(minutes=25), gc=11940),
            PosSample(dt=now - timedelta(minutes=12), gc=12000),
            PosSample(dt=now - timedelta(minutes=10), gc=12005),
            PosSample(dt=now - timedelta(minutes=5), gc=12020),
            PosSample(dt=now - timedelta(minutes=3), gc=12022),
            PosSample(dt=now - timedelta(minutes=1), gc=12025),
            PosSample(dt=now, gc=12026),
        ],
    )

    engine = Engine(repo, cfg=EngineConfig())
    payload = engine.process_pig(pig_id=pig_id, tool_type=tool_type, now=now)
    print(json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Error: {e}")
    print('Press Enter to exit...')