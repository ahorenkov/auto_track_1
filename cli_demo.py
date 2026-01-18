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
            PosSample(dt=now - timedelta(minutes=135), gc=11900),
            PosSample(dt=now - timedelta(minutes=125), gc=11940),
            PosSample(dt=now - timedelta(minutes=112), gc=11990),
            PosSample(dt=now - timedelta(minutes=110), gc=12005),
            PosSample(dt=now - timedelta(minutes=95), gc=12020),
            PosSample(dt=now - timedelta(minutes=83), gc=12022),
            PosSample(dt=now - timedelta(minutes=71), gc=12025),
            PosSample(dt=now - timedelta(minutes=65), gc=12050),
            PosSample(dt=now - timedelta(minutes=55), gc=12070),
            PosSample(dt=now - timedelta(minutes=42), gc=1209),
            PosSample(dt=now - timedelta(minutes=30), gc=12105),
            PosSample(dt=now - timedelta(minutes=25), gc=12120),
            PosSample(dt=now - timedelta(minutes=23), gc=12122),
            PosSample(dt=now - timedelta(minutes=21), gc=12125),
            PosSample(dt=now - timedelta(minutes=20), gc=12126),
            PosSample(dt=now - timedelta(minutes=19), gc=12130),
            PosSample(dt=now - timedelta(minutes=18), gc=12134),
            PosSample(dt=now - timedelta(minutes=17), gc=12137),
            PosSample(dt=now - timedelta(minutes=16), gc=12139),
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
