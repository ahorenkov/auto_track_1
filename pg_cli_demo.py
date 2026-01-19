import json
import psycopg
from datetime import datetime, timedelta, timezone
from core.engine import Engine, EngineConfig
from core.repo import PostgresRepo

MST = timezone(timedelta(hours=-7), name="MST")

def seed(repo_dsn: str, pig_id: str, now: datetime) -> None:
    
    rows = [
        (pig_id, now - timedelta(minutes=35), None, 292.123),
        (pig_id, now - timedelta(minutes=25), None, 292.789),
        (pig_id, now - timedelta(minutes=12), None, 293),
        (pig_id, now - timedelta(minutes=10), None, 293.456),
        (pig_id, now - timedelta(minutes=5), None, 294),
        (pig_id, now - timedelta(minutes=3), None, 294.567),
        (pig_id, now - timedelta(minutes=1), None, 295.123),
        (pig_id, now, None, 295.787),
    ]
    with psycopg.connect(repo_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pig_positions WHERE pig_id = %s", (pig_id,))
            cur.executemany(
                "INSERT INTO pig_positions (pig_id, ts, gc, kp) VALUES (%s, %s, %s, %s)",
                rows,
            )
        conn.commit()

def main() -> None:
    dsn = "postgresql://auto:auto@localhost:5432/auto"
    repo = PostgresRepo(dsn=dsn, root_dir=".")
    pig_id = "PIG_002"
    tool_type = ""
    now = datetime(2025, 12, 23, 8, 10, 0, tzinfo=MST)
    seed(dsn, pig_id, now)

    engine = Engine(repo, cfg=EngineConfig())
    print("POI loaded:", len(repo.get_pois()))
    print("GCtoKP loaded:", len(repo.get_gc_to_kp()))
    payload = engine.process_pig(pig_id=pig_id, tool_type=tool_type, now=now)
    print(json.dumps(payload, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()