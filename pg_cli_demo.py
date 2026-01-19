import json
import psycopg
from datetime import datetime, timedelta, timezone
from core.engine import Engine, EngineConfig
from core.repo import PostgresRepo

MST = timezone(timedelta(hours=-7), name="MST")

def seed(repo_dsn: str, pig_id: str, now: datetime) -> None:
    
    rows = [
        (pig_id, now - timedelta(minutes=35), 11900),
        (pig_id, now - timedelta(minutes=25), 11940),
        (pig_id, now - timedelta(minutes=12), 11990),
        (pig_id, now - timedelta(minutes=10), 12005),
        (pig_id, now - timedelta(minutes=5), 12020),
        (pig_id, now - timedelta(minutes=3), 12022),
        (pig_id, now - timedelta(minutes=1), 12025),
        (pig_id, now, 12026),
    ]
    with psycopg.connect(repo_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pig_positions WHERE pig_id = %s", (pig_id,))
            cur.executemany(
                "INSERT INTO pig_positions (pig_id, ts, gc) VALUES (%s, %s, %s)",
                rows,
            )
        conn.commit()

def main() -> None:
    dsn = "postgresql://auto:auto@localhost:5432/auto"
    repo = PostgresRepo(dsn=dsn, root_dir=".")
    pig_id = "PIG_001"
    tool_type = "UnknownTool"
    now = datetime(2025, 12, 23, 8, 10, 0, tzinfo=MST)
    seed(dsn, pig_id, now)

    engine = Engine(repo, cfg=EngineConfig())
    payload = engine.process_pig(pig_id=pig_id, tool_type=tool_type, now=now)
    print(json.dumps(payload, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()