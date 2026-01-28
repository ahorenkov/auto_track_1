import time
from datetime import datetime, timedelta, timezone

from core.engine import Engine, EngineConfig
from core.repo import PostgresRepo

from core.repo import make_dedup_key

def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)

def run_detector():
    dsn = "postgresql://auto:auto@localhost:5432/auto"
    repo = PostgresRepo(dsn=dsn, root_dir=".")
    engine = Engine(repo, cfg=EngineConfig())

    poll_every_seconds = 10
    active_lookback_minutes = 60 # 1 hour

    while True:
        now = utcnow()
        since = now - timedelta(minutes=active_lookback_minutes)
        

        pig_ids = repo.list_active_pigs(since_dt=since)
        print(f"[DEBUG] checking for active pigs {pig_ids}")
        for pig_id in pig_ids:
            payload = engine.process_pig(pig_id=pig_id, tool_type="", now=now)

            notif_type = payload.get("Notification Type")
            if not notif_type:
                continue

            dedup_key = make_dedup_key(payload)
            print(f"[DEBUG] notif_type='{payload.get('Notification Type')}' pig_event='{payload.get('Pig Event')}'")   
            inserted = repo.enqueue_notification(
                dedup_key=dedup_key,
                pig_id=pig_id,
                notif_type=str(notif_type),
                payload=payload,
            )
            if inserted:
                print(f"[OUTBOX] inserted {dedup_key}")
            else:
                print(f"[OUTBOX] skipped {dedup_key}")

        time.sleep(poll_every_seconds)

if __name__ == "__main__":
    print("Starting detector worker...")
    run_detector()

