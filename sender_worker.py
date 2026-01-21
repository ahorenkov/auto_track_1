import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, List

import psycopg
import requests



@dataclass
class OutboxItem:
    id: int
    dedup_key: str
    pig_id: str
    notif_type: str
    payload: dict
    attempt_count: int

def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)

def compute_backloff(attempt_count: int) -> int:
    schedule = [10, 30, 60, 120, 300]
    idx = min(attempt_count, len(schedule) - 1)
    return schedule[idx]

class OutboxSender:
    def __init__(self, dsn: str, endpoint_url: str, worker_name: str) -> None:
        self.dsn = dsn
        self.endpoint_url = endpoint_url
        self.worker_name = worker_name
    
    def fetch_batch(self, batch_size: int) -> List[OutboxItem]:
        sql = """
        SELECT id, dedup_key, pig_id, notif_type, payload, attempt_count
        FROM notifications_outbox
        WHERE status in ('NEW', 'RETRY')
        AND next_attempt_at <= now()
        ORDER BY id
        FOR UPDATE SKIP LOCKED
        LIMIT %s
        """
        items: List[OutboxItem] = []
        with psycopg.connect(self.dsn) as conn:
            conn.execute("begin")
            with conn.cursor() as cur:
                cur.execute(sql, (batch_size,))
                rows = cur.fetchall()
                # mark what we fetched in work
                
                if rows:
                    ids = [r[0] for r in rows]  # список int

                    cur.execute(
                        """
                        update notifications_outbox
                        set status='RETRY',
                            next_attempt_at=now(),
                            updated_at=now(),
                            locked_by=null,
                            locked_at=null
                        where status='SENDING'
                          and locked_at is not null
                          and locked_at < now() - interval '5 minutes';
                        """,
                        (self.worker_name, ids),
                    )
                conn.commit()
            for row in rows:
                items.append(
                    OutboxItem(
                        id=row[0],
                        dedup_key=row[1],
                        pig_id=row[2],
                        notif_type=row[3],
                        payload=row[4],
                        attempt_count=row[5],
                    )
                )

        return items

    def mark_sent(self, item_id: int) -> None:
        sql = """
        UPDATE notifications_outbox
        SET status='SENT', sent_at=now(), updated_at=now(), locked_by=NULL, locked_at=NULL
        WHERE id=%s
        """
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (item_id,))
            conn.commit()

    def mark_retry(self, item_id: int, attempt_count: int, err: str) -> None:
        backoff = compute_backloff(attempt_count)
        sql = """
        UPDATE notifications_outbox
        SET status='RETRY',
            attempt_count=%s,
            next_attempt_at=now() + (%s || ' seconds')::interval,
            last_error=%s,,
            updated_at=now(),
            locked_by=NULL,
            locked_at=NULL
        WHERE id=%s
        """
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (attempt_count, backoff, err[:1000], item_id))
            conn.commit()

    def mark_dead(self, item_id: int, attempt_count: int, err: str) -> None:
        sql = """
        UPDATE notifications_outbox
        SET status='DEAD',
            attempt_count=%s,
            last_error=%s,
            updated_at=now(),
            locked_by=NULL,
            locked_at=NULL
        WHERE id=%s
        """
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (attempt_count, err[:1000], item_id))
            conn.commit()

    def send_one(self, item: OutboxItem) -> None:
        headers = {'Content-Type': 'application/json',
                   "Idempotency-Key": item.dedup_key}
        resp = requests.post(
            self.endpoint_url,
            headers=headers,
            data=json.dumps(item.payload, default=str),
            timeout=10,
        )

        if 200 <= resp.status_code < 300:
            return
        
        raise Exception(f"HTTP {resp.status_code}: {resp.text[:300]}")
    
    def run_forever(self, batch_size: int = 10, sleep_seconds: int = 2, max_attempts: int = 10) -> None:
        print(f"[LOOP] {utcnow().isoformat()}", flush=True)
        while True:
            items = self.fetch_batch(batch_size=batch_size)
            items = items or []
            print(f"[BATCH] {len(items)} items", flush=True)  
            if not items:
                time.sleep(sleep_seconds)
                continue
            for item in items:
                try:
                    self.send_one(item)
                    self.mark_sent(item.id)
                    print(f"[SENT] id={item.id} key={item.dedup_key}")
                except Exception as e:
                    next_attempt_count = item.attempt_count + 1
                    err = str(e)

                    if next_attempt_count >= max_attempts:
                        self.mark_dead(item.id, next_attempt_count, err)
                        print(f"[DEAD] id={item.id} key={item.dedup_key} err={err}")
                    else:
                        self.mark_retry(item.id, next_attempt_count, err)
                        print(f"[RETRY] id={item.id} key={item.dedup_key} err={err}")

def main():
    dsn = "postgresql://auto:auto@localhost:5432/auto"
    endpoint_url = "http://localhost:8010/ingest"
    worker_name = "sender_1"

    sender = OutboxSender(dsn=dsn, endpoint_url=endpoint_url, worker_name=worker_name)
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("select current_database(), current_schema(), inet_server_addr(), inet_server_port()")
            print("DB info:", cur.fetchone(), flush=True)
    sender.run_forever(batch_size=5, sleep_seconds=5, max_attempts=5)

if __name__ == "__main__":
    main()