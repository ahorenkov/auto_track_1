import json
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Sequence, Tuple

import psycopg
import requests


MST = timezone(timedelta(hours=-7), name="MST")

@dataclass
class OutboxItem:
    id: int
    dedup_key: str
    pig_id: str
    notif_type: str
    payload: dict
    attempt_count: int


def utcnow() -> datetime:
    return datetime.now(tz=MST)


def compute_backoff_seconds(attempt_count: int) -> int:
    """
    Backoff schedule (seconds) by attempt_count (0-based).
    attempt_count is the current count stored in DB.
    When we schedule the *next* attempt, we pass next_attempt_count.
    """
    schedule = [10, 30, 60, 120, 300, 600]  # capped
    idx = min(max(attempt_count, 0), len(schedule) - 1)
    base = schedule[idx]
    # small jitter to avoid thundering herd
    jitter = random.randint(0, max(1, base // 10))
    return base + jitter


class OutboxSender:
    def __init__(self, dsn: str, endpoint_url: str, worker_name: str) -> None:
        self.dsn = dsn
        self.endpoint_url = endpoint_url
        self.worker_name = worker_name
        self.session = requests.Session()

    def reclaim_stale_sending(self, conn: psycopg.Connection, stale_seconds: int = 300) -> int:
        """Move stuck SENDING rows back to RETRY (e.g., if a worker crashed)."""
        sql = """
        UPDATE notifications_outbox
        SET status='RETRY',
            next_attempt_at=now(),
            updated_at=now(),
            locked_by=NULL,
            locked_at=NULL
        WHERE status='SENDING'
          AND locked_at IS NOT NULL
          AND locked_at < now() - (%s || ' seconds')::interval
        """
        with conn.cursor() as cur:
            cur.execute(sql, (stale_seconds,))
            return cur.rowcount

    def claim_batch(self, conn: psycopg.Connection, batch_size: int) -> List[OutboxItem]:
        """
        Atomically claim a batch for sending (SKIP LOCKED) and mark as SENDING.
        Must be called inside a transaction.
        """
        select_sql = """
        SELECT id, dedup_key, pig_id, notif_type, payload, attempt_count
        FROM notifications_outbox
        WHERE status IN ('NEW', 'RETRY') AND approval_status='APPROVED'
          AND next_attempt_at <= now()
        ORDER BY id
        FOR UPDATE SKIP LOCKED
        LIMIT %s
        """
        with conn.cursor() as cur:
            cur.execute(select_sql, (batch_size,))
            rows = cur.fetchall()
            if not rows:
                return []

            ids = [r[0] for r in rows]
            cur.execute(
                """
                UPDATE notifications_outbox
                SET status='SENDING',
                    locked_by=%s,
                    locked_at=now(),
                    updated_at=now()
                WHERE id = ANY(%s)
                """,
                (self.worker_name, ids),
            )

        items: List[OutboxItem] = []
        for r in rows:
            payload = r[4]
            if isinstance(payload, str):
                payload = json.loads(payload) # correct type if needed
            items.append(
                OutboxItem(
                    id=r[0],
                    dedup_key=r[1],
                    pig_id=r[2],
                    notif_type=r[3],
                    payload=r[4],
                    attempt_count=r[5],
                )
            )
        return items

    def _mark_sent_many(self, conn: psycopg.Connection, ids: Sequence[int]) -> int:
        if not ids:
            return 0
        sql = """
        UPDATE notifications_outbox
        SET status='SENT',
            sent_at=now(),
            updated_at=now(),
            locked_by=NULL,
            locked_at=NULL
        WHERE id = ANY(%s)
        """
        with conn.cursor() as cur:
            cur.execute(sql, (list(ids),))
            return cur.rowcount

    def _mark_dead_many(self, conn: psycopg.Connection, items: Sequence[Tuple[int, int, str]]) -> int:
        """
        items: (id, attempt_count, err)
        """
        if not items:
            return 0
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
        with conn.cursor() as cur:
            cur.executemany(sql, [(attempt, err[:1000], item_id) for (item_id, attempt, err) in items])
            return cur.rowcount

    def _mark_retry_many(self, conn: psycopg.Connection, items: Sequence[Tuple[int, int, int, str]]) -> int:
        """
        items: (id, next_attempt_count, backoff_seconds, err)
        """
        if not items:
            return 0
        sql = """
        UPDATE notifications_outbox
        SET status='RETRY',
            attempt_count=%s,
            next_attempt_at=now() + (%s || ' seconds')::interval,
            last_error=%s,
            updated_at=now(),
            locked_by=NULL,
            locked_at=NULL
        WHERE id=%s
        """
        with conn.cursor() as cur:
            cur.executemany(sql, [(attempt, backoff, err[:1000], item_id) for (item_id, attempt, backoff, err) in items])
            return cur.rowcount

    def send_one(self, item: OutboxItem) -> Tuple[bool, str]:
        """
        Returns (ok, err). On failure err contains a short description.
        """
        headers = {
            "Content-Type": "application/json",
            "Idempotency-Key": item.dedup_key,
        }
        t0 = time.time()
        try:
            resp = self.session.post(
                self.endpoint_url,
                headers=headers,
                data=json.dumps(item.payload, default=str),
                timeout=10,
            )
            dt_ms = int((time.time() - t0) * 1000)
            if 200 <= resp.status_code < 300:
                return True, f"ok {resp.status_code} {dt_ms}ms"
            return False, f"http {resp.status_code} {dt_ms}ms: {resp.text[:300]}"
        except Exception as e:
            dt_ms = int((time.time() - t0) * 1000)
            return False, f"exc {dt_ms}ms: {e}"

    def run_forever(
        self,
        batch_size: int = 10,
        sleep_seconds: int = 2,
        max_attempts: int = 10,
        stale_seconds: int = 300,
        reclaim_every_loops: int = 10,
    ) -> None:
        print(f"[sender] start {utcnow().isoformat()} worker={self.worker_name}", flush=True)

        loops = 0
        with psycopg.connect(self.dsn) as conn:
            # keep one connection open; psycopg will reconnect on hard failure only if you implement it
            # (simple and good enough for dev)
            while True:
                loops += 1

                # 1) claim a batch in a single transaction
                with conn.transaction():
                    if loops % max(1, reclaim_every_loops) == 0:
                        reclaimed = self.reclaim_stale_sending(conn, stale_seconds=stale_seconds)
                        if reclaimed:
                            print(f"[sender] reclaimed={reclaimed}", flush=True)

                    items = self.claim_batch(conn, batch_size=batch_size)

                if not items:
                    time.sleep(sleep_seconds)
                    continue

                # 2) send outside the transaction
                sent_ids: List[int] = []
                retry_rows: List[Tuple[int, int, int, str]] = []
                dead_rows: List[Tuple[int, int, str]] = []

                for item in items:
                    ok, info = self.send_one(item)
                    if ok:
                        sent_ids.append(item.id)
                        print(f"[SENT] id={item.id} key={item.dedup_key} {info}", flush=True)
                        continue

                    next_attempt_count = item.attempt_count + 1
                    if next_attempt_count >= max_attempts:
                        dead_rows.append((item.id, next_attempt_count, info))
                        print(f"[DEAD] id={item.id} key={item.dedup_key} {info}", flush=True)
                    else:
                        backoff = compute_backoff_seconds(next_attempt_count)
                        retry_rows.append((item.id, next_attempt_count, backoff, info))
                        print(f"[RETRY] id={item.id} key={item.dedup_key} in={backoff}s {info}", flush=True)

                # 3) persist results in one transaction
                with conn.transaction():
                    self._mark_sent_many(conn, sent_ids)
                    self._mark_retry_many(conn, retry_rows)
                    self._mark_dead_many(conn, dead_rows)


def main() -> None:
    dsn = os.getenv("AUTO_PG_DSN", "postgresql://auto:auto@localhost:5432/auto")
    endpoint_url = os.getenv("AUTO_SENDER_ENDPOINT", "http://localhost:8010/ingest")
    worker_name = os.getenv("AUTO_SENDER_NAME", "sender_1")

    sender = OutboxSender(dsn=dsn, endpoint_url=endpoint_url, worker_name=worker_name)

    # quick DB ping
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("select current_database(), current_schema(), inet_server_addr(), inet_server_port()")
            print("[sender] DB info:", cur.fetchone(), flush=True)

    sender.run_forever(
        batch_size=int(os.getenv("AUTO_SENDER_BATCH", "5")),
        sleep_seconds=float(os.getenv("AUTO_SENDER_SLEEP", "2")),
        max_attempts=int(os.getenv("AUTO_SENDER_MAX_ATTEMPTS", "5")),
        stale_seconds=int(os.getenv("AUTO_SENDER_STALE_SECONDS", "300")),
    )


if __name__ == "__main__":
    main()
