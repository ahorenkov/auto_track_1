from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

import psycopg


MST = timezone(timedelta(hours=-7), name="MST")


def seed(pig_id: str, tool_type: str, samples: List[Tuple[datetime, Optional[int], Optional[float]]]) -> None:
    dsn = os.getenv("AUTO_PG_DSN", "postgresql://auto:auto@localhost:5432/auto")

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            # Truncate existing data for the pig_id
            cur.execute("DELETE FROM pig_positions WHERE pig_id = %s", (pig_id,))

            cur.executemany(
                """
                INSERT INTO pig_positions (pig_id, tool_type, ts, gc, kp)
                VALUES (%s, %s, %s, %s, %s)
                """,
                [(pig_id, tool_type, dt, gc, kp) for (dt, gc, kp) in samples],
            )

        conn.commit()

    print(f"Inserted {len(samples)} rows into pig_positions for {pig_id}")


def main() -> None:
    pig_id = "PIG_001"
    tool_type = "Cleaning Tool"
    now = datetime.now(tz=MST)

    samples = [
        (now - timedelta(minutes=135), 11900, None),
        (now - timedelta(minutes=125), 11940, None),
        (now - timedelta(minutes=112), 11990, None),
        (now - timedelta(minutes=110), 12005, None),
        (now - timedelta(minutes=95), 12020, None),
        (now - timedelta(minutes=83), 12022, None),
        (now - timedelta(minutes=71), 12025, None),
        (now - timedelta(minutes=65), 12050, None),
        (now - timedelta(minutes=55), 12070, None),
        (now - timedelta(minutes=42), 12090, None),  
        (now - timedelta(minutes=30), 12105, None),
        (now - timedelta(minutes=25), 12120, None),
        (now - timedelta(minutes=23), 12122, None),
        (now - timedelta(minutes=21), 12125, None),
        (now - timedelta(minutes=20), 12126, None),
        (now - timedelta(minutes=19), 12130, None),
        (now - timedelta(minutes=18), 12134, None),
        (now - timedelta(minutes=17), 12137, None),
        (now - timedelta(minutes=16), 12139, None),
        (now - timedelta(minutes=15), 12142, None),
        (now - timedelta(minutes=14), 12145, None),
        (now - timedelta(minutes=13), 12147, None),
        (now - timedelta(minutes=12), 12150, None),
        (now - timedelta(minutes=11), 12152, None),
        (now - timedelta(minutes=10), 12155, None),
        (now - timedelta(minutes=9), 12157, None),
        (now - timedelta(minutes=8), 12160, None),
        (now - timedelta(minutes=7), 12162, None),
        (now - timedelta(minutes=6), 12165, None),
        (now - timedelta(minutes=5), 12167, None),
        (now - timedelta(minutes=4), 12170, None),
        (now - timedelta(minutes=3), 12172, None),
        (now - timedelta(minutes=2), 12175, None),
        (now - timedelta(minutes=1), 12178, None),
        (now, 12180, None),
    ]

    seed(pig_id, tool_type, samples)


if __name__ == "__main__":
    print("Seeding telemetry data...")
    main()


'''
-- 1. api start
-- 2. seed telemetry
-- 3. start detector worker
-- 4. start sender worker 
'''