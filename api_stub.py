from datetime import datetime, datetime, timezone
from typing import List, Any, Dict, Optional

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse

app = FastAPI(title="Local Ingest Stub")

@app.post("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}

@app.post("/ingest")
async def ingest(
    request: Request,
    idempotency_key: Optional[str] = Header(default=None, alias ="Idempotency-Key"),
) -> JSONResponse:
    body: Any = await request.json()
    now = datetime.now(tz=timezone.utc).isoformat()
    pig_id = body.get("Pig ID")
    notif_type = body.get("Notification Type")

    print("/n=== INGEST ===")
    print(f"Time: {now}")
    print(f"Idempotency-Key: {idempotency_key}")
    print(f"Pig ID: {pig_id}")
    print(f"Notification Type: {notif_type}")

    if isinstance(body, dict):
        print("payload keys:", sorted(body.keys()))
    return JSONResponse({"ok": True})
