import os
import time
import json
import requests
import urllib3
from pathlib import Path

# DEV ONLY: we use verify=False below, so disable warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from core.repo import PostgresRepo  
except Exception:
    from repo import PostgresRepo  # type: ignore


TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID_RAW = os.getenv("TELEGRAM_CHAT_ID")
PG_DSN = os.getenv("AUTO_PG_DSN")

if not TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
if not CHAT_ID_RAW:
    raise RuntimeError("TELEGRAM_CHAT_ID is not set")
if not PG_DSN:
    raise RuntimeError("AUTO_PG_DSN is not set")

CHAT_ID = int(str(CHAT_ID_RAW).strip())
API = f"https://api.telegram.org/bot{TOKEN}"

OFFSET_FILE = ".tg_update_offset.txt"  


def _truncate(s: str, limit: int = 3500) -> str:
    if len(s) <= limit:
        return s
    return s[:limit] + "\n... (truncated)"


def _load_offset() -> int:
    try:
        p = Path(OFFSET_FILE)
        if p.exists():
            return int(p.read_text(encoding="utf-8").strip())
    except Exception:
        pass
    return 0


def _save_offset(offset: int) -> None:
    try:
        Path(OFFSET_FILE).write_text(str(offset), encoding="utf-8")
    except Exception:
        pass


def _drain_offset() -> int:
    """
    Drain all pending updates so we don't process stale callback queries.
    Returns next offset.
    """
    r = requests.get(
        f"{API}/getUpdates",
        params={"timeout": 0, "offset": 0},
        timeout=10,
        verify=False,
    )
    if not r.ok:
        print("drain getUpdates failed:", r.status_code, r.text)
        return 0
    updates = r.json().get("result", [])
    if not updates:
        return 0
    return updates[-1]["update_id"] + 1


def send_approval_message(payload: dict, outbox_id: int, token: str) -> int:
    pretty = json.dumps(payload, ensure_ascii=False, indent=2)
    pretty = _truncate(pretty, 3500)

    text = (
        "🚨 Notification Approval\n\n"
        f"outbox_id: {outbox_id}\n\n"
        "payload:\n"
        f"{pretty}"
    )

    keyboard = {
        "inline_keyboard": [[
            {"text": "✅ Approve", "callback_data": f"A:{outbox_id}:{token}"},
            {"text": "❌ Reject",  "callback_data": f"R:{outbox_id}:{token}"},
        ]]
    }

    r = requests.post(
        f"{API}/sendMessage",
        json={
            "chat_id": CHAT_ID,
            "text": text,
            "reply_markup": keyboard,
        },
        timeout=15,
        verify=False,  # DEV ONLY
    )

    if not r.ok:
        print("Telegram sendMessage failed:", r.status_code)
        print(r.text)
        r.raise_for_status()

    return r.json()["result"]["message_id"]


def _answer_callback(callback_query_id: str, text: str) -> None:
    """
    Best-effort. If Telegram says 'query is too old', we ignore (UI feedback only).
    """
    r = requests.post(
        f"{API}/answerCallbackQuery",
        json={"callback_query_id": callback_query_id, "text": text},
        timeout=5,
        verify=False,  # DEV ONLY
    )
    if not r.ok:
        print("answerCallbackQuery failed:", r.status_code, r.text)


def _remove_buttons(message_id: int) -> None:
    """
    Telegram requires reply_markup to be an object, not null.
    To remove buttons: set empty inline_keyboard.
    """
    r = requests.post(
        f"{API}/editMessageReplyMarkup",
        json={
            "chat_id": CHAT_ID,
            "message_id": message_id,
            "reply_markup": {"inline_keyboard": []},
        },
        timeout=15,
        verify=False,  # DEV ONLY
    )
    if not r.ok:
        print("editMessageReplyMarkup failed:", r.status_code, r.text)


def _append_decision_text(message_id: int, old_text: str, decision: str, user: str) -> None:
    new_text = old_text + f"\n\nDECISION: {decision} by {user}"
    r = requests.post(
        f"{API}/editMessageText",
        json={
            "chat_id": CHAT_ID,
            "message_id": message_id,
            "text": new_text,
        },
        timeout=15,
        verify=False,  # DEV ONLY
    )
    if not r.ok:
        print("editMessageText failed:", r.status_code, r.text)


def main() -> None:
    repo = PostgresRepo(PG_DSN)

    # 1) On startup, prefer persisted offset; if not present, drain to avoid stale callbacks.
    offset = _load_offset()
    if offset == 0:
        offset = _drain_offset()
    _save_offset(offset)

    print("Starting telegram approval worker...")
    print("Using offset:", offset)

    while True:
        try:
            # 2) Poll for callbacks FIRST (so clicks are handled quickly)
            r = requests.get(
                f"{API}/getUpdates",
                params={
                    "timeout": 10,
                    "offset": offset,
                    "allowed_updates": json.dumps(["callback_query"]),
                },
                timeout=20,
                verify=False,  # DEV ONLY
            )

            if not r.ok:
                print("getUpdates failed:", r.status_code, r.text)
            else:
                updates = r.json().get("result", [])
                for upd in updates:
                    offset = upd["update_id"] + 1
                    _save_offset(offset)

                    cq = upd.get("callback_query")
                    if not cq:
                        continue

                    data = cq.get("data", "")
                    user = cq["from"].get("username") or str(cq["from"]["id"])
                    cq_id = cq["id"]

                    print("callback_query:", data, "from:", user)

                    # Best-effort immediate UI ack
                    _answer_callback(cq_id, "OK")

                    try:
                        action, outbox_id_str, token = data.split(":")
                        outbox_id = int(outbox_id_str)
                    except Exception:
                        print("invalid callback data:", data)
                        continue

                    decision = "APPROVED" if action == "A" else "REJECTED"
                    ok = repo.decide_approval(outbox_id, token, decision, user)
                    print("decision:", decision, "db_updated:", ok)

                    if not ok:
                        _answer_callback(cq_id, "Already decided / invalid token")
                        continue

                    msg = cq.get("message") or {}
                    msg_id = msg.get("message_id")
                    old_text = msg.get("text", "")

                    if msg_id is not None:
                        _remove_buttons(int(msg_id))
                        _append_decision_text(int(msg_id), old_text, decision, user)

            # 3) Post a small batch of waiting approvals (limit to keep loop responsive)
            rows = repo.list_waiting_for_telegram(limit=3)
            for outbox_id, token, payload in rows:
                print(f"posting to telegram, outbox_id={outbox_id}")
                msg_id = send_approval_message(payload, outbox_id, token)
                repo.set_telegram_message_id(outbox_id, msg_id)

        except Exception as e:
            print("ERROR in telegram approval worker:", repr(e))

        time.sleep(0.2)


if __name__ == "__main__":
    main()
