PIG Notification System — High-Level Flow

System Overview
---------------
The system continuously monitors PIG telemetry, detects movement-related events,
stores notifications reliably in a database, and delivers them to an external API
with retries and deduplication.

PostgreSQL is the single source of truth for both telemetry and notification state.


Main Components
---------------

PostgreSQL
- Stores raw PIG telemetry (pig_positions)
- Stores notifications in an outbox table (notifications_outbox)
- Guarantees durability and consistency

Detector Worker
- Periodically reads recent PIG positions
- Detects movement events
- Creates notification records in the outbox
- Never sends notifications directly

Sender Worker
- Reads ready notifications from the outbox
- Sends them to the external API
- Handles retries, backoff, and failures
- Can run multiple instances safely

External API
- Receives notifications
- Processes them idempotently using an Idempotency-Key


End-to-End Flow
---------------

1. Telemetry Ingestion

[PIG Telemetry Source]
        |
        v
[PostgreSQL: pig_positions]

- PIG position data is continuously written to the database.
- This data is the authoritative source for movement analysis.


2. Event Detection

[Detector Worker]
        |
        v
Read recent PIG positions

The detector:
- Determines the current PIG state:
  - Moving
  - Stopped
  - Completed
- Selects a legacy route (sticky until completion)
- Calculates speed and ETA
- Detects events such as:
  - Run completion
  - POI passage
  - Gap start / gap end
  - 15-minute / 30-minute pre-POI alerts
  - 30-minute heartbeat updates


3. Notification Creation (Outbox Pattern)

[Detector Worker]
        |
        v
[PostgreSQL: notifications_outbox]

For each detected event:
- A notification payload is created
- A deterministic deduplication key is generated
- The notification is inserted into the outbox table

If the same event already exists, the insert is skipped.

This ensures:
- No duplicate notifications
- No lost events
- Full crash safety


4. Claim Notifications for Sending

[Sender Worker]
        |
        v
[PostgreSQL: notifications_outbox]

The sender worker:
- Selects notifications with status NEW or RETRY
- Claims them atomically using database locks
- Marks them as SENDING

Multiple sender workers can run in parallel without conflicts.


5. Send to External API

[Sender Worker]
        |
        v
[External Notification API]

For each claimed notification:
- An HTTP request is sent with:
  - JSON payload
  - Idempotency-Key header
- The sender waits for the API response


6. Handle Delivery Result

Successful delivery:
status -> SENT

Temporary failure:
status -> RETRY
(next attempt scheduled with backoff)

Permanent failure:
status -> DEAD

- Retries use increasing delays (backoff)
- Failed notifications are never lost
- All states are visible and auditable in the database


One-Line Summary
----------------
Telemetry -> Detect Events -> Store in Outbox -> Send -> Retry if needed


Design Notes
------------
- The detector only detects and records events
- The sender only delivers notifications
- PostgreSQL acts as the system backbone
