-- =========
-- Telemetry
-- =========

create table if not exists pig_positions (
  id bigserial primary key,
  pig_id text not null,
  tool_type text,
  ts timestamptz not null,
  gc integer,
  kp double precision
);

alter table pig_positions
  add column if not exists tool_type text;

create index if not exists idx_pig_positions_pig_ts
  on pig_positions (pig_id, ts desc);


-- =========
-- Pig state
-- =========

create table if not exists pig_state (
  pig_id text primary key,
  state_json jsonb not null,
  updated_at timestamptz not null default now()
);

-- =========
-- Notifications Outbox
-- =========

create table if not exists notifications_outbox (
  id bigserial primary key,
  dedup_key text not null unique,
  pig_id text not null,
  notif_type text not null,
  payload jsonb not null,
  status text not null default 'NEW',
  attempt_count int not null default 0,
  next_attempt_at timestamptz not null default now(),
  last_error text,
  sent_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  locked_by text,
  locked_at timestamptz
);

ALTER TABLE notifications_outbox
  ADD COLUMN IF NOT EXISTS approval_status text NOT NULL DEFAULT 'WAITING',
  ADD COLUMN IF NOT EXISTS approval_token text,
  ADD COLUMN IF NOT EXISTS approval_decided_at timestamptz,
  ADD COLUMN IF NOT EXISTS approval_decided_by text,
  ADD COLUMN IF NOT EXISTS telegram_message_id bigint;

CREATE INDEX IF NOT EXISTS idx_outbox_waiting_telegram
  ON notifications_outbox (status, approval_status)
  WHERE approval_status = 'WAITING';