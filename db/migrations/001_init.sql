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