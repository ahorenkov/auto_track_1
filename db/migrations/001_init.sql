-- =========
-- Telemetry
-- =========

create table if not exists pig_positions (
  id bigserial primary key,
  pig_id text not null,
  ts timestamptz not null,
  gc integer,
  kp double precision
);

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