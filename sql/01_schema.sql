CREATE SCHEMA IF NOT EXISTS touchpoint_ledger;

CREATE TABLE IF NOT EXISTS touchpoint_ledger.touchpoints (
  id BIGSERIAL PRIMARY KEY,
  scholar_name TEXT NOT NULL,
  scholar_identifier TEXT,
  channel TEXT NOT NULL,
  staff_name TEXT NOT NULL,
  notes TEXT,
  follow_up_date DATE,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS touchpoint_ledger_touchpoints_occurred_at_idx
  ON touchpoint_ledger.touchpoints (occurred_at DESC);

CREATE INDEX IF NOT EXISTS touchpoint_ledger_touchpoints_follow_up_idx
  ON touchpoint_ledger.touchpoints (follow_up_date);
