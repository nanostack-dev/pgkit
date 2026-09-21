CREATE TABLE IF NOT EXISTS pgcron_schedules (
    name TEXT PRIMARY KEY,
    next_run_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
