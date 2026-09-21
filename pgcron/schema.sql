-- Share one persisted due time per job across replicas and process restarts.
-- New jobs are due immediately; successful runs advance their due time.
CREATE TABLE IF NOT EXISTS pgcron_schedules (
    name TEXT PRIMARY KEY,
    next_run_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
