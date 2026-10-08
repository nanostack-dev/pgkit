package workflow

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

const schemaVersion = 1

// EnsureSchema creates the pgworkflow_* tables and applies pending migrations. It
// does not touch the workflow_* tables of the earlier DAG-based package; drop those
// once their runs no longer matter.
func (c *Client) EnsureSchema(ctx context.Context) error {
	if _, err := c.db.ExecContext(ctx,
		`CREATE TABLE IF NOT EXISTS pgworkflow_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		return fmt.Errorf("workflow: create meta table: %w", err)
	}
	var current int
	err := c.db.QueryRowContext(ctx, `SELECT value::int FROM pgworkflow_meta WHERE key = 'schema_version'`).Scan(&current)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("workflow: read schema version: %w", err)
	}
	if current < 1 {
		if _, err := c.db.ExecContext(ctx, schemaV1); err != nil {
			return fmt.Errorf("workflow: migrate to schema 1: %w", err)
		}
	}
	if _, err := c.db.ExecContext(ctx, `
INSERT INTO pgworkflow_meta (key, value) VALUES ('schema_version', $1)
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`, fmt.Sprint(schemaVersion)); err != nil {
		return fmt.Errorf("workflow: record schema version: %w", err)
	}
	return nil
}

const schemaV1 = `
CREATE TABLE IF NOT EXISTS pgworkflow_runs (
    id            TEXT PRIMARY KEY,
    workflow      TEXT NOT NULL,
    version       INTEGER NOT NULL DEFAULT 0,
    key           TEXT,
    status        TEXT NOT NULL CHECK (status IN ('pending', 'running', 'waiting', 'succeeded', 'failed', 'cancelled')),
    input         JSONB NOT NULL,
    output        JSONB,
    error         TEXT,
    timed_out     BOOLEAN NOT NULL DEFAULT FALSE,
    parent_run_id TEXT,
    parent_step   TEXT,
    job_id        BIGINT,
    lease         BIGINT NOT NULL DEFAULT 0,
    deadline_at   TIMESTAMPTZ,
    wake_at       TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS pgworkflow_runs_key ON pgworkflow_runs (workflow, key) WHERE key IS NOT NULL;
CREATE INDEX IF NOT EXISTS pgworkflow_runs_by_workflow ON pgworkflow_runs (workflow, status, created_at DESC);
CREATE INDEX IF NOT EXISTS pgworkflow_runs_by_status ON pgworkflow_runs (status, created_at DESC);
CREATE INDEX IF NOT EXISTS pgworkflow_runs_newest ON pgworkflow_runs (created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS pgworkflow_runs_by_job ON pgworkflow_runs (job_id) WHERE status IN ('pending', 'running', 'waiting');
CREATE INDEX IF NOT EXISTS pgworkflow_runs_by_parent ON pgworkflow_runs (parent_run_id) WHERE parent_run_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS pgworkflow_runs_finished ON pgworkflow_runs (completed_at) WHERE completed_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS pgworkflow_steps (
    run_id       TEXT NOT NULL REFERENCES pgworkflow_runs (id) ON DELETE CASCADE,
    name         TEXT NOT NULL,
    kind         TEXT NOT NULL CHECK (kind IN ('step', 'sleep', 'signal', 'child')),
    status       TEXT NOT NULL CHECK (status IN ('running', 'waiting', 'retrying', 'succeeded', 'failed', 'timed_out')),
    attempts     INTEGER NOT NULL DEFAULT 0,
    output       JSONB,
    error        TEXT,
    wake_at      TIMESTAMPTZ,
    child_run_id TEXT,
    signal       TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, name)
);

CREATE TABLE IF NOT EXISTS pgworkflow_signals (
    id          BIGSERIAL PRIMARY KEY,
    run_id      TEXT NOT NULL REFERENCES pgworkflow_runs (id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    payload     JSONB NOT NULL,
    received_by TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS pgworkflow_signals_by_run ON pgworkflow_signals (run_id, name, id);
`

func jsonUnmarshal(raw json.RawMessage, target any) error {
	if len(raw) == 0 {
		raw = json.RawMessage("null")
	}
	return json.Unmarshal(raw, target)
}
