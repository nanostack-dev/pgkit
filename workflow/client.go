package workflow

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/nanostack-dev/pgkit/queue"
)

// Client starts, signals, cancels, retries and inspects runs, and builds workers.
// It stores runs in the database of its queue client.
type Client struct {
	queue *queue.Client
	db    *sql.DB
	log   Logger
}

const runJobMaxAttempts = 20

// New returns a client storing runs in the database of q. Call EnsureSchema once
// before use.
func New(q *queue.Client) (*Client, error) {
	if q == nil {
		return nil, ErrNilQueue
	}
	if q.DB() == nil {
		return nil, queue.ErrNilDB
	}
	return &Client{queue: q, db: q.DB(), log: noopLogger{}}, nil
}

// SetLogger plugs a logger in; nil restores the silent default.
func (c *Client) SetLogger(logger Logger) {
	if logger == nil {
		logger = noopLogger{}
	}
	c.log = logger
}

// Start starts a run of w with input. With a Key, a run of w that already has the
// key is returned instead.
func (c *Client) Start[In, Out any](ctx context.Context, w *Workflow[In, Out], input In, options ...StartOption) (Run[Out], error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return Run[Out]{}, fmt.Errorf("workflow: begin start: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	run, err := c.StartTx(ctx, tx, w, input, options...)
	if err != nil {
		return Run[Out]{}, err
	}
	if err := tx.Commit(); err != nil {
		return Run[Out]{}, fmt.Errorf("workflow: commit start: %w", err)
	}
	return run, nil
}

// StartTx starts a run in tx, so it exists only if tx commits, together with the
// caller's other writes.
func (c *Client) StartTx[In, Out any](ctx context.Context, tx *sql.Tx, w *Workflow[In, Out], input In, options ...StartOption) (Run[Out], error) {
	config := startConfig{}
	for _, option := range options {
		option.applyToStart(&config)
	}
	encoded, err := json.Marshal(input)
	if err == nil {
		err = checkStorable(encoded)
	}
	if err != nil {
		return Run[Out]{}, fmt.Errorf("workflow: encode %T input: %w", input, err)
	}
	runID, err := c.insertRun(ctx, tx, w.definition, encoded, config, "", "")
	if err != nil {
		return Run[Out]{}, err
	}
	return Run[Out]{ID: runID, client: c}, nil
}

// insertRun creates a run and its job, or returns the run of def that already has
// the configured key.
func (c *Client) insertRun(ctx context.Context, tx *sql.Tx, def *definition, input json.RawMessage, config startConfig, parentRunID, parentStep string) (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("workflow: new run id: %w", err)
	}
	var timeoutSeconds any
	if config.timeout > 0 {
		timeoutSeconds = config.timeout.Seconds()
	}
	var runID string
	err = tx.QueryRowContext(ctx, `
INSERT INTO pgworkflow_runs (id, workflow, version, key, status, input, parent_run_id, parent_step, deadline_at)
VALUES ($1, $2, $3, NULLIF($4, ''), 'pending', $5::jsonb, NULLIF($6, ''), NULLIF($7, ''),
        NOW() + make_interval(secs => $8::float8))
ON CONFLICT (workflow, key) WHERE key IS NOT NULL DO NOTHING
RETURNING id`,
		id.String(), def.name, def.version, config.key, string(input), parentRunID, parentStep, timeoutSeconds,
	).Scan(&runID)
	if errors.Is(err, sql.ErrNoRows) {
		err = tx.QueryRowContext(ctx,
			`SELECT id FROM pgworkflow_runs WHERE workflow = $1 AND key = $2`, def.name, config.key,
		).Scan(&runID)
		if err != nil {
			return "", fmt.Errorf("workflow: find run with key %q: %w", config.key, err)
		}
		return runID, nil
	}
	if err != nil {
		return "", fmt.Errorf("workflow: insert run: %w", err)
	}
	if err := c.enqueueRunJobTx(ctx, tx, runID, def.queueName()); err != nil {
		return "", err
	}
	return runID, nil
}

type runJob struct {
	RunID string `json:"run_id"`
}

func (c *Client) enqueueRunJobTx(ctx context.Context, tx *sql.Tx, runID, queueName string) error {
	payload, err := json.Marshal(runJob{RunID: runID})
	if err != nil {
		return err
	}
	jobID, err := c.queue.EnqueueTx(ctx, tx, queue.EnqueueParams{
		QueueName:   queueName,
		Payload:     payload,
		MaxAttempts: runJobMaxAttempts,
	})
	if err != nil {
		return fmt.Errorf("workflow: enqueue run: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE pgworkflow_runs SET job_id = $2 WHERE id = $1`, runID, jobID); err != nil {
		return fmt.Errorf("workflow: link run job: %w", err)
	}
	return nil
}

// Run is a typed handle on the run with this ID, decoding its output as w's. It
// neither checks that the run exists nor that it is a run of w.
func (c *Client) Run[In, Out any](w *Workflow[In, Out], runID string) Run[Out] {
	return Run[Out]{ID: runID, client: c}
}

// RunByKey returns the run of w's name started with this Key, whatever its
// version (keys are unique per workflow name), or ErrRunNotFound.
func (c *Client) RunByKey[In, Out any](ctx context.Context, w *Workflow[In, Out], key string) (Run[Out], error) {
	var runID string
	err := c.db.QueryRowContext(ctx,
		`SELECT id FROM pgworkflow_runs WHERE workflow = $1 AND key = $2`, w.definition.name, key,
	).Scan(&runID)
	if errors.Is(err, sql.ErrNoRows) {
		return Run[Out]{}, fmt.Errorf("%w: %s with key %q", ErrRunNotFound, w.definition.name, key)
	}
	if err != nil {
		return Run[Out]{}, fmt.Errorf("workflow: find run by key: %w", err)
	}
	return Run[Out]{ID: runID, client: c}, nil
}

// Signal sends value to a run, which takes it with Context.Receive. It returns
// ErrRunNotFound or ErrRunFinished when no run can receive it.
func (c *Client) Signal[T any](ctx context.Context, runID string, signal Signal[T], value T) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("workflow: begin signal: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := c.SignalTx(ctx, tx, runID, signal, value); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("workflow: commit signal: %w", err)
	}
	return nil
}

// SignalTx sends value to a run in tx, so the run receives it only if tx commits.
func (c *Client) SignalTx[T any](ctx context.Context, tx *sql.Tx, runID string, signal Signal[T], value T) error {
	if !validName(signal.name) {
		return fmt.Errorf("%w: signal %q", ErrInvalidName, signal.name)
	}
	payload, err := json.Marshal(value)
	if err == nil {
		err = checkStorable(payload)
	}
	if err != nil {
		return fmt.Errorf("workflow: encode %T signal: %w", value, err)
	}
	status, jobID, err := lockRunToNotifyTx(ctx, tx, runID)
	if err != nil {
		return err
	}
	if status.Finished() {
		return fmt.Errorf("%w: %s is %s", ErrRunFinished, runID, status)
	}
	// clock_timestamp, not NOW(): a transaction begun before a receive's deadline
	// must not backdate a signal sent after it.
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO pgworkflow_signals (run_id, name, payload, created_at) VALUES ($1, $2, $3::jsonb, clock_timestamp())`,
		runID, signal.name, string(payload),
	); err != nil {
		return fmt.Errorf("workflow: insert signal: %w", err)
	}
	var awaited bool
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
    SELECT 1 FROM pgworkflow_steps
    WHERE run_id = $1 AND kind = 'signal' AND status = 'waiting' AND signal = $2)`,
		runID, signal.name,
	).Scan(&awaited); err != nil {
		return fmt.Errorf("workflow: check awaited signal: %w", err)
	}
	if status == RunWaiting && awaited && jobID.Valid {
		return c.queue.WakeTx(ctx, tx, jobID.Int64)
	}
	return nil
}

// lockRunTx locks a run against every other change, for cancelling or retrying it.
func lockRunTx(ctx context.Context, tx *sql.Tx, runID string) (RunStatus, sql.NullInt64, error) {
	return lockRun(ctx, tx, runID, "FOR UPDATE")
}

// lockRunToNotifyTx locks a run against parking only. Waking a run must serialize
// with its park, but not with its checkpoint writes, so a signal or a finishing
// child does not wait for a transactional step in progress.
func lockRunToNotifyTx(ctx context.Context, tx *sql.Tx, runID string) (RunStatus, sql.NullInt64, error) {
	return lockRun(ctx, tx, runID, "FOR KEY SHARE")
}

func lockRun(ctx context.Context, tx *sql.Tx, runID, lockStrength string) (RunStatus, sql.NullInt64, error) {
	var status RunStatus
	var jobID sql.NullInt64
	err := tx.QueryRowContext(ctx,
		`SELECT status, job_id FROM pgworkflow_runs WHERE id = $1 `+lockStrength, runID,
	).Scan(&status, &jobID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", jobID, fmt.Errorf("%w: %s", ErrRunNotFound, runID)
	}
	if err != nil {
		return "", jobID, fmt.Errorf("workflow: lock run: %w", err)
	}
	return status, jobID, nil
}

// wakeParentTx locks the parent of a finishing run and wakes it if it is parked.
func (c *Client) wakeParentTx(ctx context.Context, tx *sql.Tx, parentRunID string) error {
	status, jobID, err := lockRunToNotifyTx(ctx, tx, parentRunID)
	if errors.Is(err, ErrRunNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if status == RunWaiting && jobID.Valid {
		return c.queue.WakeTx(ctx, tx, jobID.Int64)
	}
	return nil
}

// Cancel cancels a run and its unfinished descendants. A step running at that
// moment sees its context cancelled and the run starts no new work; a
// transactional step in progress commits or rolls back first. Cancelling a
// cancelled run does nothing; cancelling a succeeded or failed one returns
// ErrRunFinished.
func (c *Client) Cancel(ctx context.Context, runID string) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("workflow: begin cancel: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var parentRunID sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT parent_run_id FROM pgworkflow_runs WHERE id = $1`, runID).Scan(&parentRunID)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s", ErrRunNotFound, runID)
	}
	if err != nil {
		return fmt.Errorf("workflow: find run to cancel: %w", err)
	}
	if parentRunID.Valid {
		if err := c.wakeParentTx(ctx, tx, parentRunID.String); err != nil {
			return err
		}
	}
	status, _, err := lockRunTx(ctx, tx, runID)
	if err != nil {
		return err
	}
	switch status {
	case RunCancelled:
		return nil
	case RunSucceeded, RunFailed:
		return fmt.Errorf("%w: %s is %s", ErrRunFinished, runID, status)
	}
	if err := c.cancelTreeTx(ctx, tx, runID, true); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("workflow: commit cancel: %w", err)
	}
	return nil
}

func (c *Client) cancelDescendantsTx(ctx context.Context, tx *sql.Tx, runID string) error {
	return c.cancelTreeTx(ctx, tx, runID, false)
}

// cancelTreeTx cancels the unfinished runs below runID, and runID itself when
// includeRoot is set. It locks them from the top down and wakes their jobs, so
// parked runs settle their jobs and running ones notice through their watchers. It
// looks again until no unfinished descendant is left: a child started while the
// first lookup waited for a lock is only visible to a later one.
func (c *Client) cancelTreeTx(ctx context.Context, tx *sql.Tx, runID string, includeRoot bool) error {
	for {
		runs, err := unfinishedTreeTx(ctx, tx, runID, includeRoot)
		if err != nil || len(runs) == 0 {
			return err
		}
		for _, run := range runs {
			if _, err := tx.ExecContext(ctx, `
UPDATE pgworkflow_runs
SET status = 'cancelled', error = 'cancelled', completed_at = NOW(), wake_at = NULL, updated_at = NOW()
WHERE id = $1`, run.id); err != nil {
				return fmt.Errorf("workflow: cancel run: %w", err)
			}
			if run.jobID.Valid {
				if err := c.queue.WakeTx(ctx, tx, run.jobID.Int64); err != nil {
					return err
				}
			}
			if err := c.queue.NotifyTx(ctx, tx, runNotifyKey(run.id)); err != nil {
				return err
			}
		}
	}
}

type runToCancel struct {
	id    string
	jobID sql.NullInt64
}

func unfinishedTreeTx(ctx context.Context, tx *sql.Tx, runID string, includeRoot bool) ([]runToCancel, error) {
	rows, err := tx.QueryContext(ctx, `
WITH RECURSIVE tree AS (
    SELECT id, 0 AS depth FROM pgworkflow_runs WHERE id = $1
    UNION ALL
    SELECT child.id, tree.depth + 1
    FROM pgworkflow_runs child JOIN tree ON child.parent_run_id = tree.id
)
SELECT run.id, run.job_id
FROM pgworkflow_runs run JOIN tree ON tree.id = run.id
WHERE (tree.depth > 0 OR $2)
  AND run.status IN ('pending', 'running', 'waiting')
ORDER BY tree.depth
FOR UPDATE OF run`, runID, includeRoot)
	if err != nil {
		return nil, fmt.Errorf("workflow: lock runs to cancel: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var runs []runToCancel
	for rows.Next() {
		var run runToCancel
		if err := rows.Scan(&run.id, &run.jobID); err != nil {
			return nil, fmt.Errorf("workflow: scan run to cancel: %w", err)
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

// Retry resumes a failed or cancelled run from its checkpoints: completed steps keep
// their results, failed and interrupted steps start over with fresh attempts, and
// failed or cancelled children are retried too. The run's timeout no longer applies.
func (c *Client) Retry(ctx context.Context, runID string) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("workflow: begin retry: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := c.retryTx(ctx, tx, runID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("workflow: commit retry: %w", err)
	}
	return nil
}

func (c *Client) retryTx(ctx context.Context, tx *sql.Tx, runID string) error {
	status, _, err := lockRunTx(ctx, tx, runID)
	if err != nil {
		return err
	}
	if status != RunFailed && status != RunCancelled {
		return fmt.Errorf("%w: %s is %s", ErrRunNotRetryable, runID, status)
	}
	var workflowName string
	var version int
	if err := tx.QueryRowContext(ctx,
		`SELECT workflow, version FROM pgworkflow_runs WHERE id = $1`, runID,
	).Scan(&workflowName, &version); err != nil {
		return fmt.Errorf("workflow: read run to retry: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM pgworkflow_steps
WHERE run_id = $1 AND kind = 'step' AND status IN ('failed', 'running', 'retrying')`, runID); err != nil {
		return fmt.Errorf("workflow: reset failed steps: %w", err)
	}
	children, err := c.childrenToRetryTx(ctx, tx, runID)
	if err != nil {
		return err
	}
	for _, child := range children {
		if err := c.retryTx(ctx, tx, child); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE pgworkflow_steps
SET status = 'waiting', output = NULL, error = NULL, completed_at = NULL, updated_at = NOW()
WHERE run_id = $1 AND kind = 'child' AND status = 'failed'`, runID); err != nil {
		return fmt.Errorf("workflow: reset failed children: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE pgworkflow_runs
SET status = 'pending', error = NULL, timed_out = FALSE, output = NULL, deadline_at = NULL,
    completed_at = NULL, wake_at = NULL, updated_at = NOW()
WHERE id = $1`, runID); err != nil {
		return fmt.Errorf("workflow: reset run: %w", err)
	}
	return c.enqueueRunJobTx(ctx, tx, runID, (&definition{name: workflowName, version: version}).queueName())
}

func (c *Client) childrenToRetryTx(ctx context.Context, tx *sql.Tx, runID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT child.id
FROM pgworkflow_steps step
JOIN pgworkflow_runs child ON child.id = step.child_run_id
WHERE step.run_id = $1 AND step.kind = 'child' AND step.status IN ('waiting', 'failed')
  AND child.status IN ('failed', 'cancelled')
ORDER BY child.id`, runID)
	if err != nil {
		return nil, fmt.Errorf("workflow: find children to retry: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var children []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		children = append(children, id)
	}
	return children, rows.Err()
}

func searchPattern(search string) string {
	escaped := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(strings.TrimSpace(search))
	return "%" + escaped + "%"
}
