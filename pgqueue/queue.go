package pgqueue

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

var (
	ErrNilDB         = errors.New("pgqueue: db is nil")
	ErrInvalidQueue  = errors.New("pgqueue: queue name is required")
	ErrInvalidLimit  = errors.New("pgqueue: limit must be positive")
	ErrJobNotFound   = errors.New("pgqueue: job not found or not in expected state")
	ErrInvalidOffset = errors.New("pgqueue: offset must be non-negative")
	ErrSearchTooLong = errors.New("pgqueue: search query is too long")
	ErrJobBusy       = errors.New("pgqueue: job is currently processing")
)

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

// JobStatus represents the lifecycle state of a job.
type JobStatus string

const (
	StatusPending    JobStatus = "pending"
	StatusProcessing JobStatus = "processing"
	StatusDone       JobStatus = "done"
	StatusFailed     JobStatus = "failed"
)

const defaultMaxAttempts = 5

const maxSearchLength = 256

// schemaVersion is the current version of the pgqueue schema.
// Bump this when adding migrations.
const schemaVersion = 1

// Job represents a single queue job row.
type Job struct {
	ID          int64
	QueueName   string
	Payload     []byte
	Status      JobStatus
	Attempts    int
	MaxAttempts int
	AvailableAt time.Time
	ClaimedBy   sql.NullString
	ClaimedAt   sql.NullTime
	DoneAt      sql.NullTime
	LastError   sql.NullString
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// ---------------------------------------------------------------------------
// Params structs (P1 #6)
// ---------------------------------------------------------------------------

// EnqueueParams holds parameters for enqueuing a job.
type EnqueueParams struct {
	QueueName   string
	Payload     []byte
	AvailableAt *time.Time // nil = immediate (DB NOW())
	MaxAttempts int        // <= 0 uses defaultMaxAttempts
}

// ListJobsParams holds parameters for listing jobs.
type ListJobsParams struct {
	Limit     int       // required, must be > 0
	QueueName string    // optional filter
	Status    JobStatus // optional filter
	Offset    int       // optional pagination offset
	Search    string    // optional full-text filter over queue_name/payload/last_error
}

// PurgeParams holds parameters for purging old jobs.
type PurgeParams struct {
	Status    JobStatus     // required: which status to purge
	OlderThan time.Duration // required: minimum age
	Limit     int           // optional: max rows to delete per call (0 = no limit)
}

// ReapResult holds counts from a reap operation.
type ReapResult struct {
	Requeued int64 // jobs moved back to pending
	Failed   int64 // jobs moved to failed (attempts exhausted)
}

// ---------------------------------------------------------------------------
// Hooks / Metrics interface (P2 #12)
// ---------------------------------------------------------------------------

// EventKind identifies the type of queue event.
type EventKind string

const (
	EventEnqueue EventKind = "enqueue"
	EventClaim   EventKind = "claim"
	EventAck     EventKind = "ack"
	EventRetry   EventKind = "retry"
	EventFail    EventKind = "fail"
	EventReap    EventKind = "reap"
	EventPurge   EventKind = "purge"
)

// Hook is called after each queue operation completes.
// Implementations must be safe for concurrent use.
type Hook interface {
	OnEvent(ctx context.Context, kind EventKind, meta map[string]any)
}

// HookFunc adapts a plain function to the Hook interface.
type HookFunc func(ctx context.Context, kind EventKind, meta map[string]any)

func (f HookFunc) OnEvent(ctx context.Context, kind EventKind, meta map[string]any) {
	f(ctx, kind, meta)
}

// Logger is a minimal logging adapter for integrating with custom logging tools.
type Logger interface {
	Debug(ctx context.Context, msg string, fields map[string]any)
	Info(ctx context.Context, msg string, fields map[string]any)
	Warn(ctx context.Context, msg string, fields map[string]any)
	Error(ctx context.Context, msg string, fields map[string]any)
}

type noopLogger struct{}

func (noopLogger) Debug(context.Context, string, map[string]any) {}
func (noopLogger) Info(context.Context, string, map[string]any)  {}
func (noopLogger) Warn(context.Context, string, map[string]any)  {}
func (noopLogger) Error(context.Context, string, map[string]any) {}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

// Client is the main pgqueue client.
type Client struct {
	db    *sql.DB
	hooks []Hook
	log   Logger
}

// New creates a new pgqueue Client.
func New(db *sql.DB, hooks ...Hook) (*Client, error) {
	if db == nil {
		return nil, ErrNilDB
	}
	return &Client{db: db, hooks: hooks, log: noopLogger{}}, nil
}

// DB returns the underlying *sql.DB for advanced use (e.g. EnqueueTx).
func (c *Client) DB() *sql.DB { return c.db }

// SetLogger sets a custom logger adapter for client and dashboard events.
func (c *Client) SetLogger(logger Logger) {
	if c == nil {
		return
	}
	if logger == nil {
		c.log = noopLogger{}
		return
	}
	c.log = logger
}

func (c *Client) logDebug(ctx context.Context, msg string, fields map[string]any) {
	if c != nil && c.log != nil {
		c.log.Debug(ctx, msg, fields)
	}
}

func (c *Client) logInfo(ctx context.Context, msg string, fields map[string]any) {
	if c != nil && c.log != nil {
		c.log.Info(ctx, msg, fields)
	}
}

func (c *Client) logWarn(ctx context.Context, msg string, fields map[string]any) {
	if c != nil && c.log != nil {
		c.log.Warn(ctx, msg, fields)
	}
}

func (c *Client) logError(ctx context.Context, msg string, fields map[string]any) {
	if c != nil && c.log != nil {
		c.log.Error(ctx, msg, fields)
	}
}

func (c *Client) emit(ctx context.Context, kind EventKind, meta map[string]any) {
	for _, h := range c.hooks {
		h.OnEvent(ctx, kind, meta)
	}
	c.logDebug(ctx, "pgqueue event", map[string]any{"event": kind, "meta": meta})
}

// ---------------------------------------------------------------------------
// Schema management (P2 #10)
// ---------------------------------------------------------------------------

// EnsureSchema creates the pgqueue tables and applies incremental migrations.
func (c *Client) EnsureSchema(ctx context.Context) error {
	if c == nil || c.db == nil {
		return ErrNilDB
	}

	// Create meta table for version tracking.
	if _, err := c.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS pgqueue_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
)`); err != nil {
		return fmt.Errorf("pgqueue: create meta table: %w", err)
	}

	// Read current version.
	var currentVersion int
	err := c.db.QueryRowContext(ctx,
		`SELECT value::int FROM pgqueue_meta WHERE key = 'schema_version'`,
	).Scan(&currentVersion)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("pgqueue: read schema version: %w", err)
	}

	// Apply migrations in order.
	if currentVersion < 1 {
		if err := c.migrateV1(ctx); err != nil {
			return err
		}
	}

	// Upsert version.
	if _, err := c.db.ExecContext(ctx, `
INSERT INTO pgqueue_meta (key, value) VALUES ('schema_version', $1)
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
		fmt.Sprintf("%d", schemaVersion),
	); err != nil {
		return fmt.Errorf("pgqueue: upsert schema version: %w", err)
	}

	return nil
}

func (c *Client) migrateV1(ctx context.Context) error {
	_, err := c.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS pgqueue_jobs (
    id BIGSERIAL PRIMARY KEY,
    queue_name TEXT NOT NULL,
    payload BYTEA NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_by TEXT,
    claimed_at TIMESTAMPTZ,
    done_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_pgqueue_jobs_status CHECK (status IN ('pending','processing','done','failed'))
);

CREATE INDEX IF NOT EXISTS idx_pgqueue_pending_due
ON pgqueue_jobs(queue_name, available_at, id)
WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_pgqueue_processing
ON pgqueue_jobs(queue_name, claimed_at)
WHERE status = 'processing';
`)
	if err != nil {
		return fmt.Errorf("pgqueue: migrate v1: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Enqueue (P1 #6 params struct, P0 #4 DB NOW())
// ---------------------------------------------------------------------------

// Enqueue adds a job to the queue using a params struct.
func (c *Client) Enqueue(ctx context.Context, p EnqueueParams) (int64, error) {
	if c == nil || c.db == nil {
		return 0, ErrNilDB
	}
	return enqueueCore(ctx, c.db, p, c, nil)
}

// EnqueueTx adds a job within an existing transaction (P1 #8).
func (c *Client) EnqueueTx(ctx context.Context, tx *sql.Tx, p EnqueueParams) (int64, error) {
	if c == nil || c.db == nil {
		return 0, ErrNilDB
	}
	if tx == nil {
		return 0, fmt.Errorf("pgqueue: tx is nil")
	}
	return enqueueCore(ctx, tx, p, c, nil)
}

// queryExecer abstracts *sql.DB and *sql.Tx for shared query logic.
type queryExecer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func enqueueCore(ctx context.Context, qe queryExecer, p EnqueueParams, c *Client, _ any) (int64, error) {
	if p.QueueName == "" {
		return 0, ErrInvalidQueue
	}
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = defaultMaxAttempts
	}

	var id int64
	var err error

	if p.AvailableAt == nil {
		// P0 #4: Use DB NOW() for immediate jobs to avoid clock skew.
		err = qe.QueryRowContext(ctx,
			`INSERT INTO pgqueue_jobs(queue_name, payload, status, attempts, max_attempts, available_at)
			 VALUES ($1, $2, 'pending', 0, $3, NOW())
			 RETURNING id`,
			p.QueueName, p.Payload, p.MaxAttempts,
		).Scan(&id)
	} else {
		err = qe.QueryRowContext(ctx,
			`INSERT INTO pgqueue_jobs(queue_name, payload, status, attempts, max_attempts, available_at)
			 VALUES ($1, $2, 'pending', 0, $3, $4)
			 RETURNING id`,
			p.QueueName, p.Payload, p.MaxAttempts, p.AvailableAt.UTC(),
		).Scan(&id)
	}
	if err != nil {
		return 0, fmt.Errorf("pgqueue: enqueue: %w", err)
	}

	if c != nil {
		c.emit(ctx, EventEnqueue, map[string]any{"id": id, "queue": p.QueueName})
	}
	return id, nil
}

// ---------------------------------------------------------------------------
// Claim (P1 #7: bool return for empty queue)
// ---------------------------------------------------------------------------

// Claim atomically claims the next available job.
// Returns (job, true, nil) on success, (nil, false, nil) when no job is available.
func (c *Client) Claim(ctx context.Context, queueName, worker string) (job *Job, found bool, err error) {
	if c == nil || c.db == nil {
		return nil, false, ErrNilDB
	}
	if queueName == "" {
		return nil, false, ErrInvalidQueue
	}

	tx, txErr := c.db.BeginTx(ctx, nil)
	if txErr != nil {
		return nil, false, fmt.Errorf("pgqueue: begin claim tx: %w", txErr)
	}
	defer func() { _ = tx.Rollback() }()

	row := tx.QueryRowContext(ctx,
		`WITH next_job AS (
			SELECT id
			FROM pgqueue_jobs
			WHERE queue_name = $1
			  AND status = 'pending'
			  AND available_at <= NOW()
			  AND attempts < max_attempts
			ORDER BY available_at ASC, id ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		 )
		 UPDATE pgqueue_jobs j
		 SET status = 'processing',
		     attempts = attempts + 1,
		     claimed_by = $2,
		     claimed_at = NOW(),
		     updated_at = NOW()
		 FROM next_job
		 WHERE j.id = next_job.id
		 RETURNING j.id, j.queue_name, j.payload, j.status, j.attempts, j.max_attempts, j.available_at,
		           j.claimed_by, j.claimed_at, j.done_at, j.last_error, j.created_at, j.updated_at`,
		queueName, worker,
	)

	j := &Job{}
	scanErr := row.Scan(
		&j.ID, &j.QueueName, &j.Payload, &j.Status, &j.Attempts, &j.MaxAttempts,
		&j.AvailableAt, &j.ClaimedBy, &j.ClaimedAt, &j.DoneAt, &j.LastError,
		&j.CreatedAt, &j.UpdatedAt,
	)
	if errors.Is(scanErr, sql.ErrNoRows) {
		if commitErr := tx.Commit(); commitErr != nil {
			return nil, false, fmt.Errorf("pgqueue: commit empty claim tx: %w", commitErr)
		}
		return nil, false, nil
	}
	if scanErr != nil {
		return nil, false, fmt.Errorf("pgqueue: claim: %w", scanErr)
	}

	if commitErr := tx.Commit(); commitErr != nil {
		return nil, false, fmt.Errorf("pgqueue: commit claim tx: %w", commitErr)
	}

	c.emit(ctx, EventClaim, map[string]any{"id": j.ID, "queue": queueName, "worker": worker})
	return j, true, nil
}

// ---------------------------------------------------------------------------
// Ack (P0 #2: strict state guard + rows affected)
// ---------------------------------------------------------------------------

// Ack marks a processing job as done. Returns ErrJobNotFound if the job is not
// currently in processing state.
func (c *Client) Ack(ctx context.Context, id int64) error {
	if c == nil || c.db == nil {
		return ErrNilDB
	}

	res, err := c.db.ExecContext(ctx,
		`UPDATE pgqueue_jobs
		 SET status = 'done', done_at = NOW(), claimed_by = NULL, claimed_at = NULL, updated_at = NOW()
		 WHERE id = $1 AND status = 'processing'`,
		id,
	)
	if err != nil {
		return fmt.Errorf("pgqueue: ack: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("pgqueue: ack id=%d: %w", id, ErrJobNotFound)
	}

	c.emit(ctx, EventAck, map[string]any{"id": id})
	return nil
}

// ---------------------------------------------------------------------------
// Retry (P0 #2 + #3: state guard, attempts check, zombie prevention)
// ---------------------------------------------------------------------------

// Retry moves a processing job back to pending with a delay.
// If the job has exhausted its attempts, it is moved to failed instead (P0 #3).
// Returns ErrJobNotFound if the job is not currently processing.
func (c *Client) Retry(ctx context.Context, id int64, delay time.Duration, cause error) error {
	if c == nil || c.db == nil {
		return ErrNilDB
	}

	var errMsg *string
	if cause != nil {
		s := cause.Error()
		errMsg = &s
	}

	// Use a single atomic UPDATE that checks state and decides outcome.
	// If attempts >= max_attempts, move to failed (zombie prevention).
	// We use DB NOW() + interval for the delay to maintain clock consistency.
	res, err := c.db.ExecContext(ctx,
		`UPDATE pgqueue_jobs
		 SET status = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'pending' END,
		     available_at = CASE WHEN attempts >= max_attempts THEN available_at ELSE NOW() + $2::interval END,
		     last_error = COALESCE($3, last_error),
		     claimed_by = NULL,
		     claimed_at = NULL,
		     done_at = CASE WHEN attempts >= max_attempts THEN NOW() ELSE NULL END,
		     updated_at = NOW()
		 WHERE id = $1 AND status = 'processing'`,
		id,
		fmt.Sprintf("%d seconds", int(delay.Seconds())),
		errMsg,
	)
	if err != nil {
		return fmt.Errorf("pgqueue: retry: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("pgqueue: retry id=%d: %w", id, ErrJobNotFound)
	}

	c.emit(ctx, EventRetry, map[string]any{"id": id})
	return nil
}

// ---------------------------------------------------------------------------
// Fail (P0 #2: strict state guard + rows affected)
// ---------------------------------------------------------------------------

// Fail marks a processing job as permanently failed.
// Returns ErrJobNotFound if the job is not currently processing.
func (c *Client) Fail(ctx context.Context, id int64, cause error) error {
	if c == nil || c.db == nil {
		return ErrNilDB
	}

	var errMsg *string
	if cause != nil {
		s := cause.Error()
		errMsg = &s
	}

	res, err := c.db.ExecContext(ctx,
		`UPDATE pgqueue_jobs
		 SET status = 'failed',
		     last_error = $2,
		     done_at = NOW(),
		     updated_at = NOW()
		 WHERE id = $1 AND status = 'processing'`,
		id, errMsg,
	)
	if err != nil {
		return fmt.Errorf("pgqueue: fail: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("pgqueue: fail id=%d: %w", id, ErrJobNotFound)
	}

	c.emit(ctx, EventFail, map[string]any{"id": id})
	return nil
}

// ---------------------------------------------------------------------------
// ReapStuckJobs (P0 #1)
// ---------------------------------------------------------------------------

// ReapStuckJobs reclaims jobs stuck in processing beyond visibilityTimeout.
// Jobs with remaining attempts are requeued to pending; exhausted jobs are failed.
func (c *Client) ReapStuckJobs(ctx context.Context, visibilityTimeout time.Duration) (ReapResult, error) {
	if c == nil || c.db == nil {
		return ReapResult{}, ErrNilDB
	}

	// Requeue: processing + stale + attempts < max_attempts -> pending
	resRequeue, err := c.db.ExecContext(ctx,
		`UPDATE pgqueue_jobs
		 SET status = 'pending',
		     claimed_by = NULL,
		     claimed_at = NULL,
		     available_at = NOW(),
		     updated_at = NOW()
		 WHERE status = 'processing'
		   AND claimed_at < NOW() - $1::interval
		   AND attempts < max_attempts`,
		fmt.Sprintf("%d seconds", int(visibilityTimeout.Seconds())),
	)
	if err != nil {
		return ReapResult{}, fmt.Errorf("pgqueue: reap requeue: %w", err)
	}
	requeued, _ := resRequeue.RowsAffected()

	// Fail: processing + stale + attempts >= max_attempts -> failed
	resFail, err := c.db.ExecContext(ctx,
		`UPDATE pgqueue_jobs
		 SET status = 'failed',
		     last_error = COALESCE(last_error, 'reaped: visibility timeout exceeded'),
		     claimed_by = NULL,
		     claimed_at = NULL,
		     done_at = NOW(),
		     updated_at = NOW()
		 WHERE status = 'processing'
		   AND claimed_at < NOW() - $1::interval
		   AND attempts >= max_attempts`,
		fmt.Sprintf("%d seconds", int(visibilityTimeout.Seconds())),
	)
	if err != nil {
		return ReapResult{Requeued: requeued}, fmt.Errorf("pgqueue: reap fail: %w", err)
	}
	failed, _ := resFail.RowsAffected()

	result := ReapResult{Requeued: requeued, Failed: failed}
	c.emit(ctx, EventReap, map[string]any{"requeued": requeued, "failed": failed})
	return result, nil
}

// ---------------------------------------------------------------------------
// ListJobs (P1 #6: params struct)
// ---------------------------------------------------------------------------

// ListJobs returns jobs matching the given filters.
func (c *Client) ListJobs(ctx context.Context, p ListJobsParams) ([]Job, error) {
	if c == nil || c.db == nil {
		return nil, ErrNilDB
	}
	if p.Limit <= 0 {
		return nil, ErrInvalidLimit
	}
	if p.Offset < 0 {
		return nil, ErrInvalidOffset
	}
	p.Search = strings.TrimSpace(p.Search)
	if len(p.Search) > maxSearchLength {
		return nil, ErrSearchTooLong
	}

	// Build query dynamically based on optional filters.
	query := `SELECT id, queue_name, payload, status, attempts, max_attempts, available_at,
	                 claimed_by, claimed_at, done_at, last_error, created_at, updated_at
	          FROM pgqueue_jobs WHERE 1=1`
	args := make([]any, 0, 4)
	argIdx := 1

	if p.QueueName != "" {
		query += fmt.Sprintf(" AND queue_name = $%d", argIdx)
		args = append(args, p.QueueName)
		argIdx++
	}
	if p.Status != "" {
		query += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, string(p.Status))
		argIdx++
	}
	if p.Search != "" {
		// Security: search term is bound as a parameter. Never interpolate user input into SQL text.
		query += fmt.Sprintf(
			" AND (queue_name ILIKE $%d OR encode(payload, 'escape') ILIKE $%d OR COALESCE(last_error, '') ILIKE $%d)",
			argIdx, argIdx, argIdx,
		)
		args = append(args, "%"+p.Search+"%")
		argIdx++
	}

	query += " ORDER BY id DESC"

	query += fmt.Sprintf(" LIMIT $%d", argIdx)
	args = append(args, p.Limit)
	argIdx++

	if p.Offset > 0 {
		query += fmt.Sprintf(" OFFSET $%d", argIdx)
		args = append(args, p.Offset)
	}

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("pgqueue: list jobs: %w", err)
	}
	defer rows.Close()

	jobs := make([]Job, 0, p.Limit)
	for rows.Next() {
		var j Job
		if err := rows.Scan(
			&j.ID, &j.QueueName, &j.Payload, &j.Status, &j.Attempts, &j.MaxAttempts,
			&j.AvailableAt, &j.ClaimedBy, &j.ClaimedAt, &j.DoneAt, &j.LastError,
			&j.CreatedAt, &j.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("pgqueue: scan list jobs: %w", err)
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgqueue: iterate list jobs: %w", err)
	}

	return jobs, nil
}

// CountJobs returns the number of jobs matching optional filters.
func (c *Client) CountJobs(ctx context.Context, p ListJobsParams) (int64, error) {
	if c == nil || c.db == nil {
		return 0, ErrNilDB
	}
	if p.Offset < 0 {
		return 0, ErrInvalidOffset
	}
	p.Search = strings.TrimSpace(p.Search)
	if len(p.Search) > maxSearchLength {
		return 0, ErrSearchTooLong
	}

	query := `SELECT COUNT(*) FROM pgqueue_jobs WHERE 1=1`
	args := make([]any, 0, 4)
	argIdx := 1

	if p.QueueName != "" {
		query += fmt.Sprintf(" AND queue_name = $%d", argIdx)
		args = append(args, p.QueueName)
		argIdx++
	}
	if p.Status != "" {
		query += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, string(p.Status))
		argIdx++
	}
	if p.Search != "" {
		query += fmt.Sprintf(
			" AND (queue_name ILIKE $%d OR encode(payload, 'escape') ILIKE $%d OR COALESCE(last_error, '') ILIKE $%d)",
			argIdx, argIdx, argIdx,
		)
		args = append(args, "%"+p.Search+"%")
	}

	var total int64
	if err := c.db.QueryRowContext(ctx, query, args...).Scan(&total); err != nil {
		return 0, fmt.Errorf("pgqueue: count jobs: %w", err)
	}
	return total, nil
}

// GetJob returns a single job by ID.
func (c *Client) GetJob(ctx context.Context, id int64) (*Job, error) {
	if c == nil || c.db == nil {
		return nil, ErrNilDB
	}

	row := c.db.QueryRowContext(ctx,
		`SELECT id, queue_name, payload, status, attempts, max_attempts, available_at,
                claimed_by, claimed_at, done_at, last_error, created_at, updated_at
         FROM pgqueue_jobs WHERE id = $1`,
		id,
	)

	var j Job
	if err := row.Scan(
		&j.ID, &j.QueueName, &j.Payload, &j.Status, &j.Attempts, &j.MaxAttempts,
		&j.AvailableAt, &j.ClaimedBy, &j.ClaimedAt, &j.DoneAt, &j.LastError,
		&j.CreatedAt, &j.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrJobNotFound
		}
		return nil, fmt.Errorf("pgqueue: get job: %w", err)
	}

	return &j, nil
}

// ReplayJob resets a done/failed job to pending and clears runtime state.
func (c *Client) ReplayJob(ctx context.Context, id int64) error {
	if c == nil || c.db == nil {
		return ErrNilDB
	}

	res, err := c.db.ExecContext(ctx,
		`UPDATE pgqueue_jobs
         SET status = 'pending',
             attempts = 0,
             available_at = NOW(),
             claimed_by = NULL,
             claimed_at = NULL,
             done_at = NULL,
             last_error = NULL,
             updated_at = NOW()
         WHERE id = $1
           AND status IN ('done', 'failed')`,
		id,
	)
	if err != nil {
		return fmt.Errorf("pgqueue: replay: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("pgqueue: replay id=%d: %w", id, ErrJobNotFound)
	}

	c.emit(ctx, EventRetry, map[string]any{"id": id, "replay": true})
	return nil
}

// DeleteJob deletes a job that is not currently processing.
func (c *Client) DeleteJob(ctx context.Context, id int64) error {
	if c == nil || c.db == nil {
		return ErrNilDB
	}

	res, err := c.db.ExecContext(ctx,
		`DELETE FROM pgqueue_jobs WHERE id = $1 AND status <> 'processing'`,
		id,
	)
	if err != nil {
		return fmt.Errorf("pgqueue: delete job: %w", err)
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		c.emit(ctx, EventPurge, map[string]any{"id": id, "delete": true})
		return nil
	}

	// Distinguish not-found from busy.
	var status string
	err = c.db.QueryRowContext(ctx, `SELECT status FROM pgqueue_jobs WHERE id = $1`, id).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("pgqueue: delete job id=%d: %w", id, ErrJobNotFound)
	}
	if err != nil {
		return fmt.Errorf("pgqueue: delete job status check: %w", err)
	}
	if status == string(StatusProcessing) {
		return fmt.Errorf("pgqueue: delete job id=%d: %w", id, ErrJobBusy)
	}

	return fmt.Errorf("pgqueue: delete job id=%d: %w", id, ErrJobNotFound)
}

// ---------------------------------------------------------------------------
// Purge (P2 #13)
// ---------------------------------------------------------------------------

// Purge deletes old jobs matching the given status and age.
// Returns the number of deleted rows.
func (c *Client) Purge(ctx context.Context, p PurgeParams) (int64, error) {
	if c == nil || c.db == nil {
		return 0, ErrNilDB
	}
	if p.Status == "" {
		return 0, fmt.Errorf("pgqueue: purge: status is required")
	}

	query := `DELETE FROM pgqueue_jobs
	          WHERE status = $1
	            AND updated_at < NOW() - $2::interval`
	args := []any{string(p.Status), fmt.Sprintf("%d seconds", int(p.OlderThan.Seconds()))}

	if p.Limit > 0 {
		query = fmt.Sprintf(
			`DELETE FROM pgqueue_jobs WHERE id IN (
				SELECT id FROM pgqueue_jobs
				WHERE status = $1 AND updated_at < NOW() - $2::interval
				ORDER BY id ASC LIMIT $3
			)`, // subquery for LIMIT on DELETE
		)
		args = append(args, p.Limit)
	}

	res, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("pgqueue: purge: %w", err)
	}
	n, _ := res.RowsAffected()

	c.emit(ctx, EventPurge, map[string]any{"status": p.Status, "deleted": n})
	return n, nil
}

// ---------------------------------------------------------------------------
// Backoff helpers (P2 #11)
// ---------------------------------------------------------------------------

// ExponentialBackoff returns a delay using exponential backoff with full jitter.
// base is the initial delay, attempt is 1-indexed.
// Formula: random(0, min(cap, base * 2^(attempt-1)))
func ExponentialBackoff(base time.Duration, attempt int, cap time.Duration) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}
	exp := math.Pow(2, float64(attempt-1))
	maxDelay := time.Duration(float64(base) * exp)
	if maxDelay > cap || maxDelay <= 0 {
		maxDelay = cap
	}
	// Full jitter: uniform [0, maxDelay)
	if maxDelay <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(maxDelay)))
}

// ---------------------------------------------------------------------------
// Constant-time token compare helper (P1 #9)
// ---------------------------------------------------------------------------

// secureTokenEqual performs a constant-time comparison of two token strings.
func secureTokenEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
