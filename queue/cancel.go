package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

var ErrInvalidSnooze = errors.New("pgqueue: snooze duration must not be negative")

// cancelKeyPrefix prefixes the notification key that tells the workers of a queue
// that one of its running jobs was asked to stop.
const cancelKeyPrefix = "pgqueue.cancel:"

// Cancel stops a job. An available, scheduled or retryable job becomes cancelled at
// once. A running job gets a cancel request: a Worker that picks up jobs with
// OnEnqueue cancels its handler's context, and its outcome is recorded as
// cancelled unless the handler succeeds. Cancel returns the job's state after the
// call, and ErrJobNotFound for a missing or finalized job.
func (c *Client) Cancel(ctx context.Context, id int64) (JobState, error) {
	if c == nil || c.db == nil {
		return "", ErrNilDB
	}
	return c.cancel(ctx, c.db, id)
}

// CancelTx is Cancel within tx; workers are notified when tx commits.
func (c *Client) CancelTx(ctx context.Context, tx *sql.Tx, id int64) (JobState, error) {
	if c == nil || c.db == nil {
		return "", ErrNilDB
	}
	if tx == nil {
		return "", fmt.Errorf("pgqueue: tx is nil")
	}
	return c.cancel(ctx, tx, id)
}

func (c *Client) cancel(ctx context.Context, qe queryExecer, id int64) (JobState, error) {
	var state JobState
	var notified int
	err := qe.QueryRowContext(ctx,
		`WITH cancelled AS (
			UPDATE pgqueue_jobs
			SET state = CASE WHEN state = 'running' THEN state ELSE 'cancelled' END,
			    cancel_requested_at = COALESCE(cancel_requested_at, NOW()),
			    done_at = CASE WHEN state = 'running' THEN done_at ELSE NOW() END,
			    updated_at = NOW()
			WHERE id = $1 AND state IN ('available', 'scheduled', 'retryable', 'running')
			RETURNING queue_name, state
		 ), notified AS (
			SELECT pg_notify('`+NotifyChannel+`', left('`+cancelKeyPrefix+`' || queue_name, `+fmt.Sprint(notifyKeyLength)+`))
			FROM cancelled WHERE state = 'running'
		 )
		 SELECT state, (SELECT count(*) FROM notified) FROM cancelled`,
		id,
	).Scan(&state, &notified)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("pgqueue: cancel id=%d: %w", id, ErrJobNotFound)
	}
	if err != nil {
		return "", fmt.Errorf("pgqueue: cancel: %w", err)
	}
	c.emit(ctx, EventCancel, map[string]any{"id": id, "state": state})
	return state, nil
}

// cancelRequested reports whether the claim job describes was asked to stop.
func (c *Client) cancelRequested(ctx context.Context, job Job) (bool, error) {
	var requested bool
	err := c.db.QueryRowContext(ctx,
		`SELECT cancel_requested_at IS NOT NULL FROM pgqueue_jobs WHERE id = $1 AND claims = $2`,
		job.ID, job.Claims,
	).Scan(&requested)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return requested, err
}

// cancelJob settles the running claim job describes as cancelled.
func (c *Client) cancelJob(ctx context.Context, job Job, cause error) error {
	var errMsg *string
	if cause != nil {
		s := cause.Error()
		errMsg = &s
	}
	res, err := c.db.ExecContext(ctx,
		`UPDATE pgqueue_jobs
		 SET state = 'cancelled',
		     cancel_requested_at = COALESCE(cancel_requested_at, NOW()),
		     last_error = COALESCE($3, last_error),
		     claimed_by = NULL,
		     claimed_at = NULL,
		     done_at = NOW(),
		     updated_at = NOW()
		 WHERE id = $1 AND state = 'running' AND claims = $2`,
		job.ID, job.Claims, errMsg,
	)
	if err != nil {
		return fmt.Errorf("pgqueue: cancel: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("pgqueue: cancel id=%d: %w", job.ID, ErrJobNotFound)
	}
	c.emit(ctx, EventCancel, map[string]any{"id": job.ID, "state": StateCancelled})
	return nil
}

// snoozeFor returns the running claim job describes to scheduled for d of database
// time, like SnoozeTx; a cancel request wins over the snooze. It returns the state
// it left the job in.
func (c *Client) snoozeFor(ctx context.Context, job Job, d time.Duration) (JobState, error) {
	return c.snooze(ctx, c.db, job, `NOW() + make_interval(secs => $3::float8)`, d.Seconds())
}

// Cancel returned by a handler cancels its job instead of retrying it: the job
// becomes cancelled with err as its last error, and OnJobFailed is not called.
func Cancel(err error) error {
	return cancelError{err: err}
}

// IsCancel reports whether err asks the worker to cancel the job.
func IsCancel(err error) bool {
	var target cancelError
	return errors.As(err, &target)
}

type cancelError struct{ err error }

func (e cancelError) Error() string {
	if e.err == nil {
		return "pgqueue: job cancelled"
	}
	return e.err.Error()
}

func (e cancelError) Unwrap() error { return e.err }

// Snooze returned by a handler makes its job claimable again after d without
// counting the attempt, like SnoozeTx. A negative d is a handler bug: the job is
// retried with ErrInvalidSnooze.
func Snooze(d time.Duration) error {
	return snoozeError{after: d}
}

// IsSnooze reports whether err asks the worker to snooze the job.
func IsSnooze(err error) bool {
	var target snoozeError
	return errors.As(err, &target)
}

type snoozeError struct{ after time.Duration }

func (e snoozeError) Error() string {
	return fmt.Sprintf("pgqueue: job snoozed for %s", e.after)
}
