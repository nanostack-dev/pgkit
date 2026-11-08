package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ParkedUntil is the availability of a job snoozed with a zero time: it waits
// until WakeTx makes it due.
var ParkedUntil = time.Date(9999, time.December, 31, 0, 0, 0, 0, time.UTC)

// SnoozeTx returns the job a handler holds to scheduled, claimable again at until,
// and refunds the attempt, so a snooze never uses up the job's MaxAttempts. A zero
// until parks the job until WakeTx makes it due; a past until makes it available
// now. Use database time for until, as the claim query compares it with NOW().
// Like Heartbeat it acts only on the claim job describes, and returns
// ErrJobNotFound once that claim is gone.
//
// A cancel request wins over the snooze: a job that Cancel asked to stop becomes
// cancelled instead (EventCancel, not EventRetry), and SnoozeTx returns nil, as the
// claim is settled. Use GetJob or Job.State after commit to tell the two apart.
func (c *Client) SnoozeTx(ctx context.Context, tx *sql.Tx, job Job, until time.Time) error {
	if c == nil || c.db == nil {
		return ErrNilDB
	}
	if tx == nil {
		return fmt.Errorf("pgqueue: tx is nil")
	}
	availableAt := ParkedUntil
	if !until.IsZero() {
		availableAt = until.UTC()
	}
	_, err := c.snooze(ctx, tx, job, `GREATEST($3::timestamptz, NOW())`, availableAt)
	return err
}

// snooze returns the running claim job describes to scheduled, or to available
// when availableAt (an SQL expression of the parameter $3) is not in the future,
// and refunds the attempt. A job with a cancel request becomes cancelled instead.
// It returns the state it left the job in.
func (c *Client) snooze(ctx context.Context, qe queryExecer, job Job, availableAt string, arg any) (JobState, error) {
	var state JobState
	var notified int
	err := qe.QueryRowContext(ctx,
		`WITH snoozed AS (
			UPDATE pgqueue_jobs
			SET state = CASE
			        WHEN cancel_requested_at IS NOT NULL THEN 'cancelled'
			        WHEN `+availableAt+` <= NOW() THEN 'available'
			        ELSE 'scheduled'
			    END,
			    attempts = CASE WHEN cancel_requested_at IS NOT NULL THEN attempts ELSE GREATEST(attempts - 1, 0) END,
			    available_at = CASE WHEN cancel_requested_at IS NOT NULL THEN available_at ELSE `+availableAt+` END,
			    claimed_by = NULL,
			    claimed_at = NULL,
			    done_at = CASE WHEN cancel_requested_at IS NOT NULL THEN NOW() END,
			    updated_at = NOW()
			WHERE id = $1 AND state = 'running' AND claims = $2
			RETURNING queue_name, state
		 ), notified AS (
			SELECT `+notifySQL("snoozed")+` FROM snoozed WHERE snoozed.state = 'available'
		 )
		 SELECT state, (SELECT count(*) FROM notified) FROM snoozed`,
		job.ID, job.Claims, arg,
	).Scan(&state, &notified)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("pgqueue: snooze id=%d: %w", job.ID, ErrJobNotFound)
	}
	if err != nil {
		return "", fmt.Errorf("pgqueue: snooze: %w", err)
	}
	if state == StateCancelled {
		c.emit(ctx, EventCancel, map[string]any{"id": job.ID, "state": state})
	} else {
		c.emit(ctx, EventRetry, map[string]any{"id": job.ID, "snooze": true, "state": state})
	}
	return state, nil
}

// WakeTx makes a scheduled or retryable job that is not yet due available now and
// notifies OnEnqueue workers when tx commits. Jobs that are due, running or
// finalized are left alone.
func (c *Client) WakeTx(ctx context.Context, tx *sql.Tx, id int64) error {
	if c == nil || c.db == nil {
		return ErrNilDB
	}
	if tx == nil {
		return fmt.Errorf("pgqueue: tx is nil")
	}
	rows, err := tx.QueryContext(ctx,
		`WITH woken AS (
			UPDATE pgqueue_jobs
			SET state = 'available', available_at = NOW(), updated_at = NOW()
			WHERE id = $1 AND state IN ('scheduled', 'retryable') AND available_at > NOW()
			RETURNING queue_name
		 )
		 SELECT 1 FROM woken, `+notifySQL("woken"),
		id,
	)
	if err != nil {
		return fmt.Errorf("pgqueue: wake: %w", err)
	}
	if _, err := countRows(rows); err != nil {
		return fmt.Errorf("pgqueue: wake: %w", err)
	}
	return nil
}

// LockClaimTx locks the row of the job a handler holds, as long as that claim is
// still held, so neither the reaper nor another worker can take it until tx ends.
// It returns ErrJobNotFound once the claim is gone.
func (c *Client) LockClaimTx(ctx context.Context, tx *sql.Tx, job Job) error {
	if c == nil || c.db == nil {
		return ErrNilDB
	}
	if tx == nil {
		return fmt.Errorf("pgqueue: tx is nil")
	}
	var held bool
	err := tx.QueryRowContext(ctx,
		`SELECT TRUE FROM pgqueue_jobs WHERE id = $1 AND state = 'running' AND claims = $2 FOR UPDATE`,
		job.ID, job.Claims,
	).Scan(&held)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("pgqueue: lock claim id=%d: %w", job.ID, ErrJobNotFound)
	}
	if err != nil {
		return fmt.Errorf("pgqueue: lock claim: %w", err)
	}
	return nil
}

// Heartbeat extends the claim a handler holds on job, so the reaper leaves it alone
// for another visibility timeout and the rescuer for another lease of the same
// length. It returns ErrJobNotFound once that claim is gone: the job finished, or
// it was taken back and the handler should stop.
func (c *Client) Heartbeat(ctx context.Context, job Job) error {
	if c == nil || c.db == nil {
		return ErrNilDB
	}
	res, err := c.db.ExecContext(ctx,
		`UPDATE pgqueue_jobs
		 SET claimed_at = NOW(),
		     lease_expires_at = NOW() + (lease_expires_at - claimed_at),
		     updated_at = NOW()
		 WHERE id = $1 AND state = 'running' AND claims = $2`,
		job.ID, job.Claims,
	)
	if err != nil {
		return fmt.Errorf("pgqueue: heartbeat: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("pgqueue: heartbeat id=%d: %w", job.ID, ErrJobNotFound)
	}
	return nil
}

func countRows(rows *sql.Rows) (int, error) {
	defer func() { _ = rows.Close() }()
	count := 0
	for rows.Next() {
		count++
	}
	return count, rows.Err()
}
