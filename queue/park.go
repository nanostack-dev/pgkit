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

// SnoozeTx returns the job a handler holds to pending, claimable again at until,
// and refunds the attempt, so a snooze never uses up the job's MaxAttempts. A zero
// until parks the job until WakeTx makes it due; a past until makes it due now. Use
// database time for until, as the claim query compares it with NOW(). Like
// Heartbeat it acts only on the claim job describes, and returns ErrJobNotFound
// once that claim is gone.
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
	var snoozed, notified int
	err := tx.QueryRowContext(ctx,
		`WITH snoozed AS (
			UPDATE pgqueue_jobs
			SET status = 'pending',
			    attempts = GREATEST(attempts - 1, 0),
			    available_at = GREATEST($2::timestamptz, NOW()),
			    claimed_by = NULL,
			    claimed_at = NULL,
			    updated_at = NOW()
			WHERE id = $1 AND status = 'processing' AND claims = $3
			RETURNING queue_name, available_at <= NOW() AS due
		 ), notified AS (
			SELECT `+notifySQL("snoozed")+` FROM snoozed WHERE snoozed.due
		 )
		 SELECT (SELECT count(*) FROM snoozed), (SELECT count(*) FROM notified)`,
		job.ID, availableAt, job.Claims,
	).Scan(&snoozed, &notified)
	if err != nil {
		return fmt.Errorf("pgqueue: snooze: %w", err)
	}
	if snoozed == 0 {
		return fmt.Errorf("pgqueue: snooze id=%d: %w", job.ID, ErrJobNotFound)
	}
	c.emit(ctx, EventRetry, map[string]any{"id": job.ID, "snooze": true})
	return nil
}

// WakeTx makes a pending job that is not yet due claimable now and notifies OnEnqueue
// workers when tx commits. Jobs that are due, processing or finished are left alone.
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
			SET available_at = NOW(), updated_at = NOW()
			WHERE id = $1 AND status = 'pending' AND available_at > NOW()
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
		`SELECT TRUE FROM pgqueue_jobs WHERE id = $1 AND status = 'processing' AND claims = $2 FOR UPDATE`,
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
// for another visibility timeout. It returns ErrJobNotFound once that claim is gone:
// the job finished, or the reaper requeued it and the handler should stop.
func (c *Client) Heartbeat(ctx context.Context, job Job) error {
	if c == nil || c.db == nil {
		return ErrNilDB
	}
	res, err := c.db.ExecContext(ctx,
		`UPDATE pgqueue_jobs
		 SET claimed_at = NOW(), updated_at = NOW()
		 WHERE id = $1 AND status = 'processing' AND claims = $2`,
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
