package queue

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// SnoozeTx returns a processing job to pending, claimable again at until, without
// spending the attempt its claim used. A zero until parks the job until WakeTx makes
// it due; a past until makes it due now. Use database time for until, as the claim
// query compares it with NOW().
func (c *Client) SnoozeTx(ctx context.Context, tx *sql.Tx, id int64, until time.Time) error {
	if c == nil || c.db == nil {
		return ErrNilDB
	}
	if tx == nil {
		return fmt.Errorf("pgqueue: tx is nil")
	}
	var availableAt any
	if !until.IsZero() {
		availableAt = until.UTC()
	}
	var snoozed, notified int
	err := tx.QueryRowContext(ctx,
		`WITH snoozed AS (
			UPDATE pgqueue_jobs
			SET status = 'pending',
			    attempts = GREATEST(attempts - 1, 0),
			    available_at = GREATEST(COALESCE($2::timestamptz, 'infinity'), NOW()),
			    claimed_by = NULL,
			    claimed_at = NULL,
			    updated_at = NOW()
			WHERE id = $1 AND status = 'processing'
			RETURNING queue_name, available_at <= NOW() AS due
		 ), notified AS (
			SELECT `+notifySQL("snoozed")+` FROM snoozed WHERE snoozed.due
		 )
		 SELECT (SELECT count(*) FROM snoozed), (SELECT count(*) FROM notified)`,
		id, availableAt,
	).Scan(&snoozed, &notified)
	if err != nil {
		return fmt.Errorf("pgqueue: snooze: %w", err)
	}
	if snoozed == 0 {
		return fmt.Errorf("pgqueue: snooze id=%d: %w", id, ErrJobNotFound)
	}
	c.emit(ctx, EventRetry, map[string]any{"id": id, "snooze": true})
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
		 WHERE id = $1 AND status = 'processing' AND attempts = $2`,
		job.ID, job.Attempts,
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
