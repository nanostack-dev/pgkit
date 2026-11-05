package queue

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrInvalidRetentionPolicy = errors.New("pgqueue: invalid retention policy")

const (
	defaultRetentionInterval  = time.Hour
	defaultRetentionBatchSize = 1000
)

// RetentionPolicy sets how long finished jobs of every queue stay in
// pgqueue_jobs, measured from their last update. A zero KeepDone or KeepFailed
// keeps jobs of that status forever.
type RetentionPolicy struct {
	KeepDone   time.Duration
	KeepFailed time.Duration
	// Interval is the time between purges; it defaults to one hour.
	Interval time.Duration
	// BatchSize is the number of jobs deleted per transaction; it defaults to 1000.
	BatchSize int
}

// RetentionResult counts the jobs a purge deleted.
type RetentionResult struct {
	Done   int64
	Failed int64
}

func (p RetentionPolicy) withDefaults() RetentionPolicy {
	if p.Interval == 0 {
		p.Interval = defaultRetentionInterval
	}
	if p.BatchSize == 0 {
		p.BatchSize = defaultRetentionBatchSize
	}
	return p
}

// Validate reports whether the policy, with its defaults applied, is usable.
func (p RetentionPolicy) Validate() error {
	return p.withDefaults().validate()
}

func (p RetentionPolicy) validate() error {
	if p.KeepDone < 0 || p.KeepFailed < 0 || p.Interval <= 0 || p.BatchSize <= 0 {
		return ErrInvalidRetentionPolicy
	}
	if p.KeepDone == 0 && p.KeepFailed == 0 {
		return fmt.Errorf("%w: set KeepDone or KeepFailed", ErrInvalidRetentionPolicy)
	}
	return nil
}

// The status is a literal, not a parameter, so that every plan, generic ones
// included, can use the partial index idx_pgqueue_finished. SKIP LOCKED lets
// replicas purge at the same time without waiting on each other.
const (
	deleteExpiredDoneSQL = `DELETE FROM pgqueue_jobs WHERE id IN (
	SELECT id FROM pgqueue_jobs
	WHERE status = 'done' AND updated_at < NOW() - make_interval(secs => $1)
	LIMIT $2
	FOR UPDATE SKIP LOCKED
)`
	deleteExpiredFailedSQL = `DELETE FROM pgqueue_jobs WHERE id IN (
	SELECT id FROM pgqueue_jobs
	WHERE status = 'failed' AND updated_at < NOW() - make_interval(secs => $1)
	LIMIT $2
	FOR UPDATE SKIP LOCKED
)`
)

// PurgeFinished deletes the done and failed jobs of every queue that are older
// than policy keeps them. It deletes in batches of policy.BatchSize, each in its
// own transaction, until no expired job is left or ctx is canceled.
func (c *Client) PurgeFinished(ctx context.Context, policy RetentionPolicy) (RetentionResult, error) {
	if c == nil || c.db == nil {
		return RetentionResult{}, ErrNilDB
	}
	policy = policy.withDefaults()
	if err := policy.validate(); err != nil {
		return RetentionResult{}, err
	}

	var result RetentionResult
	var err error
	if policy.KeepDone > 0 {
		result.Done, err = c.deleteExpired(ctx, deleteExpiredDoneSQL, policy.KeepDone, policy.BatchSize)
		if err != nil {
			return result, fmt.Errorf("pgqueue: purge done jobs: %w", err)
		}
	}
	if policy.KeepFailed > 0 {
		result.Failed, err = c.deleteExpired(ctx, deleteExpiredFailedSQL, policy.KeepFailed, policy.BatchSize)
		if err != nil {
			return result, fmt.Errorf("pgqueue: purge failed jobs: %w", err)
		}
	}

	if result.Done > 0 {
		c.emit(ctx, EventPurge, map[string]any{"status": StatusDone, "deleted": result.Done})
	}
	if result.Failed > 0 {
		c.emit(ctx, EventPurge, map[string]any{"status": StatusFailed, "deleted": result.Failed})
	}
	if result.Done > 0 || result.Failed > 0 {
		c.logInfo(ctx, "queue purged finished jobs", map[string]any{"done": result.Done, "failed": result.Failed})
	}
	return result, nil
}

func (c *Client) deleteExpired(ctx context.Context, query string, keep time.Duration, batchSize int) (int64, error) {
	var total int64
	for ctx.Err() == nil {
		res, err := c.db.ExecContext(ctx, query, keep.Seconds(), batchSize)
		if err != nil {
			return total, err
		}
		deleted, err := res.RowsAffected()
		if err != nil {
			return total, err
		}
		total += deleted
		if deleted < int64(batchSize) {
			return total, nil
		}
	}
	return total, ctx.Err()
}

// RunRetention purges finished jobs at once and then every policy.Interval,
// until ctx is canceled. A failed purge is logged and retried at the next
// interval. Run it once per process: replicas may run it at the same time.
func (c *Client) RunRetention(ctx context.Context, policy RetentionPolicy) error {
	if c == nil || c.db == nil {
		return ErrNilDB
	}
	policy = policy.withDefaults()
	if err := policy.validate(); err != nil {
		return err
	}

	ticker := time.NewTicker(policy.Interval)
	defer ticker.Stop()
	for {
		if _, err := c.PurgeFinished(ctx, policy); err != nil && ctx.Err() == nil {
			c.logFailure(ctx, "queue retention purge failed", map[string]any{"error": err.Error()}, err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
