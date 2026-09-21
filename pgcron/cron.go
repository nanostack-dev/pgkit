package pgcron

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nanostack-dev/pgkit/pglock"
)

// Schema must be applied by the application's migrations before starting jobs.
//
//go:embed schema.sql
var Schema string

type Outcome string

const (
	Busy   Outcome = "busy"
	NotDue Outcome = "not_due"
	Ran    Outcome = "ran"
)

type Result struct {
	Outcome   Outcome
	NextRunAt time.Time
	wakeAt    time.Time
}

type Params struct {
	Name          string
	Interval      time.Duration
	RetryInterval time.Duration
	Run           func(context.Context, *sql.Tx) error
	AfterRun      func(context.Context) error
}

type Job struct {
	db     *sql.DB
	params Params
}

func New(db *sql.DB, params Params) (*Job, error) {
	switch {
	case db == nil:
		return nil, pglock.ErrNilDB
	case strings.TrimSpace(params.Name) == "":
		return nil, errors.New("pgcron: name is required")
	case params.Interval < time.Millisecond:
		return nil, errors.New("pgcron: interval must be at least one millisecond")
	case params.Run == nil:
		return nil, errors.New("pgcron: run callback is required")
	case params.RetryInterval < 0:
		return nil, errors.New("pgcron: retry interval must not be negative")
	}
	if params.RetryInterval == 0 {
		params.RetryInterval = time.Second
	}
	return &Job{db: db, params: params}, nil
}

// Run blocks until cancellation. report is called synchronously after each pass,
// including errors; it must not be nil. Busy/failed passes retry after RetryInterval.
func (j *Job) Run(ctx context.Context, report func(Result, error)) error {
	if report == nil {
		return errors.New("pgcron: report callback is required")
	}
	for ctx.Err() == nil {
		result, err := j.RunOnce(ctx)
		if ctx.Err() != nil {
			break
		}
		if result.wakeAt.IsZero() {
			result.wakeAt = time.Now().Add(j.params.RetryInterval)
		}
		timer := time.NewTimer(time.Until(result.wakeAt))
		report(result, err)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ctx.Err()
}

// RunOnce runs one due occurrence. Run shares the schedule transaction, so callers
// can atomically enqueue work with it. Failure (including panic) rolls back.
// AfterRun is outside the transaction; its failure cannot undo a committed run.
func (j *Job) RunOnce(ctx context.Context) (Result, error) {
	result, err := j.runLocked(ctx)
	if err != nil {
		return result, fmt.Errorf("pgcron %q: %w", j.params.Name, err)
	}
	if result.Outcome == Ran && j.params.AfterRun != nil {
		if err := j.params.AfterRun(ctx); err != nil {
			return result, fmt.Errorf("pgcron %q: after run: %w", j.params.Name, err)
		}
	}
	return result, nil
}

func (j *Job) runLocked(ctx context.Context) (Result, error) {
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return Result{}, fmt.Errorf("begin schedule transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	acquired, err := pglock.TryAdvisoryXactLock(ctx, tx, j.params.Name)
	if err != nil {
		return Result{}, fmt.Errorf("acquire schedule lock: %w", err)
	}
	if !acquired {
		return Result{Outcome: Busy}, nil
	}
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO pgcron_schedules (name) VALUES ($1) ON CONFLICT DO NOTHING", j.params.Name); err != nil {
		return Result{}, fmt.Errorf("initialize schedule: %w", err)
	}

	var nextRun, databaseNow time.Time
	if err := tx.QueryRowContext(ctx,
		"SELECT next_run_at, now() FROM pgcron_schedules WHERE name = $1", j.params.Name).
		Scan(&nextRun, &databaseNow); err != nil {
		return Result{}, fmt.Errorf("read schedule: %w", err)
	}
	if nextRun.After(databaseNow) {
		return Result{
			Outcome: NotDue, NextRunAt: nextRun,
			wakeAt: time.Now().Add(nextRun.Sub(databaseNow)),
		}, nil
	}
	if err := j.params.Run(ctx, tx); err != nil {
		return Result{}, fmt.Errorf("run: %w", err)
	}

	// Keep the original cadence, but coalesce missed runs. Use completion-time
	// DB time rather than now(), which is frozen at transaction start.
	const advance = `
UPDATE pgcron_schedules
SET next_run_at = date_bin(make_interval(secs => $2), clock_timestamp(), next_run_at)
                  + make_interval(secs => $2)
WHERE name = $1
RETURNING next_run_at, clock_timestamp()`
	if err := tx.QueryRowContext(ctx, advance, j.params.Name, j.params.Interval.Seconds()).
		Scan(&nextRun, &databaseNow); err != nil {
		return Result{}, fmt.Errorf("advance schedule: %w", err)
	}
	wakeAt := time.Now().Add(nextRun.Sub(databaseNow))
	if err := tx.Commit(); err != nil {
		return Result{}, fmt.Errorf("commit schedule: %w", err)
	}
	return Result{Outcome: Ran, NextRunAt: nextRun, wakeAt: wakeAt}, nil
}
