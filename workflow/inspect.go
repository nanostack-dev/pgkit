package workflow

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

const runColumns = `id, workflow, version, COALESCE(key, ''), status, input, output, COALESCE(error, ''),
	timed_out, COALESCE(parent_run_id, ''), wake_at, deadline_at, created_at, started_at, completed_at, updated_at`

func scanRun(row rowScanner) (RunInfo, error) {
	var (
		run                                     RunInfo
		input, output                           []byte
		wakeAt, deadlineAt, startedAt, complete sql.NullTime
	)
	err := row.Scan(&run.ID, &run.Workflow, &run.Version, &run.Key, &run.Status, &input, &output, &run.Error,
		&run.TimedOut, &run.ParentRunID, &wakeAt, &deadlineAt, &run.CreatedAt, &startedAt, &complete, &run.UpdatedAt)
	if err != nil {
		return RunInfo{}, err
	}
	run.Input, run.Output = input, output
	run.WakeAt, run.DeadlineAt = wakeAt.Time, deadlineAt.Time
	run.StartedAt, run.CompletedAt = startedAt.Time, complete.Time
	return run, nil
}

// GetRun returns a run, or ErrRunNotFound.
func (c *Client) GetRun(ctx context.Context, runID string) (RunInfo, error) {
	run, err := scanRun(c.db.QueryRowContext(ctx, `SELECT `+runColumns+` FROM pgworkflow_runs WHERE id = $1`, runID))
	if errors.Is(err, sql.ErrNoRows) {
		return RunInfo{}, fmt.Errorf("%w: %s", ErrRunNotFound, runID)
	}
	if err != nil {
		return RunInfo{}, fmt.Errorf("workflow: get run: %w", err)
	}
	return run, nil
}

// ListSteps returns the checkpoints of a run in the order the run first reached them.
func (c *Client) ListSteps(ctx context.Context, runID string) ([]StepInfo, error) {
	rows, err := c.db.QueryContext(ctx, `
SELECT run_id, name, kind, status, attempts, output, COALESCE(error, ''), wake_at,
       COALESCE(child_run_id, ''), COALESCE(signal, ''), created_at, completed_at, updated_at
FROM pgworkflow_steps
WHERE run_id = $1
ORDER BY created_at, name`, runID)
	if err != nil {
		return nil, fmt.Errorf("workflow: list steps: %w", err)
	}
	defer func() { _ = rows.Close() }()
	steps := []StepInfo{}
	for rows.Next() {
		var (
			step                StepInfo
			output              []byte
			wakeAt, completedAt sql.NullTime
		)
		if err := rows.Scan(&step.RunID, &step.Name, &step.Kind, &step.Status, &step.Attempts, &output, &step.Error,
			&wakeAt, &step.ChildRunID, &step.Signal, &step.CreatedAt, &completedAt, &step.UpdatedAt); err != nil {
			return nil, fmt.Errorf("workflow: scan step: %w", err)
		}
		step.Output, step.WakeAt, step.CompletedAt = output, wakeAt.Time, completedAt.Time
		steps = append(steps, step)
	}
	return steps, rows.Err()
}

func runFilter(params ListRunsParams) (string, []any) {
	var conditions []string
	var args []any
	add := func(condition string, value any) {
		args = append(args, value)
		conditions = append(conditions, fmt.Sprintf(condition, len(args)))
	}
	if params.Workflow != "" {
		add("workflow = $%d", params.Workflow)
	}
	if params.Status != "" {
		add("status = $%d", string(params.Status))
	}
	if params.ParentRunID != "" {
		add("parent_run_id = $%d", params.ParentRunID)
	}
	if strings.TrimSpace(params.Search) != "" {
		add("(id ILIKE $%[1]d OR key ILIKE $%[1]d OR workflow ILIKE $%[1]d)", searchPattern(params.Search))
	}
	if len(conditions) == 0 {
		return "", args
	}
	return " WHERE " + strings.Join(conditions, " AND "), args
}

// ListRuns returns runs matching params, newest first. Limit defaults to 50.
func (c *Client) ListRuns(ctx context.Context, params ListRunsParams) ([]RunInfo, error) {
	where, args := runFilter(params)
	limit := params.Limit
	if limit <= 0 {
		limit = 50
	}
	args = append(args, limit, max(params.Offset, 0))
	rows, err := c.db.QueryContext(ctx, fmt.Sprintf(`SELECT %s FROM pgworkflow_runs%s ORDER BY created_at DESC, id DESC LIMIT $%d OFFSET $%d`,
		runColumns, where, len(args)-1, len(args)), args...)
	if err != nil {
		return nil, fmt.Errorf("workflow: list runs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	runs := []RunInfo{}
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, fmt.Errorf("workflow: scan run: %w", err)
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

// CountRuns counts runs matching params, ignoring Limit and Offset.
func (c *Client) CountRuns(ctx context.Context, params ListRunsParams) (int64, error) {
	where, args := runFilter(params)
	var count int64
	if err := c.db.QueryRowContext(ctx, `SELECT count(*) FROM pgworkflow_runs`+where, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("workflow: count runs: %w", err)
	}
	return count, nil
}

// Purge deletes finished runs completed more than params.OlderThan ago, with their
// checkpoints and signals, and returns how many runs it deleted. A child whose
// parent is still unfinished is kept, as the parent may still read its result. Run
// Purge periodically to bound the tables; the queue's own Purge removes finished
// jobs.
func (c *Client) Purge(ctx context.Context, params PurgeParams) (int64, error) {
	var limit any
	if params.Limit > 0 {
		limit = params.Limit
	}
	res, err := c.db.ExecContext(ctx, `
DELETE FROM pgworkflow_runs
WHERE id IN (
    SELECT run.id FROM pgworkflow_runs run
    WHERE run.status IN ('succeeded', 'failed', 'cancelled')
      AND run.completed_at < NOW() - make_interval(secs => $1)
      AND NOT EXISTS (
          SELECT 1 FROM pgworkflow_runs parent
          WHERE parent.id = run.parent_run_id AND parent.status IN ('pending', 'running', 'waiting'))
    ORDER BY run.completed_at
    LIMIT $2)`, max(params.OlderThan, 0).Seconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("workflow: purge runs: %w", err)
	}
	return res.RowsAffected()
}

// Run is a typed handle on a run, returned by Client.Start, Client.Run and
// Client.RunByKey.
type Run[Out any] struct {
	ID     string
	client *Client
}

const (
	resultPollInterval    = 250 * time.Millisecond
	resultRecheckInterval = 5 * time.Second
)

// Result waits until the run finishes and returns its output. A failed or
// cancelled run returns a *RunError; errors.Is(err, ErrCancelled) and
// errors.Is(err, ErrTimeout) identify cancellation and timeouts. It returns as soon
// as the run finishes when the queue client receives notifications, and polls
// otherwise.
func (r Run[Out]) Result(ctx context.Context) (Out, error) {
	var zero Out
	if r.client == nil {
		return zero, fmt.Errorf("%w: empty run handle", ErrRunNotFound)
	}
	interval := resultPollInterval
	var wake <-chan struct{}
	if subscription, err := r.client.queue.Subscribe(runNotifyKey(r.ID)); err == nil {
		defer subscription.Close()
		wake, interval = subscription.Wake(), resultRecheckInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		run, err := r.client.GetRun(ctx, r.ID)
		if err != nil {
			return zero, err
		}
		if run.Status.Finished() {
			return decodeResult[Out](run)
		}
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-wake:
		case <-ticker.C:
		}
	}
}

func decodeResult[Out any](run RunInfo) (Out, error) {
	var output Out
	if run.Status != RunSucceeded {
		return output, &RunError{RunID: run.ID, Workflow: run.Workflow, Status: run.Status, TimedOut: run.TimedOut, Message: run.Error}
	}
	if err := jsonUnmarshal(run.Output, &output); err != nil {
		return output, fmt.Errorf("workflow: run %s output does not decode into %T: %w", run.ID, output, err)
	}
	return output, nil
}
