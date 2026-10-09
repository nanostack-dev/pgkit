package workflow

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nanostack-dev/pgkit/queue"
)

// WorkerConfig tunes a worker. Zero fields take the defaults.
type WorkerConfig struct {
	// Concurrency is how many runs the worker executes at once. Default 4.
	Concurrency int
	// VisibilityTimeout is how long a run's job may go without a heartbeat before
	// another worker takes the run over. Heartbeats come every third of it.
	// Default 30s.
	VisibilityTimeout time.Duration
	// ReapInterval is how often the worker looks for runs whose worker stopped
	// heartbeating. Default 10s.
	ReapInterval time.Duration
	// OnRunFailed is called after a run this worker executed or reconciled fails.
	OnRunFailed func(ctx context.Context, run RunInfo)
}

func (c WorkerConfig) withDefaults() WorkerConfig {
	if c.Concurrency == 0 {
		c.Concurrency = 4
	}
	if c.VisibilityTimeout == 0 {
		c.VisibilityTimeout = 30 * time.Second
	}
	if c.ReapInterval == 0 {
		c.ReapInterval = 10 * time.Second
	}
	return c
}

// WorkerBuilder configures a worker. Methods return independent copies; Build
// validates the configuration without touching the database.
//
//	worker, err := client.Worker("billing").
//		Workflows(Checkout, Refund).
//		Pickup(queue.OnEnqueue()).
//		Build()
type WorkerBuilder struct {
	client    *Client
	id        string
	pickup    queue.Pickup
	config    WorkerConfig
	workflows []Definition
}

// Worker starts a builder for a worker that claims runs as id.
func (c *Client) Worker(id string) WorkerBuilder {
	return WorkerBuilder{client: c, id: id}
}

// Workflows adds workflows the worker executes. A run is executed only by workers
// listing its workflow and version.
func (b WorkerBuilder) Workflows(workflows ...Definition) WorkerBuilder {
	b.workflows = slices.Concat(b.workflows, workflows)
	return b
}

// Pickup sets when the worker looks for runs to execute. By default it claims runs
// when they are started or woken (queue.OnEnqueue) if the queue client receives
// notifications, and polls every second otherwise.
func (b WorkerBuilder) Pickup(pickup queue.Pickup) WorkerBuilder {
	b.pickup = pickup
	return b
}

// Tune sets the remaining worker settings.
func (b WorkerBuilder) Tune(config WorkerConfig) WorkerBuilder {
	b.config = config
	return b
}

// Worker executes the runs of its workflows.
type Worker struct {
	client       *Client
	config       WorkerConfig
	queueWorkers []*queue.Worker
	ready        chan struct{}
	running      atomic.Bool
}

// Build validates the configuration and builds the worker without starting it.
func (b WorkerBuilder) Build() (*Worker, error) {
	if len(b.workflows) == 0 {
		return nil, ErrNoWorkflows
	}
	config := b.config.withDefaults()
	if config.Concurrency < 0 || config.VisibilityTimeout < 0 || config.ReapInterval < 0 {
		return nil, ErrInvalidWorkerConfig
	}
	definitions := map[string]*definition{}
	for _, workflow := range b.workflows {
		def := workflow.definitionOf()
		if _, listed := definitions[def.queueName()]; listed {
			return nil, fmt.Errorf("%w: %s version %d", ErrDuplicateWorkflow, def.name, def.version)
		}
		definitions[def.queueName()] = def
	}

	worker := &Worker{client: b.client, config: config, ready: make(chan struct{})}
	for slot := range config.Concurrency {
		builder := b.client.queue.Worker(fmt.Sprintf("%s/%d", b.id, slot+1)).Tune(queue.WorkerConfig{
			VisibilityTimeout: config.VisibilityTimeout,
			ReapInterval:      config.ReapInterval,
		})
		for queueName, def := range definitions {
			builder = builder.HandleRaw(queueName, worker.handler(def, config))
		}
		queueWorker, err := buildQueueWorker(builder, b.pickup)
		if err != nil {
			return nil, err
		}
		worker.queueWorkers = append(worker.queueWorkers, queueWorker)
	}
	return worker, nil
}

func buildQueueWorker(builder queue.WorkerBuilder, pickup queue.Pickup) (*queue.Worker, error) {
	if pickup != (queue.Pickup{}) {
		return builder.Pickup(pickup).Build()
	}
	worker, err := builder.Pickup(queue.OnEnqueue()).Build()
	if errors.Is(err, queue.ErrNotificationsUnsupported) {
		return builder.Pickup(queue.PollEvery(time.Second)).Build()
	}
	return worker, err
}

// Run executes runs until ctx is cancelled. A worker runs at most once at a time.
// On cancellation, steps in progress see their context cancelled and their runs are
// handed back to the queue for another worker.
func (w *Worker) Run(ctx context.Context) error {
	if !w.running.CompareAndSwap(false, true) {
		return queue.ErrWorkerRunning
	}
	defer w.running.Store(false)
	var group sync.WaitGroup
	errs := make([]error, len(w.queueWorkers))
	for i, queueWorker := range w.queueWorkers {
		group.Go(func() { errs[i] = queueWorker.Run(ctx) })
	}
	group.Go(func() { w.reconcile(ctx) })
	group.Go(func() {
		for _, queueWorker := range w.queueWorkers {
			select {
			case <-queueWorker.Ready():
			case <-ctx.Done():
				return
			}
		}
		select {
		case <-w.ready:
		default:
			close(w.ready)
		}
	})
	group.Wait()
	return errors.Join(errs...)
}

// reconcile periodically fails runs whose job the queue gave up on.
func (w *Worker) reconcile(ctx context.Context) {
	ticker := time.NewTicker(w.config.ReapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.client.failOrphanedRuns(ctx, w.config.OnRunFailed); err != nil && ctx.Err() == nil {
				w.client.log.Warn(ctx, "workflow reconcile failed", map[string]any{"error": err.Error()})
			}
		}
	}
}

// Ready is closed once every slot of the worker is ready: from then on a run that
// is started or woken wakes the worker. See queue.Worker.Ready.
func (w *Worker) Ready() <-chan struct{} {
	return w.ready
}

func (w *Worker) handler(def *definition, config WorkerConfig) queue.Handler {
	return func(ctx context.Context, job queue.Job) error {
		var payload runJob
		if err := json.Unmarshal(job.Payload, &payload); err != nil || payload.RunID == "" {
			return queue.NonRetryable(fmt.Errorf("workflow: job %d does not name a run", job.ID))
		}
		act, err := w.client.beginActivation(ctx, def, config, job, payload.RunID)
		if err != nil {
			return err
		}
		if act == nil {
			return queue.Handled()
		}
		return act.execute(ctx)
	}
}

// beginActivation takes ownership of the run named by job by bumping its lease,
// which fences writes from any activation still running elsewhere. It holds the
// job's claim while doing so, so a worker whose claim the reaper took back cannot
// displace the worker that holds it now. It returns nil, with nothing left to do,
// when the claim is gone or the job is stale (the run finished, or a retry gave it a
// new job); a stale job is acknowledged.
func (c *Client) beginActivation(workerCtx context.Context, def *definition, config WorkerConfig, job queue.Job, runID string) (*activation, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(workerCtx), bookkeepingTimeout)
	defer cancel()
	act := &activation{
		client:      c,
		def:         def,
		config:      config,
		job:         job,
		runID:       runID,
		checkpoints: map[string]checkpoint{},
		occurrences: map[string]int{},
		runCtx:      workerCtx,
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("workflow: begin run %s: %w", runID, err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := c.queue.LockClaimTx(ctx, tx, job); errors.Is(err, queue.ErrJobNotFound) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	var (
		input       []byte
		deadlineAt  sql.NullTime
		secondsLeft sql.NullFloat64
	)
	err = tx.QueryRowContext(ctx, `
UPDATE pgworkflow_runs
SET status = 'running', lease = lease + 1, started_at = COALESCE(started_at, NOW()), wake_at = NULL, updated_at = NOW()
WHERE id = $1 AND job_id = $2 AND status IN ('pending', 'running', 'waiting')
RETURNING lease, input, COALESCE(parent_run_id, ''), deadline_at, EXTRACT(EPOCH FROM deadline_at - NOW())::float8`,
		runID, job.ID,
	).Scan(&act.lease, &input, &act.parentRunID, &deadlineAt, &secondsLeft)
	if errors.Is(err, sql.ErrNoRows) {
		if err := c.queue.AckTx(ctx, tx, job.ID); err != nil {
			return nil, err
		}
		return nil, tx.Commit()
	}
	if err != nil {
		return nil, fmt.Errorf("workflow: begin run %s: %w", runID, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("workflow: commit run %s start: %w", runID, err)
	}
	act.input = input
	act.deadline = deadlineFrom(deadlineAt, secondsLeft)
	if err := act.loadCheckpoints(ctx); err != nil {
		return nil, err
	}
	return act, nil
}

const orphanedRunCondition = `run.status IN ('pending', 'running', 'waiting')
  AND (job.id IS NULL OR job.status IN ('failed', 'done'))`

// failOrphanedRuns fails unfinished runs that no activation will ever finish: their
// job failed after crashes or reaps used all its attempts, was settled outside the
// runtime, or was deleted.
func (c *Client) failOrphanedRuns(ctx context.Context, onRunFailed func(context.Context, RunInfo)) error {
	rows, err := c.db.QueryContext(ctx, `
SELECT run.id, COALESCE(job.status, 'deleted'), COALESCE(job.last_error, '')
FROM pgworkflow_runs run
LEFT JOIN pgqueue_jobs job ON job.id = run.job_id
WHERE `+orphanedRunCondition+`
LIMIT 100`)
	if err != nil {
		return fmt.Errorf("workflow: find orphaned runs: %w", err)
	}
	type orphan struct{ runID, jobStatus, lastError string }
	var orphans []orphan
	for rows.Next() {
		var found orphan
		if err := rows.Scan(&found.runID, &found.jobStatus, &found.lastError); err != nil {
			_ = rows.Close()
			return err
		}
		orphans = append(orphans, found)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, found := range orphans {
		failed, err := c.failOrphanedRun(ctx, found.runID, orphanMessage(found.jobStatus, found.lastError))
		if err != nil {
			return err
		}
		if failed && onRunFailed != nil {
			if info, err := c.GetRun(ctx, found.runID); err == nil {
				onRunFailed(ctx, info)
			}
		}
	}
	return nil
}

func orphanMessage(jobStatus, lastError string) string {
	switch jobStatus {
	case "deleted":
		return "the run's job was deleted"
	case "done":
		return "the run's job was settled while the run was unfinished"
	}
	if lastError == "" {
		return "the queue gave up on the run's job"
	}
	return "the queue gave up on the run's job: " + lastError
}

func (c *Client) failOrphanedRun(ctx context.Context, runID, message string) (bool, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	var parentRunID sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT parent_run_id FROM pgworkflow_runs WHERE id = $1`, runID).Scan(&parentRunID); err != nil {
		return false, err
	}
	if parentRunID.Valid {
		if err := c.wakeParentTx(ctx, tx, parentRunID.String); err != nil {
			return false, err
		}
	}
	var orphaned bool
	err = tx.QueryRowContext(ctx, `
SELECT TRUE FROM pgworkflow_runs run LEFT JOIN pgqueue_jobs job ON job.id = run.job_id
WHERE run.id = $1 AND `+orphanedRunCondition+`
FOR UPDATE OF run`, runID).Scan(&orphaned)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE pgworkflow_runs
SET status = 'failed', error = $2, completed_at = NOW(), wake_at = NULL, updated_at = NOW()
WHERE id = $1`, runID, message); err != nil {
		return false, err
	}
	if err := c.cancelDescendantsTx(ctx, tx, runID); err != nil {
		return false, err
	}
	if err := c.queue.NotifyTx(ctx, tx, runNotifyKey(runID)); err != nil {
		return false, err
	}
	return true, tx.Commit()
}
