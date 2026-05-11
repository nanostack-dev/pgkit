package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestWorkerRawHandler(t *testing.T) {
	ctx := context.Background()
	db := createWorkerTestDB(t, ctx)

	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("schema: %v", err)
	}

	registry := NewHandlerRegistry()
	var calls atomic.Int32
	if err := registry.Register("raw", func(_ context.Context, job Job) error {
		if string(job.Payload) != `{"ok":true}` {
			return fmt.Errorf("unexpected payload: %s", string(job.Payload))
		}
		calls.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	worker, err := NewWorker(q, registry, WorkerConfig{
		PollInterval:      20 * time.Millisecond,
		ReapInterval:      1 * time.Second,
		BackoffBase:       10 * time.Millisecond,
		BackoffMax:        1 * time.Second,
		WorkerID:          "t-worker",
		BatchSizePerQueue: 10,
	})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}

	if _, err := q.Enqueue(ctx, EnqueueParams{QueueName: "raw", Payload: []byte(`{"ok":true}`)}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = worker.Run(runCtx) }()

	requireEventually(t, 4*time.Second, 40*time.Millisecond, func() bool {
		return calls.Load() == 1
	})

	jobs, err := q.ListJobs(ctx, ListJobsParams{Limit: 10, QueueName: "raw"})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].Status != StatusDone {
		t.Fatalf("expected done job, got %+v", jobs)
	}
}

func TestWorkerTypedJSONHandler(t *testing.T) {
	type event struct {
		EventID string `json:"event_id"`
	}

	ctx := context.Background()
	db := createWorkerTestDB(t, ctx)

	q, _ := New(db)
	_ = q.EnsureSchema(ctx)

	registry := NewHandlerRegistry()
	var got atomic.Value
	got.Store("")
	err := RegisterJSONTyped[event](registry, "typed", func(_ context.Context, payload event, job Job) error {
		got.Store(payload.EventID)
		if job.QueueName != "typed" {
			return errors.New("wrong queue")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("register typed: %v", err)
	}

	worker, _ := NewWorker(q, registry, WorkerConfig{PollInterval: 20 * time.Millisecond, ReapInterval: time.Second, BackoffBase: 10 * time.Millisecond, BackoffMax: time.Second})
	_, _ = q.Enqueue(ctx, EnqueueParams{QueueName: "typed", Payload: []byte(`{"event_id":"evt_1"}`)})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = worker.Run(runCtx) }()

	requireEventually(t, 4*time.Second, 40*time.Millisecond, func() bool {
		v, _ := got.Load().(string)
		return v == "evt_1"
	})
}

func TestWorkerRetryThenSuccess(t *testing.T) {
	ctx := context.Background()
	db := createWorkerTestDB(t, ctx)

	q, _ := New(db)
	_ = q.EnsureSchema(ctx)

	registry := NewHandlerRegistry()
	var attempt atomic.Int32
	_ = registry.Register("retry", func(_ context.Context, _ Job) error {
		n := attempt.Add(1)
		if n == 1 {
			return errors.New("temporary")
		}
		return nil
	})

	worker, _ := NewWorker(q, registry, WorkerConfig{
		PollInterval:      20 * time.Millisecond,
		ReapInterval:      1 * time.Second,
		BackoffBase:       10 * time.Millisecond,
		BackoffMax:        100 * time.Millisecond,
		BatchSizePerQueue: 5,
	})

	_, _ = q.Enqueue(ctx, EnqueueParams{QueueName: "retry", Payload: []byte(`{"x":1}`), MaxAttempts: 3})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = worker.Run(runCtx) }()

	requireEventually(t, 5*time.Second, 50*time.Millisecond, func() bool {
		jobs, err := q.ListJobs(ctx, ListJobsParams{Limit: 1, QueueName: "retry"})
		return err == nil && len(jobs) == 1 && jobs[0].Status == StatusDone && jobs[0].Attempts >= 2
	})
}

func TestWorkerNonRetryableFailsImmediately(t *testing.T) {
	ctx := context.Background()
	db := createWorkerTestDB(t, ctx)

	q, _ := New(db)
	_ = q.EnsureSchema(ctx)

	registry := NewHandlerRegistry()
	_ = registry.Register("fail", func(_ context.Context, _ Job) error {
		return NonRetryable(errors.New("bad payload"))
	})

	worker, _ := NewWorker(q, registry, WorkerConfig{PollInterval: 20 * time.Millisecond, ReapInterval: time.Second, BackoffBase: 10 * time.Millisecond, BackoffMax: time.Second})
	_, _ = q.Enqueue(ctx, EnqueueParams{QueueName: "fail", Payload: []byte(`not-json`), MaxAttempts: 5})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = worker.Run(runCtx) }()

	requireEventually(t, 4*time.Second, 40*time.Millisecond, func() bool {
		jobs, err := q.ListJobs(ctx, ListJobsParams{Limit: 1, QueueName: "fail"})
		return err == nil && len(jobs) == 1 && jobs[0].Status == StatusFailed && jobs[0].Attempts == 1
	})
}

func TestWorkerDecodeErrorNonRetryable(t *testing.T) {
	type payload struct {
		Value int `json:"value"`
	}

	ctx := context.Background()
	db := createWorkerTestDB(t, ctx)

	q, _ := New(db)
	_ = q.EnsureSchema(ctx)

	registry := NewHandlerRegistry()
	err := RegisterJSON[payload](registry, "decode", func(_ context.Context, _ payload) error {
		return nil
	})
	if err != nil {
		t.Fatalf("register json: %v", err)
	}

	worker, _ := NewWorker(q, registry, WorkerConfig{PollInterval: 20 * time.Millisecond, ReapInterval: time.Second, BackoffBase: 10 * time.Millisecond, BackoffMax: time.Second})
	_, _ = q.Enqueue(ctx, EnqueueParams{QueueName: "decode", Payload: []byte(`{"value":"bad"}`), MaxAttempts: 5})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = worker.Run(runCtx) }()

	requireEventually(t, 4*time.Second, 40*time.Millisecond, func() bool {
		jobs, err := q.ListJobs(ctx, ListJobsParams{Limit: 1, QueueName: "decode"})
		return err == nil && len(jobs) == 1 && jobs[0].Status == StatusFailed
	})
}

func TestWorkerOnJobFailedNonRetryable(t *testing.T) {
	ctx := context.Background()
	db := createWorkerTestDB(t, ctx)

	q, _ := New(db)
	_ = q.EnsureSchema(ctx)

	registry := NewHandlerRegistry()
	_ = registry.Register("cb-fail", func(_ context.Context, _ Job) error {
		return NonRetryable(errors.New("permanent error"))
	})

	var failedJob atomic.Value
	worker, _ := NewWorker(q, registry, WorkerConfig{
		PollInterval:      20 * time.Millisecond,
		ReapInterval:      1 * time.Second,
		BackoffBase:       10 * time.Millisecond,
		BackoffMax:        1 * time.Second,
		BatchSizePerQueue: 10,
		OnJobFailed: func(_ context.Context, job Job) {
			failedJob.Store(job)
		},
	})

	_, _ = q.Enqueue(ctx, EnqueueParams{QueueName: "cb-fail", Payload: []byte(`{"x":1}`), MaxAttempts: 3})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = worker.Run(runCtx) }()

	requireEventually(t, 4*time.Second, 40*time.Millisecond, func() bool {
		v := failedJob.Load()
		if v == nil {
			return false
		}
		job := v.(Job)
		return job.Status == StatusFailed && job.LastError.Valid && job.LastError.String == "permanent error"
	})
}

func TestWorkerOnJobFailedRetryExhaustion(t *testing.T) {
	ctx := context.Background()
	db := createWorkerTestDB(t, ctx)

	q, _ := New(db)
	_ = q.EnsureSchema(ctx)

	registry := NewHandlerRegistry()
	_ = registry.Register("cb-exhaust", func(_ context.Context, _ Job) error {
		return errors.New("always fails")
	})

	var failedJob atomic.Value
	worker, _ := NewWorker(q, registry, WorkerConfig{
		PollInterval:      20 * time.Millisecond,
		ReapInterval:      1 * time.Second,
		BackoffBase:       10 * time.Millisecond,
		BackoffMax:        50 * time.Millisecond,
		BatchSizePerQueue: 10,
		OnJobFailed: func(_ context.Context, job Job) {
			failedJob.Store(job)
		},
	})

	// MaxAttempts=2: first attempt retries, second attempt exhausts
	_, _ = q.Enqueue(ctx, EnqueueParams{QueueName: "cb-exhaust", Payload: []byte(`{"x":1}`), MaxAttempts: 2})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = worker.Run(runCtx) }()

	requireEventually(t, 6*time.Second, 50*time.Millisecond, func() bool {
		v := failedJob.Load()
		if v == nil {
			return false
		}
		job := v.(Job)
		return job.Status == StatusFailed
	})
}

func TestWorkerOnJobStuck(t *testing.T) {
	ctx := context.Background()
	db := createWorkerTestDB(t, ctx)

	q, _ := New(db)
	_ = q.EnsureSchema(ctx)

	// Enqueue and claim a job manually to simulate a stuck processing job.
	_, _ = q.Enqueue(ctx, EnqueueParams{QueueName: "cb-stuck", Payload: []byte(`{"x":1}`), MaxAttempts: 3})
	job, found, err := q.Claim(ctx, "cb-stuck", "ghost-worker")
	if err != nil || !found {
		t.Fatalf("claim: found=%v err=%v", found, err)
	}

	// Backdate claimed_at so the reaper considers it stuck.
	_, err = db.ExecContext(ctx,
		`UPDATE pgqueue_jobs SET claimed_at = NOW() - INTERVAL '10 minutes' WHERE id = $1`, job.ID)
	if err != nil {
		t.Fatalf("backdate claimed_at: %v", err)
	}

	// Create a worker with a short reap interval and a tiny visibility timeout.
	registry := NewHandlerRegistry()
	_ = registry.Register("cb-stuck", func(_ context.Context, _ Job) error { return nil })

	var stuckResult atomic.Value
	worker, _ := NewWorker(q, registry, WorkerConfig{
		PollInterval:      100 * time.Millisecond,
		ReapInterval:      100 * time.Millisecond,
		VisibilityTimeout: 1 * time.Second, // job claimed 10min ago > 1s timeout
		BackoffBase:       10 * time.Millisecond,
		BackoffMax:        1 * time.Second,
		BatchSizePerQueue: 10,
		OnJobStuck: func(_ context.Context, result ReapResult) {
			stuckResult.Store(result)
		},
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = worker.Run(runCtx) }()

	requireEventually(t, 4*time.Second, 50*time.Millisecond, func() bool {
		v := stuckResult.Load()
		if v == nil {
			return false
		}
		result := v.(ReapResult)
		return result.Requeued > 0 || result.Failed > 0
	})
}

func TestWorkerOnJobFailedNotCalledOnSuccess(t *testing.T) {
	ctx := context.Background()
	db := createWorkerTestDB(t, ctx)

	q, _ := New(db)
	_ = q.EnsureSchema(ctx)

	registry := NewHandlerRegistry()
	_ = registry.Register("cb-ok", func(_ context.Context, _ Job) error {
		return nil // success
	})

	var called atomic.Bool
	worker, _ := NewWorker(q, registry, WorkerConfig{
		PollInterval:      20 * time.Millisecond,
		ReapInterval:      1 * time.Second,
		BackoffBase:       10 * time.Millisecond,
		BackoffMax:        1 * time.Second,
		BatchSizePerQueue: 10,
		OnJobFailed: func(_ context.Context, _ Job) {
			called.Store(true)
		},
	})

	_, _ = q.Enqueue(ctx, EnqueueParams{QueueName: "cb-ok", Payload: []byte(`{"x":1}`)})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = worker.Run(runCtx) }()

	// Wait for the job to be processed.
	requireEventually(t, 4*time.Second, 40*time.Millisecond, func() bool {
		jobs, err := q.ListJobs(ctx, ListJobsParams{Limit: 1, QueueName: "cb-ok"})
		return err == nil && len(jobs) == 1 && jobs[0].Status == StatusDone
	})

	// Callback should NOT have been called.
	if called.Load() {
		t.Fatal("OnJobFailed should not be called on success")
	}
}

func createWorkerTestDB(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()

	pg, connString := startWorkerPostgres(t, ctx)
	t.Cleanup(func() { _ = pg.Terminate(ctx) })

	db, err := sql.Open("pgx", connString)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := waitWorkerPing(ctx, db, 20*time.Second); err != nil {
		t.Fatalf("db ping: %v", err)
	}

	return db
}

func startWorkerPostgres(t *testing.T, ctx context.Context) (testcontainers.Container, string) {
	t.Helper()

	pg, err := postgres.Run(
		ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("pgkit_worker_test"),
		postgres.WithUsername("pgkit"),
		postgres.WithPassword("pgkit"),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}

	connString, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = pg.Terminate(ctx)
		t.Fatalf("postgres connection string: %v", err)
	}

	return pg, connString
}

func waitWorkerPing(ctx context.Context, db *sql.DB, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := db.PingContext(pingCtx)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(250 * time.Millisecond)
	}

	return lastErr
}

func requireEventually(t *testing.T, timeout, tick time.Duration, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(tick)
	}
	t.Fatal("condition not met in time")
}
