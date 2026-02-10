package pgqueue

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
