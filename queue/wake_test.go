package queue

import (
	"context"
	"database/sql"
	"sync/atomic"
	"testing"
	"time"
)

const wakeTestPollInterval = time.Hour

type wakeHarness struct {
	t      *testing.T
	ctx    context.Context
	db     *sql.DB
	q      *Client
	queue  string
	calls  atomic.Int32
	worker *Worker
}

func newWakeHarness(t *testing.T, batchSize int) *wakeHarness {
	t.Helper()
	ctx := context.Background()
	db := createWorkerTestDB(t, ctx)
	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("schema: %v", err)
	}
	h := &wakeHarness{t: t, ctx: ctx, db: db, q: q, queue: "wake"}
	registry := NewHandlerRegistry()
	if err := registry.Register(h.queue, func(context.Context, Job) error {
		h.calls.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	h.worker, err = NewWorker(q, registry, WorkerConfig{
		WorkerID:          "wake-worker",
		PollInterval:      wakeTestPollInterval,
		WakeOnEnqueue:     true,
		BatchSizePerQueue: batchSize,
	})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	return h
}

func (h *wakeHarness) start() {
	h.t.Helper()
	runCtx, cancel := context.WithCancel(h.ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = h.worker.Run(runCtx)
	}()
	h.t.Cleanup(func() {
		cancel()
		<-done
	})
	h.waitListening()
}

func (h *wakeHarness) waitListening() {
	h.t.Helper()
	requireEventually(h.t, 5*time.Second, 20*time.Millisecond, func() bool {
		return h.listeners() == 1
	})
}

func (h *wakeHarness) listeners() int {
	var n int
	if err := h.db.QueryRowContext(h.ctx,
		"SELECT count(*) FROM pg_stat_activity WHERE query = 'LISTEN "+NotifyChannel+"'").Scan(&n); err != nil {
		h.t.Fatalf("count listeners: %v", err)
	}
	return n
}

func (h *wakeHarness) enqueue() int64 {
	h.t.Helper()
	id, err := h.q.Enqueue(h.ctx, EnqueueParams{QueueName: h.queue, Payload: []byte(`{}`)})
	if err != nil {
		h.t.Fatalf("enqueue: %v", err)
	}
	return id
}

func (h *wakeHarness) requireCalls(n int32) {
	h.t.Helper()
	requireEventually(h.t, 3*time.Second, 20*time.Millisecond, func() bool {
		return h.calls.Load() == n
	})
}

func TestWakeOnEnqueueClaimsWithoutWaitingForThePoll(t *testing.T) {
	h := newWakeHarness(t, 10)
	h.start()
	h.enqueue()
	h.requireCalls(1)
}

func TestWakeOnEnqueueClaimsJobsEnqueuedBeforeListening(t *testing.T) {
	h := newWakeHarness(t, 10)
	h.enqueue()
	h.start()
	h.requireCalls(1)
}

func TestWakeOnEnqueueWaitsForTheEnqueueingCommit(t *testing.T) {
	h := newWakeHarness(t, 10)
	h.start()
	tx, err := h.db.BeginTx(h.ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := h.q.EnqueueTx(h.ctx, tx, EnqueueParams{QueueName: h.queue, Payload: []byte(`{}`)}); err != nil {
		t.Fatalf("enqueue tx: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if got := h.calls.Load(); got != 0 {
		t.Fatalf("handled %d jobs before commit", got)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	h.requireCalls(1)
}

func TestWakeOnEnqueueDrainsMoreThanOneBatch(t *testing.T) {
	h := newWakeHarness(t, 2)
	h.start()
	tx, err := h.db.BeginTx(h.ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	for range 7 {
		if _, err := h.q.EnqueueTx(h.ctx, tx, EnqueueParams{QueueName: h.queue, Payload: []byte(`{}`)}); err != nil {
			t.Fatalf("enqueue tx: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	h.requireCalls(7)
}

func TestWakeOnEnqueueClaimsReplayedJobs(t *testing.T) {
	h := newWakeHarness(t, 10)
	h.start()
	id := h.enqueue()
	h.requireCalls(1)
	requireEventually(t, 3*time.Second, 20*time.Millisecond, func() bool {
		job, err := h.q.GetJob(h.ctx, id)
		return err == nil && job.Status == StatusDone
	})
	if err := h.q.ReplayJob(h.ctx, id); err != nil {
		t.Fatalf("replay: %v", err)
	}
	h.requireCalls(2)
}

func TestWakeOnEnqueueRecoversJobsMissedWhileTheListenerWasDown(t *testing.T) {
	h := newWakeHarness(t, 10)
	h.start()
	var terminated bool
	if err := h.db.QueryRowContext(h.ctx,
		"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query = 'LISTEN "+NotifyChannel+"'").
		Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate listener: terminated=%v err=%v", terminated, err)
	}
	h.enqueue()
	h.requireCalls(1)
	h.waitListening()
}

func TestWakeOnEnqueueReleasesTheListenerOnShutdown(t *testing.T) {
	h := newWakeHarness(t, 10)
	runCtx, cancel := context.WithCancel(h.ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = h.worker.Run(runCtx)
	}()
	h.waitListening()
	cancel()
	<-done
	requireEventually(t, 3*time.Second, 20*time.Millisecond, func() bool {
		return h.listeners() == 0
	})
}

func TestWorkerWithoutWakeOnEnqueueDoesNotListen(t *testing.T) {
	h := newWakeHarness(t, 10)
	h.worker.cfg.WakeOnEnqueue = false
	runCtx, cancel := context.WithCancel(h.ctx)
	defer cancel()
	go func() { _ = h.worker.Run(runCtx) }()
	time.Sleep(300 * time.Millisecond)
	if got := h.listeners(); got != 0 {
		t.Fatalf("expected no listener, got %d", got)
	}
}
