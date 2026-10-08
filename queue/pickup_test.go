package queue

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// noRescan keeps OnEnqueue workers from finding jobs by scanning, so every claim in
// these tests after Ready can only come from a notification.
const noRescan = time.Hour

type pickupHarness struct {
	t   *testing.T
	ctx context.Context
	db  *sql.DB
	q   *Client
}

type countingWorker struct {
	*Worker
	queue string
	calls atomic.Int32
}

func newPickupHarness(t *testing.T) *pickupHarness {
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
	return &pickupHarness{t: t, ctx: ctx, db: db, q: q}
}

func (h *pickupHarness) newWorker(queue string, batchSize int) *countingWorker {
	h.t.Helper()
	w := &countingWorker{queue: queue}
	registry := NewHandlerRegistry()
	if err := registry.Register(queue, func(context.Context, Job) error {
		w.calls.Add(1)
		return nil
	}); err != nil {
		h.t.Fatalf("register: %v", err)
	}
	worker, err := NewWorker(h.q, registry, WorkerConfig{
		WorkerID:          "pickup-" + queue[:min(len(queue), 20)],
		Pickup:            OnEnqueue().RescanEvery(noRescan),
		ReapInterval:      noRescan,
		BatchSizePerQueue: batchSize,
	})
	if err != nil {
		h.t.Fatalf("new worker: %v", err)
	}
	w.Worker = worker
	return w
}

// start runs the worker until stop is called or the test ends, and waits until it
// is Ready.
func (h *pickupHarness) start(w *countingWorker) (stop func()) {
	h.t.Helper()
	runCtx, cancel := context.WithCancel(h.ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Run(runCtx)
	}()
	stop = func() {
		cancel()
		<-done
	}
	h.t.Cleanup(stop)
	select {
	case <-w.Ready():
	case <-time.After(5 * time.Second):
		h.t.Fatal("worker never became ready")
	}
	return stop
}

func (h *pickupHarness) listeners() int {
	h.t.Helper()
	var n int
	if err := h.db.QueryRowContext(h.ctx,
		"SELECT count(*) FROM pg_stat_activity WHERE query = 'LISTEN "+NotifyChannel+"'").Scan(&n); err != nil {
		h.t.Fatalf("count listeners: %v", err)
	}
	return n
}

func (h *pickupHarness) enqueue(queue string) int64 {
	h.t.Helper()
	id, err := h.q.Enqueue(h.ctx, EnqueueParams{QueueName: queue, Payload: []byte(`{}`)})
	if err != nil {
		h.t.Fatalf("enqueue: %v", err)
	}
	return id
}

func (h *pickupHarness) inTx(enqueue func(tx *sql.Tx)) *sql.Tx {
	h.t.Helper()
	tx, err := h.db.BeginTx(h.ctx, nil)
	if err != nil {
		h.t.Fatalf("begin: %v", err)
	}
	enqueue(tx)
	return tx
}

func (h *pickupHarness) enqueueTx(tx *sql.Tx, queue string) {
	h.t.Helper()
	if _, err := h.q.EnqueueTx(h.ctx, tx, EnqueueParams{QueueName: queue, Payload: []byte(`{}`)}); err != nil {
		h.t.Fatalf("enqueue tx: %v", err)
	}
}

func (w *countingWorker) requireCalls(t *testing.T, n int32) {
	t.Helper()
	requireEventually(t, 3*time.Second, 10*time.Millisecond, func() bool {
		return w.calls.Load() == n
	})
}

func (w *countingWorker) requireCallsStay(t *testing.T, n int32) {
	t.Helper()
	time.Sleep(300 * time.Millisecond)
	if got := w.calls.Load(); got != n {
		t.Fatalf("expected %d handled jobs, got %d", n, got)
	}
}

func TestOnEnqueueClaimsANewJobWithoutRescanning(t *testing.T) {
	h := newPickupHarness(t)
	w := h.newWorker("new_job", 10)
	h.start(w)
	h.enqueue(w.queue)
	w.requireCalls(t, 1)
}

func TestOnEnqueueClaimsJobsEnqueuedBeforeTheWorkerStarted(t *testing.T) {
	h := newPickupHarness(t)
	w := h.newWorker("before_start", 10)
	h.enqueue(w.queue)
	h.start(w)
	w.requireCalls(t, 1)
}

func TestOnEnqueueWaitsForTheEnqueueingCommit(t *testing.T) {
	h := newPickupHarness(t)
	w := h.newWorker("commit", 10)
	h.start(w)
	tx := h.inTx(func(tx *sql.Tx) { h.enqueueTx(tx, w.queue) })
	w.requireCallsStay(t, 0)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	w.requireCalls(t, 1)
}

func TestOnEnqueueIgnoresRolledBackJobs(t *testing.T) {
	h := newPickupHarness(t)
	w := h.newWorker("rollback", 10)
	h.start(w)
	tx := h.inTx(func(tx *sql.Tx) { h.enqueueTx(tx, w.queue) })
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	w.requireCallsStay(t, 0)
	h.enqueue(w.queue)
	w.requireCalls(t, 1)
}

func TestOnEnqueueDrainsMoreThanOneBatch(t *testing.T) {
	h := newPickupHarness(t)
	w := h.newWorker("burst", 2)
	h.start(w)
	tx := h.inTx(func(tx *sql.Tx) {
		for range 7 {
			h.enqueueTx(tx, w.queue)
		}
	})
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	w.requireCalls(t, 7)
}

func TestOnEnqueueClaimsReplayedJobs(t *testing.T) {
	h := newPickupHarness(t)
	w := h.newWorker("replay", 10)
	h.start(w)
	id := h.enqueue(w.queue)
	w.requireCalls(t, 1)
	requireEventually(t, 3*time.Second, 10*time.Millisecond, func() bool {
		job, err := h.q.GetJob(h.ctx, id)
		return err == nil && job.Status == StatusDone
	})
	if err := h.q.ReplayJob(h.ctx, id); err != nil {
		t.Fatalf("replay: %v", err)
	}
	w.requireCalls(t, 2)
}

func TestOnEnqueueClaimsJobsRequeuedByAnotherReplicasReaper(t *testing.T) {
	h := newPickupHarness(t)
	w := h.newWorker("reaped", 10)
	id := h.enqueue(w.queue)
	if _, found, err := h.q.Claim(h.ctx, w.queue, "crashed-replica"); err != nil || !found {
		t.Fatalf("claim as crashed replica: found=%v err=%v", found, err)
	}
	h.start(w)
	if _, err := h.db.ExecContext(h.ctx,
		"UPDATE pgqueue_jobs SET claimed_at = NOW() - interval '1 hour' WHERE id = $1", id); err != nil {
		t.Fatalf("age claim: %v", err)
	}
	if _, err := h.q.ReapStuckJobs(h.ctx, time.Minute); err != nil {
		t.Fatalf("reap: %v", err)
	}
	w.requireCalls(t, 1)
}

func TestOnEnqueueRecoversJobsMissedWhileTheListenerWasDown(t *testing.T) {
	h := newPickupHarness(t)
	w := h.newWorker("missed", 10)
	var connects atomic.Int32
	reconnecting := make(chan struct{})
	reconnect := make(chan struct{})
	h.q.notifier.beforeConnect = func() {
		if connects.Add(1) == 2 {
			close(reconnecting)
			<-reconnect
		}
	}
	h.start(w)

	var terminated bool
	if err := h.db.QueryRowContext(h.ctx,
		"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query = 'LISTEN "+NotifyChannel+"'").
		Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate listener: terminated=%v err=%v", terminated, err)
	}
	<-reconnecting
	h.enqueue(w.queue)
	w.requireCallsStay(t, 0)
	close(reconnect)
	w.requireCalls(t, 1)
}

func TestWorkersOfOneClientShareOneListener(t *testing.T) {
	h := newPickupHarness(t)
	first := h.newWorker("shared_first", 10)
	second := h.newWorker("shared_second", 10)
	h.start(first)
	h.start(second)
	if got := h.listeners(); got != 1 {
		t.Fatalf("expected 1 listener, got %d", got)
	}
	h.enqueue(second.queue)
	second.requireCalls(t, 1)
	first.requireCallsStay(t, 0)
	h.enqueue(first.queue)
	first.requireCalls(t, 1)
}

func TestOnEnqueueListensOutsideTheConnectionPool(t *testing.T) {
	h := newPickupHarness(t)
	h.db.SetMaxOpenConns(1)
	w := h.newWorker("one_connection", 10)
	h.start(w)
	h.enqueue(w.queue)
	w.requireCalls(t, 1)
}

func TestOnEnqueueStopsListeningWithTheLastWorker(t *testing.T) {
	h := newPickupHarness(t)
	stopFirst := h.start(h.newWorker("stop_first", 10))
	stopSecond := h.start(h.newWorker("stop_second", 10))
	stopFirst()
	if got := h.listeners(); got != 1 {
		t.Fatalf("expected the listener to stay for the remaining worker, got %d", got)
	}
	stopSecond()
	requireEventually(t, 3*time.Second, 10*time.Millisecond, func() bool {
		return h.listeners() == 0
	})
}

func TestOnEnqueueMatchesQueueNamesLongerThanTheNotifyLimit(t *testing.T) {
	h := newPickupHarness(t)
	w := h.newWorker(strings.Repeat("é", 9000), 10)
	h.start(w)
	h.enqueue(w.queue)
	w.requireCalls(t, 1)
}

func TestNilNotificationWakesEveryWorker(t *testing.T) {
	n := newNotifier(nil)
	first := &subscription{keys: map[string]struct{}{"a": {}}, wake: make(chan struct{}, 1)}
	second := &subscription{keys: map[string]struct{}{"b": {}}, wake: make(chan struct{}, 1)}
	n.subs[first], n.subs[second] = struct{}{}, struct{}{}
	n.dispatch(nil)
	for _, sub := range []*subscription{first, second} {
		select {
		case <-sub.wake:
		default:
			t.Fatal("a subscriber was not woken")
		}
	}
}

func TestPollEveryWorkerDoesNotListen(t *testing.T) {
	h := newPickupHarness(t)
	registry := NewHandlerRegistry()
	if err := registry.Register("polled", func(context.Context, Job) error { return nil }); err != nil {
		t.Fatalf("register: %v", err)
	}
	worker, err := NewWorker(h.q, registry, WorkerConfig{Pickup: PollEvery(20 * time.Millisecond)})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	h.start(&countingWorker{Worker: worker})
	if got := h.listeners(); got != 0 {
		t.Fatalf("expected no listener, got %d", got)
	}
}

func openWithoutConnecting(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", "postgres://unused")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestWorkerRefusesBothPickupAndPollInterval(t *testing.T) {
	q, err := New(openWithoutConnecting(t))
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	_, err = NewWorker(q, NewHandlerRegistry(), WorkerConfig{Pickup: OnEnqueue(), PollInterval: time.Second})
	if !errors.Is(err, ErrInvalidWorkerConfig) {
		t.Fatalf("expected ErrInvalidWorkerConfig, got %v", err)
	}
}

type driverWithoutNotifications struct{}

func (driverWithoutNotifications) Open(string) (driver.Conn, error) {
	return nil, errors.New("not used")
}

func init() {
	sql.Register("pgkit-without-notifications", driverWithoutNotifications{})
}

func TestOnEnqueueRefusesDriversWithoutNotifications(t *testing.T) {
	db, err := sql.Open("pgkit-without-notifications", "")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	_, err = NewWorker(q, NewHandlerRegistry(), WorkerConfig{Pickup: OnEnqueue()})
	if !errors.Is(err, ErrNotificationsUnsupported) {
		t.Fatalf("expected ErrNotificationsUnsupported, got %v", err)
	}
}
