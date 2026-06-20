package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// ---------------------------------------------------------------------------
// Lifecycle: enqueue -> claim -> ack
// ---------------------------------------------------------------------------

func TestQueueLifecycle(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	jobID, err := q.Enqueue(ctx, EnqueueParams{
		QueueName:   "emails",
		Payload:     []byte(`{"to":"a@b.com"}`),
		MaxAttempts: 3,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	job, found, err := q.Claim(ctx, "emails", "worker-1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if !found || job == nil || job.ID != jobID {
		t.Fatalf("unexpected claimed job: found=%v job=%+v", found, job)
	}

	if err := q.Ack(ctx, job.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	jobs, err := q.ListJobs(ctx, ListJobsParams{Limit: 10})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if jobs[0].Status != StatusDone {
		t.Fatalf("expected done status, got %s", jobs[0].Status)
	}
}

// ---------------------------------------------------------------------------
// Retry then fail
// ---------------------------------------------------------------------------

func TestQueueRetryThenFail(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	_, err = q.Enqueue(ctx, EnqueueParams{
		QueueName:   "webhooks",
		Payload:     []byte("payload"),
		MaxAttempts: 2,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	job, found, err := q.Claim(ctx, "webhooks", "worker-1")
	if err != nil || !found {
		t.Fatalf("claim #1 failed: %v found=%v", err, found)
	}

	if err := q.Retry(ctx, job.ID, 0, errSample("temporary")); err != nil {
		t.Fatalf("retry: %v", err)
	}

	job, found, err = q.Claim(ctx, "webhooks", "worker-1")
	if err != nil || !found {
		t.Fatalf("claim #2 failed: %v found=%v", err, found)
	}
	if job.Attempts != 2 {
		t.Fatalf("expected attempts=2, got %d", job.Attempts)
	}

	if err := q.Fail(ctx, job.ID, errSample("permanent")); err != nil {
		t.Fatalf("fail: %v", err)
	}

	jobs, err := q.ListJobs(ctx, ListJobsParams{Limit: 1})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if jobs[0].Status != StatusFailed {
		t.Fatalf("expected failed status, got %s", jobs[0].Status)
	}
}

// ---------------------------------------------------------------------------
// P0 #2: State guard failures
// ---------------------------------------------------------------------------

func TestAckFromNonProcessingFails(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	// Enqueue a job (status=pending)
	jobID, err := q.Enqueue(ctx, EnqueueParams{
		QueueName: "test",
		Payload:   []byte("x"),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Ack from pending should fail
	err = q.Ack(ctx, jobID)
	if !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}

	// Ack for non-existent job should fail
	err = q.Ack(ctx, 999999)
	if !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound for missing job, got %v", err)
	}
}

func TestRetryFromNonProcessingFails(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	jobID, err := q.Enqueue(ctx, EnqueueParams{
		QueueName: "test",
		Payload:   []byte("x"),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	err = q.Retry(ctx, jobID, 0, nil)
	if !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

func TestFailFromNonProcessingFails(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	jobID, err := q.Enqueue(ctx, EnqueueParams{
		QueueName: "test",
		Payload:   []byte("x"),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	err = q.Fail(ctx, jobID, errSample("nope"))
	if !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// P0 #3: Retry exhaustion -> auto-fail (zombie prevention)
// ---------------------------------------------------------------------------

func TestRetryExhaustionAutoFails(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	// max_attempts=1: first claim uses the only attempt
	_, err = q.Enqueue(ctx, EnqueueParams{
		QueueName:   "test",
		Payload:     []byte("x"),
		MaxAttempts: 1,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	job, found, err := q.Claim(ctx, "test", "w1")
	if err != nil || !found {
		t.Fatalf("claim: %v found=%v", err, found)
	}

	// Retry should auto-fail since attempts(1) >= max_attempts(1)
	if err := q.Retry(ctx, job.ID, 0, errSample("exhausted")); err != nil {
		t.Fatalf("retry: %v", err)
	}

	// Job should now be failed, not pending
	jobs, err := q.ListJobs(ctx, ListJobsParams{Limit: 1})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if jobs[0].Status != StatusFailed {
		t.Fatalf("expected failed after retry exhaustion, got %s", jobs[0].Status)
	}

	// Should NOT be claimable
	_, found, err = q.Claim(ctx, "test", "w2")
	if err != nil {
		t.Fatalf("claim after exhaustion: %v", err)
	}
	if found {
		t.Fatal("should not find a job after retry exhaustion")
	}
}

// ---------------------------------------------------------------------------
// P1 #7: Claim empty queue returns (nil, false, nil)
// ---------------------------------------------------------------------------

func TestClaimEmptyQueue(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	job, found, err := q.Claim(ctx, "empty-queue", "w1")
	if err != nil {
		t.Fatalf("claim empty: %v", err)
	}
	if found {
		t.Fatal("expected found=false for empty queue")
	}
	if job != nil {
		t.Fatal("expected nil job for empty queue")
	}
}

// ---------------------------------------------------------------------------
// P0 #1: ReapStuckJobs
// ---------------------------------------------------------------------------

func TestReapStuckJobs(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	// Create two jobs: one with retries left, one exhausted
	_, err = q.Enqueue(ctx, EnqueueParams{QueueName: "reap", Payload: []byte("a"), MaxAttempts: 3})
	if err != nil {
		t.Fatalf("enqueue a: %v", err)
	}
	_, err = q.Enqueue(ctx, EnqueueParams{QueueName: "reap", Payload: []byte("b"), MaxAttempts: 1})
	if err != nil {
		t.Fatalf("enqueue b: %v", err)
	}

	// Claim both
	jobA, found, err := q.Claim(ctx, "reap", "w1")
	if err != nil || !found {
		t.Fatalf("claim a: %v found=%v", err, found)
	}
	jobB, found, err := q.Claim(ctx, "reap", "w1")
	if err != nil || !found {
		t.Fatalf("claim b: %v found=%v", err, found)
	}

	// Backdate claimed_at to simulate stuck jobs
	_, err = db.ExecContext(ctx,
		`UPDATE pgqueue_jobs SET claimed_at = NOW() - INTERVAL '10 minutes' WHERE id IN ($1, $2)`,
		jobA.ID, jobB.ID,
	)
	if err != nil {
		t.Fatalf("backdate: %v", err)
	}

	// Reap with 5-minute timeout
	result, err := q.ReapStuckJobs(ctx, 5*time.Minute)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}

	if result.Requeued != 1 {
		t.Fatalf("expected 1 requeued, got %d", result.Requeued)
	}
	if result.Failed != 1 {
		t.Fatalf("expected 1 failed, got %d", result.Failed)
	}

	// Verify job A is pending again
	jobs, err := q.ListJobs(ctx, ListJobsParams{Limit: 10})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	statusMap := make(map[int64]JobStatus)
	for _, j := range jobs {
		statusMap[j.ID] = j.Status
	}
	if statusMap[jobA.ID] != StatusPending {
		t.Fatalf("job A expected pending, got %s", statusMap[jobA.ID])
	}
	if statusMap[jobB.ID] != StatusFailed {
		t.Fatalf("job B expected failed, got %s", statusMap[jobB.ID])
	}
}

// ---------------------------------------------------------------------------
// P0 #5: Concurrent claim correctness (no duplicate claims)
// ---------------------------------------------------------------------------

func TestConcurrentClaimNoDuplicates(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	const numJobs = 5
	const numWorkers = 20

	// Enqueue N jobs
	for i := range numJobs {
		_, err := q.Enqueue(ctx, EnqueueParams{
			QueueName: "concurrent",
			Payload:   fmt.Appendf(nil, "job-%d", i),
		})
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	// N workers race to claim
	var mu sync.Mutex
	claimed := make(map[int64]string) // jobID -> worker
	var wg sync.WaitGroup

	for w := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				job, found, err := q.Claim(ctx, "concurrent", fmt.Sprintf("w-%d", workerID))
				if err != nil {
					t.Errorf("worker %d claim error: %v", workerID, err)
					return
				}
				if !found {
					return // no more jobs
				}
				mu.Lock()
				if prev, dup := claimed[job.ID]; dup {
					t.Errorf("DUPLICATE CLAIM: job %d claimed by both %s and w-%d", job.ID, prev, workerID)
				}
				claimed[job.ID] = fmt.Sprintf("w-%d", workerID)
				mu.Unlock()
			}
		}(w)
	}

	wg.Wait()

	if len(claimed) != numJobs {
		t.Fatalf("expected %d unique claims, got %d", numJobs, len(claimed))
	}
}

// ---------------------------------------------------------------------------
// P1 #8: EnqueueTx
// ---------------------------------------------------------------------------

func TestEnqueueTx(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	// Enqueue within a transaction that we roll back
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	_, err = q.EnqueueTx(ctx, tx, EnqueueParams{
		QueueName: "tx-test",
		Payload:   []byte("should-not-persist"),
	})
	if err != nil {
		t.Fatalf("enqueue tx: %v", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	// Job should not exist
	jobs, err := q.ListJobs(ctx, ListJobsParams{Limit: 10, QueueName: "tx-test"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("expected 0 jobs after rollback, got %d", len(jobs))
	}

	// Enqueue within a committed transaction
	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}

	id, err := q.EnqueueTx(ctx, tx2, EnqueueParams{
		QueueName: "tx-test",
		Payload:   []byte("should-persist"),
	})
	if err != nil {
		t.Fatalf("enqueue tx2: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("commit tx2: %v", err)
	}

	jobs, err = q.ListJobs(ctx, ListJobsParams{Limit: 10, QueueName: "tx-test"})
	if err != nil {
		t.Fatalf("list after commit: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != id {
		t.Fatalf("expected 1 job with id=%d, got %d jobs", id, len(jobs))
	}
}

// ---------------------------------------------------------------------------
// P2 #10: Schema versioning (idempotent EnsureSchema)
// ---------------------------------------------------------------------------

func TestEnsureSchemaIdempotent(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	// Call EnsureSchema multiple times
	for i := range 3 {
		if err := q.EnsureSchema(ctx); err != nil {
			t.Fatalf("ensure schema iteration %d: %v", i, err)
		}
	}

	// Verify version is set
	var version int
	err = db.QueryRowContext(ctx,
		`SELECT value::int FROM pgqueue_meta WHERE key = 'schema_version'`,
	).Scan(&version)
	if err != nil {
		t.Fatalf("read version: %v", err)
	}
	if version != schemaVersion {
		t.Fatalf("expected version %d, got %d", schemaVersion, version)
	}
}

// ---------------------------------------------------------------------------
// P2 #11: Backoff helpers
// ---------------------------------------------------------------------------

func TestExponentialBackoff(t *testing.T) {
	base := 1 * time.Second
	cap := 30 * time.Second

	for attempt := 1; attempt <= 10; attempt++ {
		delay := ExponentialBackoff(base, attempt, cap)
		if delay < 0 {
			t.Fatalf("attempt %d: negative delay %v", attempt, delay)
		}
		if delay > cap {
			t.Fatalf("attempt %d: delay %v exceeds cap %v", attempt, delay, cap)
		}
	}

	// Edge case: attempt 0
	delay := ExponentialBackoff(base, 0, cap)
	if delay < 0 || delay > base {
		t.Fatalf("attempt 0: unexpected delay %v", delay)
	}
}

// ---------------------------------------------------------------------------
// P2 #12: Hooks
// ---------------------------------------------------------------------------

func TestHooksAreCalled(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	var events []EventKind
	var mu sync.Mutex
	hook := HookFunc(func(_ context.Context, kind EventKind, _ map[string]any) {
		mu.Lock()
		events = append(events, kind)
		mu.Unlock()
	})

	q, err := New(db, hook)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	_, err = q.Enqueue(ctx, EnqueueParams{QueueName: "hooks", Payload: []byte("x")})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	_, _, err = q.Claim(ctx, "hooks", "w1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	mu.Lock()
	if len(events) < 2 {
		t.Fatalf("expected at least 2 events, got %d", len(events))
	}
	if events[0] != EventEnqueue {
		t.Fatalf("expected first event=enqueue, got %s", events[0])
	}
	if events[1] != EventClaim {
		t.Fatalf("expected second event=claim, got %s", events[1])
	}
	mu.Unlock()
}

// ---------------------------------------------------------------------------
// P2 #13: Purge
// ---------------------------------------------------------------------------

func TestPurge(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	// Create and complete a job
	_, err = q.Enqueue(ctx, EnqueueParams{QueueName: "purge", Payload: []byte("old")})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	job, found, err := q.Claim(ctx, "purge", "w1")
	if err != nil || !found {
		t.Fatalf("claim: %v found=%v", err, found)
	}
	if err := q.Ack(ctx, job.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// Backdate the job
	_, err = db.ExecContext(ctx,
		`UPDATE pgqueue_jobs SET updated_at = NOW() - INTERVAL '2 hours' WHERE id = $1`, job.ID)
	if err != nil {
		t.Fatalf("backdate: %v", err)
	}

	// Purge done jobs older than 1 hour
	n, err := q.Purge(ctx, PurgeParams{Status: StatusDone, OlderThan: 1 * time.Hour})
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 purged, got %d", n)
	}

	// Verify gone
	jobs, err := q.ListJobs(ctx, ListJobsParams{Limit: 10})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("expected 0 jobs after purge, got %d", len(jobs))
	}
}

// ---------------------------------------------------------------------------
// P0 #4: DB NOW() for immediate jobs
// ---------------------------------------------------------------------------

func TestEnqueueUsesDBNow(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	// Enqueue with nil AvailableAt (should use DB NOW())
	id, err := q.Enqueue(ctx, EnqueueParams{
		QueueName: "now-test",
		Payload:   []byte("x"),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// The job should be immediately claimable
	job, found, err := q.Claim(ctx, "now-test", "w1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if !found || job.ID != id {
		t.Fatalf("expected to claim job %d immediately, found=%v", id, found)
	}
}

// ---------------------------------------------------------------------------
// P1 #6: ListJobs with filters
// ---------------------------------------------------------------------------

func TestListJobsWithFilters(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	// Create jobs in different queues
	_, _ = q.Enqueue(ctx, EnqueueParams{QueueName: "alpha", Payload: []byte("1")})
	_, _ = q.Enqueue(ctx, EnqueueParams{QueueName: "beta", Payload: []byte("2")})
	_, _ = q.Enqueue(ctx, EnqueueParams{QueueName: "alpha", Payload: []byte("3")})

	// Filter by queue
	jobs, err := q.ListJobs(ctx, ListJobsParams{Limit: 10, QueueName: "alpha"})
	if err != nil {
		t.Fatalf("list alpha: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("expected 2 alpha jobs, got %d", len(jobs))
	}

	// Filter by status
	jobs, err = q.ListJobs(ctx, ListJobsParams{Limit: 10, Status: StatusPending})
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	if len(jobs) != 3 {
		t.Fatalf("expected 3 pending jobs, got %d", len(jobs))
	}

	// Full-text search should filter safely (including SQL-like payload).
	_, _ = q.Enqueue(ctx, EnqueueParams{QueueName: "alpha", Payload: []byte("drop table users; --")})
	jobs, err = q.ListJobs(ctx, ListJobsParams{Limit: 10, Search: "drop table"})
	if err != nil {
		t.Fatalf("list search: %v", err)
	}
	if len(jobs) < 1 {
		t.Fatal("expected at least one search match")
	}

	// Count with same filters should be consistent.
	total, err := q.CountJobs(ctx, ListJobsParams{Search: "drop table"})
	if err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	if total < 1 {
		t.Fatalf("expected search total >= 1, got %d", total)
	}
}

func TestReplayAndDeleteJob(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	jobID, err := q.Enqueue(ctx, EnqueueParams{QueueName: "ops", Payload: []byte("x"), MaxAttempts: 1})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	job, found, err := q.Claim(ctx, "ops", "w1")
	if err != nil || !found {
		t.Fatalf("claim: %v found=%v", err, found)
	}
	if err := q.Fail(ctx, job.ID, errSample("fail once")); err != nil {
		t.Fatalf("fail: %v", err)
	}

	if err := q.ReplayJob(ctx, jobID); err != nil {
		t.Fatalf("replay: %v", err)
	}

	replayed, err := q.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("get replayed: %v", err)
	}
	if replayed.Status != StatusPending || replayed.Attempts != 0 {
		t.Fatalf("expected replayed pending with attempts=0, got status=%s attempts=%d", replayed.Status, replayed.Attempts)
	}

	if err := q.DeleteJob(ctx, jobID); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if _, err := q.GetJob(ctx, jobID); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("expected not found after delete, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Dashboard: auth + enqueue + CSRF
// ---------------------------------------------------------------------------

func TestDashboardBasicAuthAndEnqueue(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	t.Setenv("PGKIT_DASHBOARD_TOKEN", "super-secret")
	dash, err := NewDashboardFromEnv(q)
	if err != nil {
		t.Fatalf("new dashboard: %v", err)
	}

	srv := httptest.NewServer(dash.Handler())
	defer srv.Close()

	// No auth -> 401
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("get dashboard no auth: %v", err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// POST /enqueue with HX-Request header (CSRF mitigation)
	form := url.Values{}
	form.Set("queue", "ui")
	form.Set("payload", `{"hello":"world"}`)
	form.Set("max_attempts", "5")
	form.Set("delay_seconds", "0")

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/enqueue", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("new enqueue request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("HX-Request", "true") // CSRF token
	req.SetBasicAuth("admin", "super-secret")

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post enqueue: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// GET /fragment/jobs
	req, err = http.NewRequest(http.MethodGet, srv.URL+"/fragment/jobs", nil)
	if err != nil {
		t.Fatalf("new jobs request: %v", err)
	}
	req.SetBasicAuth("admin", "super-secret")

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get jobs fragment: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

func TestDashboardJSONAPIEnqueue(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	t.Setenv("PGKIT_DASHBOARD_TOKEN", "api-secret")
	dash, err := NewDashboardFromEnv(q)
	if err != nil {
		t.Fatalf("new dashboard: %v", err)
	}

	srv := httptest.NewServer(dash.Handler())
	defer srv.Close()

	body := strings.NewReader(`{"queue_name":"json-api","payload":{"k":"v"},"max_attempts":4}`)
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/jobs", body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("HX-Request", "true")
	req.SetBasicAuth("admin", "api-secret")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post /api/jobs: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}

	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if _, ok := out["id"]; !ok {
		t.Fatalf("expected id in response, got %+v", out)
	}
}

func TestDashboardSearchReplayDeleteFlow(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	id, err := q.Enqueue(ctx, EnqueueParams{QueueName: "admin", Payload: []byte("needle")})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	job, found, err := q.Claim(ctx, "admin", "w1")
	if err != nil || !found {
		t.Fatalf("claim: %v found=%v", err, found)
	}
	if err := q.Fail(ctx, job.ID, errSample("for replay")); err != nil {
		t.Fatalf("fail: %v", err)
	}

	t.Setenv("PGKIT_DASHBOARD_TOKEN", "ops-secret")
	dash, err := NewDashboardFromEnvWithOptions(q, DashboardOptions{Limit: 10})
	if err != nil {
		t.Fatalf("dashboard: %v", err)
	}
	srv := httptest.NewServer(dash.Handler())
	defer srv.Close()

	// Search fragment should include the row.
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/fragment/jobs?search=needle&limit=10", nil)
	if err != nil {
		t.Fatalf("request search: %v", err)
	}
	req.SetBasicAuth("admin", "ops-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("search request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on search, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// Replay via HTMX endpoint.
	req, err = http.NewRequest(http.MethodPost, fmt.Sprintf("%s/jobs/%d/replay", srv.URL, id), nil)
	if err != nil {
		t.Fatalf("request replay: %v", err)
	}
	req.Header.Set("HX-Request", "true")
	req.SetBasicAuth("admin", "ops-secret")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("replay request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on replay, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// Delete via HTMX endpoint.
	req, err = http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/jobs/%d", srv.URL, id), nil)
	if err != nil {
		t.Fatalf("request delete: %v", err)
	}
	req.Header.Set("HX-Request", "true")
	req.SetBasicAuth("admin", "ops-secret")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on delete, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

func TestDashboardDisableEnqueueAPI(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	t.Setenv("PGKIT_DASHBOARD_TOKEN", "disabled-secret")
	t.Setenv("PGKIT_DASHBOARD_ENABLE_API", "false")

	dash, err := NewDashboardFromEnv(q)
	if err != nil {
		t.Fatalf("new dashboard: %v", err)
	}

	srv := httptest.NewServer(dash.Handler())
	defer srv.Close()

	// HTMX enqueue endpoint should not exist
	form := url.Values{}
	form.Set("queue", "disabled")
	form.Set("payload", "x")
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/enqueue", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth("admin", "disabled-secret")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post /enqueue: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 404/405 when enqueue disabled, got %d", resp.StatusCode)
	}

	// JSON API endpoint should also not exist
	req, err = http.NewRequest(http.MethodPost, srv.URL+"/api/jobs", strings.NewReader(`{"queue_name":"x","payload":{}}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("admin", "disabled-secret")

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post /api/jobs: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 404/405 for /api/jobs when disabled, got %d", resp.StatusCode)
	}
}

func TestDashboardCSRFBlocksPlainPost(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	t.Setenv("PGKIT_DASHBOARD_TOKEN", "secret")
	dash, err := NewDashboardFromEnv(q)
	if err != nil {
		t.Fatalf("new dashboard: %v", err)
	}

	srv := httptest.NewServer(dash.Handler())
	defer srv.Close()

	// POST without HX-Request or matching Origin -> 403
	form := url.Values{}
	form.Set("queue", "csrf-test")
	form.Set("payload", "x")

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/enqueue", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth("admin", "secret")
	// No HX-Request, no Origin

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

func TestDashboardCSRFBlocksReplayAndDeleteWithoutHX(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	id, err := q.Enqueue(ctx, EnqueueParams{QueueName: "csrf", Payload: []byte("x")})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	job, found, err := q.Claim(ctx, "csrf", "w1")
	if err != nil || !found {
		t.Fatalf("claim: %v found=%v", err, found)
	}
	if err := q.Fail(ctx, job.ID, errSample("fail")); err != nil {
		t.Fatalf("fail: %v", err)
	}

	t.Setenv("PGKIT_DASHBOARD_TOKEN", "secret")
	dash, err := NewDashboardFromEnv(q)
	if err != nil {
		t.Fatalf("new dashboard: %v", err)
	}

	srv := httptest.NewServer(dash.Handler())
	defer srv.Close()

	replayReq, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/jobs/%d/replay", srv.URL, id), nil)
	if err != nil {
		t.Fatalf("new replay request: %v", err)
	}
	replayReq.SetBasicAuth("admin", "secret")
	replayResp, err := http.DefaultClient.Do(replayReq)
	if err != nil {
		t.Fatalf("replay call: %v", err)
	}
	if replayResp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected replay 403 without HX, got %d", replayResp.StatusCode)
	}
	_ = replayResp.Body.Close()

	deleteReq, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/jobs/%d", srv.URL, id), nil)
	if err != nil {
		t.Fatalf("new delete request: %v", err)
	}
	deleteReq.SetBasicAuth("admin", "secret")
	deleteResp, err := http.DefaultClient.Do(deleteReq)
	if err != nil {
		t.Fatalf("delete call: %v", err)
	}
	if deleteResp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected delete 403 without HX, got %d", deleteResp.StatusCode)
	}
	_ = deleteResp.Body.Close()
}

func TestDashboardConstantTimeTokenCompare(t *testing.T) {
	// Verify secureTokenEqual works correctly
	if !secureTokenEqual("abc", "abc") {
		t.Fatal("equal tokens should match")
	}
	if secureTokenEqual("abc", "def") {
		t.Fatal("different tokens should not match")
	}
	if secureTokenEqual("abc", "abcd") {
		t.Fatal("different length tokens should not match")
	}
}

// ---------------------------------------------------------------------------
// P2 #12: Hook events for all operations
// ---------------------------------------------------------------------------

func TestHooksFullLifecycle(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	var events []EventKind
	var mu sync.Mutex
	hook := HookFunc(func(_ context.Context, kind EventKind, _ map[string]any) {
		mu.Lock()
		events = append(events, kind)
		mu.Unlock()
	})

	q, err := New(db, hook)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("schema: %v", err)
	}

	// enqueue
	_, _ = q.Enqueue(ctx, EnqueueParams{QueueName: "h", Payload: []byte("x"), MaxAttempts: 2})
	// claim
	job, _, _ := q.Claim(ctx, "h", "w")
	// retry
	_ = q.Retry(ctx, job.ID, 0, nil)
	// claim again
	job, _, _ = q.Claim(ctx, "h", "w")
	// fail
	_ = q.Fail(ctx, job.ID, nil)

	mu.Lock()
	defer mu.Unlock()

	expected := []EventKind{EventEnqueue, EventClaim, EventRetry, EventClaim, EventFail}
	if len(events) != len(expected) {
		t.Fatalf("expected %d events, got %d: %v", len(expected), len(events), events)
	}
	for i, e := range expected {
		if events[i] != e {
			t.Fatalf("event[%d]: expected %s, got %s", i, e, events[i])
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type errSample string

func (e errSample) Error() string { return string(e) }

// createTestDB spins up a Postgres container and returns a connected *sql.DB.
func createTestDB(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()

	pg, connString := startPostgres(t, ctx)
	t.Cleanup(func() {
		_ = pg.Terminate(ctx)
	})

	db, err := sql.Open("pgx", connString)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
	})

	if err := waitForPing(ctx, db, 20*time.Second); err != nil {
		t.Fatalf("db ping: %v", err)
	}

	return db
}

func startPostgres(t *testing.T, ctx context.Context) (testcontainers.Container, string) {
	t.Helper()

	pg, err := postgres.Run(
		ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("pgkit_test"),
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

func waitForPing(ctx context.Context, db *sql.DB, timeout time.Duration) error {
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

// Ensure unused imports don't cause issues
var _ = atomic.Int32{}
