package queue

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// The tests in this file run the released pgkit queue (testdata/oldpgkit pins the
// version consumers deploy) as a separate process next to the current code, on
// one database. A rolling deploy (Docker Swarm start-first, rollback on failure)
// runs both versions at once, and a rollback runs the old one alone on the
// migrated schema.

// oldBuild is the released driver binary, built once per test process.
var oldBuild struct {
	once sync.Once
	dir  string
	path string
	err  error
}

func oldPgkitBinary(t *testing.T) string {
	t.Helper()
	oldBuild.once.Do(func() {
		goTool, err := exec.LookPath("go")
		if err != nil {
			goTool = filepath.Join(os.Getenv("GOROOT"), "bin", "go")
		}
		dir, err := os.MkdirTemp("", "oldpgkit")
		if err != nil {
			oldBuild.err = err
			return
		}
		oldBuild.dir = dir
		oldBuild.path = filepath.Join(dir, "oldpgkit")
		cmd := exec.Command(goTool, "build", "-o", oldBuild.path, ".")
		cmd.Dir = filepath.Join("testdata", "oldpgkit")
		cmd.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=-mod=readonly")
		if out, err := cmd.CombinedOutput(); err != nil {
			oldBuild.err = fmt.Errorf("build released pgkit driver: %w\n%s", err, out)
		}
	})
	if oldBuild.err != nil {
		t.Fatal(oldBuild.err)
	}
	return oldBuild.path
}

// oldPgkit is one running process of the released queue.
type oldPgkit struct {
	mu     sync.Mutex
	stdin  io.WriteCloser
	stdout *bufio.Scanner
	stderr *bytes.Buffer
}

type oldJob struct {
	ID          int64     `json:"id"`
	Queue       string    `json:"queue"`
	Status      string    `json:"status"`
	Attempts    int       `json:"attempts"`
	MaxAttempts int       `json:"max_attempts"`
	Claims      int       `json:"claims"`
	AvailableAt time.Time `json:"available_at"`
}

type oldReply struct {
	OK       bool     `json:"ok"`
	Error    string   `json:"error"`
	NotFound bool     `json:"not_found"`
	Busy     bool     `json:"busy"`
	ID       int64    `json:"id"`
	Found    bool     `json:"found"`
	Job      *oldJob  `json:"job"`
	Jobs     []oldJob `json:"jobs"`
	Count    int64    `json:"count"`
	Requeued int64    `json:"requeued"`
	Failed   int64    `json:"failed"`
	Done     int64    `json:"done"`
	Deleted  int64    `json:"deleted"`
}

// oldOp is one command for the released driver; see testdata/oldpgkit/main.go.
type oldOp map[string]any

func startOldPgkit(t *testing.T, dsn string) *oldPgkit {
	t.Helper()
	cmd := exec.Command(oldPgkitBinary(t), dsn)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start released pgkit driver: %v", err)
	}
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	old := &oldPgkit{stdin: stdin, stdout: scanner, stderr: stderr}
	t.Cleanup(func() {
		_ = stdin.Close()
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("released pgkit driver exited: %v\n%s", err, stderr)
			}
		case <-time.After(30 * time.Second):
			_ = cmd.Process.Kill()
			<-done
			t.Errorf("released pgkit driver did not stop\n%s", stderr)
		}
	})
	return old
}

func (o *oldPgkit) do(t *testing.T, op oldOp) oldReply {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	line, err := json.Marshal(op)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.stdin.Write(append(line, '\n')); err != nil {
		t.Fatalf("send %s: %v\n%s", line, err, o.stderr)
	}
	if !o.stdout.Scan() {
		t.Fatalf("no reply to %s: %v\n%s", line, o.stdout.Err(), o.stderr)
	}
	var r oldReply
	if err := json.Unmarshal(o.stdout.Bytes(), &r); err != nil {
		t.Fatalf("reply to %s: %v", line, err)
	}
	return r
}

// must runs op and fails the test unless the released code returned no error.
func (o *oldPgkit) must(t *testing.T, op oldOp) oldReply {
	t.Helper()
	r := o.do(t, op)
	if !r.OK {
		t.Fatalf("released pgkit %v: %s", op, r.Error)
	}
	return r
}

func (o *oldPgkit) enqueue(t *testing.T, queueName string, delay time.Duration, maxAttempts int) int64 {
	t.Helper()
	return o.must(t, oldOp{"op": "enqueue", "queue": queueName, "delay_ms": delay.Milliseconds(), "max_attempts": maxAttempts}).ID
}

func (o *oldPgkit) claim(t *testing.T, queueName string) oldJob {
	t.Helper()
	r := o.must(t, oldOp{"op": "claim", "queue": queueName})
	if !r.Found || r.Job == nil {
		t.Fatalf("released pgkit claim on %s found nothing", queueName)
	}
	return *r.Job
}

func (o *oldPgkit) claimNothing(t *testing.T, queueName string) {
	t.Helper()
	if r := o.must(t, oldOp{"op": "claim", "queue": queueName}); r.Found {
		t.Fatalf("released pgkit claimed job %d on %s, want none", r.Job.ID, queueName)
	}
}

func (o *oldPgkit) get(t *testing.T, id int64) oldJob {
	t.Helper()
	return *o.must(t, oldOp{"op": "get", "id": id}).Job
}

// legacyStatusSQL is the status each state must show to readers of the legacy
// column (otelcol queue receivers, echopoint queuestats). It is written out here,
// not taken from pgqueue_status_of_state, so a wrong mapping cannot pass.
const legacyStatusSQL = `CASE state
	WHEN 'available' THEN 'pending' WHEN 'scheduled' THEN 'pending' WHEN 'retryable' THEN 'pending'
	WHEN 'running' THEN 'processing' WHEN 'completed' THEN 'done'
	WHEN 'cancelled' THEN 'failed' WHEN 'discarded' THEN 'failed' END`

// schemaSnapshot describes pgqueue_jobs as the catalog sees it: columns, checks,
// indexes by oid (a rebuilt index differs), triggers and the pgqueue functions.
func schemaSnapshot(t *testing.T, d driverDB) string {
	t.Helper()
	var snapshot string
	if err := d.db.QueryRowContext(context.Background(), `
SELECT string_agg(x, E'\n' ORDER BY x) FROM (
    SELECT 'column ' || attname || ' ' || format_type(atttypid, atttypmod) || ' not null ' || attnotnull
    FROM pg_attribute WHERE attrelid = 'pgqueue_jobs'::regclass AND attnum > 0 AND NOT attisdropped
    UNION ALL
    SELECT 'constraint ' || conname || ' valid ' || convalidated || ' ' || pg_get_constraintdef(oid)
    FROM pg_constraint WHERE conrelid = 'pgqueue_jobs'::regclass
    UNION ALL
    SELECT 'index ' || c.relname || ' oid ' || c.oid || ' valid ' || i.indisvalid
    FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE i.indrelid = 'pgqueue_jobs'::regclass
    UNION ALL
    SELECT 'trigger ' || pg_get_triggerdef(oid) FROM pg_trigger WHERE tgrelid = 'pgqueue_jobs'::regclass AND NOT tgisinternal
    UNION ALL
    SELECT 'function ' || proname || ' ' || md5(prosrc) FROM pg_proc WHERE proname LIKE 'pgqueue\_%'
) AS catalog(x)`).Scan(&snapshot); err != nil {
		t.Fatalf("schema snapshot: %v", err)
	}
	return snapshot
}

func schemaVersionOf(t *testing.T, d driverDB) int {
	t.Helper()
	return queryCount(t, d, `SELECT value::int FROM pgqueue_meta WHERE key = 'schema_version'`)
}

// holdSchemaCompletion keeps CompleteSchema (and the leader's schema completer)
// from running until the returned function is called, so a test can work on a
// schema that EnsureSchema expanded but nothing completed.
func holdSchemaCompletion(t *testing.T, d driverDB) (release func()) {
	t.Helper()
	ctx := context.Background()
	conn, err := d.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock(hashtextextended($1, 0))`, completionLockKey); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	release = func() {
		once.Do(func() {
			_, _ = conn.ExecContext(ctx, `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, completionLockKey)
			_ = conn.Close()
		})
	}
	t.Cleanup(release)
	return release
}

// ageRow moves a timestamp column of the given jobs into the past. It sets neither
// state nor status, so the sync trigger does not run.
func ageRow(t *testing.T, d driverDB, column string, by time.Duration, ids ...int64) {
	t.Helper()
	for _, id := range ids {
		d.exec(t, `UPDATE pgqueue_jobs SET `+column+` = NOW() - make_interval(secs => $2) WHERE id = $1`, id, by.Seconds())
	}
}

func jobExists(t *testing.T, d driverDB, id int64) bool {
	t.Helper()
	return queryCount(t, d, `SELECT count(*) FROM pgqueue_jobs WHERE id = $1`, id) == 1
}

func newEnqueueAt(t *testing.T, q *Client, queueName string, at time.Time) int64 {
	t.Helper()
	return mustEnqueue(t, q, EnqueueParams{QueueName: queueName, AvailableAt: &at})
}

func TestReleasedBinaryAgainstSchemaV5(t *testing.T) {
	t.Run("expanded by EnsureSchema", func(t *testing.T) { testReleasedBinaryAgainstSchemaV5(t, false) })
	t.Run("completed by CompleteSchema", func(t *testing.T) { testReleasedBinaryAgainstSchemaV5(t, true) })
}

func testReleasedBinaryAgainstSchemaV5(t *testing.T, complete bool) {
	ctx := context.Background()
	d := freshDatabase(t, "pgx")
	old := startOldPgkit(t, d.dsn)

	// The released binary creates its schema (v4) and leaves jobs in every legacy
	// status before the new code starts.
	old.must(t, oldOp{"op": "ensure_schema"})
	if v := schemaVersionOf(t, d); v != 4 {
		t.Fatalf("released binary migrated to v%d, want v4", v)
	}
	d.exec(t, `CREATE TABLE compat_processed (
		job_id BIGINT NOT NULL, binary_name TEXT NOT NULL, attempt INT NOT NULL, succeeded BOOLEAN NOT NULL)`)
	prePending := old.enqueue(t, "pre-pending", 0, 0)
	old.enqueue(t, "pre-running", 0, 0)
	preRunning := old.claim(t, "pre-running")
	old.enqueue(t, "pre-parked", 0, 0)
	preParked := old.claim(t, "pre-parked")
	old.must(t, oldOp{"op": "snooze", "id": preParked.ID, "claims": preParked.Claims})
	old.enqueue(t, "pre-done", 0, 0)
	preDone := old.claim(t, "pre-done").ID
	old.must(t, oldOp{"op": "ack", "id": preDone})
	old.enqueue(t, "pre-failed", 0, 0)
	preFailed := old.claim(t, "pre-failed").ID
	old.must(t, oldOp{"op": "fail", "id": preFailed})
	ageRow(t, d, "updated_at", 48*time.Hour, preFailed)
	insertLegacyFinished(t, d, 10)

	// The new code starts. With inlineRows 0, EnsureSchema leaves the table scans
	// to CompleteSchema, as it does on a production-sized table.
	release := holdSchemaCompletion(t, d)
	q := d.bareClient(t)
	q.schema.inlineRows = 0
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	if complete {
		release()
		if err := q.CompleteSchema(ctx); err != nil {
			t.Fatalf("complete schema: %v", err)
		}
		requireSchemaComplete(t, d)
	} else {
		progress := schemaProgressOf(t, d)
		if !progress.expanded || progress.stateRequired || progress.stateChecked || progress.complete() {
			t.Fatalf("progress after EnsureSchema = %+v, want expanded only", progress)
		}
		if nullStates(t, d) == 0 {
			t.Fatal("EnsureSchema backfilled finished jobs; the test needs some left for CompleteSchema")
		}
	}
	d.requireState(t, prePending, StateAvailable)
	d.requireState(t, preRunning.ID, StateRunning)
	d.requireState(t, preParked.ID, StateScheduled)

	t.Run("released EnsureSchema leaves schema v5 alone", func(t *testing.T) {
		before := schemaSnapshot(t, d)
		// An index build CONCURRENTLY holds SHARE UPDATE EXCLUSIVE for its whole run.
		// Neither binary's EnsureSchema may queue a stronger lock behind it.
		lockTx, err := d.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = lockTx.Rollback() }()
		if _, err := lockTx.ExecContext(ctx, `LOCK TABLE pgqueue_jobs IN SHARE UPDATE EXCLUSIVE MODE`); err != nil {
			t.Fatal(err)
		}
		old.must(t, oldOp{"op": "ensure_schema", "timeout_ms": 5000})
		if v := schemaVersionOf(t, d); v != schemaVersion {
			t.Fatalf("schema version after the released EnsureSchema = %d, want %d", v, schemaVersion)
		}
		startCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := q.EnsureSchema(startCtx); err != nil {
			t.Fatalf("ensure schema after the released one, during an index build: %v", err)
		}
		if err := lockTx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if after := schemaSnapshot(t, d); after != before {
			t.Fatalf("schema changed:\nbefore:\n%s\nafter:\n%s", before, after)
		}
	})

	t.Run("released enqueue gets a state from the trigger", func(t *testing.T) {
		due := old.enqueue(t, "old-enqueue", 0, 0)
		delayed := old.enqueue(t, "old-enqueue", time.Hour, 0)
		d.requireState(t, due, StateAvailable)
		d.requireState(t, delayed, StateScheduled)
		if complete && queryCount(t, d, `SELECT count(*) FROM pg_attribute
			WHERE attrelid = 'pgqueue_jobs'::regclass AND attname = 'state' AND attnotnull`) != 1 {
			t.Fatal("state is not NOT NULL after CompleteSchema")
		}
	})

	t.Run("released claim, heartbeat, lock and ack", func(t *testing.T) {
		id := old.enqueue(t, "old-ack", 0, 0)
		job := old.claim(t, "old-ack")
		if job.ID != id || job.Status != "processing" || job.Attempts != 1 {
			t.Fatalf("released claim = %+v, want job %d processing on attempt 1", job, id)
		}
		d.requireState(t, id, StateRunning)
		if queryCount(t, d, `SELECT count(*) FROM pgqueue_jobs WHERE id = $1 AND lease_expires_at IS NULL`, id) != 1 {
			t.Fatal("released claim got a lease")
		}
		ageRow(t, d, "claimed_at", 10*time.Minute, id)
		old.must(t, oldOp{"op": "heartbeat", "id": id, "claims": job.Claims})
		if queryCount(t, d, `SELECT count(*) FROM pgqueue_jobs WHERE id = $1 AND claimed_at > NOW() - interval '1 minute'`, id) != 1 {
			t.Fatal("released heartbeat did not extend the claim")
		}
		old.must(t, oldOp{"op": "lock_claim", "id": id, "claims": job.Claims})
		old.must(t, oldOp{"op": "ack", "id": id})
		d.requireState(t, id, StateCompleted)
		if r := old.do(t, oldOp{"op": "ack", "id": id}); r.OK || !r.NotFound {
			t.Fatalf("second released ack = %+v, want ErrJobNotFound", r)
		}

		old.must(t, oldOp{"op": "ack", "id": preRunning.ID})
		d.requireState(t, preRunning.ID, StateCompleted)
	})

	t.Run("released retry and fail", func(t *testing.T) {
		later := old.enqueue(t, "old-retry-later", 0, 0)
		old.claim(t, "old-retry-later")
		old.must(t, oldOp{"op": "retry", "id": later, "delay_ms": time.Hour.Milliseconds()})
		d.requireState(t, later, StateRetryable)
		old.claimNothing(t, "old-retry-later")

		now := old.enqueue(t, "old-retry-now", 0, 0)
		old.claim(t, "old-retry-now")
		old.must(t, oldOp{"op": "retry", "id": now, "delay_ms": 0})
		d.requireState(t, now, StateAvailable)
		if job := old.claim(t, "old-retry-now"); job.ID != now || job.Attempts != 2 {
			t.Fatalf("released claim after retry = %+v, want job %d on attempt 2", job, now)
		}
		old.must(t, oldOp{"op": "ack", "id": now})

		exhausted := old.enqueue(t, "old-retry-exhausted", 0, 1)
		old.claim(t, "old-retry-exhausted")
		old.must(t, oldOp{"op": "retry", "id": exhausted, "delay_ms": 0})
		d.requireState(t, exhausted, StateDiscarded)

		failed := old.enqueue(t, "old-fail", 0, 0)
		old.claim(t, "old-fail")
		old.must(t, oldOp{"op": "fail", "id": failed})
		d.requireState(t, failed, StateDiscarded)
	})

	t.Run("released reap", func(t *testing.T) {
		oldClaimed := old.enqueue(t, "old-reap", 0, 0)
		old.claim(t, "old-reap")
		newClaimed := mustEnqueue(t, q, EnqueueParams{QueueName: "old-reap-leased"})
		mustClaim(t, q, "old-reap-leased", time.Hour)
		exhausted := old.enqueue(t, "old-reap-exhausted", 0, 1)
		old.claim(t, "old-reap-exhausted")
		ageRow(t, d, "claimed_at", 10*time.Minute, oldClaimed, newClaimed, exhausted)

		r := old.must(t, oldOp{
			"op": "reap", "visibility_ms": time.Minute.Milliseconds(),
			"queues": []string{"old-reap", "old-reap-leased", "old-reap-exhausted"},
		})
		if r.Requeued != 2 || r.Failed != 1 {
			t.Fatalf("released reap = requeued %d failed %d, want 2 and 1", r.Requeued, r.Failed)
		}
		d.requireState(t, oldClaimed, StateAvailable)
		d.requireState(t, newClaimed, StateAvailable)
		d.requireState(t, exhausted, StateDiscarded)
		if queryCount(t, d, `SELECT count(*) FROM pgqueue_jobs WHERE id = $1 AND lease_expires_at IS NULL`, newClaimed) != 1 {
			t.Fatal("a job the released binary reaped kept its lease")
		}
		if job := mustClaim(t, q, "old-reap-leased", time.Hour); job.ID != newClaimed {
			t.Fatalf("new claim after the released reap took %d, want %d", job.ID, newClaimed)
		}
	})

	t.Run("released snooze and wake", func(t *testing.T) {
		id := old.enqueue(t, "old-park", 0, 0)
		job := old.claim(t, "old-park")
		old.must(t, oldOp{"op": "snooze", "id": id, "claims": job.Claims})
		d.requireState(t, id, StateScheduled)
		if parked := old.get(t, id); !parked.AvailableAt.Equal(ParkedUntil) || parked.Attempts != 0 {
			t.Fatalf("released snooze = %+v, want parked with the attempt refunded", parked)
		}
		old.claimNothing(t, "old-park")
		if _, found, err := q.Claim(ctx, "old-park", "new"); err != nil || found {
			t.Fatalf("new claim of a parked job: found %v err %v", found, err)
		}
		old.must(t, oldOp{"op": "wake", "id": id})
		if woken := old.claim(t, "old-park"); woken.ID != id {
			t.Fatalf("released claim after wake took %d, want %d", woken.ID, id)
		}
		old.must(t, oldOp{"op": "ack", "id": id})

		// A job parked before the migration, woken by the released binary, is due
		// for the new claim even though only available_at changed.
		old.must(t, oldOp{"op": "wake", "id": preParked.ID})
		if woken := mustClaim(t, q, "pre-parked", time.Minute); woken.ID != preParked.ID {
			t.Fatalf("new claim took %d, want the woken job %d", woken.ID, preParked.ID)
		}
		if err := q.Ack(ctx, preParked.ID); err != nil {
			t.Fatalf("ack: %v", err)
		}
	})

	t.Run("released replay", func(t *testing.T) {
		old.must(t, oldOp{"op": "replay", "id": preDone})
		d.requireState(t, preDone, StateAvailable)
		if job := mustClaim(t, q, "pre-done", time.Minute); job.ID != preDone {
			t.Fatalf("new claim took %d, want the replayed job %d", job.ID, preDone)
		}
		if err := q.Ack(ctx, preDone); err != nil {
			t.Fatalf("ack: %v", err)
		}

		completed := mustEnqueue(t, q, EnqueueParams{QueueName: "old-replay-completed"})
		mustClaim(t, q, "old-replay-completed", time.Minute)
		if err := q.Ack(ctx, completed); err != nil {
			t.Fatalf("ack: %v", err)
		}
		discarded := mustEnqueue(t, q, EnqueueParams{QueueName: "old-replay-discarded"})
		mustClaim(t, q, "old-replay-discarded", time.Minute)
		if err := q.Fail(ctx, discarded, errors.New("boom")); err != nil {
			t.Fatalf("fail: %v", err)
		}
		cancelled := mustEnqueue(t, q, EnqueueParams{QueueName: "old-replay-cancelled"})
		if _, err := q.Cancel(ctx, cancelled); err != nil {
			t.Fatalf("cancel: %v", err)
		}
		for _, id := range []int64{completed, discarded, cancelled} {
			old.must(t, oldOp{"op": "replay", "id": id})
			d.requireState(t, id, StateAvailable)
		}

		// The released claim does not know cancel_requested_at, so a replay must not
		// leave the cancel request of the job's earlier life on it: the new rescuer
		// would cancel the replayed job instead of retrying it.
		job := old.claim(t, "old-replay-cancelled")
		if job.ID != cancelled {
			t.Fatalf("released claim took %d, want %d", job.ID, cancelled)
		}
		got, err := q.GetJob(ctx, cancelled)
		if err != nil {
			t.Fatalf("get job: %v", err)
		}
		if got.State != StateRunning || got.CancelRequestedAt.Valid {
			t.Fatalf("replayed cancelled job after released claim = state %s cancel request %v, want running without one",
				got.State, got.CancelRequestedAt)
		}
		old.must(t, oldOp{"op": "ack", "id": cancelled})
		d.requireState(t, cancelled, StateCompleted)
	})

	t.Run("released delete", func(t *testing.T) {
		finished := old.enqueue(t, "old-delete", 0, 0)
		old.claim(t, "old-delete")
		old.must(t, oldOp{"op": "ack", "id": finished})
		old.must(t, oldOp{"op": "delete", "id": finished})
		if jobExists(t, d, finished) {
			t.Fatal("released delete left the job")
		}
		running := mustEnqueue(t, q, EnqueueParams{QueueName: "old-delete-running"})
		mustClaim(t, q, "old-delete-running", time.Minute)
		if r := old.do(t, oldOp{"op": "delete", "id": running}); r.OK || !r.Busy {
			t.Fatalf("released delete of a running job = %+v, want ErrJobBusy", r)
		}
	})

	t.Run("released retention", func(t *testing.T) {
		// Each job finishes on a queue of its own, so its claim cannot take another.
		var seq int
		finish := func(queueName string, settle func(id int64)) int64 {
			seq++
			queueName = fmt.Sprintf("%s-%d", queueName, seq)
			id := mustEnqueue(t, q, EnqueueParams{QueueName: queueName})
			if job := mustClaim(t, q, queueName, time.Minute); job.ID != id {
				t.Fatalf("claim took %d, want %d", job.ID, id)
			}
			settle(id)
			return id
		}
		completed := func(queueName string) int64 {
			return finish(queueName, func(id int64) {
				if err := q.Ack(ctx, id); err != nil {
					t.Fatalf("ack: %v", err)
				}
			})
		}
		discarded := func(queueName string) int64 {
			return finish(queueName, func(id int64) {
				if err := q.Fail(ctx, id, errors.New("boom")); err != nil {
					t.Fatalf("fail: %v", err)
				}
			})
		}
		cancelled := func(queueName string) int64 {
			id := mustEnqueue(t, q, EnqueueParams{QueueName: queueName})
			if _, err := q.Cancel(ctx, id); err != nil {
				t.Fatalf("cancel: %v", err)
			}
			return id
		}
		oldDone := func(queueName string) int64 {
			id := old.enqueue(t, queueName, 0, 0)
			old.claim(t, queueName)
			old.must(t, oldOp{"op": "ack", "id": id})
			return id
		}

		// Purge by legacy status: failed covers discarded and cancelled jobs.
		purgeFailed := []int64{discarded("old-purge"), cancelled("old-purge")}
		purgeKept := []int64{completed("old-purge"), mustEnqueue(t, q, EnqueueParams{QueueName: "old-purge"})}
		ageRow(t, d, "updated_at", 48*time.Hour, append(purgeFailed, purgeKept...)...)
		freshFailed := discarded("old-purge")
		r := old.must(t, oldOp{"op": "purge", "status": "failed", "older_than_ms": time.Hour.Milliseconds()})
		if r.Deleted < int64(len(purgeFailed)) {
			t.Fatalf("released purge deleted %d jobs, want at least %d", r.Deleted, len(purgeFailed))
		}
		for _, id := range purgeFailed {
			if jobExists(t, d, id) {
				t.Fatalf("released purge kept expired failed job %d", id)
			}
		}
		for _, id := range append(purgeKept, freshFailed) {
			if !jobExists(t, d, id) {
				t.Fatalf("released purge deleted job %d", id)
			}
		}

		// PurgeFinished deletes expired done and failed jobs, whoever finished them
		// and whether or not CompleteSchema has backfilled their state yet.
		expired := []int64{
			completed("old-retention"), discarded("old-retention"), cancelled("old-retention"), oldDone("old-retention"),
		}
		ageRow(t, d, "updated_at", 48*time.Hour, expired...)
		expired = append(expired, preFailed)
		kept := []int64{completed("old-retention"), discarded("old-retention"), mustEnqueue(t, q, EnqueueParams{QueueName: "old-retention"})}
		live := mustEnqueue(t, q, EnqueueParams{QueueName: "old-retention"})
		ageRow(t, d, "updated_at", 48*time.Hour, live)
		kept = append(kept, live)
		legacyBefore := queryCount(t, d, `SELECT count(*) FROM pgqueue_jobs WHERE queue_name = 'legacy-done'`)

		res := old.must(t, oldOp{
			"op": "purge_finished", "keep_done_ms": time.Hour.Milliseconds(), "keep_failed_ms": time.Hour.Milliseconds(),
		})
		if res.Done+res.Failed < int64(len(expired)+legacyBefore) {
			t.Fatalf("released PurgeFinished deleted %d done and %d failed, want at least %d",
				res.Done, res.Failed, len(expired)+legacyBefore)
		}
		for _, id := range expired {
			if jobExists(t, d, id) {
				t.Fatalf("released PurgeFinished kept expired job %d", id)
			}
		}
		for _, id := range kept {
			if !jobExists(t, d, id) {
				t.Fatalf("released PurgeFinished deleted job %d", id)
			}
		}
		if n := queryCount(t, d, `SELECT count(*) FROM pgqueue_jobs WHERE queue_name = 'legacy-done'`); n != 0 {
			t.Fatalf("released PurgeFinished kept %d expired jobs from before the migration", n)
		}
	})

	t.Run("released reads see the legacy status", func(t *testing.T) {
		available := mustEnqueue(t, q, EnqueueParams{QueueName: "old-read"})
		scheduled := newEnqueueAt(t, q, "old-read", time.Now().Add(time.Hour))
		running := mustEnqueue(t, q, EnqueueParams{QueueName: "old-read-running"})
		mustClaim(t, q, "old-read-running", time.Minute)
		cancelled := mustEnqueue(t, q, EnqueueParams{QueueName: "old-read"})
		if _, err := q.Cancel(ctx, cancelled); err != nil {
			t.Fatalf("cancel: %v", err)
		}
		for id, want := range map[int64]string{available: "pending", scheduled: "pending", running: "processing", cancelled: "failed"} {
			if got := old.get(t, id).Status; got != want {
				t.Fatalf("released GetJob(%d).Status = %s, want %s", id, got, want)
			}
		}
		if n := old.must(t, oldOp{"op": "count", "queue": "old-read", "status": "pending"}).Count; n != 2 {
			t.Fatalf("released CountJobs pending = %d, want 2", n)
		}
		if jobs := old.must(t, oldOp{"op": "list", "queue": "old-read", "status": "failed"}).Jobs; len(jobs) != 1 || jobs[0].ID != cancelled {
			t.Fatalf("released ListJobs failed = %+v, want the cancelled job %d", jobs, cancelled)
		}
	})

	t.Run("each binary claims what the other enqueued", func(t *testing.T) {
		fromOld := old.enqueue(t, "cross-old", 0, 0)
		if job := mustClaim(t, q, "cross-old", time.Minute); job.ID != fromOld || job.State != StateRunning {
			t.Fatalf("new claim = %+v, want the released binary's job %d", job, fromOld)
		}
		if err := q.Ack(ctx, fromOld); err != nil {
			t.Fatalf("ack: %v", err)
		}
		if got := old.get(t, fromOld).Status; got != "done" {
			t.Fatalf("released GetJob after new ack = %s, want done", got)
		}

		fromNew := mustEnqueue(t, q, EnqueueParams{QueueName: "cross-new"})
		if job := old.claim(t, "cross-new"); job.ID != fromNew {
			t.Fatalf("released claim took %d, want the new job %d", job.ID, fromNew)
		}
		old.must(t, oldOp{"op": "ack", "id": fromNew})
		d.requireState(t, fromNew, StateCompleted)

		// A delayed job falls due while its state still says scheduled (the leader's
		// scheduler has not run): the released claim takes it by status.
		delayed := newEnqueueAt(t, q, "cross-delayed", time.Now().Add(time.Hour))
		old.claimNothing(t, "cross-delayed")
		ageRow(t, d, "available_at", time.Second, delayed)
		d.requireState(t, delayed, StateScheduled)
		if job := old.claim(t, "cross-delayed"); job.ID != delayed {
			t.Fatalf("released claim took %d, want the due delayed job %d", job.ID, delayed)
		}
		d.requireState(t, delayed, StateRunning)
		old.must(t, oldOp{"op": "ack", "id": delayed})
	})

	t.Run("released and new workers share a queue", func(t *testing.T) {
		runSharedQueue(t, d, q, old, "shared", 20, func() {})
	})

	t.Run("legacy status stays correct for dashboards", func(t *testing.T) {
		requireLegacyStatusForDashboards(t, d, q)
	})

	if complete {
		requireSchemaComplete(t, d)
		return
	}

	t.Run("still expanded only", func(t *testing.T) {
		if progress := schemaProgressOf(t, d); progress.complete() || progress.stateRequired {
			t.Fatalf("schema progressed while held: %+v", progress)
		}
	})

	// The new leader completes the schema while released workers keep claiming,
	// retrying and enqueuing on it.
	t.Run("CompleteSchema runs while released workers work", func(t *testing.T) {
		// Enough backfill batches that the completion overlaps the workers.
		insertLegacyFinished(t, d, 500)
		q.schema.backfillBatch = 5
		runSharedQueue(t, d, q, old, "during-completion", 30, func() {
			release()
			if err := q.CompleteSchema(ctx); err != nil {
				t.Errorf("complete schema: %v", err)
			}
		})
		requireSchemaComplete(t, d)
		old.must(t, oldOp{"op": "ensure_schema"})
		requireSchemaComplete(t, d)
	})
}

// runSharedQueue runs a released worker and a new one on queueName, enqueues n
// jobs from each binary, runs during while they work, and requires that every job
// completes on its second attempt (each first attempt fails) with exactly one
// successful handling.
func runSharedQueue(t *testing.T, d driverDB, q *Client, old *oldPgkit, queueName string, n int, during func()) {
	t.Helper()
	ctx := context.Background()
	old.must(t, oldOp{"op": "worker_start", "name": queueName, "queues": []string{queueName}, "fail_first": true})
	defer old.must(t, oldOp{"op": "worker_stop", "name": queueName})

	registry := NewHandlerRegistry()
	if err := registry.Register(queueName, func(ctx context.Context, job Job) error {
		failed := job.Attempts == 1
		if _, err := q.DB().ExecContext(ctx,
			`INSERT INTO compat_processed (job_id, binary_name, attempt, succeeded) VALUES ($1, 'new', $2, $3)`,
			job.ID, job.Attempts, !failed,
		); err != nil {
			return err
		}
		if failed {
			return errors.New("first attempt fails")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	w, err := NewWorker(q, registry, WorkerConfig{
		WorkerID:          "new-" + queueName,
		Pickup:            OnEnqueue().RescanEvery(time.Minute),
		VisibilityTimeout: time.Minute,
		BackoffBase:       10 * time.Millisecond,
		BackoffMax:        50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		_ = w.Run(runCtx)
	}()
	defer func() {
		cancel()
		<-stopped
	}()
	select {
	case <-w.Ready():
	case <-time.After(30 * time.Second):
		t.Fatal("new worker not ready")
	}

	duringDone := make(chan struct{})
	go func() {
		defer close(duringDone)
		during()
	}()
	ids := make([]int64, 0, 2*n)
	for range n {
		ids = append(ids, old.enqueue(t, queueName, 0, 0), mustEnqueue(t, q, EnqueueParams{QueueName: queueName}))
	}
	<-duringDone
	if t.Failed() {
		t.FailNow()
	}

	deadline := time.Now().Add(60 * time.Second)
	for {
		left := queryCount(t, d, `SELECT count(*) FROM pgqueue_jobs WHERE queue_name = $1 AND state <> 'completed'`, queueName)
		if left == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d jobs on %s did not complete", left, len(ids), queueName)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if n := queryCount(t, d, `SELECT count(*) FROM pgqueue_jobs WHERE queue_name = $1 AND (attempts <> 2 OR status <> 'done')`, queueName); n != 0 {
		t.Fatalf("%d jobs on %s did not complete on their second attempt", n, queueName)
	}
	if n := queryCount(t, d, `SELECT count(*) FROM (
		SELECT j.id FROM pgqueue_jobs j LEFT JOIN compat_processed p ON p.job_id = j.id AND p.succeeded
		WHERE j.queue_name = $1 GROUP BY j.id HAVING count(p.job_id) <> 1) AS wrong`, queueName); n != 0 {
		t.Fatalf("%d jobs on %s did not succeed exactly once", n, queueName)
	}
	rows, err := d.db.QueryContext(ctx, `SELECT binary_name, count(*) FROM compat_processed p
		JOIN pgqueue_jobs j ON j.id = p.job_id WHERE j.queue_name = $1 GROUP BY binary_name ORDER BY binary_name`, queueName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name string
		var handled int
		if err := rows.Scan(&name, &handled); err != nil {
			t.Fatal(err)
		}
		t.Logf("%s binary handled %d attempts", name, handled)
	}
}

// requireLegacyStatusForDashboards puts one job in each state and runs the
// queries of the otelcol queue receivers (infra stacks/otelcol-db.yaml) as they
// are written there: they group by status and must keep seeing the four legacy
// values with the same meaning.
func requireLegacyStatusForDashboards(t *testing.T, d driverDB, q *Client) {
	t.Helper()
	ctx := context.Background()
	const queueName = "dashboard"
	claimOne := func() int64 {
		mustEnqueue(t, q, EnqueueParams{QueueName: queueName})
		return mustClaim(t, q, queueName, time.Minute).ID
	}
	ids := map[JobState]int64{}
	ids[StateRunning] = claimOne()
	ids[StateCompleted] = claimOne()
	if err := q.Ack(ctx, ids[StateCompleted]); err != nil {
		t.Fatal(err)
	}
	ids[StateRetryable] = claimOne()
	if err := q.Retry(ctx, ids[StateRetryable], time.Hour, errors.New("later")); err != nil {
		t.Fatal(err)
	}
	ids[StateDiscarded] = claimOne()
	if err := q.Fail(ctx, ids[StateDiscarded], errors.New("boom")); err != nil {
		t.Fatal(err)
	}
	ids[StateCancelled] = mustEnqueue(t, q, EnqueueParams{QueueName: queueName})
	if _, err := q.Cancel(ctx, ids[StateCancelled]); err != nil {
		t.Fatal(err)
	}
	ids[StateScheduled] = newEnqueueAt(t, q, queueName, time.Now().Add(time.Hour))
	ids[StateAvailable] = mustEnqueue(t, q, EnqueueParams{QueueName: queueName})
	for state, id := range ids {
		d.requireState(t, id, state)
	}

	// otelcol sqlquery/queues and sqlquery/queues_anchor: depth and age per status.
	rows, err := d.db.QueryContext(ctx, `SELECT queue_name, status, count(*) AS job_count,
		COALESCE(EXTRACT(EPOCH FROM (now() - min(created_at))), 0) AS oldest_age_seconds,
		COALESCE(EXTRACT(EPOCH FROM (now() - min(available_at))), 0) AS oldest_available_age_seconds
		FROM pgqueue_jobs GROUP BY queue_name, status`)
	if err != nil {
		t.Fatalf("otelcol depth query: %v", err)
	}
	depth := map[string]int{}
	for rows.Next() {
		var queue, status string
		var count int
		var oldest, oldestAvailable float64
		if err := rows.Scan(&queue, &status, &count, &oldest, &oldestAvailable); err != nil {
			t.Fatal(err)
		}
		switch status {
		case "pending", "processing", "done", "failed":
		default:
			t.Fatalf("otelcol depth query saw status %q on %s", status, queue)
		}
		if queue == queueName {
			depth[status] = count
		}
	}
	_ = rows.Close()
	if want := map[string]int{"pending": 3, "processing": 1, "done": 1, "failed": 2}; fmt.Sprint(depth) != fmt.Sprint(want) {
		t.Fatalf("otelcol depth for %s = %v, want %v", queueName, depth, want)
	}

	// otelcol retry pressure: attempted at least once and not finished.
	var retrying int
	if err := d.db.QueryRowContext(ctx, `SELECT count(*) AS retrying FROM pgqueue_jobs
		WHERE attempts > 0 AND status NOT IN ('done', 'failed') AND queue_name = $1 GROUP BY queue_name`, queueName,
	).Scan(&retrying); err != nil {
		t.Fatalf("otelcol retry query: %v", err)
	}
	if retrying != 2 {
		t.Fatalf("otelcol retrying for %s = %d, want 2 (the running and the retryable job)", queueName, retrying)
	}

	if n := queryCount(t, d, `SELECT count(*) FROM pgqueue_jobs
		WHERE state IS NOT NULL AND status IS DISTINCT FROM `+legacyStatusSQL); n != 0 {
		t.Fatalf("%d jobs show a status that does not match their state", n)
	}
}

// removeOldPgkitBuild deletes the driver binary; TestMain calls it.
func removeOldPgkitBuild() {
	if oldBuild.dir != "" {
		_ = os.RemoveAll(oldBuild.dir)
	}
}
