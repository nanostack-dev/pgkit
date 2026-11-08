package queue

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// migrateToV4 builds the schema as a binary from before job states left it.
func migrateToV4(t *testing.T, d driverDB) {
	t.Helper()
	ctx := context.Background()
	d.exec(t, `CREATE TABLE pgqueue_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`)
	for _, apply := range []func(context.Context, schemaConn) error{migrateV1, migrateV2, migrateV3, migrateV4} {
		if err := apply(ctx, d.db); err != nil {
			t.Fatalf("migrate: %v", err)
		}
	}
	d.exec(t, `INSERT INTO pgqueue_meta VALUES ('schema_version', '4')`)
}

// The statements below are what binaries from before schema v5 run.
const (
	legacyEnqueueSQL = `INSERT INTO pgqueue_jobs(queue_name, payload, status, attempts, max_attempts, available_at)
		VALUES ($1, '\x7b7d', 'pending', 0, 5, NOW() + make_interval(secs => $2::float8)) RETURNING id`
	legacyClaimSQL = `UPDATE pgqueue_jobs SET status = 'processing', attempts = attempts + 1, claims = claims + 1,
		claimed_by = 'old-binary', claimed_at = NOW(), updated_at = NOW()
		WHERE id = (SELECT id FROM pgqueue_jobs WHERE queue_name = $1 AND status = 'pending'
		            AND available_at <= NOW() AND attempts < max_attempts ORDER BY available_at, id
		            FOR UPDATE SKIP LOCKED LIMIT 1)
		RETURNING id`
	legacyRetrySQL = `UPDATE pgqueue_jobs SET status = 'pending', available_at = NOW() + interval '1 hour',
		claimed_by = NULL, claimed_at = NULL, updated_at = NOW() WHERE id = $1 AND status = 'processing'`
	legacySnoozeSQL = `UPDATE pgqueue_jobs SET status = 'pending', attempts = GREATEST(attempts - 1, 0),
		available_at = GREATEST($2::timestamptz, NOW()), claimed_by = NULL, claimed_at = NULL, updated_at = NOW()
		WHERE id = $1 AND status = 'processing'`
	legacyAckSQL  = `UPDATE pgqueue_jobs SET status = 'done', done_at = NOW(), updated_at = NOW() WHERE id = $1 AND status = 'processing'`
	legacyFailSQL = `UPDATE pgqueue_jobs SET status = 'failed', done_at = NOW(), updated_at = NOW() WHERE id = $1 AND status = 'processing'`
)

func legacyEnqueue(t *testing.T, d driverDB, queueName string, delay time.Duration) int64 {
	t.Helper()
	var id int64
	if err := d.db.QueryRowContext(context.Background(), legacyEnqueueSQL, queueName, delay.Seconds()).Scan(&id); err != nil {
		t.Fatalf("legacy enqueue: %v", err)
	}
	return id
}

func legacyClaim(t *testing.T, d driverDB, queueName string) int64 {
	t.Helper()
	var id int64
	if err := d.db.QueryRowContext(context.Background(), legacyClaimSQL, queueName).Scan(&id); err != nil {
		t.Fatalf("legacy claim: %v", err)
	}
	return id
}

func TestMigrationGivesEveryLegacyJobItsState(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		ctx := context.Background()
		migrateToV4(t, d)
		due := legacyEnqueue(t, d, "legacy", 0)
		delayed := legacyEnqueue(t, d, "legacy", time.Hour)
		backingOff := legacyEnqueue(t, d, "legacy", time.Hour)
		d.exec(t, `UPDATE pgqueue_jobs SET attempts = 1 WHERE id = $1`, backingOff)
		processing := legacyEnqueue(t, d, "legacy", 0)
		done := legacyEnqueue(t, d, "legacy", 0)
		failed := legacyEnqueue(t, d, "legacy", 0)
		d.exec(t, `UPDATE pgqueue_jobs SET status = 'processing', claimed_at = NOW() WHERE id = $1`, processing)
		d.exec(t, `UPDATE pgqueue_jobs SET status = 'done', updated_at = NOW() - interval '3 days' WHERE id = $1`, done)
		d.exec(t, `UPDATE pgqueue_jobs SET status = 'failed' WHERE id = $1`, failed)
		var doneUpdatedAt time.Time
		if err := d.db.QueryRowContext(ctx, `SELECT updated_at FROM pgqueue_jobs WHERE id = $1`, done).Scan(&doneUpdatedAt); err != nil {
			t.Fatalf("read updated_at: %v", err)
		}

		q := d.bareClient(t)
		if err := q.EnsureSchema(ctx); err != nil {
			t.Fatalf("ensure schema: %v", err)
		}

		for id, want := range map[int64]JobState{
			due: StateAvailable, delayed: StateScheduled, backingOff: StateRetryable,
			processing: StateRunning, done: StateCompleted, failed: StateDiscarded,
		} {
			d.requireState(t, id, want)
		}
		var afterUpdatedAt time.Time
		if err := d.db.QueryRowContext(ctx, `SELECT updated_at FROM pgqueue_jobs WHERE id = $1`, done).Scan(&afterUpdatedAt); err != nil {
			t.Fatalf("read updated_at: %v", err)
		}
		if !afterUpdatedAt.Equal(doneUpdatedAt) {
			t.Fatalf("backfill moved updated_at from %s to %s, which would reset retention", doneUpdatedAt, afterUpdatedAt)
		}

		var notNull bool
		if err := d.db.QueryRowContext(ctx,
			`SELECT attnotnull FROM pg_attribute WHERE attrelid = 'pgqueue_jobs'::regclass AND attname = 'state'`,
		).Scan(&notNull); err != nil || !notNull {
			t.Fatalf("state NOT NULL = %v, err %v", notNull, err)
		}
		var invalid int
		if err := d.db.QueryRowContext(ctx,
			`SELECT count(*) FROM pg_index WHERE indrelid = 'pgqueue_jobs'::regclass AND NOT indisvalid`,
		).Scan(&invalid); err != nil || invalid != 0 {
			t.Fatalf("invalid indexes = %d, err %v", invalid, err)
		}
		if err := d.db.QueryRowContext(ctx, `SELECT count(*) FROM pg_indexes WHERE indexname IN
			('idx_pgqueue_claimable', 'idx_pgqueue_due', 'idx_pgqueue_running', 'idx_pgqueue_finalized')`,
		).Scan(&invalid); err != nil || invalid != 4 {
			t.Fatalf("state indexes = %d, err %v", invalid, err)
		}
	})
}

func TestAnOldBinaryAndANewOneShareTheQueueDuringARollingDeploy(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		ctx := context.Background()
		q := d.client(t)

		oldJob := legacyEnqueue(t, d, "mixed", 0)
		d.requireState(t, oldJob, StateAvailable)
		claimed := mustClaim(t, q, "mixed", time.Minute)
		if claimed.ID != oldJob || claimed.State != StateRunning || claimed.Status != StatusProcessing {
			t.Fatalf("new claim of an old binary's job = %+v", claimed)
		}
		if err := q.Ack(ctx, oldJob); err != nil {
			t.Fatalf("ack: %v", err)
		}
		d.requireState(t, oldJob, StateCompleted)

		newJob := mustEnqueue(t, q, EnqueueParams{QueueName: "mixed"})
		if got := legacyClaim(t, d, "mixed"); got != newJob {
			t.Fatalf("old claim took %d, want %d", got, newJob)
		}
		d.requireState(t, newJob, StateRunning)
		d.exec(t, legacyRetrySQL, newJob)
		d.requireState(t, newJob, StateRetryable)
		d.exec(t, `UPDATE pgqueue_jobs SET available_at = NOW() WHERE id = $1`, newJob)
		if got := legacyClaim(t, d, "mixed"); got != newJob {
			t.Fatalf("old claim of a due retry took %d, want %d", got, newJob)
		}
		d.exec(t, legacyFailSQL, newJob)
		d.requireState(t, newJob, StateDiscarded)

		other := mustEnqueue(t, q, EnqueueParams{QueueName: "mixed"})
		legacyClaim(t, d, "mixed")
		d.exec(t, legacyAckSQL, other)
		d.requireState(t, other, StateCompleted)
		if err := q.ReplayJob(ctx, other); err != nil {
			t.Fatalf("replay: %v", err)
		}
		d.requireState(t, other, StateAvailable)
	})
}

func TestAnOldBinaryClaimNeverInheritsALease(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		q := d.client(t)
		id := mustEnqueue(t, q, EnqueueParams{QueueName: "lease"})
		mustClaim(t, q, "lease", time.Minute)
		d.exec(t, `UPDATE pgqueue_jobs SET status = 'pending', available_at = NOW() WHERE id = $1`, id)
		legacyClaim(t, d, "lease")

		job, err := q.GetJob(context.Background(), id)
		if err != nil {
			t.Fatalf("get job: %v", err)
		}
		if job.State != StateRunning || job.LeaseExpiresAt.Valid {
			t.Fatalf("old claim = state %s lease %v, want running without lease", job.State, job.LeaseExpiresAt)
		}
	})
}

func TestAJobAnOldBinaryReplaysAfterACancelIsRetriedNotCancelled(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		ctx := context.Background()
		q := d.client(t)
		id := mustEnqueue(t, q, EnqueueParams{QueueName: "old-replay"})
		if _, err := q.Cancel(ctx, id); err != nil {
			t.Fatalf("cancel: %v", err)
		}
		d.exec(t, `UPDATE pgqueue_jobs SET status = 'pending', attempts = 0, available_at = NOW(), done_at = NULL
			WHERE id = $1 AND status IN ('done', 'failed')`, id)
		d.requireState(t, id, StateAvailable)
		// An old claim does not clear the request, so the replay itself must.
		if n := queryCount(t, d, `SELECT count(*) FROM pgqueue_jobs WHERE id = $1 AND cancel_requested_at IS NOT NULL`, id); n != 0 {
			t.Fatal("an old binary's replay kept the cancel request of the job's earlier life")
		}

		job := mustClaim(t, q, "old-replay", 0)
		if job.CancelRequestedAt.Valid {
			t.Fatal("a new claim kept the cancel request of an earlier life")
		}
		if err := q.Retry(ctx, id, 0, errors.New("transient")); err != nil {
			t.Fatalf("retry: %v", err)
		}
		d.requireState(t, id, StateAvailable)
	})
}

// Binaries older than schema v5 end EnsureSchema by writing their own version
// back. The marker keeps the higher one, so later starts do not re-run the
// migrations, whose table locks would queue behind a CompleteSchema index build.
func TestAnOldBinaryCannotLowerTheSchemaVersion(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		ctx := context.Background()
		q := d.client(t)
		id := mustEnqueue(t, q, EnqueueParams{QueueName: "rerun"})
		for _, downgrade := range []string{
			`UPDATE pgqueue_meta SET value = '4' WHERE key = 'schema_version'`,
			`INSERT INTO pgqueue_meta (key, value) VALUES ('schema_version', '3')
			ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
		} {
			d.exec(t, downgrade)
			if v := schemaVersionOf(t, d); v != schemaVersion {
				t.Fatalf("schema version after %q = %d, want %d", downgrade, v, schemaVersion)
			}
		}

		var migrations atomic.Int64
		q.schema.observe = func(event string) {
			if event == eventMigrateV5 {
				migrations.Add(1)
			}
		}
		if err := q.EnsureSchema(ctx); err != nil {
			t.Fatalf("ensure schema again: %v", err)
		}
		if n := migrations.Load(); n != 0 {
			t.Fatalf("EnsureSchema re-ran migration v5 %d times", n)
		}
		d.requireState(t, id, StateAvailable)
	})
}

func TestReplicasStartingTogetherMigrateOnce(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		migrateToV4(t, d)
		for range 20 {
			legacyEnqueue(t, d, "together", 0)
		}
		var wg sync.WaitGroup
		var migrations atomic.Int64
		errs := make(chan error, 4)
		for range 4 {
			wg.Go(func() {
				q, _ := New(d.db)
				q.schema.observe = func(event string) {
					if event == eventMigrateV5 {
						migrations.Add(1)
					}
				}
				errs <- q.EnsureSchema(context.Background())
			})
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent ensure schema: %v", err)
			}
		}
		if got := migrations.Load(); got != 1 {
			t.Fatalf("schema v5 migration ran %d times across 4 replicas, want once", got)
		}
		var missing int
		if err := d.db.QueryRowContext(context.Background(), `SELECT count(*) FROM pgqueue_jobs WHERE state IS DISTINCT FROM 'available'`).Scan(&missing); err != nil || missing != 0 {
			t.Fatalf("jobs without state = %d, err %v", missing, err)
		}
	})
}

func TestJobStatesFollowTheLifecycle(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		ctx := context.Background()
		q := d.client(t)
		later := time.Now().Add(time.Hour)

		scheduled := mustEnqueue(t, q, EnqueueParams{QueueName: "life", AvailableAt: &later})
		d.requireState(t, scheduled, StateScheduled)

		available := mustEnqueue(t, q, EnqueueParams{QueueName: "life", MaxAttempts: 2})
		d.requireState(t, available, StateAvailable)
		job := mustClaim(t, q, "life", time.Minute)
		if job.ID != available || !job.LeaseExpiresAt.Valid {
			t.Fatalf("claim = %+v, want job %d with a lease", job, available)
		}
		if err := q.Retry(ctx, available, time.Hour, errors.New("try later")); err != nil {
			t.Fatalf("retry: %v", err)
		}
		d.requireState(t, available, StateRetryable)
		after, _ := q.GetJob(ctx, available)
		if after.LeaseExpiresAt.Valid || after.ClaimedBy.Valid {
			t.Fatalf("retried job kept its claim: %+v", after)
		}

		d.exec(t, `UPDATE pgqueue_jobs SET available_at = NOW() - interval '1 second' WHERE id = $1`, available)
		if again := mustClaim(t, q, "life", 0); again.ID != available {
			t.Fatalf("claim took %d, want the due retryable job %d without waiting for the scheduler", again.ID, available)
		}
		if err := q.Retry(ctx, available, 0, errors.New("out of attempts")); err != nil {
			t.Fatalf("retry: %v", err)
		}
		d.requireState(t, available, StateDiscarded)

		if err := q.ReplayJob(ctx, available); err != nil {
			t.Fatalf("replay: %v", err)
		}
		d.requireState(t, available, StateAvailable)
		mustClaim(t, q, "life", 0)
		if err := q.Retry(ctx, available, 0, errors.New("again")); err != nil {
			t.Fatalf("retry: %v", err)
		}
		d.requireState(t, available, StateAvailable)
		mustClaim(t, q, "life", 0)
		if err := q.Fail(ctx, available, errors.New("refused")); err != nil {
			t.Fatalf("fail: %v", err)
		}
		d.requireState(t, available, StateDiscarded)

		d.exec(t, `UPDATE pgqueue_jobs SET available_at = NOW() WHERE id = $1`, scheduled)
		mustClaim(t, q, "life", 0)
		if err := q.Ack(ctx, scheduled); err != nil {
			t.Fatalf("ack: %v", err)
		}
		d.requireState(t, scheduled, StateCompleted)

		running, err := q.ListJobs(ctx, ListJobsParams{Limit: 10, QueueName: "life", State: StateCompleted})
		if err != nil || len(running) != 1 || running[0].ID != scheduled {
			t.Fatalf("list by state = %+v, err %v", running, err)
		}
		count, err := q.CountJobs(ctx, ListJobsParams{QueueName: "life", Status: StatusFailed})
		if err != nil || count != 1 {
			t.Fatalf("count by legacy status = %d, err %v", count, err)
		}
	})
}

func TestSnoozeAndWakeUseScheduledAndAvailable(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		ctx := context.Background()
		q := d.client(t)
		id := mustEnqueue(t, q, EnqueueParams{QueueName: "park"})
		job := mustClaim(t, q, "park", time.Minute)
		if err := inTxOn(d.db, func(tx *sql.Tx) error { return q.SnoozeTx(ctx, tx, *job, time.Time{}) }); err != nil {
			t.Fatalf("park: %v", err)
		}
		d.requireState(t, id, StateScheduled)
		if err := inTxOn(d.db, func(tx *sql.Tx) error { return q.WakeTx(ctx, tx, id) }); err != nil {
			t.Fatalf("wake: %v", err)
		}
		d.requireState(t, id, StateAvailable)
	})
}

func inTxOn(db *sql.DB, run func(tx *sql.Tx) error) error {
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := run(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func TestAJobAnOldBinaryParksIsScheduledNotRetryable(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		q := d.client(t)
		id := mustEnqueue(t, q, EnqueueParams{QueueName: "old-park"})
		mustClaim(t, q, "old-park", time.Minute)
		d.exec(t, `UPDATE pgqueue_jobs SET attempts = 3 WHERE id = $1`, id)

		d.exec(t, legacySnoozeSQL, id, ParkedUntil)
		d.requireState(t, id, StateScheduled)

		var stillParked bool
		if err := d.db.QueryRowContext(context.Background(),
			`SELECT available_at = $2 AND attempts = 2 FROM pgqueue_jobs WHERE id = $1`, id, ParkedUntil,
		).Scan(&stillParked); err != nil || !stillParked {
			t.Fatalf("legacy park left available_at/attempts unexpected: %v, err %v", stillParked, err)
		}
	})
}
