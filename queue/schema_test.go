package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq"
)

// insertLegacyFinished adds n jobs that a binary older than schema v5 finished,
// the bulk of a table that keeps its history.
func insertLegacyFinished(t *testing.T, d driverDB, n int) {
	t.Helper()
	d.exec(t, `INSERT INTO pgqueue_jobs (queue_name, payload, status, attempts, done_at, updated_at)
		SELECT 'legacy-done', '\x7b7d', CASE WHEN g % 3 = 0 THEN 'failed' ELSE 'done' END, 1, NOW(), NOW() - interval '1 day'
		FROM generate_series(1, $1::int) AS g`, n)
}

func queryCount(t *testing.T, d driverDB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := d.db.QueryRowContext(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func nullStates(t *testing.T, d driverDB) int {
	t.Helper()
	return queryCount(t, d, `SELECT count(*) FROM pgqueue_jobs WHERE state IS NULL`)
}

func schemaProgressOf(t *testing.T, d driverDB) schemaProgress {
	t.Helper()
	progress, err := readSchemaProgress(context.Background(), d.db)
	if err != nil {
		t.Fatalf("read schema progress: %v", err)
	}
	return progress
}

func requireSchemaComplete(t *testing.T, d driverDB) {
	t.Helper()
	if progress := schemaProgressOf(t, d); !progress.complete() {
		t.Fatalf("schema incomplete: %+v", progress)
	}
	if n := nullStates(t, d); n != 0 {
		t.Fatalf("%d jobs without state", n)
	}
	if n := queryCount(t, d, `SELECT count(*) FROM pgqueue_jobs WHERE status <> pgqueue_status_of_state(state)`); n != 0 {
		t.Fatalf("%d jobs whose status disagrees with their state", n)
	}
}

func liveIndexOIDs(t *testing.T, d driverDB) map[string]int64 {
	t.Helper()
	oids := map[string]int64{}
	for _, index := range liveStateIndexes {
		var oid int64
		if err := d.db.QueryRowContext(context.Background(),
			`SELECT i.indexrelid::bigint FROM pg_index i WHERE i.indexrelid = to_regclass($1) AND i.indisvalid`, index.name,
		).Scan(&oid); err != nil {
			t.Fatalf("index %s: %v", index.name, err)
		}
		oids[index.name] = oid
	}
	return oids
}

// On a table too large to scan at app start, EnsureSchema gives every live job a
// state and leaves the table scans to CompleteSchema, so it fits a start deadline
// whatever the history size.
func TestEnsureSchemaLeavesTableScansToCompleteSchemaOnALargeTable(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		ctx := context.Background()
		migrateToV4(t, d)
		finished := defaultSchemaSettings().inlineRows + 2000
		insertLegacyFinished(t, d, finished)
		due := legacyEnqueue(t, d, "large", 0)
		processing := legacyEnqueue(t, d, "large", 0)
		d.exec(t, `UPDATE pgqueue_jobs SET status = 'processing', claimed_at = NOW() WHERE id = $1`, processing)

		q := d.bareClient(t)
		startCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := q.EnsureSchema(startCtx); err != nil {
			t.Fatalf("ensure schema under a start deadline: %v", err)
		}

		d.requireState(t, due, StateAvailable)
		d.requireState(t, processing, StateRunning)
		if n := nullStates(t, d); n != finished {
			t.Fatalf("EnsureSchema backfilled %d finished jobs, want none of %d", finished-n, finished)
		}
		progress := schemaProgressOf(t, d)
		if !progress.expanded || progress.stateRequired || progress.stateChecked {
			t.Fatalf("progress after EnsureSchema = %+v, want expanded only", progress)
		}
		for _, index := range append(liveStateIndexes, finalizedStateIndexes...) {
			if n := queryCount(t, d, `SELECT count(*) FROM pg_class WHERE relname = $1`, index.name); n != 0 {
				t.Fatalf("EnsureSchema built %s on a large table", index.name)
			}
		}

		listed, err := q.ListJobs(ctx, ListJobsParams{QueueName: "legacy-done", Limit: 3})
		if err != nil || len(listed) != 3 || listed[0].State == "" {
			t.Fatalf("list of not yet backfilled jobs = %+v, err %v", listed, err)
		}
		if job := mustClaim(t, q, "large", time.Minute); job.ID != due {
			t.Fatalf("claim took %d, want the old binary's due job %d", job.ID, due)
		}

		if err := q.CompleteSchema(ctx); err != nil {
			t.Fatalf("complete schema: %v", err)
		}
		requireSchemaComplete(t, d)
		if n := queryCount(t, d, `SELECT count(*) FROM pgqueue_jobs WHERE queue_name = 'legacy-done'
			AND updated_at > NOW() - interval '1 hour'`); n != 0 {
			t.Fatalf("backfill moved updated_at of %d jobs, which would reset retention", n)
		}
	})
}

// An interrupted CompleteSchema keeps what it did: the rows it backfilled and the
// indexes it built. A restart's EnsureSchema touches neither, and the next run
// converges from there.
func TestCompleteSchemaResumesWhereAnInterruptedRunStopped(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		migrateToV4(t, d)
		insertLegacyFinished(t, d, 500)
		q := d.bareClient(t)
		q.schema.inlineRows = 10
		q.schema.backfillBatch = 50
		if err := q.EnsureSchema(context.Background()); err != nil {
			t.Fatalf("ensure schema: %v", err)
		}

		ctx, interrupt := context.WithCancel(context.Background())
		var batches atomic.Int64
		q.schema.observe = func(event string) {
			if event == eventBackfillBatch && batches.Add(1) == 3 {
				interrupt()
			}
		}
		if err := q.CompleteSchema(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("interrupted complete schema = %v, want context.Canceled", err)
		}
		// The batches reported before the interruption stay committed. The statement
		// in flight as ctx ended may commit too (lib/pq lets it finish while reporting
		// the cancellation), so allow one more batch.
		reported := int(batches.Load())
		remaining := nullStates(t, d)
		if remaining > 500-50*reported || remaining < 500-50*(reported+1) || remaining == 0 {
			t.Fatalf("%d jobs without state after %d reported batches of 50", remaining, reported)
		}
		built := liveIndexOIDs(t, d)

		q.schema.observe = nil
		if err := q.EnsureSchema(context.Background()); err != nil {
			t.Fatalf("ensure schema on restart: %v", err)
		}
		if n := nullStates(t, d); n != remaining {
			t.Fatalf("%d jobs without state after a restart, want %d", n, remaining)
		}

		batches.Store(0)
		q.schema.observe = func(event string) {
			if event == eventBackfillBatch {
				batches.Add(1)
			}
		}
		if err := q.CompleteSchema(context.Background()); err != nil {
			t.Fatalf("complete schema: %v", err)
		}
		requireSchemaComplete(t, d)
		if got, want := int(batches.Load()), remaining/50; got != want {
			t.Fatalf("resumed backfill ran %d batches, want %d for the %d remaining jobs", got, want, remaining)
		}
		if after := liveIndexOIDs(t, d); fmt.Sprint(after) != fmt.Sprint(built) {
			t.Fatalf("indexes rebuilt: %v, then %v", built, after)
		}
	})
}

// The backfill spans many batches while an old binary keeps writing, and every
// job ends with a state that matches its status.
func TestCompleteSchemaBackfillsAcrossBatchesWhileAnOldBinaryWrites(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		migrateToV4(t, d)
		insertLegacyFinished(t, d, 1200)
		for range 30 {
			legacyEnqueue(t, d, "old-writer", time.Hour)
		}
		q := d.bareClient(t)
		q.schema.inlineRows = 10
		q.schema.backfillBatch = 100
		if err := q.EnsureSchema(context.Background()); err != nil {
			t.Fatalf("ensure schema: %v", err)
		}
		var batches atomic.Int64
		q.schema.observe = func(event string) {
			if event == eventBackfillBatch {
				batches.Add(1)
			}
		}

		stop := make(chan struct{})
		writes := make(chan error, 1)
		go func() {
			var err error
			defer func() { writes <- err }()
			for {
				select {
				case <-stop:
					return
				default:
				}
				var id int64
				if err = d.db.QueryRow(legacyEnqueueSQL, "old-writer", 0).Scan(&id); err != nil {
					return
				}
				if err = d.db.QueryRow(legacyClaimSQL, "old-writer").Scan(&id); err != nil {
					return
				}
				if _, err = d.db.Exec(legacyAckSQL, id); err != nil {
					return
				}
			}
		}()
		err := q.CompleteSchema(context.Background())
		close(stop)
		if writeErr := <-writes; writeErr != nil {
			t.Fatalf("old binary write during the backfill: %v", writeErr)
		}
		if err != nil {
			t.Fatalf("complete schema: %v", err)
		}
		if got := batches.Load(); got < 12 {
			t.Fatalf("backfill ran %d batches, want at least 12 for 1200 jobs in batches of 100", got)
		}
		requireSchemaComplete(t, d)
		if n := queryCount(t, d, `SELECT count(*) FROM pgqueue_jobs WHERE queue_name = 'legacy-done'
			AND state NOT IN ('completed', 'discarded')`); n != 0 {
			t.Fatalf("%d finished legacy jobs got a live state", n)
		}
	})
}

// A table lock held elsewhere makes the migration wait in short steps, so writers
// queued behind its DDL wait at most one lock_timeout, and the migration still
// succeeds once the lock is released.
func TestEnsureSchemaWaitsOutABusyTableWithoutStallingTheQueue(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		ctx := context.Background()
		migrateToV4(t, d)
		held := legacyEnqueue(t, d, "busy", 0)
		legacyEnqueue(t, d, "busy", 0)

		holder, err := d.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		if _, err := holder.ExecContext(ctx, `SELECT id FROM pgqueue_jobs WHERE id = $1 FOR UPDATE`, held); err != nil {
			t.Fatalf("hold row: %v", err)
		}
		const holdFor = 4 * time.Second
		released := time.AfterFunc(holdFor, func() { _ = holder.Rollback() })
		defer released.Stop()
		defer func() { _ = holder.Rollback() }()

		q := d.bareClient(t)
		q.schema.lockTimeout = 500 * time.Millisecond
		started := time.Now()
		migrated := make(chan error, 1)
		go func() { migrated <- q.EnsureSchema(ctx) }()
		waitForLockedDDL(t, d)

		claimStarted := time.Now()
		var claimed int64
		if err := d.db.QueryRowContext(ctx, legacyClaimSQL, "busy").Scan(&claimed); err != nil {
			t.Fatalf("legacy claim behind the migration: %v", err)
		}
		if waited := time.Since(claimStarted); waited > 2*time.Second {
			t.Fatalf("a claim queued behind the migration waited %s; the DDL lock wait must stay short", waited)
		}

		if err := <-migrated; err != nil {
			t.Fatalf("ensure schema on a busy table: %v", err)
		}
		if took := time.Since(started); took < holdFor-time.Second {
			t.Fatalf("ensure schema took %s, before the lock holder released after %s", took, holdFor)
		}
		d.requireState(t, held, StateAvailable)
	})
}

// waitForLockedDDL waits until an ALTER TABLE of d's database waits for a lock.
func waitForLockedDDL(t *testing.T, d driverDB) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for queryCount(t, d, `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()
		AND wait_event_type = 'Lock' AND query ILIKE '%ALTER TABLE%'`) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the migration never waited for its lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestADDLTransactionGivesUpAfterItsAttempts(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		ctx := context.Background()
		q := d.client(t)
		holder, err := d.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = holder.Rollback() }()
		if _, err := holder.ExecContext(ctx, `LOCK TABLE pgqueue_jobs IN ROW SHARE MODE`); err != nil {
			t.Fatalf("lock: %v", err)
		}
		settings := q.schema
		settings.lockTimeout = 50 * time.Millisecond
		settings.ddlAttempts = 2
		err = inDDLTx(ctx, d.db, settings, `ALTER TABLE pgqueue_jobs ADD COLUMN IF NOT EXISTS never TEXT`)
		if !isLockTimeout(err) {
			t.Fatalf("ddl on a locked table = %v, want lock_not_available", err)
		}
	})
}

func TestLockTimeoutsAreRecognisedFromBothDrivers(t *testing.T) {
	for _, err := range []error{
		fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: "55P03"}),
		fmt.Errorf("wrapped: %w", &pq.Error{Code: "55P03"}),
	} {
		if !isLockTimeout(err) {
			t.Errorf("isLockTimeout(%v) = false", err)
		}
	}
	for _, err := range []error{&pgconn.PgError{Code: "40P01"}, &pq.Error{Code: "57014"}, errors.New("lock timeout")} {
		if isLockTimeout(err) {
			t.Errorf("isLockTimeout(%v) = true", err)
		}
	}
}

func TestTheLeaderCompletesTheSchema(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		migrateToV4(t, d)
		insertLegacyFinished(t, d, 40)
		q := d.bareClient(t)
		q.schema.inlineRows = 10
		if err := q.EnsureSchema(context.Background()); err != nil {
			t.Fatalf("ensure schema: %v", err)
		}
		if schemaProgressOf(t, d).complete() {
			t.Fatal("schema complete before any leader ran")
		}

		ctx, stop := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Go(func() { _ = q.RunMaintenance(ctx) })
		defer func() {
			stop()
			wg.Wait()
		}()
		deadline := time.Now().Add(20 * time.Second)
		for !schemaProgressOf(t, d).complete() {
			if time.Now().After(deadline) {
				t.Fatalf("the leader left the schema incomplete: %+v", schemaProgressOf(t, d))
			}
			time.Sleep(50 * time.Millisecond)
		}
		requireSchemaComplete(t, d)
	})
}
