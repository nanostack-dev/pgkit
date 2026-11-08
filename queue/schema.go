package queue

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"time"
)

// schemaVersion is the current version of the pgqueue schema.
// Bump this when adding migrations.
const schemaVersion = 5

const (
	migrationLockKey  = "pgqueue.schema"
	completionLockKey = "pgqueue.schema.complete"

	schemaLockPollInterval = 100 * time.Millisecond

	// sqlStateLockNotAvailable is lock_not_available, raised when lock_timeout ends
	// a wait for a lock.
	sqlStateLockNotAvailable = "55P03"
	ddlRetryFirstBackoff     = 200 * time.Millisecond
	ddlRetryMaxBackoff       = 5 * time.Second
)

// ErrSchemaNotExpanded is returned by CompleteSchema on a database that
// EnsureSchema has not migrated to schema v5 yet.
var ErrSchemaNotExpanded = errors.New("pgqueue: schema v5 is not expanded: run EnsureSchema first")

// schemaSettings tunes the migrations. Tests lower the thresholds.
type schemaSettings struct {
	// backfillBatch is the number of rows each backfill statement updates.
	backfillBatch int
	// inlineRows is the largest table EnsureSchema completes itself.
	inlineRows int
	// lockTimeout bounds each wait for a table lock in a DDL transaction. Writers
	// that queue behind the waiting DDL wait at most this long.
	lockTimeout time.Duration
	// ddlAttempts bounds the tries of a DDL transaction that times out on a lock.
	ddlAttempts int
	// observe, when set, is told each migration step: tests count them.
	observe func(event string)
}

const (
	eventMigrateV5     = "migrate v5"
	eventBackfillBatch = "backfill batch"
)

func defaultSchemaSettings() schemaSettings {
	return schemaSettings{backfillBatch: 5000, inlineRows: 10000, lockTimeout: time.Second, ddlAttempts: 8}
}

func (s schemaSettings) note(event string) {
	if s.observe != nil {
		s.observe(event)
	}
}

type schemaConn interface {
	execer
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// EnsureSchema creates the pgqueue tables and applies incremental migrations. It
// is meant for app start: it takes table locks only briefly, retries when a busy
// table makes it wait, and its work grows with the live jobs (available,
// scheduled, retryable, running), not with the finished ones.
//
// The steps of schema v5 that scan the whole table run in CompleteSchema, which
// the maintenance leader calls on its own. On a table of at most 10,000 rows,
// a fresh database included, EnsureSchema completes the schema itself.
//
// Replicas that start together run each migration once: they hold a session
// advisory lock for the duration. The version marker only grows: a binary older
// than schema v5 that writes its own version back leaves it unchanged.
func (c *Client) EnsureSchema(ctx context.Context) error {
	if c == nil || c.db == nil {
		return ErrNilDB
	}
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("pgqueue: schema connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if err := lockSchema(ctx, conn, migrationLockKey); err != nil {
		return fmt.Errorf("pgqueue: lock schema: %w", err)
	}
	defer unlockSchema(ctx, conn, migrationLockKey)

	if err := migrate(ctx, conn, c.schema); err != nil {
		return err
	}
	if err := completeSmallSchema(ctx, conn, c.schema); err != nil {
		return fmt.Errorf("pgqueue: complete schema: %w", err)
	}
	return nil
}

// CompleteSchema finishes schema v5 after EnsureSchema: it builds the state
// indexes CONCURRENTLY, backfills the state of jobs that finished before the
// migration, validates the state constraint and makes the column NOT NULL. None
// of this blocks writers, but each index build and validation reads the whole
// table: expect about 3 s per index per million rows on an idle database, plus the
// backfill, which rewrites each finished row once.
//
// The maintenance leader runs it on its own, so a deployment that runs a Worker
// or RunMaintenance needs no call. Call it from a migration job or CLI to finish
// at a chosen time. It waits while another client completes the schema, and
// returns at once when the schema is complete. An interrupted call keeps the
// backfilled rows and finished steps; only an index build in progress starts
// again. Until it finishes, jobs that finished before the migration match no
// state filter, are not purged, and ReplayJob returns ErrJobNotFound for them;
// everything else works.
func (c *Client) CompleteSchema(ctx context.Context) error {
	if c == nil || c.db == nil {
		return ErrNilDB
	}
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("pgqueue: schema connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if err := lockSchema(ctx, conn, completionLockKey); err != nil {
		return fmt.Errorf("pgqueue: lock schema completion: %w", err)
	}
	defer unlockSchema(ctx, conn, completionLockKey)
	if err := completeSchema(ctx, conn, c.schema, true); err != nil {
		return fmt.Errorf("pgqueue: complete schema: %w", err)
	}
	return nil
}

// completeSchemaOnLeader runs CompleteSchema unless the schema is complete or
// another client is completing it. It reports whether the schema is complete.
func (c *Client) completeSchemaOnLeader(ctx context.Context) (bool, error) {
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return false, fmt.Errorf("pgqueue: schema connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	progress, err := readSchemaProgress(ctx, conn)
	if err != nil {
		return false, fmt.Errorf("pgqueue: read schema progress: %w", err)
	}
	if progress.complete() {
		return true, nil
	}
	locked, err := tryLockSchema(ctx, conn, completionLockKey)
	if err != nil || !locked {
		return false, err
	}
	defer unlockSchema(ctx, conn, completionLockKey)
	started := time.Now()
	c.logInfo(ctx, "queue schema completion started", nil)
	if err := completeSchema(ctx, conn, c.schema, true); err != nil {
		return false, fmt.Errorf("pgqueue: complete schema: %w", err)
	}
	c.logInfo(ctx, "queue schema completed", map[string]any{"duration": time.Since(started).String()})
	return true, nil
}

// lockSchema polls instead of blocking in pg_advisory_lock: a blocked call holds a
// snapshot, and CREATE INDEX CONCURRENTLY in the client holding the lock would
// wait for that snapshot, which waits for the lock.
func lockSchema(ctx context.Context, conn *sql.Conn, key string) error {
	ticker := time.NewTicker(schemaLockPollInterval)
	defer ticker.Stop()
	for {
		locked, err := tryLockSchema(ctx, conn, key)
		if err != nil {
			return err
		}
		if locked {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func tryLockSchema(ctx context.Context, conn *sql.Conn, key string) (bool, error) {
	var locked bool
	err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtextextended($1, 0))`, key).Scan(&locked)
	return locked, err
}

// unlockSchema releases a schema lock. If that fails, it discards the connection
// instead of returning it to the pool still holding the lock, which would block
// every other replica's migration until the connection died.
func unlockSchema(ctx context.Context, conn *sql.Conn, key string) {
	unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), listenerCloseBudget)
	defer cancel()
	if _, err := conn.ExecContext(unlockCtx, `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, key); err != nil {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	}
}

func migrate(ctx context.Context, conn schemaConn, settings schemaSettings) error {
	if _, err := conn.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS pgqueue_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
)`); err != nil {
		return fmt.Errorf("pgqueue: create meta table: %w", err)
	}

	var currentVersion int
	err := conn.QueryRowContext(ctx,
		`SELECT value::int FROM pgqueue_meta WHERE key = 'schema_version'`,
	).Scan(&currentVersion)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("pgqueue: read schema version: %w", err)
	}

	migrateV5 := func(ctx context.Context, conn schemaConn) error { return migrateV5(ctx, conn, settings) }
	migrations := []func(context.Context, schemaConn) error{migrateV1, migrateV2, migrateV3, migrateV4, migrateV5}
	for version, apply := range migrations {
		if currentVersion > version {
			continue
		}
		if err := apply(ctx, conn); err != nil {
			return err
		}
	}

	if _, err := conn.ExecContext(ctx, `
INSERT INTO pgqueue_meta (key, value) VALUES ('schema_version', $1)
ON CONFLICT (key) DO UPDATE SET value = GREATEST(pgqueue_meta.value::int, EXCLUDED.value::int)::text`,
		strconv.Itoa(schemaVersion),
	); err != nil {
		return fmt.Errorf("pgqueue: upsert schema version: %w", err)
	}
	return nil
}

func migrateV1(ctx context.Context, conn schemaConn) error {
	_, err := conn.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS pgqueue_jobs (
    id BIGSERIAL PRIMARY KEY,
    queue_name TEXT NOT NULL,
    payload BYTEA NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_by TEXT,
    claimed_at TIMESTAMPTZ,
    done_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_pgqueue_jobs_status CHECK (status IN ('pending','processing','done','failed'))
);

CREATE INDEX IF NOT EXISTS idx_pgqueue_pending_due
ON pgqueue_jobs(queue_name, available_at, id)
WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_pgqueue_processing
ON pgqueue_jobs(queue_name, claimed_at)
WHERE status = 'processing';
`)
	if err != nil {
		return fmt.Errorf("pgqueue: migrate v1: %w", err)
	}
	return nil
}

// migrateV2 adds the claim counter that fences handlers. Unlike attempts, which
// a replay resets and a snooze refunds, it only grows.
func migrateV2(ctx context.Context, conn schemaConn) error {
	if _, err := conn.ExecContext(ctx, `ALTER TABLE pgqueue_jobs ADD COLUMN IF NOT EXISTS claims INTEGER NOT NULL DEFAULT 0`); err != nil {
		return fmt.Errorf("pgqueue: migrate v2: %w", err)
	}
	return nil
}

// migrateV3 adds the W3C trace context of the enqueuing span. Jobs enqueued
// without one, or before this migration, keep NULL and start a new trace.
func migrateV3(ctx context.Context, conn schemaConn) error {
	if _, err := conn.ExecContext(ctx, `ALTER TABLE pgqueue_jobs ADD COLUMN IF NOT EXISTS trace_context JSONB`); err != nil {
		return fmt.Errorf("pgqueue: migrate v3: %w", err)
	}
	return nil
}

// migrateV4 indexes finished jobs by age, so that PurgeFinished finds expired
// jobs without scanning the table.
func migrateV4(ctx context.Context, conn schemaConn) error {
	if _, err := conn.ExecContext(ctx, `
CREATE INDEX IF NOT EXISTS idx_pgqueue_finished
ON pgqueue_jobs(status, updated_at)
WHERE status IN ('done', 'failed')`); err != nil {
		return fmt.Errorf("pgqueue: migrate v4: %w", err)
	}
	return nil
}

// migrateV5 adds job states without stopping writers. It expands the table: the
// state column, kept in sync with the legacy status column by a trigger in both
// directions, so binaries that only know status keep working during a rolling
// deploy. It backfills the state of live jobs only, through the legacy partial
// indexes, so that every job a claim, the scheduler or the rescuer can act on has
// a state when it returns. CompleteSchema does the rest. See
// docs/technical/queue-job-lifecycle.md.
func migrateV5(ctx context.Context, conn schemaConn, settings schemaSettings) error {
	settings.note(eventMigrateV5)
	if err := expandForStates(ctx, conn, settings); err != nil {
		return fmt.Errorf("pgqueue: migrate v5 states: %w", err)
	}
	for _, status := range []JobStatus{StatusPending, StatusProcessing} {
		if err := backfillStates(ctx, conn, settings, `status = '`+string(status)+`'`); err != nil {
			return fmt.Errorf("pgqueue: migrate v5 %s state: %w", status, err)
		}
	}
	if _, err := conn.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS pgqueue_leader (
    name       TEXT PRIMARY KEY,
    leader_id  TEXT NOT NULL,
    elected_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
)`); err != nil {
		return fmt.Errorf("pgqueue: migrate v5 leader table: %w", err)
	}
	if err := guardSchemaVersion(ctx, conn, settings); err != nil {
		return fmt.Errorf("pgqueue: migrate v5 version guard: %w", err)
	}
	return nil
}

const schemaVersionGuardSQL = `
CREATE OR REPLACE FUNCTION pgqueue_meta_keep_schema_version()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.key = 'schema_version' AND OLD.key = 'schema_version'
       AND NEW.value ~ '^[0-9]+$' AND OLD.value ~ '^[0-9]+$'
       AND NEW.value::int < OLD.value::int THEN
        NEW.value := OLD.value;
    END IF;
    RETURN NEW;
END
$$`

// guardSchemaVersion makes the schema version marker only grow. Binaries older
// than schema v5 end EnsureSchema with an upsert that writes their own version
// back (4, or less for older ones). Without the guard every later start would
// re-run the migrations above that version, and those before v5 take table locks
// even when they change nothing, which would queue behind a CompleteSchema index
// build and stall every writer behind them.
func guardSchemaVersion(ctx context.Context, conn schemaConn, settings schemaSettings) error {
	if _, err := conn.ExecContext(ctx, schemaVersionGuardSQL); err != nil {
		return err
	}
	var exists bool
	if err := conn.QueryRowContext(ctx, `
SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = 'pgqueue_meta'::regclass AND tgname = 'pgqueue_meta_keep_schema_version')`,
	).Scan(&exists); err != nil || exists {
		return err
	}
	return inDDLTx(ctx, conn, settings, `
DROP TRIGGER IF EXISTS pgqueue_meta_keep_schema_version ON pgqueue_meta;
CREATE TRIGGER pgqueue_meta_keep_schema_version
    BEFORE UPDATE ON pgqueue_meta
    FOR EACH ROW EXECUTE FUNCTION pgqueue_meta_keep_schema_version()`)
}

const stateColumnsSQL = `
ALTER TABLE pgqueue_jobs
    ADD COLUMN IF NOT EXISTS state TEXT,
    ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS cancel_requested_at TIMESTAMPTZ`

const stateFunctionsSQL = `
CREATE OR REPLACE FUNCTION pgqueue_state_of_status(status TEXT, available_at TIMESTAMPTZ, attempts INTEGER)
RETURNS TEXT LANGUAGE sql STABLE AS $$
    SELECT CASE status
        WHEN 'pending' THEN CASE
            WHEN available_at <= NOW() THEN 'available'
            WHEN available_at >= TIMESTAMPTZ '9999-12-31 00:00:00+00' THEN 'scheduled'
            WHEN attempts > 0 THEN 'retryable'
            ELSE 'scheduled'
        END
        WHEN 'processing' THEN 'running'
        WHEN 'done' THEN 'completed'
        ELSE 'discarded'
    END
$$;

CREATE OR REPLACE FUNCTION pgqueue_status_of_state(state TEXT)
RETURNS TEXT LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE state
        WHEN 'running' THEN 'processing'
        WHEN 'completed' THEN 'done'
        WHEN 'cancelled' THEN 'failed'
        WHEN 'discarded' THEN 'failed'
        ELSE 'pending'
    END
$$;

CREATE OR REPLACE FUNCTION pgqueue_jobs_sync_state()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state IS NULL
       OR (TG_OP = 'UPDATE' AND NEW.state IS NOT DISTINCT FROM OLD.state AND NEW.status IS DISTINCT FROM OLD.status) THEN
        NEW.state := pgqueue_state_of_status(NEW.status, NEW.available_at, NEW.attempts);
        -- A writer older than schema v5 that replays a finished job does not know
        -- cancel_requested_at, and its claim does not clear it: drop the request of
        -- the job's earlier life here, as ReplayJob does.
        IF TG_OP = 'UPDATE' AND OLD.status IN ('done', 'failed') AND NEW.state IN ('available', 'scheduled', 'retryable') THEN
            NEW.cancel_requested_at := NULL;
        END IF;
    END IF;
    NEW.status := pgqueue_status_of_state(NEW.state);
    IF NEW.state <> 'running' THEN
        NEW.lease_expires_at := NULL;
    END IF;
    RETURN NEW;
END
$$`

const stateTriggerSQL = `
DROP TRIGGER IF EXISTS pgqueue_jobs_sync_state ON pgqueue_jobs;
CREATE TRIGGER pgqueue_jobs_sync_state
    BEFORE INSERT OR UPDATE OF state, status ON pgqueue_jobs
    FOR EACH ROW EXECUTE FUNCTION pgqueue_jobs_sync_state();
ALTER TABLE pgqueue_jobs ADD CONSTRAINT chk_pgqueue_jobs_state CHECK (state IN (
    'available', 'scheduled', 'running', 'retryable', 'completed', 'cancelled', 'discarded'
)) NOT VALID`

// expandForStates adds the state columns, the sync trigger and the state check
// (NOT VALID; CompleteSchema validates it). Each step that locks pgqueue_jobs runs
// only when it is missing. This is load-bearing when the migration runs again (a
// replica that starts while another migrates, or a database whose version marker
// was lowered before the guard existed): while CompleteSchema builds an index
// (SHARE UPDATE EXCLUSIVE) an ALTER TABLE would queue an exclusive lock behind it
// and stall every writer behind that.
func expandForStates(ctx context.Context, conn schemaConn, settings schemaSettings) error {
	var columns int
	var triggerExists, constraintExists bool
	if err := conn.QueryRowContext(ctx, `
SELECT
    (SELECT count(*) FROM pg_attribute WHERE attrelid = 'pgqueue_jobs'::regclass AND NOT attisdropped
        AND attname IN ('state', 'lease_expires_at', 'cancel_requested_at')),
    EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = 'pgqueue_jobs'::regclass AND tgname = 'pgqueue_jobs_sync_state'),
    EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'pgqueue_jobs'::regclass AND conname = 'chk_pgqueue_jobs_state')`,
	).Scan(&columns, &triggerExists, &constraintExists); err != nil {
		return err
	}
	if columns < 3 {
		if err := inDDLTx(ctx, conn, settings, stateColumnsSQL); err != nil {
			return err
		}
	}
	if _, err := conn.ExecContext(ctx, stateFunctionsSQL); err != nil {
		return err
	}
	if !triggerExists || !constraintExists {
		if err := inDDLTx(ctx, conn, settings, `ALTER TABLE pgqueue_jobs DROP CONSTRAINT IF EXISTS chk_pgqueue_jobs_state;`+stateTriggerSQL); err != nil {
			return err
		}
	}
	return nil
}

// inDDLTx runs statements in a transaction with a short lock_timeout. While DDL
// waits for its lock, every writer of the table queues behind it, so the wait
// stays short: on a timeout the transaction is retried with jittered backoff, up
// to settings.ddlAttempts tries and while ctx lasts. A busy table thus delays the
// migration instead of stalling the queue.
func inDDLTx(ctx context.Context, conn schemaConn, settings schemaSettings, statements string) error {
	backoff := ddlRetryFirstBackoff
	for attempt := 1; ; attempt++ {
		err := ddlTxOnce(ctx, conn, settings.lockTimeout, statements)
		if err == nil || !isLockTimeout(err) || attempt >= settings.ddlAttempts {
			return err
		}
		wait := backoff/2 + rand.N(backoff/2+1)
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(wait):
		}
		backoff = min(2*backoff, ddlRetryMaxBackoff)
	}
}

func ddlTxOnce(ctx context.Context, conn schemaConn, lockTimeout time.Duration, statements string) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`SET LOCAL lock_timeout = '%dms'`, lockTimeout.Milliseconds())); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, statements); err != nil {
		return err
	}
	return tx.Commit()
}

// isLockTimeout reports whether err is PostgreSQL's lock_not_available. Both
// supported drivers' errors (pgconn.PgError, pq.Error) expose SQLState.
func isLockTimeout(err error) bool {
	pgErr, ok := errors.AsType[interface {
		error
		SQLState() string
	}](err)
	return ok && pgErr.SQLState() == sqlStateLockNotAvailable
}

// backfillStates walks the rows without a state that match filter once by id.
// Rows written after the trigger was installed already have a state, so one
// forward pass covers every older row. It leaves updated_at alone, so retention
// ages are unchanged.
func backfillStates(ctx context.Context, conn schemaConn, settings schemaSettings, filter string) error {
	var afterID int64
	for {
		var lastID sql.NullInt64
		err := conn.QueryRowContext(ctx, `
WITH batch AS (
    SELECT id FROM pgqueue_jobs
    WHERE id > $1 AND state IS NULL AND `+filter+`
    ORDER BY id
    LIMIT $2
), updated AS (
    UPDATE pgqueue_jobs j
    SET state = pgqueue_state_of_status(j.status, j.available_at, j.attempts)
    FROM batch
    WHERE j.id = batch.id AND j.state IS NULL
    RETURNING j.id
)
SELECT max(id) FROM updated`, afterID, settings.backfillBatch).Scan(&lastID)
		if err != nil {
			return err
		}
		if !lastID.Valid {
			return nil
		}
		settings.note(eventBackfillBatch)
		afterID = lastID.Int64
	}
}

// schemaProgress is how far schema v5 has come, read from the catalog. The
// version marker cannot tell: EnsureSchema writes it before CompleteSchema runs.
type schemaProgress struct {
	expanded      bool
	stateRequired bool
	stateChecked  bool
	validIndexes  []string
}

func readSchemaProgress(ctx context.Context, conn schemaConn) (schemaProgress, error) {
	var p schemaProgress
	var indexes string
	err := conn.QueryRowContext(ctx, `
SELECT
    (SELECT count(*) FROM pg_attribute WHERE attrelid = to_regclass('pgqueue_jobs') AND NOT attisdropped
        AND attname IN ('state', 'lease_expires_at', 'cancel_requested_at')) = 3
    AND EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = to_regclass('pgqueue_jobs') AND tgname = 'pgqueue_jobs_sync_state')
    AND EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = to_regclass('pgqueue_jobs') AND conname = 'chk_pgqueue_jobs_state'),
    COALESCE((SELECT attnotnull FROM pg_attribute WHERE attrelid = to_regclass('pgqueue_jobs')
        AND attname = 'state' AND NOT attisdropped), FALSE),
    COALESCE((SELECT convalidated FROM pg_constraint WHERE conrelid = to_regclass('pgqueue_jobs')
        AND conname = 'chk_pgqueue_jobs_state'), FALSE),
    COALESCE((SELECT string_agg(c.relname, ',') FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
        WHERE i.indrelid = to_regclass('pgqueue_jobs') AND i.indisvalid), '')`,
	).Scan(&p.expanded, &p.stateRequired, &p.stateChecked, &indexes)
	if indexes != "" {
		p.validIndexes = strings.Split(indexes, ",")
	}
	return p, err
}

func (p schemaProgress) complete() bool {
	if !p.expanded || !p.stateRequired || !p.stateChecked {
		return false
	}
	for _, index := range append(liveStateIndexes, finalizedStateIndexes...) {
		if !slices.Contains(p.validIndexes, index.name) {
			return false
		}
	}
	return true
}

// completeSmallSchema completes the schema inside EnsureSchema when the table is
// small enough for that to take milliseconds, so a fresh database is complete at
// once. It builds the indexes without CONCURRENTLY: on a small table the brief
// write lock costs less than waiting, as CONCURRENTLY does, for every older
// transaction in the database. It leaves the work to a client already doing it.
func completeSmallSchema(ctx context.Context, conn *sql.Conn, settings schemaSettings) error {
	progress, err := readSchemaProgress(ctx, conn)
	if err != nil || progress.complete() {
		return err
	}
	var small bool
	if err := conn.QueryRowContext(ctx,
		`SELECT count(*) <= $1 FROM (SELECT 1 FROM pgqueue_jobs LIMIT $1::int + 1) AS sample`, settings.inlineRows,
	).Scan(&small); err != nil {
		return err
	}
	if !small {
		return nil
	}
	locked, err := tryLockSchema(ctx, conn, completionLockKey)
	if err != nil || !locked {
		return err
	}
	defer unlockSchema(ctx, conn, completionLockKey)
	return completeSchema(ctx, conn, settings, false)
}

// completeSchema runs the steps of schema v5 that read the whole table, each only
// when the catalog shows it missing. The indexes over live states come first:
// every live job has a state once EnsureSchema returns, and these are the indexes
// the claim, the scheduler and the rescuer use.
func completeSchema(ctx context.Context, conn schemaConn, settings schemaSettings, concurrently bool) error {
	progress, err := readSchemaProgress(ctx, conn)
	if err != nil {
		return err
	}
	if !progress.expanded {
		return ErrSchemaNotExpanded
	}
	for _, index := range liveStateIndexes {
		if err := createIndex(ctx, conn, settings, index, concurrently); err != nil {
			return fmt.Errorf("index %s: %w", index.name, err)
		}
	}
	if !progress.stateRequired {
		if err := requireState(ctx, conn, settings); err != nil {
			return fmt.Errorf("state: %w", err)
		}
	}
	if !progress.stateChecked {
		if err := inDDLTx(ctx, conn, settings, `ALTER TABLE pgqueue_jobs VALIDATE CONSTRAINT chk_pgqueue_jobs_state`); err != nil {
			return fmt.Errorf("validate state: %w", err)
		}
	}
	for _, index := range finalizedStateIndexes {
		if err := createIndex(ctx, conn, settings, index, concurrently); err != nil {
			return fmt.Errorf("index %s: %w", index.name, err)
		}
	}
	return nil
}

// requireState backfills state on every row, then sets NOT NULL through a
// validated CHECK, so PostgreSQL skips the table scan it would otherwise run
// under an exclusive lock. A column that is already NOT NULL needs neither.
func requireState(ctx context.Context, conn schemaConn, settings schemaSettings) error {
	var nullable bool
	if err := conn.QueryRowContext(ctx, `
SELECT NOT attnotnull FROM pg_attribute
WHERE attrelid = 'pgqueue_jobs'::regclass AND attname = 'state'`).Scan(&nullable); err != nil {
		return err
	}
	if !nullable {
		return nil
	}
	if err := backfillStates(ctx, conn, settings, "TRUE"); err != nil {
		return err
	}
	if err := inDDLTx(ctx, conn, settings, `
ALTER TABLE pgqueue_jobs DROP CONSTRAINT IF EXISTS chk_pgqueue_jobs_state_not_null;
ALTER TABLE pgqueue_jobs ADD CONSTRAINT chk_pgqueue_jobs_state_not_null CHECK (state IS NOT NULL) NOT VALID`); err != nil {
		return err
	}
	if err := inDDLTx(ctx, conn, settings, `ALTER TABLE pgqueue_jobs VALIDATE CONSTRAINT chk_pgqueue_jobs_state_not_null`); err != nil {
		return err
	}
	return inDDLTx(ctx, conn, settings, `
ALTER TABLE pgqueue_jobs ALTER COLUMN state SET NOT NULL;
ALTER TABLE pgqueue_jobs DROP CONSTRAINT chk_pgqueue_jobs_state_not_null`)
}

type indexDefinition struct {
	name       string
	definition string
}

// The state lists are literals, not parameters, so every plan can use these
// partial indexes.
var (
	liveStateIndexes = []indexDefinition{
		{"idx_pgqueue_claimable", `(queue_name, available_at, id) WHERE state IN ('available', 'scheduled', 'retryable')`},
		{"idx_pgqueue_due", `(available_at, id) WHERE state IN ('scheduled', 'retryable')`},
		{"idx_pgqueue_running", `(queue_name, claimed_at) WHERE state = 'running'`},
	}
	finalizedStateIndexes = []indexDefinition{
		{"idx_pgqueue_finalized", `(state, updated_at) WHERE state IN ('completed', 'cancelled', 'discarded')`},
	}
)

// createIndex builds index unless a valid one exists. A failed or interrupted
// concurrent build leaves an invalid index that IF NOT EXISTS would keep, so it
// is dropped first. Without concurrently, the build takes a short write lock
// under the DDL lock_timeout; completeSmallSchema uses that on small tables.
func createIndex(ctx context.Context, conn schemaConn, settings schemaSettings, index indexDefinition, concurrently bool) error {
	var valid sql.NullBool
	err := conn.QueryRowContext(ctx,
		`SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass($1)`, index.name,
	).Scan(&valid)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if valid.Valid && valid.Bool {
		return nil
	}
	if !concurrently {
		return inDDLTx(ctx, conn, settings, `DROP INDEX IF EXISTS `+index.name+`;
CREATE INDEX IF NOT EXISTS `+index.name+` ON pgqueue_jobs `+index.definition)
	}
	if valid.Valid {
		if _, err := conn.ExecContext(ctx, `DROP INDEX CONCURRENTLY IF EXISTS `+index.name); err != nil {
			return err
		}
	}
	_, err = conn.ExecContext(ctx, `CREATE INDEX CONCURRENTLY IF NOT EXISTS `+index.name+` ON pgqueue_jobs `+index.definition)
	return err
}
