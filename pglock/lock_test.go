package pglock

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestNewNilDB(t *testing.T) {
	_, err := New(nil)
	if !errors.Is(err, ErrNilDB) {
		t.Fatalf("expected ErrNilDB, got %v", err)
	}
}

func TestTryWithLockConcurrentExclusion(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	client, err := New(db)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	const workers = 10
	var maxConcurrent atomic.Int32
	var currentHolders atomic.Int32

	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			_, err := client.TryWithLock(ctx, "shared-lock", func(_ context.Context, _ *sql.Tx) error {
				now := currentHolders.Add(1)
				for {
					prev := maxConcurrent.Load()
					if now <= prev {
						break
					}
					if maxConcurrent.CompareAndSwap(prev, now) {
						break
					}
				}

				time.Sleep(20 * time.Millisecond)
				currentHolders.Add(-1)
				return nil
			})
			if err != nil {
				t.Errorf("TryWithLock failed: %v", err)
			}
		})
	}

	wg.Wait()

	if got := maxConcurrent.Load(); got > 1 {
		t.Fatalf("lock exclusion violated: %d holders", got)
	}
}

func TestTryWithSessionLockExclusive(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	clientA, err := New(db)
	if err != nil {
		t.Fatalf("new clientA: %v", err)
	}
	clientB, err := New(db)
	if err != nil {
		t.Fatalf("new clientB: %v", err)
	}

	ready := make(chan struct{})
	release := make(chan struct{})

	go func() {
		_, _ = clientA.TryWithSessionLock(ctx, "session-shared", func(_ context.Context) error {
			close(ready)
			<-release
			return nil
		})
	}()

	<-ready

	acquired, err := clientB.TryWithSessionLock(ctx, "session-shared", nil)
	if err != nil {
		t.Fatalf("clientB TryWithSessionLock: %v", err)
	}
	if acquired {
		t.Fatalf("expected session lock to be held by clientA")
	}

	close(release)
	time.Sleep(30 * time.Millisecond)

	acquiredAfter, err := clientB.TryWithSessionLock(ctx, "session-shared", nil)
	if err != nil {
		t.Fatalf("clientB TryWithSessionLock after release: %v", err)
	}
	if !acquiredAfter {
		t.Fatalf("expected session lock to be released")
	}
}

func TestTryAdvisoryXactLockNilTx(t *testing.T) {
	_, err := TryAdvisoryXactLock(context.Background(), nil, "any")
	if !errors.Is(err, ErrNilTx) {
		t.Fatalf("expected ErrNilTx, got %v", err)
	}
}

func TestTryAdvisoryXactLockBoundToCallerTx(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)

	const key = "byo-tx-lock"

	tx1, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}

	locked1, err := TryAdvisoryXactLock(ctx, tx1, key)
	if err != nil {
		t.Fatalf("tx1 acquire: %v", err)
	}
	if !locked1 {
		t.Fatalf("tx1 expected to acquire lock")
	}

	// tx2 runs on a different pooled connection (tx1 still holds its own), so it
	// must not take the same key while tx1 is open.
	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	locked2, err := TryAdvisoryXactLock(ctx, tx2, key)
	if err != nil {
		t.Fatalf("tx2 acquire: %v", err)
	}
	if locked2 {
		t.Fatalf("tx2 acquired a lock held by tx1")
	}
	if err := tx2.Rollback(); err != nil {
		t.Fatalf("rollback tx2: %v", err)
	}

	// Committing tx1 releases the xact lock; a fresh transaction can then take it.
	if err := tx1.Commit(); err != nil {
		t.Fatalf("commit tx1: %v", err)
	}

	tx3, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx3: %v", err)
	}
	defer func() { _ = tx3.Rollback() }()
	locked3, err := TryAdvisoryXactLock(ctx, tx3, key)
	if err != nil {
		t.Fatalf("tx3 acquire: %v", err)
	}
	if !locked3 {
		t.Fatalf("tx3 expected to acquire lock after tx1 committed")
	}
}

func TestKeyHashStability(t *testing.T) {
	const expected int64 = -4125332225188682556
	if got := KeyHash("integration-events-monitor"); got != expected {
		t.Fatalf("KeyHash changed: got=%d expected=%d", got, expected)
	}
}

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

	if lastErr == nil {
		return errors.New("ping timeout")
	}

	return lastErr
}
