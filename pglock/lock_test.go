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
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

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
		}()
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
