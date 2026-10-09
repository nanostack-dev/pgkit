package adminui

import (
	"context"
	"database/sql"
	"net/http"
	"testing"
	"time"

	qpkg "github.com/nanostack-dev/pgkit/queue"
)

func holdAdvisoryLocks(t *testing.T, ctx context.Context, db *sql.DB, statements ...string) *sql.Conn {
	t.Helper()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	for _, statement := range statements {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	return conn
}

func findLock(locks []advisoryLockSession, key string, granted bool) *advisoryLockSession {
	for i := range locks {
		if locks[i].Key != nil && *locks[i].Key == key && locks[i].Granted == granted {
			return &locks[i]
		}
	}
	return nil
}

func TestLocksListAdvisoryLocksWithTheirKeys(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)
	queue, _ := newClients(t, ctx, db)
	server := newServer(t, queue, Options{Token: "secret"})
	locksURL := server.URL + "/api/dashboard/locks"

	var empty advisoryLocksResponse
	getJSON(t, locksURL, http.StatusOK, &empty)
	if empty.Items == nil || len(empty.Items) != 0 {
		t.Fatalf("locks without holders = %+v", empty)
	}
	if _, err := time.Parse(time.RFC3339Nano, empty.Now); err != nil {
		t.Fatalf("now = %q: %v", empty.Now, err)
	}

	holder := holdAdvisoryLocks(t, ctx, db,
		"SELECT pg_advisory_lock(-5)",
		"SELECT pg_advisory_lock(1, 2)",
		"SELECT pg_advisory_lock(8589934593)",
		"SELECT pg_advisory_lock(-9223372036854775808)",
		"SELECT pg_advisory_lock(9223372036854775807)",
		"SELECT pg_advisory_lock(0)",
	)
	var holderPID int64
	if err := holder.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&holderPID); err != nil {
		t.Fatal(err)
	}

	var locks advisoryLocksResponse
	getJSON(t, locksURL, http.StatusOK, &locks)
	if len(locks.Items) != 6 {
		t.Fatalf("locks = %+v", locks.Items)
	}
	wantKeys := map[string]struct {
		classID, objectID, subID int64
	}{
		"-5":                   {4294967295, 4294967291, 1},
		"1,2":                  {1, 2, 2},
		"8589934593":           {2, 1, 1},
		"-9223372036854775808": {2147483648, 0, 1},
		"9223372036854775807":  {2147483647, 4294967295, 1},
		"0":                    {0, 0, 1},
	}
	for key, want := range wantKeys {
		lock := findLock(locks.Items, key, true)
		if lock == nil {
			t.Fatalf("no granted lock with key %q in %+v", key, locks.Items)
		}
		if lock.PID != holderPID || lock.Mode != "ExclusiveLock" || lock.ClassID != want.classID || lock.ObjectID != want.objectID || lock.ObjectSubID != want.subID {
			t.Fatalf("lock %q = %+v, want pid %d classid %d objid %d objsubid %d", key, *lock, holderPID, want.classID, want.objectID, want.subID)
		}
		if lock.State == nil || lock.StateChange == nil {
			t.Fatalf("lock %q has no session activity: %+v", key, *lock)
		}
	}

	var summary struct {
		AdvisoryLocks int `json:"advisory_locks"`
	}
	getJSON(t, server.URL+"/api/dashboard/queue/summary", http.StatusOK, &summary)
	if summary.AdvisoryLocks != 6 {
		t.Fatalf("summary advisory_locks = %d", summary.AdvisoryLocks)
	}

	var legacy []qpkg.AdvisoryLock
	getJSON(t, server.URL+"/api/dashboard/queue/locks", http.StatusOK, &legacy)
	if len(legacy) != 6 {
		t.Fatalf("legacy locks = %+v", legacy)
	}

	raw := getMap(t, locksURL, http.StatusOK)
	requireKeys(t, "locks response", raw, "now", "items")
	for _, item := range raw["items"].([]any) {
		requireKeys(t, "lock", item.(map[string]any), "pid", "mode", "granted", "key", "classid", "objid", "objsubid", "application_name", "state", "xact_start", "state_change", "wait_event_type", "wait_event")
	}
}

func TestLocksShowWaitersAfterGrantedLocks(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)
	queue, _ := newClients(t, ctx, db)
	server := newServer(t, queue, Options{Token: "secret"})
	locksURL := server.URL + "/api/dashboard/locks"

	holder := holdAdvisoryLocks(t, ctx, db, "SELECT pg_advisory_lock(-5)")
	waiter, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = waiter.Close() })
	acquired := make(chan error, 1)
	go func() {
		_, err := waiter.ExecContext(ctx, "SELECT pg_advisory_lock(-5)")
		acquired <- err
	}()

	var locks advisoryLocksResponse
	requireEventually(t, 10*time.Second, 20*time.Millisecond, func() bool {
		getJSON(t, locksURL, http.StatusOK, &locks)
		return findLock(locks.Items, "-5", false) != nil
	})
	if len(locks.Items) != 2 || !locks.Items[0].Granted || locks.Items[1].Granted {
		t.Fatalf("locks are not granted first: %+v", locks.Items)
	}
	waiting := locks.Items[1]
	if waiting.WaitEventType == nil || *waiting.WaitEventType != "Lock" || waiting.WaitEvent == nil || *waiting.WaitEvent != "advisory" {
		t.Fatalf("waiting lock = %+v", waiting)
	}
	if locks.Items[0].PID == waiting.PID {
		t.Fatalf("holder and waiter share pid %d", waiting.PID)
	}

	if _, err := holder.ExecContext(ctx, "SELECT pg_advisory_unlock_all()"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("waiter: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("waiter never acquired the lock")
	}
}
