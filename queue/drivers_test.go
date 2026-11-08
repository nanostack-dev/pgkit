package queue

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// driverDB is a fresh database on the shared container, opened with one of the
// database/sql drivers pgkit supports.
type driverDB struct {
	driver string
	db     *sql.DB
	dsn    string
}

var databaseCounter atomic.Int64

// eachDriver runs test once per supported driver (pgx stdlib and lib/pq), each on
// its own fresh database, so maintenance services that act on every queue cannot
// touch another test's jobs.
func eachDriver(t *testing.T, test func(t *testing.T, d driverDB)) {
	t.Helper()
	for _, driver := range []string{"pgx", "postgres"} {
		t.Run(driverLabel(driver), func(t *testing.T) {
			test(t, freshDatabase(t, driver))
		})
	}
}

func driverLabel(driver string) string {
	if driver == "postgres" {
		return "libpq"
	}
	return driver
}

func freshDatabase(t *testing.T, driver string) driverDB {
	t.Helper()
	sharedQueue(t)
	ctx := context.Background()
	name := fmt.Sprintf("pgkit_queue_%d_%d", time.Now().UnixNano(), databaseCounter.Add(1))
	if _, err := shared.db.ExecContext(ctx, `CREATE DATABASE `+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	parsed, err := url.Parse(shared.connString)
	if err != nil {
		t.Fatalf("parse connection string: %v", err)
	}
	parsed.Path = "/" + name
	dsn := parsed.String()
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("open %s: %v", driver, err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		_, _ = shared.db.ExecContext(context.Background(), `DROP DATABASE IF EXISTS `+name+` WITH (FORCE)`)
	})
	return driverDB{driver: driver, db: db, dsn: dsn}
}

// client returns a migrated client whose OnEnqueue workers can listen with either
// driver, with fast leader election for tests.
func (d driverDB) client(t *testing.T) *Client {
	t.Helper()
	q := d.bareClient(t)
	if err := q.EnsureSchema(context.Background()); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	return q
}

func (d driverDB) bareClient(t *testing.T) *Client {
	t.Helper()
	q, err := New(d.db)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	if d.driver != "pgx" {
		if err := q.ListenOn(d.dsn); err != nil {
			t.Fatalf("listen on: %v", err)
		}
	}
	q.maintainer.electEvery = 100 * time.Millisecond
	q.maintainer.ttl = 2 * time.Second
	return q
}

// exec runs a statement and fails the test on error.
func (d driverDB) exec(t *testing.T, query string, args ...any) {
	t.Helper()
	if _, err := d.db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("%s: %v", strings.TrimSpace(strings.SplitN(query, "\n", 2)[0]), err)
	}
}

func (d driverDB) state(t *testing.T, id int64) (JobState, JobStatus) {
	t.Helper()
	var state JobState
	var status JobStatus
	if err := d.db.QueryRowContext(context.Background(),
		`SELECT state, status FROM pgqueue_jobs WHERE id = $1`, id).Scan(&state, &status); err != nil {
		t.Fatalf("read state of job %d: %v", id, err)
	}
	return state, status
}

func (d driverDB) requireState(t *testing.T, id int64, want JobState) {
	t.Helper()
	state, status := d.state(t, id)
	if state != want || status != want.LegacyStatus() {
		t.Fatalf("job %d: state %s status %s, want %s (%s)", id, state, status, want, want.LegacyStatus())
	}
}

func (d driverDB) requireStateEventually(t *testing.T, id int64, want JobState, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		state, _ := d.state(t, id)
		if state == want {
			d.requireState(t, id, want)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %d stayed %s, want %s within %s", id, state, want, within)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func mustEnqueue(t *testing.T, q *Client, p EnqueueParams) int64 {
	t.Helper()
	if p.Payload == nil {
		p.Payload = []byte(`{}`)
	}
	id, err := q.Enqueue(context.Background(), p)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return id
}

func mustClaim(t *testing.T, q *Client, queueName string, lease time.Duration) *Job {
	t.Helper()
	job, found, err := q.claim(context.Background(), queueName, "test-worker", lease)
	if err != nil || !found {
		t.Fatalf("claim %s: found=%v err=%v", queueName, found, err)
	}
	return job
}

// runWorker runs w until the test ends and waits for it to stop.
func runWorker(t *testing.T, w *Worker) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}
