package queue

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

var shared struct {
	once      sync.Once
	container testcontainers.Container
	db        *sql.DB
	err       error
}

func TestMain(m *testing.M) {
	code := m.Run()
	if shared.db != nil {
		_ = shared.db.Close()
	}
	if shared.container != nil {
		_ = shared.container.Terminate(context.Background())
	}
	os.Exit(code)
}

// sharedQueue returns a client on one PostgreSQL container shared by the tests that
// use it. Such tests isolate themselves with unique queue names.
func sharedQueue(t *testing.T) *Client {
	t.Helper()
	ctx := context.Background()
	shared.once.Do(func() {
		container, connString := startWorkerPostgres(t, ctx)
		shared.container = container
		shared.db, shared.err = sql.Open("pgx", connString)
		if shared.err == nil {
			shared.err = waitWorkerPing(ctx, shared.db, 20*time.Second)
		}
		if shared.err == nil {
			var q *Client
			q, shared.err = New(shared.db)
			if shared.err == nil {
				shared.err = q.EnsureSchema(ctx)
			}
		}
	})
	if shared.err != nil {
		t.Fatalf("shared postgres: %v", shared.err)
	}
	q, err := New(shared.db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	return q
}

func uniqueQueueName(t *testing.T) string {
	return fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
}
