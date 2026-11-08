package queue

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"
)

func newRetentionQueue(t *testing.T) (*Client, *sql.DB) {
	t.Helper()
	ctx := context.Background()
	db := createTestDB(t, ctx)
	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	return q, db
}

func enqueueInStatus(t *testing.T, q *Client, queueName string, status JobStatus, age time.Duration) int64 {
	t.Helper()
	ctx := context.Background()
	id, err := q.Enqueue(ctx, EnqueueParams{QueueName: queueName, Payload: []byte(`{}`)})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if status != StatusPending {
		job, found, err := q.Claim(ctx, queueName, "retention-test")
		if err != nil || !found || job.ID != id {
			t.Fatalf("claim: job=%v found=%v err=%v", job, found, err)
		}
		switch status {
		case StatusDone:
			err = q.Ack(ctx, id)
		case StatusFailed:
			err = q.Fail(ctx, id, errors.New("boom"))
		}
		if err != nil {
			t.Fatalf("settle %s: %v", status, err)
		}
	}
	if _, err := q.DB().ExecContext(ctx,
		`UPDATE pgqueue_jobs SET updated_at = NOW() - make_interval(secs => $2) WHERE id = $1`,
		id, age.Seconds(),
	); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	return id
}

func remainingJobIDs(t *testing.T, db *sql.DB) map[int64]bool {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `SELECT id FROM pgqueue_jobs`)
	if err != nil {
		t.Fatalf("list ids: %v", err)
	}
	defer func() { _ = rows.Close() }()
	ids := map[int64]bool{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan id: %v", err)
		}
		ids[id] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return ids
}

func TestPurgeFinishedDeletesOnlyExpiredFinishedJobsOfEveryQueue(t *testing.T) {
	q, db := newRetentionQueue(t)
	ctx := context.Background()

	var expired []int64
	for range 3 {
		expired = append(expired, enqueueInStatus(t, q, "emails", StatusDone, 48*time.Hour))
	}
	expired = append(expired, enqueueInStatus(t, q, "receipts", StatusDone, 48*time.Hour))
	expired = append(expired, enqueueInStatus(t, q, "receipts", StatusFailed, 10*24*time.Hour))

	kept := []int64{
		enqueueInStatus(t, q, "emails", StatusDone, time.Hour),
		enqueueInStatus(t, q, "emails", StatusFailed, 48*time.Hour),
		enqueueInStatus(t, q, "emails", StatusProcessing, 30*24*time.Hour),
		enqueueInStatus(t, q, "emails", StatusPending, 30*24*time.Hour),
	}

	result, err := q.PurgeFinished(ctx, RetentionPolicy{
		KeepDone:   24 * time.Hour,
		KeepFailed: 7 * 24 * time.Hour,
		BatchSize:  2,
	})
	if err != nil {
		t.Fatalf("purge finished: %v", err)
	}
	if result != (RetentionResult{Done: 4, Failed: 1}) {
		t.Fatalf("result = %+v, want 4 done and 1 failed", result)
	}

	remaining := remainingJobIDs(t, db)
	for _, id := range expired {
		if remaining[id] {
			t.Errorf("expired job %d was kept", id)
		}
	}
	for _, id := range kept {
		if !remaining[id] {
			t.Errorf("job %d was deleted", id)
		}
	}
}

func TestPurgeFinishedKeepsStatusWithZeroAgeForever(t *testing.T) {
	q, db := newRetentionQueue(t)

	done := enqueueInStatus(t, q, "emails", StatusDone, 365*24*time.Hour)
	failed := enqueueInStatus(t, q, "emails", StatusFailed, 365*24*time.Hour)

	result, err := q.PurgeFinished(context.Background(), RetentionPolicy{KeepDone: time.Hour})
	if err != nil {
		t.Fatalf("purge finished: %v", err)
	}
	if result != (RetentionResult{Done: 1}) {
		t.Fatalf("result = %+v, want 1 done", result)
	}
	remaining := remainingJobIDs(t, db)
	if remaining[done] || !remaining[failed] {
		t.Fatalf("remaining = %v, want only failed job %d", remaining, failed)
	}
}

func TestRetentionPolicyRejectsInvalidSettings(t *testing.T) {
	q, _ := newRetentionQueue(t)

	for name, policy := range map[string]RetentionPolicy{
		"nothing to purge":    {},
		"negative done age":   {KeepDone: -time.Hour},
		"negative failed age": {KeepDone: time.Hour, KeepFailed: -time.Hour},
		"negative interval":   {KeepDone: time.Hour, Interval: -time.Second},
		"negative batch size": {KeepDone: time.Hour, BatchSize: -1},
	} {
		t.Run(name, func(t *testing.T) {
			if err := policy.Validate(); !errors.Is(err, ErrInvalidRetentionPolicy) {
				t.Fatalf("Validate() = %v, want ErrInvalidRetentionPolicy", err)
			}
			if _, err := q.PurgeFinished(context.Background(), policy); !errors.Is(err, ErrInvalidRetentionPolicy) {
				t.Fatalf("PurgeFinished() = %v, want ErrInvalidRetentionPolicy", err)
			}
			if err := q.RunRetention(context.Background(), policy); !errors.Is(err, ErrInvalidRetentionPolicy) {
				t.Fatalf("RunRetention() = %v, want ErrInvalidRetentionPolicy", err)
			}
		})
	}
}

func TestRunRetentionPurgesAtStartAndStopsOnCancel(t *testing.T) {
	q, db := newRetentionQueue(t)
	expired := enqueueInStatus(t, q, "emails", StatusDone, 48*time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- q.RunRetention(ctx, RetentionPolicy{KeepDone: 24 * time.Hour}) }()

	deadline := time.Now().Add(10 * time.Second)
	for remainingJobIDs(t, db)[expired] {
		if time.Now().After(deadline) {
			t.Fatal("expired job was not purged at start")
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("RunRetention() = %v, want nil after cancel", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunRetention did not stop after cancel")
	}
}

func TestPurgeFinishedUsesFinishedIndexUnderGenericPlans(t *testing.T) {
	_, db := newRetentionQueue(t)
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	for _, stmt := range []string{
		`SET enable_seqscan = off`,
		`SET plan_cache_mode = force_generic_plan`,
	} {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	for state, query := range map[JobState]string{
		StateCompleted: deleteExpiredCompletedSQL,
		StateDiscarded: deleteExpiredDiscardedSQL,
		StateCancelled: deleteExpiredCancelledSQL,
	} {
		t.Run(string(state), func(t *testing.T) {
			if _, err := conn.ExecContext(ctx, `PREPARE purge_plan(float8, int) AS `+query); err != nil {
				t.Fatalf("prepare: %v", err)
			}
			defer func() { _, _ = conn.ExecContext(ctx, `DEALLOCATE purge_plan`) }()

			rows, err := conn.QueryContext(ctx, `EXPLAIN EXECUTE purge_plan(3600, 1000)`)
			if err != nil {
				t.Fatalf("explain: %v", err)
			}
			defer func() { _ = rows.Close() }()
			var plan strings.Builder
			for rows.Next() {
				var line string
				if err := rows.Scan(&line); err != nil {
					t.Fatalf("scan plan: %v", err)
				}
				plan.WriteString(line + "\n")
			}
			if !strings.Contains(plan.String(), "idx_pgqueue_finalized") {
				t.Fatalf("plan does not use idx_pgqueue_finalized:\n%s", plan.String())
			}
		})
	}
}
