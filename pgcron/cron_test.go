package pgcron_test

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nanostack-dev/pgkit/pgcron"
	"github.com/nanostack-dev/pgkit/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestValidation(t *testing.T) {
	db := &sql.DB{}
	run := func(context.Context, *sql.Tx) error { return nil }
	for _, params := range []pgcron.Params{
		{Interval: time.Minute, Run: run},
		{Name: "empty_callback", Interval: time.Minute},
		{Name: "zero_interval", Run: run},
		{Name: "tiny_interval", Interval: time.Microsecond, Run: run},
		{Name: "negative_retry", Interval: time.Minute, RetryInterval: -1, Run: run},
	} {
		_, err := pgcron.New(db, params)
		require.Error(t, err)
	}
	_, err := pgcron.New(nil, pgcron.Params{Name: "nil_db", Interval: time.Minute, Run: run})
	require.Error(t, err)

	for _, builder := range []pgcron.Builder{
		pgcron.Named(nil, "nil_db").Every(time.Minute),
		pgcron.Named(db, " ").Every(time.Minute),
		pgcron.Named(db, "zero_interval"),
		pgcron.Named(db, "tiny_interval").Every(time.Microsecond),
		pgcron.Named(db, "negative_retry").Every(time.Minute).RetryEvery(-1),
		{},
	} {
		_, err := builder.DoTx(run)
		require.Error(t, err)
	}
	_, err = pgcron.Named(db, "nil_callback").Every(time.Minute).DoTx(nil)
	require.Error(t, err)
}

func TestPostgresScheduling(t *testing.T) {
	db := testDB(t)
	newJob := func(t *testing.T, run func(context.Context, *sql.Tx) error, after func(context.Context) error) *pgcron.Job {
		t.Helper()
		job, err := pgcron.Named(db, t.Name()).Every(time.Hour).AfterRun(after).DoTx(run)
		require.NoError(t, err)
		return job
	}
	succeed := func(context.Context, *sql.Tx) error { return nil }

	t.Run("builder copies do not change previously built jobs", func(t *testing.T) {
		base := pgcron.Named(db, t.Name()).Every(time.Hour)
		original, err := base.DoTx(succeed)
		require.NoError(t, err)
		_, err = base.Every(0).DoTx(succeed)
		require.Error(t, err)
		result, err := original.RunOnce(t.Context())
		require.NoError(t, err)
		require.Equal(t, pgcron.Ran, result.Outcome)
		peer, err := base.DoTx(succeed)
		require.NoError(t, err)
		result, err = peer.RunOnce(t.Context())
		require.NoError(t, err)
		assert.Equal(t, pgcron.NotDue, result.Outcome)
	})

	t.Run("staggered replicas and restart preserve schedule", func(t *testing.T) {
		calls := 0
		run := func(context.Context, *sql.Tx) error { calls++; return nil }
		first, err := newJob(t, run, nil).RunOnce(t.Context())
		require.NoError(t, err)
		require.Equal(t, pgcron.Ran, first.Outcome)
		for range 4 {
			result, err := newJob(t, run, nil).RunOnce(t.Context())
			require.NoError(t, err)
			assert.Equal(t, pgcron.NotDue, result.Outcome)
			assert.Equal(t, first.NextRunAt, result.NextRunAt)
		}
		assert.Equal(t, 1, calls)
	})

	t.Run("missed occurrences coalesce and keep cadence", func(t *testing.T) {
		_, err := db.ExecContext(t.Context(),
			"INSERT INTO pgcron_schedules VALUES ($1, date_trunc('hour', now()) - interval '2 days')", t.Name())
		require.NoError(t, err)
		result, err := newJob(t, succeed, nil).RunOnce(t.Context())
		require.NoError(t, err)
		require.Equal(t, pgcron.Ran, result.Outcome)
		var expected time.Time
		require.NoError(t, db.QueryRowContext(t.Context(),
			"SELECT date_trunc('hour', now()) + interval '1 hour'").Scan(&expected))
		assert.Equal(t, expected, result.NextRunAt)
		result, err = newJob(t, succeed, nil).RunOnce(t.Context())
		require.NoError(t, err)
		assert.Equal(t, pgcron.NotDue, result.Outcome)
	})

	t.Run("long runs advance beyond completion", func(t *testing.T) {
		var completedAt time.Time
		job, err := pgcron.New(db, pgcron.Params{
			Name: t.Name(), Interval: time.Millisecond,
			Run: func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, "SELECT pg_sleep(0.02)")
				if err != nil {
					return err
				}
				return tx.QueryRowContext(ctx, "SELECT clock_timestamp()").Scan(&completedAt)
			},
		})
		require.NoError(t, err)
		result, err := job.RunOnce(t.Context())
		require.NoError(t, err)
		assert.Equal(t, pgcron.Ran, result.Outcome)
		assert.True(t, result.NextRunAt.After(completedAt))
	})

	t.Run("lock covers callback but not post commit hook", func(t *testing.T) {
		hooks := 0
		peer := newJob(t, succeed, nil)
		job := newJob(t, func(ctx context.Context, _ *sql.Tx) error {
			result, err := peer.RunOnce(ctx)
			require.NoError(t, err)
			assert.Equal(t, pgcron.Busy, result.Outcome)
			return nil
		}, func(ctx context.Context) error {
			hooks++
			result, err := peer.RunOnce(ctx)
			require.NoError(t, err)
			assert.Equal(t, pgcron.NotDue, result.Outcome)
			return nil
		})
		_, err := job.RunOnce(t.Context())
		require.NoError(t, err)
		assert.Equal(t, 1, hooks)
	})

	t.Run("concurrent replicas commit one occurrence", func(t *testing.T) {
		var calls atomic.Int32
		job := newJob(t, func(context.Context, *sql.Tx) error {
			calls.Add(1)
			return nil
		}, nil)
		var wg sync.WaitGroup
		for range 12 {
			wg.Go(func() {
				_, err := job.RunOnce(t.Context())
				assert.NoError(t, err)
			})
		}
		wg.Wait()
		assert.EqualValues(t, 1, calls.Load())
	})

	t.Run("failure rolls back schedule and queued work", func(t *testing.T) {
		q, err := queue.New(db)
		require.NoError(t, err)
		require.NoError(t, q.EnsureSchema(t.Context()))
		cause := errors.New("enqueue then fail")
		fail := true
		job := newJob(t, func(ctx context.Context, tx *sql.Tx) error {
			_, err := q.EnqueueTx(ctx, tx, queue.EnqueueParams{QueueName: t.Name(), Payload: []byte("{}")})
			if err != nil {
				return err
			}
			if fail {
				return cause
			}
			return nil
		}, nil)
		_, err = job.RunOnce(t.Context())
		require.ErrorIs(t, err, cause)
		var count int
		require.NoError(t, db.QueryRowContext(t.Context(),
			"SELECT count(*) FROM pgqueue_jobs WHERE queue_name = $1", t.Name()).Scan(&count))
		assert.Zero(t, count)
		require.NoError(t, db.QueryRowContext(t.Context(),
			"SELECT count(*) FROM pgcron_schedules WHERE name = $1", t.Name()).Scan(&count))
		assert.Zero(t, count)
		fail = false
		result, err := job.RunOnce(t.Context())
		require.NoError(t, err)
		assert.Equal(t, pgcron.Ran, result.Outcome)
		require.NoError(t, db.QueryRowContext(t.Context(),
			"SELECT count(*) FROM pgqueue_jobs WHERE queue_name = $1", t.Name()).Scan(&count))
		assert.Equal(t, 1, count)
	})

	t.Run("failure keeps an existing due time", func(t *testing.T) {
		var due time.Time
		require.NoError(t, db.QueryRowContext(t.Context(),
			"INSERT INTO pgcron_schedules VALUES ($1, now() - interval '1 hour') RETURNING next_run_at",
			t.Name()).Scan(&due))
		cause := errors.New("failed")
		_, err := newJob(t, func(context.Context, *sql.Tx) error { return cause }, nil).RunOnce(t.Context())
		require.ErrorIs(t, err, cause)
		var retained time.Time
		require.NoError(t, db.QueryRowContext(t.Context(),
			"SELECT next_run_at FROM pgcron_schedules WHERE name = $1", t.Name()).Scan(&retained))
		assert.Equal(t, due, retained)
		result, err := newJob(t, succeed, nil).RunOnce(t.Context())
		require.NoError(t, err)
		assert.Equal(t, pgcron.Ran, result.Outcome)
	})

	t.Run("panic rolls back and releases connection", func(t *testing.T) {
		job := newJob(t, func(context.Context, *sql.Tx) error { panic("broken callback") }, nil)
		assert.Panics(t, func() { _, _ = job.RunOnce(t.Context()) })
		result, err := newJob(t, succeed, nil).RunOnce(t.Context())
		require.NoError(t, err)
		assert.Equal(t, pgcron.Ran, result.Outcome)
	})

	t.Run("cancellation rolls back and allows recovery", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		job := newJob(t, func(context.Context, *sql.Tx) error {
			cancel()
			return context.Canceled
		}, nil)
		_, err := job.RunOnce(ctx)
		require.ErrorIs(t, err, context.Canceled)
		result, err := newJob(t, succeed, nil).RunOnce(t.Context())
		require.NoError(t, err)
		assert.Equal(t, pgcron.Ran, result.Outcome)
	})

	t.Run("hook failure leaves committed schedule", func(t *testing.T) {
		cause := errors.New("hook failed")
		result, err := newJob(t, succeed, func(context.Context) error { return cause }).RunOnce(t.Context())
		require.ErrorIs(t, err, cause)
		assert.Equal(t, pgcron.Ran, result.Outcome)
		peer, err := newJob(t, succeed, nil).RunOnce(t.Context())
		require.NoError(t, err)
		assert.Equal(t, pgcron.NotDue, peer.Outcome)
		assert.Equal(t, result.NextRunAt, peer.NextRunAt)
	})

	t.Run("lost database connection leaves occurrence due", func(t *testing.T) {
		job := newJob(t, func(ctx context.Context, tx *sql.Tx) error {
			var pid int
			require.NoError(t, tx.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid))
			var terminated bool
			require.NoError(t, db.QueryRowContext(ctx, "SELECT pg_terminate_backend($1)", pid).Scan(&terminated))
			require.True(t, terminated)
			return nil
		}, nil)
		_, err := job.RunOnce(t.Context())
		require.Error(t, err)
		result, err := newJob(t, succeed, nil).RunOnce(t.Context())
		require.NoError(t, err)
		assert.Equal(t, pgcron.Ran, result.Outcome)
	})

	t.Run("interval changes do not reset the stored due time", func(t *testing.T) {
		first, err := newJob(t, succeed, nil).RunOnce(t.Context())
		require.NoError(t, err)
		changed, err := pgcron.New(db, pgcron.Params{
			Name: t.Name(), Interval: 2 * time.Hour, Run: succeed,
		})
		require.NoError(t, err)
		result, err := changed.RunOnce(t.Context())
		require.NoError(t, err)
		assert.Equal(t, pgcron.NotDue, result.Outcome)
		assert.Equal(t, first.NextRunAt, result.NextRunAt)
		var due time.Time
		require.NoError(t, db.QueryRowContext(t.Context(),
			"UPDATE pgcron_schedules SET next_run_at = now() - interval '1 minute' WHERE name = $1 RETURNING next_run_at",
			t.Name()).Scan(&due))
		result, err = changed.RunOnce(t.Context())
		require.NoError(t, err)
		assert.Equal(t, pgcron.Ran, result.Outcome)
		assert.Equal(t, due.Add(2*time.Hour), result.NextRunAt)
	})

	t.Run("runtime respects persisted schedule and cancellation", func(t *testing.T) {
		job := newJob(t, succeed, nil)
		_, err := job.RunOnce(t.Context())
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		reports := 0
		err = job.Run(ctx, func(result pgcron.Result, err error) {
			reports++
			assert.NoError(t, err)
			assert.Equal(t, pgcron.NotDue, result.Outcome)
			cancel()
		})
		require.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, 1, reports)
	})

	t.Run("runtime reports failures retries and stops", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		cause := errors.New("temporary failure")
		calls, reports := 0, 0
		job, err := pgcron.Named(db, t.Name()).Every(time.Hour).RetryEvery(time.Millisecond).
			DoTx(func(context.Context, *sql.Tx) error {
				calls++
				if calls == 1 {
					return cause
				}
				return nil
			})
		require.NoError(t, err)
		require.Error(t, job.Run(ctx, nil))
		err = job.Run(ctx, func(result pgcron.Result, err error) {
			reports++
			if reports == 1 {
				assert.ErrorIs(t, err, cause)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, pgcron.Ran, result.Outcome)
			cancel()
		})
		require.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, 2, calls)
		assert.Equal(t, 2, reports)
	})
}

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	ctx := t.Context()
	container, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("pgcron_test"), postgres.WithUsername("pgkit"),
		postgres.WithPassword("pgkit"), postgres.BasicWaitStrategies())
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.ExecContext(ctx, pgcron.Schema)
	require.NoError(t, err)
	return db
}
