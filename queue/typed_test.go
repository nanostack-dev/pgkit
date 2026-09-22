package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testEmail struct {
	To string `json:"to"`
}

func TestTypedQueue(t *testing.T) {
	ctx := t.Context()
	db := createTestDB(t, ctx)
	client, err := New(db)
	require.NoError(t, err)
	require.NoError(t, client.EnsureSchema(ctx))
	emails := client.Queue[testEmail]("emails")
	payload := testEmail{To: "recipient@example.com"}

	t.Run("typed producer and worker preserve payload and metadata", func(t *testing.T) {
		id, err := emails.Enqueue(ctx, payload)
		require.NoError(t, err)
		registry := NewHandlerRegistry()
		calls := 0
		require.NoError(t, emails.Register(registry, func(_ context.Context, got testEmail, job Job) error {
			calls++
			assert.Equal(t, payload, got)
			assert.Equal(t, id, job.ID)
			assert.Equal(t, "emails", job.QueueName)
			assert.Equal(t, 1, job.Attempts)
			return nil
		}))
		worker, err := NewWorker(client, registry, WorkerConfig{})
		require.NoError(t, err)
		worker.runOnce(ctx)
		assert.Equal(t, 1, calls)
		job, err := client.GetJob(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, StatusDone, job.Status)
		assert.JSONEq(t, `{"to":"recipient@example.com"}`, string(job.Payload))
	})

	t.Run("transaction rollback and commit", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		id, err := emails.EnqueueTx(ctx, tx, payload)
		require.NoError(t, err)
		_, err = client.GetJob(ctx, id)
		require.ErrorIs(t, err, ErrJobNotFound)
		require.NoError(t, tx.Rollback())
		_, err = client.GetJob(ctx, id)
		require.ErrorIs(t, err, ErrJobNotFound)

		tx, err = db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		id, err = emails.EnqueueTx(ctx, tx, payload)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		job, err := client.GetJob(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, StatusPending, job.Status)
	})

	t.Run("enqueue options are independent", func(t *testing.T) {
		later := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
		delayed := emails.WithOptions(EnqueueOptions{AvailableAt: later, MaxAttempts: 9})
		id, err := delayed.Enqueue(ctx, payload)
		require.NoError(t, err)
		job, err := client.GetJob(ctx, id)
		require.NoError(t, err)
		assert.True(t, later.Equal(job.AvailableAt))
		assert.Equal(t, 9, job.MaxAttempts)

		id, err = emails.Enqueue(ctx, payload)
		require.NoError(t, err)
		job, err = client.GetJob(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, defaultMaxAttempts, job.MaxAttempts)
		assert.Equal(t, job.CreatedAt, job.AvailableAt, "immediate jobs use database time")
	})

	t.Run("malformed stored JSON fails without invoking handler", func(t *testing.T) {
		broken := client.Queue[testEmail]("broken")
		id, err := client.Enqueue(ctx, EnqueueParams{QueueName: "broken", Payload: []byte("not-json")})
		require.NoError(t, err)
		registry := NewHandlerRegistry()
		require.NoError(t, broken.Register(registry, func(context.Context, testEmail, Job) error {
			t.Error("invalid JSON reached handler")
			return nil
		}))
		worker, err := NewWorker(client, registry, WorkerConfig{})
		require.NoError(t, err)
		worker.runOnce(ctx)
		job, err := client.GetJob(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, StatusFailed, job.Status)
		assert.Equal(t, 1, job.Attempts)
	})
}

func TestTypedQueueErrors(t *testing.T) {
	client, err := New(&sql.DB{})
	require.NoError(t, err)
	ctx := t.Context()
	var zero TypedQueue[testEmail]
	_, err = zero.Enqueue(ctx, testEmail{})
	require.ErrorIs(t, err, ErrNilDB)
	_, err = client.Queue[testEmail]("").Enqueue(ctx, testEmail{})
	require.ErrorIs(t, err, ErrInvalidQueue)
	_, err = client.Queue[testEmail]("emails").EnqueueTx(ctx, nil, testEmail{})
	require.EqualError(t, err, "pgqueue: tx is nil")
	_, err = client.Queue[chan int]("unsupported").Enqueue(ctx, make(chan int))
	var unsupported *json.UnsupportedTypeError
	require.ErrorAs(t, err, &unsupported)
	_, err = client.Queue[chan int]("unsupported").EnqueueTx(ctx, &sql.Tx{}, make(chan int))
	require.ErrorAs(t, err, &unsupported)

	emails := client.Queue[testEmail]("emails")
	handlerError := errors.New("transient failure")
	handler := func(context.Context, testEmail, Job) error { return handlerError }
	require.ErrorIs(t, emails.Register(nil, handler), ErrNilRegistry)
	registry := NewHandlerRegistry()
	require.ErrorIs(t, emails.Register(registry, nil), ErrInvalidHandler)
	require.ErrorIs(t, client.Queue[testEmail]("").Register(registry, handler), ErrInvalidQueue)
	require.NoError(t, emails.Register(registry, handler))
	require.ErrorIs(t, emails.Register(registry, handler), ErrHandlerAlreadySet)
	registered, ok := registry.get("emails")
	require.True(t, ok)
	require.ErrorIs(t, registered(ctx, Job{Payload: []byte(`{"to":"a@b.com"}`)}), handlerError)
}
