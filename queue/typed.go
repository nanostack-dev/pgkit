package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

type EnqueueOptions struct {
	AvailableAt time.Time // zero = immediate (database time)
	MaxAttempts int       // <= 0 uses the queue default
}

// TypedQueue binds a queue name to its JSON payload type. This is a local
// compile-time contract, not a database schema: all producers and workers must
// agree on the JSON shape, including during rolling deployments.
type TypedQueue[P any] struct {
	client  *Client
	name    string
	options EnqueueOptions
}

// Queue builds a handle without database access. Enqueue and Register validate
// the queue name when used; existing raw queue methods remain available.
func (c *Client) Queue[P any](name string) TypedQueue[P] {
	return TypedQueue[P]{client: c, name: name}
}

// WithOptions returns an independent handle with the supplied enqueue defaults.
func (q TypedQueue[P]) WithOptions(options EnqueueOptions) TypedQueue[P] {
	q.options = options
	return q
}

func (q TypedQueue[P]) Enqueue(ctx context.Context, payload P) (int64, error) {
	params, err := q.encode(payload)
	if err != nil {
		return 0, err
	}
	return q.client.Enqueue(ctx, params)
}

func (q TypedQueue[P]) EnqueueTx(ctx context.Context, tx *sql.Tx, payload P) (int64, error) {
	params, err := q.encode(payload)
	if err != nil {
		return 0, err
	}
	return q.client.EnqueueTx(ctx, tx, params)
}

// Register uses the existing worker machinery; malformed JSON is non-retryable.
// The handler still validates business fields, and receives the raw job metadata.
func (q TypedQueue[P]) Register(registry *HandlerRegistry, handler func(context.Context, P, Job) error) error {
	return RegisterJSONTyped(registry, q.name, handler)
}

func (q TypedQueue[P]) encode(payload P) (EnqueueParams, error) {
	if q.client == nil || q.client.db == nil {
		return EnqueueParams{}, ErrNilDB
	}
	if q.name == "" {
		return EnqueueParams{}, ErrInvalidQueue
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return EnqueueParams{}, fmt.Errorf("pgqueue: encode payload for %q: %w", q.name, err)
	}
	params := EnqueueParams{QueueName: q.name, Payload: encoded, MaxAttempts: q.options.MaxAttempts}
	if !q.options.AvailableAt.IsZero() {
		params.AvailableAt = &q.options.AvailableAt
	}
	return params, nil
}
