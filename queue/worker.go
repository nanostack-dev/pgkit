package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var (
	ErrNilRegistry         = errors.New("pgqueue: registry is nil")
	ErrInvalidWorkerConfig = errors.New("pgqueue: invalid worker config")
	ErrHandlerAlreadySet   = errors.New("pgqueue: handler already registered for queue")
	ErrInvalidHandler      = errors.New("pgqueue: handler is nil")
)

// Handled marks a job as already finalized by custom runtime logic.
// Worker skips Ack/Retry/Fail when this error is returned.
func Handled() error {
	return handledError{}
}

func IsHandled(err error) bool {
	var target handledError
	return errors.As(err, &target)
}

type handledError struct{}

func (handledError) Error() string {
	return "pgqueue: job already handled"
}

// NonRetryable wraps an error to mark it as terminal.
// Worker will fail the job immediately instead of retrying.
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return nonRetryableError{err: err}
}

func IsNonRetryable(err error) bool {
	var target nonRetryableError
	return errors.As(err, &target)
}

type nonRetryableError struct {
	err error
}

func (e nonRetryableError) Error() string {
	return e.err.Error()
}

func (e nonRetryableError) Unwrap() error {
	return e.err
}

// Handler receives raw job payload + metadata.
type Handler func(ctx context.Context, job Job) error

// HandlerRegistry maps queue names to handlers.
type HandlerRegistry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{handlers: map[string]Handler{}}
}

func (r *HandlerRegistry) Register(queueName string, handler Handler) error {
	if r == nil {
		return ErrNilRegistry
	}
	if queueName == "" {
		return ErrInvalidQueue
	}
	if handler == nil {
		return ErrInvalidHandler
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.handlers[queueName]; ok {
		return fmt.Errorf("%w: %s", ErrHandlerAlreadySet, queueName)
	}

	r.handlers[queueName] = handler
	return nil
}

func (r *HandlerRegistry) get(queueName string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[queueName]
	return h, ok
}

func (r *HandlerRegistry) queueNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.handlers))
	for name := range r.handlers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// RegisterJSONTyped registers a typed JSON handler that also receives job metadata.
func RegisterJSONTyped[T any](
	r *HandlerRegistry,
	queueName string,
	handler func(ctx context.Context, payload T, job Job) error,
) error {
	if handler == nil {
		return ErrInvalidHandler
	}

	return r.Register(queueName, func(ctx context.Context, job Job) error {
		var payload T
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return NonRetryable(fmt.Errorf("decode queue payload: %w", err))
		}
		return handler(ctx, payload, job)
	})
}

// RegisterJSON registers a typed JSON handler without job metadata.
func RegisterJSON[T any](
	r *HandlerRegistry,
	queueName string,
	handler func(ctx context.Context, payload T) error,
) error {
	if handler == nil {
		return ErrInvalidHandler
	}

	return RegisterJSONTyped(r, queueName, func(ctx context.Context, payload T, _ Job) error {
		return handler(ctx, payload)
	})
}

// RetryDelayFunc computes delay before next retry from job metadata and error.
type RetryDelayFunc func(job Job, cause error) time.Duration

type WorkerConfig struct {
	WorkerID          string
	PollInterval      time.Duration
	ReapInterval      time.Duration
	VisibilityTimeout time.Duration
	BatchSizePerQueue int
	BackoffBase       time.Duration
	BackoffMax        time.Duration
	RetryDelay        RetryDelayFunc

	// OnJobFailed is called when a job permanently fails (NonRetryable error
	// or max retries exhausted via Retry's zombie-prevention path).
	// nil = no callback.
	OnJobFailed func(ctx context.Context, job Job)

	// OnJobStuck is called when the reaper recovers stuck jobs.
	// result contains the count of requeued and failed jobs.
	// nil = no callback.
	OnJobStuck func(ctx context.Context, result ReapResult)
}

func (c WorkerConfig) withDefaults() WorkerConfig {
	if c.WorkerID == "" {
		c.WorkerID = "pgqueue-worker"
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 1 * time.Second
	}
	if c.ReapInterval <= 0 {
		c.ReapInterval = 30 * time.Second
	}
	if c.VisibilityTimeout <= 0 {
		c.VisibilityTimeout = 2 * time.Minute
	}
	if c.BatchSizePerQueue <= 0 {
		c.BatchSizePerQueue = 25
	}
	if c.BackoffBase <= 0 {
		c.BackoffBase = 1 * time.Second
	}
	if c.BackoffMax <= 0 {
		c.BackoffMax = 5 * time.Minute
	}
	if c.RetryDelay == nil {
		c.RetryDelay = func(job Job, _ error) time.Duration {
			return ExponentialBackoff(c.BackoffBase, job.Attempts, c.BackoffMax)
		}
	}
	return c
}

func (c WorkerConfig) validate() error {
	if c.PollInterval <= 0 || c.ReapInterval <= 0 || c.VisibilityTimeout <= 0 {
		return ErrInvalidWorkerConfig
	}
	if c.BatchSizePerQueue <= 0 {
		return ErrInvalidWorkerConfig
	}
	if c.BackoffBase <= 0 || c.BackoffMax <= 0 {
		return ErrInvalidWorkerConfig
	}
	if c.RetryDelay == nil {
		return ErrInvalidWorkerConfig
	}
	return nil
}

type Worker struct {
	client   *Client
	registry *HandlerRegistry
	cfg      WorkerConfig
}

func NewWorker(client *Client, registry *HandlerRegistry, cfg WorkerConfig) (*Worker, error) {
	if client == nil || client.db == nil {
		return nil, ErrNilDB
	}
	if registry == nil {
		return nil, ErrNilRegistry
	}

	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &Worker{client: client, registry: registry, cfg: cfg}, nil
}

// Run blocks until context is canceled.
func (w *Worker) Run(ctx context.Context) error {
	pollTicker := time.NewTicker(w.cfg.PollInterval)
	defer pollTicker.Stop()

	reapTicker := time.NewTicker(w.cfg.ReapInterval)
	defer reapTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-pollTicker.C:
			w.runOnce(ctx)
		case <-reapTicker.C:
			if err := w.reap(ctx); err != nil {
				w.client.logError(ctx, "queue reap cycle failed", map[string]any{"error": err.Error()})
			}
		}
	}
}

func (w *Worker) runOnce(ctx context.Context) {
	for _, queueName := range w.registry.queueNames() {
		h, ok := w.registry.get(queueName)
		if !ok {
			continue
		}

		for i := 0; i < w.cfg.BatchSizePerQueue; i++ {
			job, found, err := w.client.Claim(ctx, queueName, w.cfg.WorkerID)
			if err != nil {
				w.client.logError(ctx, "queue claim failed", map[string]any{"queue": queueName, "error": err.Error()})
				break
			}
			if !found || job == nil {
				break
			}

			if err := h(ctx, *job); err != nil {
				if IsHandled(err) {
					continue
				}
				if IsNonRetryable(err) {
					if failErr := w.client.Fail(ctx, job.ID, err); failErr != nil {
						w.client.logError(ctx, "queue fail failed", map[string]any{"id": job.ID, "error": failErr.Error()})
					} else if w.cfg.OnJobFailed != nil {
						job.Status = StatusFailed
						job.LastError = sql.NullString{String: err.Error(), Valid: true}
						w.cfg.OnJobFailed(ctx, *job)
					}
					continue
				}

				delay := w.cfg.RetryDelay(*job, err)
				if retryErr := w.client.Retry(ctx, job.ID, delay, err); retryErr != nil {
					w.client.logError(ctx, "queue retry failed", map[string]any{"id": job.ID, "error": retryErr.Error()})
				} else if job.Attempts >= job.MaxAttempts && w.cfg.OnJobFailed != nil {
					// Retry's zombie-prevention moved it to failed.
					job.Status = StatusFailed
					job.LastError = sql.NullString{String: err.Error(), Valid: true}
					w.cfg.OnJobFailed(ctx, *job)
				}
				continue
			}

			if ackErr := w.client.Ack(ctx, job.ID); ackErr != nil {
				w.client.logError(ctx, "queue ack failed", map[string]any{"id": job.ID, "error": ackErr.Error()})
			}
		}
	}
}

func (w *Worker) reap(ctx context.Context) error {
	result, err := w.client.ReapStuckJobs(ctx, w.cfg.VisibilityTimeout)
	if err != nil {
		return err
	}
	if result.Requeued > 0 || result.Failed > 0 {
		w.client.logWarn(ctx, "queue reaped stuck jobs", map[string]any{"requeued": result.Requeued, "failed": result.Failed})
		if w.cfg.OnJobStuck != nil {
			w.cfg.OnJobStuck(ctx, result)
		}
	}
	return nil
}
