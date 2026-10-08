package queue

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrNilRegistry         = errors.New("pgqueue: registry is nil")
	ErrInvalidWorkerConfig = errors.New("pgqueue: invalid worker config")
	ErrHandlerAlreadySet   = errors.New("pgqueue: handler already registered for queue")
	ErrInvalidHandler      = errors.New("pgqueue: handler is nil")
	ErrWorkerRunning       = errors.New("pgqueue: worker is already running")
)

// PostgreSQL SQLSTATE codes that mark the queue database as unreachable, shutting
// down, or torn down. Named after the condition names in the PostgreSQL error-code
// appendix. Postgres reports SQLSTATEs uppercase on the wire, so these are matched
// verbatim against pgconn.PgError.Code.
//
// The set is deliberately narrow. Note in particular that undefined_table (42P01)
// is absent: a missing *relation* is schema drift, a real fault that must keep
// logging at error, whereas invalid_catalog_name (3D000) means the whole database
// is gone — a torn-down preview environment, not a bug.
const settleTimeout = 30 * time.Second

const (
	sqlStateInvalidCatalogName     = "3D000" // database does not exist (torn down)
	sqlStateConnectionDoesNotExist = "08003"
	sqlStateConnectionFailure      = "08006"
	sqlStateAdminShutdown          = "57P01" // terminated by administrator command
	sqlStateCannotConnectNow       = "57P03" // server starting up or shutting down
)

var connectivitySQLStates = []string{
	sqlStateInvalidCatalogName,
	sqlStateConnectionDoesNotExist,
	sqlStateConnectionFailure,
	sqlStateAdminShutdown,
	sqlStateCannotConnectNow,
}

// connectivityErrorFragments are lowercased substrings that identify a network or
// driver-level connectivity failure that carries no SQLSTATE at all: the peer
// dropped the socket, DNS vanished, the dial never completed. Postgres never
// assigns these a code, so there is nothing structural to match on.
//
// They double as the fallback for non-pgx drivers. pgqueue is handed a *sql.DB and
// does not choose the driver, so when the caller wires up lib/pq rather than pgx
// the errors.As check below cannot match and only the message text remains. Every
// fragment names a specific condition, never a generic phrase.
var connectivityErrorFragments = []string{
	"connection reset",             // peer dropped the TCP connection
	"connection refused",           // database not accepting connections
	"broken pipe",                  // wrote to a closed connection
	"no such host",                 // DNS gone: the DB service was removed
	"i/o timeout",                  // network stalled
	"server closed the connection", // driver-reported disconnect
}

// isConnectivityError reports whether err reflects the queue database being
// unreachable, shutting down, or torn down, rather than a logic or data fault.
// The worker's poll loop retries on the next tick, so such failures are transient
// and non-actionable; logging them at error turns routine events — a client
// disconnect, a graceful shutdown, or a preview database being decommissioned —
// into false-positive alert noise. A genuine fault (bad SQL, constraint, a missing
// relation / schema drift) does not match and stays at error.
//
// Detection is structural wherever the error carries structure: the standard
// sentinels via errors.Is, then a *pgconn.PgError's SQLSTATE via errors.As, which
// finds the code however deeply the error is wrapped. Substring matching is the
// last resort, reserved for failures that have no SQLSTATE behind them and for
// drivers other than pgx.
func isConnectivityError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, driver.ErrBadConn) ||
		errors.Is(err, sql.ErrConnDone) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, io.EOF) {
		return true
	}
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return slices.Contains(connectivitySQLStates, pgErr.Code)
	}
	msg := strings.ToLower(err.Error())
	for _, frag := range connectivityErrorFragments {
		if strings.Contains(msg, frag) {
			return true
		}
	}
	return false
}

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
	WorkerID string
	// Pickup decides when the worker looks for jobs: queue.OnEnqueue() or
	// queue.PollEvery(d). Set it or PollInterval, not both.
	Pickup Pickup
	// PollInterval is PollEvery(PollInterval), kept for existing callers.
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
	if c.Pickup.isZero() {
		interval := c.PollInterval
		if interval <= 0 {
			interval = time.Second
		}
		c.Pickup = PollEvery(interval)
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
	if c.Pickup.scanEvery <= 0 || c.ReapInterval <= 0 || c.VisibilityTimeout <= 0 {
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
	client    *Client
	registry  *HandlerRegistry
	cfg       WorkerConfig
	ready     chan struct{}
	readyOnce sync.Once
	running   atomic.Bool
}

func NewWorker(client *Client, registry *HandlerRegistry, cfg WorkerConfig) (*Worker, error) {
	if client == nil || client.db == nil {
		return nil, ErrNilDB
	}
	if registry == nil {
		return nil, ErrNilRegistry
	}

	if cfg.PollInterval > 0 && !cfg.Pickup.isZero() {
		return nil, fmt.Errorf("%w: set Pickup or PollInterval, not both", ErrInvalidWorkerConfig)
	}
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.Pickup.onEnqueue && !client.supportsNotifications() {
		return nil, ErrNotificationsUnsupported
	}

	return &Worker{client: client, registry: registry, cfg: cfg, ready: make(chan struct{})}, nil
}

// Ready is closed after the worker's first scan in which every claim succeeded and,
// with OnEnqueue, its listener was up: from then on a job that becomes claimable
// wakes it. Delayed jobs and retries still wait for the next scan once due.
func (w *Worker) Ready() <-chan struct{} {
	return w.ready
}

// Run blocks until context is canceled. A Worker runs at most once at a time.
func (w *Worker) Run(ctx context.Context) error {
	if !w.running.CompareAndSwap(false, true) {
		return ErrWorkerRunning
	}
	defer w.running.Store(false)

	scanTicker := time.NewTicker(w.cfg.Pickup.scanEvery)
	defer scanTicker.Stop()

	reapTicker := time.NewTicker(w.cfg.ReapInterval)
	defer reapTicker.Stop()

	wake, seesEveryLaterJob, stopListening := w.listen()
	defer stopListening()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-scanTicker.C:
		case <-wake:
		case <-reapTicker.C:
			if err := w.reap(ctx); err != nil {
				w.client.logFailure(ctx, "queue reap cycle failed", map[string]any{"error": err.Error()}, err)
			}
			continue
		}
		scanSeesEveryLaterJob := seesEveryLaterJob()
		result := w.scan(ctx)
		if result.batchFilled {
			signal(wake)
		}
		if scanSeesEveryLaterJob && !result.claimFailed {
			w.readyOnce.Do(func() { close(w.ready) })
		}
	}
}

// listen returns what wakes the worker for a scan, and whether a scan starting now
// would leave no later job unnoticed. A polling worker scans immediately.
func (w *Worker) listen() (wake chan struct{}, seesEveryLaterJob func() bool, stop func()) {
	if !w.cfg.Pickup.onEnqueue {
		first := make(chan struct{}, 1)
		signal(first)
		return first, func() bool { return true }, func() {}
	}
	sub := w.client.notifier.subscribe(w.registry.queueNames())
	return sub.wake, sub.listening.Load, func() { w.client.notifier.unsubscribe(sub) }
}

type scanResult struct {
	batchFilled bool
	claimFailed bool
}

// scan claims and handles up to a batch per queue.
func (w *Worker) scan(ctx context.Context) scanResult {
	var result scanResult
	for _, queueName := range w.registry.queueNames() {
		h, ok := w.registry.get(queueName)
		if !ok {
			continue
		}

		claimed := 0
		for ; claimed < w.cfg.BatchSizePerQueue && ctx.Err() == nil; claimed++ {
			job, found, err := w.claim(ctx, queueName)
			if err != nil {
				w.client.logFailure(ctx, "queue claim failed", map[string]any{"queue": queueName, "error": err.Error()}, err)
				result.claimFailed = true
				break
			}
			if !found || job == nil {
				break
			}
			if ctx.Err() != nil {
				w.handBack(ctx, job)
				break
			}

			w.settle(ctx, job, h(ctx, *job))
		}
		if claimed == w.cfg.BatchSizePerQueue {
			result.batchFilled = true
		}
	}
	return result
}

// claim runs the claim transaction on a context that shutdown does not cancel:
// cancelling a commit in flight could leave a job claimed that the worker believes
// it never got.
func (w *Worker) claim(workerCtx context.Context, queueName string) (*Job, bool, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(workerCtx), settleTimeout)
	defer cancel()
	return w.client.Claim(ctx, queueName, w.cfg.WorkerID)
}

// handBack returns a job claimed as the worker began stopping, due now and with its
// attempt refunded, so another worker takes it without waiting for the reaper.
func (w *Worker) handBack(workerCtx context.Context, job *Job) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(workerCtx), settleTimeout)
	defer cancel()
	tx, err := w.client.db.BeginTx(ctx, nil)
	if err != nil {
		w.client.logError(ctx, "queue hand back failed", map[string]any{"id": job.ID, "error": err.Error()})
		return
	}
	defer func() { _ = tx.Rollback() }()
	if err := w.client.SnoozeTx(ctx, tx, job.ID, time.Unix(0, 0)); err != nil {
		w.client.logError(ctx, "queue hand back failed", map[string]any{"id": job.ID, "error": err.Error()})
		return
	}
	if err := tx.Commit(); err != nil {
		w.client.logError(ctx, "queue hand back failed", map[string]any{"id": job.ID, "error": err.Error()})
	}
}

// settle records the handler's outcome for job. It uses a context that shutdown
// does not cancel, so a handler finishing while the worker stops still settles its
// job instead of leaving it processing until the reaper takes it back.
func (w *Worker) settle(workerCtx context.Context, job *Job, handlerErr error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(workerCtx), settleTimeout)
	defer cancel()
	switch {
	case handlerErr == nil:
		if ackErr := w.client.Ack(ctx, job.ID); ackErr != nil {
			w.client.logError(ctx, "queue ack failed", map[string]any{"id": job.ID, "error": ackErr.Error()})
		}
	case IsHandled(handlerErr):
	case IsNonRetryable(handlerErr):
		if failErr := w.client.Fail(ctx, job.ID, handlerErr); failErr != nil {
			w.client.logError(ctx, "queue fail failed", map[string]any{"id": job.ID, "error": failErr.Error()})
		} else if w.cfg.OnJobFailed != nil {
			job.Status = StatusFailed
			job.LastError = sql.NullString{String: handlerErr.Error(), Valid: true}
			w.cfg.OnJobFailed(ctx, *job)
		}
	default:
		delay := w.cfg.RetryDelay(*job, handlerErr)
		if retryErr := w.client.Retry(ctx, job.ID, delay, handlerErr); retryErr != nil {
			w.client.logError(ctx, "queue retry failed", map[string]any{"id": job.ID, "error": retryErr.Error()})
		} else if job.Attempts >= job.MaxAttempts && w.cfg.OnJobFailed != nil {
			job.Status = StatusFailed
			job.LastError = sql.NullString{String: handlerErr.Error(), Valid: true}
			w.cfg.OnJobFailed(ctx, *job)
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
