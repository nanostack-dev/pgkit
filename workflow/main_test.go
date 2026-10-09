package workflow

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nanostack-dev/pgkit/queue"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Every test gets its own database, cloned from a migrated template on one shared
// container, so tests are isolated (notifications included) and run in parallel.
var postgresServer struct {
	once      sync.Once
	container testcontainers.Container
	baseURL   *url.URL
	err       error
	createMu  sync.Mutex
	databases atomic.Int64
}

const templateDatabase = "pgkit_workflow_template"

func TestMain(m *testing.M) {
	code := m.Run()
	if postgresServer.container != nil {
		_ = postgresServer.container.Terminate(context.Background())
	}
	os.Exit(code)
}

func startPostgresServer() error {
	ctx := context.Background()
	container, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("postgres"),
		postgres.WithUsername("pgkit"),
		postgres.WithPassword("pgkit"),
		postgres.BasicWaitStrategies(),
		testcontainers.WithCmdArgs("-c", "max_connections=1000", "-c", "fsync=off", "-c", "synchronous_commit=off"),
	)
	if err != nil {
		return fmt.Errorf("start postgres: %w", err)
	}
	postgresServer.container = container
	connString, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return err
	}
	postgresServer.baseURL, err = url.Parse(connString)
	if err != nil {
		return err
	}
	admin, err := sql.Open("pgx", databaseURL("postgres"))
	if err != nil {
		return err
	}
	defer func() { _ = admin.Close() }()
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE `+templateDatabase); err != nil {
		return err
	}
	template, err := sql.Open("pgx", databaseURL(templateDatabase))
	if err != nil {
		return err
	}
	defer func() { _ = template.Close() }()
	q, err := queue.New(template)
	if err != nil {
		return err
	}
	if err := q.EnsureSchema(ctx); err != nil {
		return err
	}
	client, err := New(q)
	if err != nil {
		return err
	}
	return client.EnsureSchema(ctx)
}

func databaseURL(name string) string {
	u := *postgresServer.baseURL
	u.Path = "/" + name
	return u.String()
}

// newDatabase creates an empty migrated database for t and returns its URL.
func newDatabase(t *testing.T) string {
	t.Helper()
	postgresServer.once.Do(func() { postgresServer.err = startPostgresServer() })
	if postgresServer.err != nil {
		t.Fatalf("postgres: %v", postgresServer.err)
	}
	name := fmt.Sprintf("test_%d", postgresServer.databases.Add(1))
	postgresServer.createMu.Lock()
	defer postgresServer.createMu.Unlock()
	admin, err := sql.Open("pgx", databaseURL("postgres"))
	if err != nil {
		t.Fatalf("open admin: %v", err)
	}
	defer func() { _ = admin.Close() }()
	if _, err := admin.Exec(`CREATE DATABASE ` + name + ` TEMPLATE ` + templateDatabase); err != nil {
		t.Fatalf("create database: %v", err)
	}
	return databaseURL(name)
}

type harness struct {
	t      *testing.T
	ctx    context.Context
	url    string
	db     *sql.DB
	queue  *queue.Client
	client *Client
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	t.Parallel()
	url := newDatabase(t)
	return newHarnessOn(t, openPGX(t, url), url)
}

func openPGX(t *testing.T, url string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(30)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func newHarnessOn(t *testing.T, db *sql.DB, url string) *harness {
	t.Helper()
	q, err := queue.New(db)
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	client, err := New(q)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return &harness{t: t, ctx: ctx, url: url, db: db, queue: q, client: client}
}

// testPickup claims on notification and rescans often, so delayed jobs (retries,
// sleeps) are picked up quickly.
var testPickup = queue.OnEnqueue().RescanEvery(100 * time.Millisecond)

// notifiedOnly never rescans: a run it picks up was woken by a notification.
var notifiedOnly = queue.OnEnqueue().RescanEvery(time.Hour)

type workerOptions struct {
	id     string
	pickup queue.Pickup
	config WorkerConfig
}

// startWorker runs a worker for workflows until the test ends and waits until it is
// ready.
func (h *harness) startWorker(workflows ...Definition) (stop func()) {
	h.t.Helper()
	return h.startWorkerWith(workerOptions{}, workflows...)
}

func (h *harness) startWorkerWith(options workerOptions, workflows ...Definition) (stop func()) {
	h.t.Helper()
	if options.id == "" {
		options.id = "test-worker"
	}
	if options.pickup == (queue.Pickup{}) {
		options.pickup = testPickup
	}
	worker, err := h.client.Worker(options.id).Workflows(workflows...).Pickup(options.pickup).Tune(options.config).Build()
	if err != nil {
		h.t.Fatalf("build worker: %v", err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(runCtx) }()
	select {
	case <-worker.Ready():
	case err := <-done:
		cancel()
		h.t.Fatalf("worker stopped before it was ready: %v", err)
	case <-time.After(30 * time.Second):
		cancel()
		h.t.Fatal("worker not ready")
	}
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			if err := <-done; err != nil {
				h.t.Errorf("worker: %v", err)
			}
		})
	}
	h.t.Cleanup(stop)
	return stop
}

func result[Out any](h *harness, run Run[Out]) (Out, error) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(h.ctx, 30*time.Second)
	defer cancel()
	return run.Result(ctx)
}

func mustResult[Out any](h *harness, run Run[Out]) Out {
	h.t.Helper()
	output, err := result(h, run)
	if err != nil {
		h.t.Fatalf("run %s: %v\n%s", run.ID, err, h.dump(run.ID))
	}
	return output
}

// dump describes a run, its checkpoints and its job, for failure messages.
func (h *harness) dump(runID string) string {
	ctx := context.Background()
	var out strings.Builder
	if run, err := h.client.GetRun(ctx, runID); err == nil {
		fmt.Fprintf(&out, "run: status=%s wake=%v lease=%d error=%q\n", run.Status, run.WakeAt, h.lease(runID), run.Error)
	}
	if steps, err := h.client.ListSteps(ctx, runID); err == nil {
		for _, step := range steps {
			fmt.Fprintf(&out, "  step %s kind=%s status=%s attempts=%d wake=%v child=%s\n", step.Name, step.Kind, step.Status, step.Attempts, step.WakeAt, step.ChildRunID)
		}
	}
	var status, claimedBy sql.NullString
	var attempts int
	var availableAt, claimedAt sql.NullTime
	err := h.db.QueryRowContext(ctx, `SELECT j.status, j.attempts, j.available_at, j.claimed_by, j.claimed_at
		FROM pgqueue_jobs j JOIN pgworkflow_runs r ON r.job_id = j.id WHERE r.id = $1`, runID).
		Scan(&status, &attempts, &availableAt, &claimedBy, &claimedAt)
	fmt.Fprintf(&out, "job: status=%s attempts=%d available=%v claimed_by=%s claimed_at=%v err=%v now=%v\n",
		status.String, attempts, availableAt.Time, claimedBy.String, claimedAt.Time, err, time.Now().UTC())
	return out.String()
}

func mustStart[In, Out any](h *harness, w *Workflow[In, Out], input In, options ...StartOption) Run[Out] {
	h.t.Helper()
	run, err := h.client.Start(h.ctx, w, input, options...)
	if err != nil {
		h.t.Fatalf("start %s: %v", w.Name(), err)
	}
	return run
}

func (h *harness) run(runID string) RunInfo {
	h.t.Helper()
	run, err := h.client.GetRun(h.ctx, runID)
	if err != nil {
		h.t.Fatalf("get run: %v", err)
	}
	return run
}

func (h *harness) steps(runID string) []StepInfo {
	h.t.Helper()
	steps, err := h.client.ListSteps(h.ctx, runID)
	if err != nil {
		h.t.Fatalf("list steps: %v", err)
	}
	return steps
}

func (h *harness) step(runID, name string) StepInfo {
	h.t.Helper()
	for _, step := range h.steps(runID) {
		if step.Name == name {
			return step
		}
	}
	h.t.Fatalf("run %s has no step %q: %+v", runID, name, h.steps(runID))
	return StepInfo{}
}

func (h *harness) waitForStatus(runID string, status RunStatus) RunInfo {
	h.t.Helper()
	var run RunInfo
	h.eventually(fmt.Sprintf("run %s is %s", runID, status), func() bool {
		run = h.run(runID)
		return run.Status == status
	})
	return run
}

func (h *harness) waitForStep(runID, name string, status StepStatus) StepInfo {
	h.t.Helper()
	var found StepInfo
	h.eventually(fmt.Sprintf("step %q of run %s is %s", name, runID, status), func() bool {
		for _, step := range h.steps(runID) {
			if step.Name == name && step.Status == status {
				found = step
				return true
			}
		}
		return false
	})
	return found
}

func (h *harness) eventually(what string, condition func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	h.t.Fatalf("timed out waiting until %s", what)
}

func (h *harness) exec(query string, args ...any) {
	h.t.Helper()
	if _, err := h.db.ExecContext(h.ctx, query, args...); err != nil {
		h.t.Fatalf("exec %q: %v", query, err)
	}
}

func (h *harness) queryInt(query string, args ...any) int {
	h.t.Helper()
	var value int
	if err := h.db.QueryRowContext(h.ctx, query, args...).Scan(&value); err != nil {
		h.t.Fatalf("query %q: %v", query, err)
	}
	return value
}

// fastForward makes everything a parked run waits on in time (sleeps, retries,
// receive timeouts) due now and wakes its job, as if the time had passed.
func (h *harness) fastForward(runID string) {
	h.t.Helper()
	h.wakeRun(runID, true)
}

// wakeJob makes a parked run's job due now without changing what it waits for, so
// the run replays and parks again unless something it waits for happened.
func (h *harness) wakeJob(runID string) {
	h.t.Helper()
	h.wakeRun(runID, false)
}

// wakeRun waits until no activation of the run is in flight, as one could park
// on the wake times it read before, then makes the job due while holding the run,
// so an activation starting meanwhile reads the new wake times.
func (h *harness) wakeRun(runID string, dueNow bool) {
	h.t.Helper()
	h.eventually("no activation of the run is in flight", func() bool { return h.tryWakeRun(runID, dueNow) })
}

func (h *harness) tryWakeRun(runID string, dueNow bool) bool {
	h.t.Helper()
	tx, err := h.db.BeginTx(h.ctx, nil)
	if err != nil {
		h.t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	var jobID int64
	var jobStatus queue.JobStatus
	if err := tx.QueryRowContext(h.ctx, `
SELECT job.id, job.status FROM pgworkflow_runs run JOIN pgqueue_jobs job ON job.id = run.job_id
WHERE run.id = $1 FOR UPDATE OF run`, runID).Scan(&jobID, &jobStatus); err != nil {
		h.t.Fatalf("job of run: %v", err)
	}
	if jobStatus == queue.StatusProcessing {
		return false
	}
	if dueNow {
		if _, err := tx.ExecContext(h.ctx, `UPDATE pgworkflow_steps SET wake_at = NOW() WHERE run_id = $1 AND wake_at > NOW()`, runID); err != nil {
			h.t.Fatalf("fast forward: %v", err)
		}
	}
	if err := h.queue.WakeTx(h.ctx, tx, jobID); err != nil {
		h.t.Fatalf("wake: %v", err)
	}
	if err := tx.Commit(); err != nil {
		h.t.Fatalf("commit: %v", err)
	}
	return true
}

func (h *harness) lease(runID string) int {
	h.t.Helper()
	return h.queryInt(`SELECT lease FROM pgworkflow_runs WHERE id = $1`, runID)
}

func requireErrorIs(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("expected %v, got %v", target, err)
	}
}

func requireFailed(t *testing.T, err error) {
	t.Helper()
	if runErr := requireRunError(t, err); runErr.Status != RunFailed {
		t.Fatalf("run status = %s, want failed", runErr.Status)
	}
}

func requireRunError(t *testing.T, err error) *RunError {
	t.Helper()
	var runErr *RunError
	if !errors.As(err, &runErr) {
		t.Fatalf("expected *RunError, got %T: %v", err, err)
	}
	return runErr
}

// counter counts calls by key, safely across the goroutines of a worker.
type counter struct {
	mu     sync.Mutex
	counts map[string]int
}

func newCounter() *counter {
	return &counter{counts: map[string]int{}}
}

func (c *counter) add(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[key]++
	return c.counts[key]
}

func (c *counter) get(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[key]
}

// gate blocks steps until the test opens it.
type gate struct {
	reached chan struct{}
	open    chan struct{}
	once    sync.Once
}

func newGate() *gate {
	return &gate{reached: make(chan struct{}, 100), open: make(chan struct{})}
}

func (g *gate) wait(ctx context.Context) error {
	g.reached <- struct{}{}
	select {
	case <-g.open:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *gate) awaitReached(t *testing.T) {
	t.Helper()
	select {
	case <-g.reached:
	case <-time.After(20 * time.Second):
		t.Fatal("step never reached the gate")
	}
}

func (g *gate) release() {
	g.once.Do(func() { close(g.open) })
}

func queueListParams() queue.ListJobsParams {
	return queue.ListJobsParams{Limit: 100}
}

// mustFail awaits a run that must fail and returns its error.
func mustFail[Out any](h *harness, run Run[Out]) *RunError {
	h.t.Helper()
	_, err := result(h, run)
	runErr := requireRunError(h.t, err)
	if runErr.Status != RunFailed {
		h.t.Fatalf("run status = %s, want failed", runErr.Status)
	}
	return runErr
}

// awaitFailure awaits a run that must fail.
func awaitFailure[Out any](h *harness, run Run[Out]) {
	h.t.Helper()
	_, err := result(h, run)
	requireFailed(h.t, err)
}

func (h *harness) requireStep(runID, name string, status StepStatus, attempts int) StepInfo {
	h.t.Helper()
	step := h.step(runID, name)
	if step.Status != status || step.Attempts != attempts {
		h.t.Fatalf("step %q = %s after %d attempt(s), want %s after %d", name, step.Status, step.Attempts, status, attempts)
	}
	return step
}
