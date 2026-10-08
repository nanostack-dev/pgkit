package workflow

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/nanostack-dev/pgkit/queue"
)

func TestBuildRefusesAWorkerWithoutWorkflows(t *testing.T) {
	h := newHarness(t)
	_, err := h.client.Worker("empty").Build()
	requireErrorIs(t, err, ErrNoWorkflows)
}

func TestBuildRefusesTheSameWorkflowVersionTwice(t *testing.T) {
	h := newHarness(t)
	a := Define("twice", func(wf *Context, _ struct{}) (int, error) { return 1, nil })
	b := Define("twice", func(wf *Context, _ struct{}) (int, error) { return 2, nil })
	_, err := h.client.Worker("dup").Workflows(a, b).Build()
	requireErrorIs(t, err, ErrDuplicateWorkflow)
}

func TestBuildRefusesANegativeSetting(t *testing.T) {
	h := newHarness(t)
	flow := Define("tuned", func(wf *Context, _ struct{}) (int, error) { return 1, nil })
	for _, config := range []WorkerConfig{{Concurrency: -1}, {VisibilityTimeout: -time.Second}, {ReapInterval: -time.Second}} {
		if _, err := h.client.Worker("bad").Workflows(flow).Tune(config).Build(); !errors.Is(err, ErrInvalidWorkerConfig) {
			t.Fatalf("config %+v: expected ErrInvalidWorkerConfig, got %v", config, err)
		}
	}
}

func TestAWorkerRunsAtMostOnceAtATime(t *testing.T) {
	h := newHarness(t)
	flow := Define("solo", func(wf *Context, _ struct{}) (int, error) { return 1, nil })
	worker, err := h.client.Worker("solo").Workflows(flow).Pickup(testPickup).Build()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(h.ctx)
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	<-worker.Ready()

	requireErrorIs(t, worker.Run(ctx), queue.ErrWorkerRunning)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestBuilderMethodsReturnIndependentCopies(t *testing.T) {
	h := newHarness(t)
	a := Define("copy-a", func(wf *Context, _ struct{}) (int, error) { return 1, nil })
	b := Define("copy-b", func(wf *Context, _ struct{}) (int, error) { return 2, nil })
	base := h.client.Worker("base").Workflows(a)
	_ = base.Workflows(b)

	if len(base.workflows) != 1 {
		t.Fatalf("extending a builder changed its base: %d workflows", len(base.workflows))
	}
}

func TestRunsStayWithTheVersionTheyStartedWith(t *testing.T) {
	h := newHarness(t)
	v1 := Define("versioned", func(wf *Context, _ struct{}) (string, error) { return "v1", nil }, Version(1))
	v2 := Define("versioned", func(wf *Context, _ struct{}) (string, error) { return "v2", nil }, Version(2))
	old := mustStart(h, v1, struct{}{})
	current := mustStart(h, v2, struct{}{})

	h.startWorkerWith(workerOptions{id: "v2-only"}, v2)
	if got := mustResult(h, current); got != "v2" {
		t.Fatalf("result = %q", got)
	}
	time.Sleep(300 * time.Millisecond)
	if status := h.run(old.ID).Status; status != RunPending {
		t.Fatalf("a v2 worker touched a v1 run: %s", status)
	}

	h.startWorkerWith(workerOptions{id: "v1-drain"}, v1)
	if got := mustResult(h, old); got != "v1" {
		t.Fatalf("result = %q", got)
	}
	if h.run(old.ID).Version != 1 || h.run(current.ID).Version != 2 {
		t.Fatal("runs do not record their version")
	}
}

func TestOneWorkerCanListSeveralVersions(t *testing.T) {
	h := newHarness(t)
	v1 := Define("multi-version", func(wf *Context, _ struct{}) (string, error) { return "v1", nil })
	v2 := Define("multi-version", func(wf *Context, _ struct{}) (string, error) { return "v2", nil }, Version(2))
	h.startWorker(v1, v2)

	if mustResult(h, mustStart(h, v1, struct{}{})) != "v1" || mustResult(h, mustStart(h, v2, struct{}{})) != "v2" {
		t.Fatal("a worker listing two versions mixed them up")
	}
}

func TestConcurrencyExecutesSeveralRunsAtOnce(t *testing.T) {
	h := newHarness(t)
	const runs = 3
	var arrived sync.WaitGroup
	arrived.Add(runs)
	allArrived := make(chan struct{})
	go func() { arrived.Wait(); close(allArrived) }()
	flow := Define("rendezvous", func(wf *Context, _ struct{}) (bool, error) {
		return wf.Step("meet", func(ctx context.Context) (bool, error) {
			arrived.Done()
			select {
			case <-allArrived:
				return true, nil
			case <-ctx.Done():
				return false, ctx.Err()
			}
		}, NoRetry)
	})
	h.startWorkerWith(workerOptions{config: WorkerConfig{Concurrency: runs}}, flow)

	var started []Run[bool]
	for range runs {
		started = append(started, mustStart(h, flow, struct{}{}))
	}
	for _, run := range started {
		if !mustResult(h, run) {
			t.Fatal("runs did not execute concurrently")
		}
	}
}

func TestEveryStepRunsExactlyOnceAcrossCompetingWorkers(t *testing.T) {
	h := newHarness(t)
	calls := newCounter()
	flow := Define("contended", func(wf *Context, n int) (int, error) {
		total := 0
		for i := range 3 {
			value, err := wf.Step("part", func(context.Context) (int, error) {
				calls.add(fmt.Sprintf("%d/%d", n, i))
				return n + i, nil
			})
			if err != nil {
				return 0, err
			}
			total += value
		}
		return total, nil
	})
	for worker := range 3 {
		h.startWorkerWith(workerOptions{id: fmt.Sprint("competitor-", worker), config: WorkerConfig{Concurrency: 4}}, flow)
	}

	var runs []Run[int]
	for n := range 30 {
		runs = append(runs, mustStart(h, flow, n))
	}
	for n, run := range runs {
		if got := mustResult(h, run); got != 3*n+3 {
			t.Fatalf("run %d = %d", n, got)
		}
	}
	for n := range 30 {
		for i := range 3 {
			if got := calls.get(fmt.Sprintf("%d/%d", n, i)); got != 1 {
				t.Fatalf("step %d/%d ran %d times", n, i, got)
			}
		}
	}
}

func TestHeartbeatsKeepALongStepOnItsWorker(t *testing.T) {
	h := newHarness(t)
	calls := newCounter()
	flow := Define("marathon", func(wf *Context, _ struct{}) (string, error) {
		return wf.Step("long", func(ctx context.Context) (string, error) {
			calls.add("long")
			select {
			case <-time.After(3 * time.Second):
				return "finished", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}, NoRetry)
	})
	short := WorkerConfig{VisibilityTimeout: time.Second, ReapInterval: 50 * time.Millisecond}
	h.startWorkerWith(workerOptions{id: "first", config: short}, flow)
	h.startWorkerWith(workerOptions{id: "second", config: short}, flow)

	if got := mustResult(h, mustStart(h, flow, struct{}{})); got != "finished" {
		t.Fatalf("result = %q", got)
	}
	if calls.get("long") != 1 {
		t.Fatalf("the long step ran %d times", calls.get("long"))
	}
}

func TestAnActivationThatLostItsClaimStops(t *testing.T) {
	h := newHarness(t)
	reached := newGate()
	causes := make(chan error, 1)
	flow := Define("dispossessed", func(wf *Context, _ struct{}) (string, error) {
		return wf.Step("long", func(ctx context.Context) (string, error) {
			err := reached.wait(ctx)
			causes <- context.Cause(ctx)
			return "", err
		}, NoRetry)
	})
	h.startWorkerWith(workerOptions{config: WorkerConfig{VisibilityTimeout: 150 * time.Millisecond, ReapInterval: time.Hour}}, flow)
	run := mustStart(h, flow, struct{}{})
	reached.awaitReached(t)

	h.exec(`UPDATE pgqueue_jobs SET attempts = attempts + 1 WHERE id = (SELECT job_id FROM pgworkflow_runs WHERE id = $1)`, run.ID)

	select {
	case cause := <-causes:
		requireErrorIs(t, cause, errLeaseLost)
	case <-time.After(10 * time.Second):
		t.Fatal("the step kept running after its claim was lost")
	}
	if h.run(run.ID).Status == RunFailed {
		t.Fatal("an activation without its claim failed the run")
	}
}

func TestAStaleActivationCannotWriteCheckpoints(t *testing.T) {
	h := newHarness(t)
	flow := Define("fenced", func(wf *Context, _ struct{}) (int, error) { return 1, nil })
	run := mustStart(h, flow, struct{}{})
	job := claimRunJob(t, h, flow, run.ID)

	stale := beginTestActivation(t, h, flow, job, run.ID)
	current := beginTestActivation(t, h, flow, job, run.ID)

	err := stale.checkpointWrite(func(ctx context.Context, tx *sql.Tx) error { return nil })
	requireErrorIs(t, err, ErrSuspended)
	if reason, _ := stale.stopState(); reason != leaseLost {
		t.Fatalf("stale activation stopped for reason %d", reason)
	}
	if err := current.checkpointWrite(func(ctx context.Context, tx *sql.Tx) error { return nil }); err != nil {
		t.Fatalf("current activation: %v", err)
	}
}

func claimRunJob(t *testing.T, h *harness, def Definition, runID string) queue.Job {
	t.Helper()
	job, found, err := h.queue.Claim(h.ctx, def.definitionOf().queueName(), "manual")
	if err != nil || !found {
		t.Fatalf("claim: found=%v err=%v", found, err)
	}
	return *job
}

func beginTestActivation(t *testing.T, h *harness, def Definition, job queue.Job, runID string) *activation {
	t.Helper()
	act, err := h.client.beginActivation(h.ctx, def.definitionOf(), WorkerConfig{}.withDefaults(), job, runID)
	if err != nil || act == nil {
		t.Fatalf("begin activation: %v", err)
	}
	return act
}

func TestStoppingAWorkerHandsItsRunsToAnotherWithoutCountingAnAttempt(t *testing.T) {
	h := newHarness(t)
	firstAttempt := newGate()
	calls := newCounter()
	flow := Define("handed-over", func(wf *Context, _ struct{}) (string, error) {
		return wf.Step("work", func(ctx context.Context) (string, error) {
			if calls.add("work") == 1 {
				return "", firstAttempt.wait(ctx)
			}
			return "finished elsewhere", nil
		}, NoRetry)
	})
	stopFirst := h.startWorkerWith(workerOptions{id: "leaving"}, flow)
	run := mustStart(h, flow, struct{}{})
	firstAttempt.awaitReached(t)

	stopFirst()
	h.startWorkerWith(workerOptions{id: "arriving", pickup: notifiedOnly}, flow)

	if got := mustResult(h, run); got != "finished elsewhere" {
		t.Fatalf("result = %q", got)
	}
	if step := h.step(run.ID, "work"); step.Attempts != 1 {
		t.Fatalf("the interrupted attempt was counted: %+v", step)
	}
}

func TestAWorkerFallsBackToPollingWithoutNotifications(t *testing.T) {
	t.Parallel()
	url := newDatabase(t)
	db := openWithoutNotifications(t, url)
	h := newHarnessOn(t, db, url)
	flow := approvalFlow("polled", Forever)
	worker, err := h.client.Worker("poller").Workflows(flow).Build()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(h.ctx)
	defer cancel()
	go func() { _ = worker.Run(ctx) }()
	run := mustStart(h, flow, struct{}{})
	h.waitForStatus(run.ID, RunWaiting)

	if err := h.client.Signal(h.ctx, run.ID, approved, approval{By: "poll"}); err != nil {
		t.Fatal(err)
	}

	if got := mustResult(h, run); got != "approved by poll" {
		t.Fatalf("result = %q", got)
	}
}

func TestListenOnGivesAnotherDriverNotifications(t *testing.T) {
	t.Parallel()
	url := newDatabase(t)
	db := openWithoutNotifications(t, url)
	h := newHarnessOn(t, db, url)
	if err := h.queue.ListenOn(url); err != nil {
		t.Fatal(err)
	}
	flow := approvalFlow("listened", Forever)
	h.startWorkerWith(workerOptions{pickup: notifiedOnly}, flow)
	run := mustStart(h, flow, struct{}{})
	h.waitForStatus(run.ID, RunWaiting)

	if err := h.client.Signal(h.ctx, run.ID, approved, approval{By: "listener"}); err != nil {
		t.Fatal(err)
	}

	if got := mustResult(h, run); got != "approved by listener" {
		t.Fatalf("result = %q", got)
	}
}

// foreignDriver hides the pgx stdlib driver from database/sql, as lib/pq or an
// instrumentation wrapper would, while the connections still work.
type foreignDriver struct{ driver.Driver }

type foreignConnector struct{ driver.Connector }

func (foreignConnector) Driver() driver.Driver { return foreignDriver{} }

func openWithoutNotifications(t *testing.T, url string) *sql.DB {
	t.Helper()
	config, err := pgx.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(foreignConnector{stdlib.GetConnector(*config)})
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestReadyClosesOnceEverySlotIsReady(t *testing.T) {
	h := newHarness(t)
	flow := Define("ready", func(wf *Context, _ struct{}) (int, error) { return 1, nil })
	worker, err := h.client.Worker("ready").Workflows(flow).Pickup(notifiedOnly).Tune(WorkerConfig{Concurrency: 3}).Build()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-worker.Ready():
		t.Fatal("ready before running")
	default:
	}
	ctx, cancel := context.WithCancel(h.ctx)
	defer cancel()
	go func() { _ = worker.Run(ctx) }()
	select {
	case <-worker.Ready():
	case <-time.After(10 * time.Second):
		t.Fatal("never ready")
	}

	if got := mustResult(h, mustStart(h, flow, struct{}{})); got != 1 {
		t.Fatal("a ready notified-only worker missed a new run")
	}
}

func TestAMalformedRunJobFailsWithoutRetry(t *testing.T) {
	h := newHarness(t)
	flow := Define("malformed", func(wf *Context, _ struct{}) (int, error) { return 1, nil })
	id, err := h.queue.Enqueue(h.ctx, queue.EnqueueParams{QueueName: flow.definition.queueName(), Payload: []byte(`{"nope":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	h.startWorker(flow)

	h.eventually("the job fails", func() bool {
		job, err := h.queue.GetJob(h.ctx, id)
		return err == nil && job.Status == queue.StatusFailed && job.Attempts == 1
	})
}

func TestAJobOfAFinishedRunIsSettledWithoutExecuting(t *testing.T) {
	h := newHarness(t)
	calls := atomic.Int32{}
	flow := Define("stale-job", func(wf *Context, _ struct{}) (int, error) { return int(calls.Add(1)), nil })
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	mustResult(h, run)

	id, err := h.queue.Enqueue(h.ctx, queue.EnqueueParams{QueueName: flow.definition.queueName(), Payload: fmt.Appendf(nil, `{"run_id":%q}`, run.ID)})
	if err != nil {
		t.Fatal(err)
	}

	h.eventually("the stale job is settled", func() bool {
		job, err := h.queue.GetJob(h.ctx, id)
		return err == nil && job.Status == queue.StatusDone
	})
	if calls.Load() != 1 {
		t.Fatal("a stale job executed the run again")
	}
}

func TestSetLoggerReceivesRuntimeEvents(t *testing.T) {
	h := newHarness(t)
	logs := &recordingLogger{}
	h.client.SetLogger(logs)
	flow := Define("logged", func(wf *Context, _ struct{}) (int, error) {
		return wf.Step("explode", func(context.Context) (int, error) { panic("kaboom") }, NoRetry)
	})
	h.startWorker(flow)

	_, _ = result(h, mustStart(h, flow, struct{}{}))

	if !logs.has("workflow step panicked") {
		t.Fatalf("logs = %v", logs.messages())
	}
	h.client.SetLogger(nil)
}

type recordingLogger struct {
	mu   sync.Mutex
	logs []string
}

func (l *recordingLogger) record(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logs = append(l.logs, msg)
}

func (l *recordingLogger) messages() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.logs...)
}

func (l *recordingLogger) has(msg string) bool {
	return slices.Contains(l.messages(), msg)
}

func (l *recordingLogger) Debug(_ context.Context, msg string, _ map[string]any) { l.record(msg) }
func (l *recordingLogger) Info(_ context.Context, msg string, _ map[string]any)  { l.record(msg) }
func (l *recordingLogger) Warn(_ context.Context, msg string, _ map[string]any)  { l.record(msg) }
func (l *recordingLogger) Error(_ context.Context, msg string, _ map[string]any) { l.record(msg) }
