package workflow

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nanostack-dev/pgkit/queue"
)

func TestListRunsFiltersAndPages(t *testing.T) {
	h := newHarness(t)
	billing := Define("billing", func(wf *Context, _ struct{}) (int, error) { return 1, nil })
	shipping := Define("shipping", func(wf *Context, _ struct{}) (int, error) { return 0, errors.New("no carrier") })
	h.startWorker(billing, shipping)
	var billingRuns []Run[int]
	for i := range 3 {
		billingRuns = append(billingRuns, mustStart(h, billing, struct{}{}, Key("invoice-"+string(rune('a'+i)))))
	}
	failed := mustStart(h, shipping, struct{}{})
	for _, run := range billingRuns {
		mustResult(h, run)
	}
	_, _ = result(h, failed)

	cases := []struct {
		name   string
		params ListRunsParams
		want   int
	}{
		{"everything", ListRunsParams{}, 4},
		{"by workflow", ListRunsParams{Workflow: "billing"}, 3},
		{"by status", ListRunsParams{Status: RunFailed}, 1},
		{"by search on key", ListRunsParams{Search: "invoice-b"}, 1},
		{"by search on id", ListRunsParams{Search: failed.ID}, 1},
		{"by search on workflow", ListRunsParams{Search: "shipp"}, 1},
		{"search escapes wildcards", ListRunsParams{Search: "%"}, 0},
		{"page", ListRunsParams{Workflow: "billing", Limit: 2}, 2},
		{"next page", ListRunsParams{Workflow: "billing", Limit: 2, Offset: 2}, 1},
	}
	for _, tc := range cases {
		runs, err := h.client.ListRuns(h.ctx, tc.params)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if len(runs) != tc.want {
			t.Fatalf("%s: %d runs, want %d", tc.name, len(runs), tc.want)
		}
		params := tc.params
		params.Limit, params.Offset = 0, 0
		count, err := h.client.CountRuns(h.ctx, params)
		if err != nil {
			t.Fatalf("%s: count: %v", tc.name, err)
		}
		if tc.name != "page" && tc.name != "next page" && count != int64(tc.want) {
			t.Fatalf("%s: count %d, want %d", tc.name, count, tc.want)
		}
	}
	newest, err := h.client.ListRuns(h.ctx, ListRunsParams{Limit: 1})
	if err != nil || newest[0].ID != failed.ID {
		t.Fatalf("runs are not newest first: %+v %v", newest, err)
	}
}

func TestGetRunReportsAnUnknownRun(t *testing.T) {
	h := newHarness(t)
	_, err := h.client.GetRun(h.ctx, "missing")
	requireErrorIs(t, err, ErrRunNotFound)
}

func TestListStepsOfAnUnknownRunIsEmpty(t *testing.T) {
	h := newHarness(t)
	steps, err := h.client.ListSteps(h.ctx, "missing")
	if err != nil || len(steps) != 0 {
		t.Fatalf("steps = %v, err = %v", steps, err)
	}
}

func TestPurgeDeletesOnlyOldFinishedRuns(t *testing.T) {
	h := newHarness(t)
	done := Define("purgeable", func(wf *Context, _ struct{}) (int, error) {
		return wf.Step("work", func(context.Context) (int, error) { return 1, nil })
	})
	waiting := Define("unfinished", func(wf *Context, _ struct{}) (int, error) {
		return wf.Receive(NewSignal[int]("go"), Forever)
	})
	h.startWorker(done, waiting)
	old := mustStart(h, done, struct{}{})
	recent := mustStart(h, done, struct{}{})
	open := mustStart(h, waiting, struct{}{})
	mustResult(h, old)
	mustResult(h, recent)
	h.waitForStatus(open.ID, RunWaiting)
	h.exec(`UPDATE pgworkflow_runs SET completed_at = NOW() - interval '8 days' WHERE id = $1`, old.ID)
	h.exec(`UPDATE pgworkflow_runs SET created_at = NOW() - interval '8 days' WHERE id = $1`, open.ID)

	deleted, err := h.client.Purge(h.ctx, PurgeParams{OlderThan: 7 * 24 * time.Hour})

	if err != nil || deleted != 1 {
		t.Fatalf("deleted = %d, err = %v", deleted, err)
	}
	if _, err := h.client.GetRun(h.ctx, old.ID); !errors.Is(err, ErrRunNotFound) {
		t.Fatal("the old run survived")
	}
	if h.queryInt(`SELECT count(*) FROM pgworkflow_steps WHERE run_id = $1`, old.ID) != 0 {
		t.Fatal("the old run's checkpoints survived")
	}
	h.run(recent.ID)
	h.run(open.ID)
}

func TestPurgeHonorsItsLimit(t *testing.T) {
	h := newHarness(t)
	flow := Define("bulk", func(wf *Context, _ struct{}) (int, error) { return 1, nil })
	h.startWorker(flow)
	for range 5 {
		mustResult(h, mustStart(h, flow, struct{}{}))
	}

	deleted, err := h.client.Purge(h.ctx, PurgeParams{Limit: 2})

	if err != nil || deleted != 2 {
		t.Fatalf("deleted = %d, err = %v", deleted, err)
	}
}

func TestEnsureSchemaIsIdempotent(t *testing.T) {
	h := newHarness(t)
	for range 2 {
		if err := h.client.EnsureSchema(h.ctx); err != nil {
			t.Fatal(err)
		}
	}
	if h.queryInt(`SELECT value::int FROM pgworkflow_meta WHERE key = 'schema_version'`) != schemaVersion {
		t.Fatal("schema version not recorded")
	}
}

func TestEnsureSchemaLeavesTheEarlierWorkflowTablesAlone(t *testing.T) {
	t.Parallel()
	url := newDatabase(t)
	h := newHarnessOn(t, openPGX(t, url), url)
	h.exec(`DROP TABLE pgworkflow_signals, pgworkflow_steps, pgworkflow_runs, pgworkflow_meta`)
	h.exec(`CREATE TABLE workflow_runs (id TEXT PRIMARY KEY, legacy TEXT)`)
	h.exec(`INSERT INTO workflow_runs VALUES ('old', 'kept')`)

	if err := h.client.EnsureSchema(h.ctx); err != nil {
		t.Fatal(err)
	}

	if h.queryInt(`SELECT count(*) FROM workflow_runs WHERE legacy = 'kept'`) != 1 {
		t.Fatal("EnsureSchema touched the earlier workflow tables")
	}
	if h.queryInt(`SELECT count(*) FROM pgworkflow_runs`) != 0 {
		t.Fatal("pgworkflow_runs not created")
	}
}

func TestNewRefusesANilQueue(t *testing.T) {
	_, err := New(nil)
	requireErrorIs(t, err, ErrNilQueue)
	q, _ := queue.New(nil)
	if q != nil {
		t.Fatal("queue.New accepted a nil db")
	}
}

func TestDefineRefusesAnEmptyNameOrFunction(t *testing.T) {
	requirePanic(t, "empty name", func() { Define("", func(*Context, int) (int, error) { return 0, nil }) })
	requirePanic(t, "name with @", func() { Define("billing@1", func(*Context, int) (int, error) { return 0, nil }) })
	requirePanic(t, "nil function", func() { Define[int, int]("nil", nil) })
}

func TestNewSignalRefusesAnInvalidName(t *testing.T) {
	requirePanic(t, "empty", func() { NewSignal[int]("") })
	requirePanic(t, "hash", func() { NewSignal[int]("a#b") })
	if NewSignal[int]("ok").Name() != "ok" {
		t.Fatal("Name does not return the signal name")
	}
}

func requirePanic(t *testing.T, what string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatalf("%s did not panic", what)
		}
	}()
	fn()
}

func TestRetryDefaultsAndBackoff(t *testing.T) {
	defaults := Retry{}.withDefaults()
	if defaults != (Retry{MaxAttempts: 3, Backoff: time.Second, MaxBackoff: time.Minute}) {
		t.Fatalf("defaults = %+v", defaults)
	}
	if long := (Retry{Backoff: time.Hour}).withDefaults(); long.MaxBackoff != time.Hour {
		t.Fatalf("a long backoff is capped below itself: %+v", long)
	}
	policy := Retry{MaxAttempts: 10, Backoff: time.Second, MaxBackoff: 5 * time.Second}
	var delays []time.Duration
	for attempt := 1; attempt <= 5; attempt++ {
		delays = append(delays, policy.delayAfter(attempt))
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second}
	for i := range want {
		if delays[i] != want[i] {
			t.Fatalf("delays = %v, want %v", delays, want)
		}
	}
}

func TestAVersionSeparatesTheQueues(t *testing.T) {
	unversioned := Define("q", func(*Context, int) (int, error) { return 0, nil })
	versioned := Define("q", func(*Context, int) (int, error) { return 0, nil }, Version(3))
	if unversioned.definition.queueName() != "pgworkflow:q" || versioned.definition.queueName() != "pgworkflow:q@3" {
		t.Fatalf("queues = %q, %q", unversioned.definition.queueName(), versioned.definition.queueName())
	}
	if versioned.Name() != "q" {
		t.Fatal("Name includes the version")
	}
}

func TestRunErrorMatchesCancellationAndTimeout(t *testing.T) {
	cancelled := &RunError{Status: RunCancelled}
	timedOut := &RunError{Status: RunFailed, TimedOut: true}
	failed := &RunError{Status: RunFailed}
	if !errors.Is(cancelled, ErrCancelled) || errors.Is(cancelled, ErrTimeout) {
		t.Fatal("cancelled")
	}
	if !errors.Is(timedOut, ErrTimeout) || errors.Is(timedOut, ErrCancelled) {
		t.Fatal("timed out")
	}
	if errors.Is(failed, ErrCancelled) || errors.Is(failed, ErrTimeout) {
		t.Fatal("failed")
	}
}

func TestNonRetryableKeepsTheErrorChain(t *testing.T) {
	cause := context.DeadlineExceeded
	err := NonRetryable(cause)
	if !errors.Is(err, cause) || !isNonRetryable(err) || err.Error() != cause.Error() {
		t.Fatalf("err = %v", err)
	}
	if NonRetryable(nil) != nil {
		t.Fatal("NonRetryable(nil) is not nil")
	}
}

func TestStatusFinished(t *testing.T) {
	for status, finished := range map[RunStatus]bool{
		RunPending: false, RunRunning: false, RunWaiting: false,
		RunSucceeded: true, RunFailed: true, RunCancelled: true,
	} {
		if status.Finished() != finished {
			t.Fatalf("%s.Finished() = %v", status, status.Finished())
		}
	}
}

func TestPurgeDeletesWholeTreesAndKeepsChildrenOfRetainedParents(t *testing.T) {
	h := newHarness(t)
	failing := Define("purge-child", func(wf *Context, _ struct{}) (int, error) { return 0, errors.New("child failed") })
	parent := Define("purge-parent", func(wf *Context, _ struct{}) (int, error) {
		return wf.Call("child", failing, struct{}{})
	})
	h.startWorker(parent, failing)
	run := mustStart(h, parent, struct{}{})
	awaitFailure(h, run)
	childID := h.step(run.ID, "child").ChildRunID
	h.exec(`UPDATE pgworkflow_runs SET completed_at = NOW() - interval '31 days' WHERE id = $1`, childID)

	deleted, err := h.client.Purge(h.ctx, PurgeParams{OlderThan: 30 * 24 * time.Hour})
	if err != nil || deleted != 0 {
		t.Fatalf("purged %d runs (err %v) while the parent is retained", deleted, err)
	}
	h.exec(`UPDATE pgworkflow_runs SET completed_at = NOW() - interval '31 days' WHERE id = $1`, run.ID)

	deleted, err = h.client.Purge(h.ctx, PurgeParams{OlderThan: 30 * 24 * time.Hour})
	if err != nil || deleted != 2 {
		t.Fatalf("purged %d runs (err %v), want the parent and its child", deleted, err)
	}
}

func TestPurgeKeepsATreeRetriedWhileItWaited(t *testing.T) {
	h := newHarness(t)
	flow := Define("purge-retried", func(wf *Context, _ struct{}) (int, error) { return 0, errors.New("boom") })
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	awaitFailure(h, run)
	h.exec(`UPDATE pgworkflow_runs SET completed_at = NOW() - interval '8 days' WHERE id = $1`, run.ID)
	tx, err := h.db.BeginTx(h.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := h.client.retryTx(h.ctx, tx, run.ID); err != nil {
		t.Fatal(err)
	}
	purged := make(chan error, 1)
	var deleted int64
	go func() {
		var err error
		deleted, err = h.client.Purge(h.ctx, PurgeParams{OlderThan: 7 * 24 * time.Hour})
		purged <- err
	}()
	h.waitForLockWaiters(1)

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if err := <-purged; err != nil || deleted != 0 {
		t.Fatalf("purged %d runs (err %v) of a tree retried while Purge waited", deleted, err)
	}
	h.run(run.ID)
}

func TestPurgeKeepsATreeWhoseNewDescendantIsRetried(t *testing.T) {
	h := newHarness(t)
	// a-retried sorts first, so Purge waits on it before it locks r-tree.
	h.insertFinishedRun("a-retried", "")
	h.insertFinishedRun("r-tree", "")
	h.insertFinishedRun("r-tree/child", "r-tree")
	retryA := h.holdTx(`UPDATE pgworkflow_runs SET status = 'pending', completed_at = NULL WHERE id = 'a-retried'`)
	purged := make(chan error, 1)
	var deleted int64
	go func() {
		var err error
		deleted, err = h.client.Purge(h.ctx, PurgeParams{OlderThan: 7 * 24 * time.Hour})
		purged <- err
	}()
	h.waitForBlockedBy(retryA)

	h.insertFinishedRun("r-tree/grandchild", "r-tree/child")
	retryGrandchild := h.holdTx(`UPDATE pgworkflow_runs SET status = 'pending', completed_at = NULL WHERE id = 'r-tree/grandchild'`)
	retryA.commit()
	h.waitForBlockedBy(retryGrandchild)
	retryGrandchild.commit()

	if err := <-purged; err != nil || deleted != 0 {
		t.Fatalf("purged %d runs (err %v) of trees retried while Purge waited", deleted, err)
	}
	if left := h.queryInt(`SELECT count(*) FROM pgworkflow_runs WHERE id LIKE 'r-tree%'`); left != 3 {
		t.Fatalf("%d runs of r-tree left, want all 3", left)
	}
}

func (h *harness) insertFinishedRun(id, parentID string) {
	h.t.Helper()
	h.exec(`INSERT INTO pgworkflow_runs (id, workflow, status, input, parent_run_id, completed_at)
		VALUES ($1, 'synthetic', 'failed', '{}', NULLIF($2, ''), NOW() - interval '8 days')`, id, parentID)
}

type heldTx struct {
	h   *harness
	tx  *sql.Tx
	pid int
}

// holdTx runs statement in a transaction it leaves open until commit.
func (h *harness) holdTx(statement string) heldTx {
	h.t.Helper()
	tx, err := h.db.BeginTx(h.ctx, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = tx.Rollback() })
	held := heldTx{h: h, tx: tx}
	if err := tx.QueryRowContext(h.ctx, `SELECT pg_backend_pid()`).Scan(&held.pid); err != nil {
		h.t.Fatal(err)
	}
	if _, err := tx.ExecContext(h.ctx, statement); err != nil {
		h.t.Fatal(err)
	}
	return held
}

func (held heldTx) commit() {
	held.h.t.Helper()
	if err := held.tx.Commit(); err != nil {
		held.h.t.Fatal(err)
	}
}

func (h *harness) waitForBlockedBy(held heldTx) {
	h.t.Helper()
	h.eventually(fmt.Sprintf("a session waits for backend %d", held.pid), func() bool {
		return h.queryInt(`SELECT count(*) FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))`, held.pid) > 0
	})
}
