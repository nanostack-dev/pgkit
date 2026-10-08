package workflow

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

func TestAParkedRunKeepsTheQueueListable(t *testing.T) {
	h := newHarness(t)
	flow := approvalFlow("listable", Forever)
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	h.waitForStatus(run.ID, RunWaiting)

	jobs, err := h.queue.ListJobs(h.ctx, queueListParams())
	if err != nil {
		t.Fatalf("a parked run broke the job listing: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("jobs = %d", len(jobs))
	}
}

func TestCancelReachesAGrandchildStartedDuringTheCancel(t *testing.T) {
	h := newHarness(t)
	root := mustStart(h, Define("cancel-root", func(wf *Context, _ struct{}) (int, error) { return 0, nil }), struct{}{})
	childID, grandchildID := "child-"+root.ID, "grandchild-"+root.ID
	h.exec(`INSERT INTO pgworkflow_runs (id, workflow, status, input, parent_run_id, lease) VALUES ($1, 'child', 'running', '{}', $2, 1)`, childID, root.ID)

	startingGrandchild, err := h.db.BeginTx(h.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = startingGrandchild.Rollback() }()
	var status string
	if err := startingGrandchild.QueryRow(`SELECT status FROM pgworkflow_runs WHERE id = $1 AND lease = 1 FOR SHARE`, childID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if _, err := startingGrandchild.Exec(`INSERT INTO pgworkflow_runs (id, workflow, status, input, parent_run_id, key)
		VALUES ($1, 'grandchild', 'pending', '{}', $2, $3)`, grandchildID, childID, childID+"/g"); err != nil {
		t.Fatal(err)
	}

	cancelled := make(chan error, 1)
	go func() { cancelled <- h.client.Cancel(h.ctx, root.ID) }()
	time.Sleep(300 * time.Millisecond)
	if err := startingGrandchild.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-cancelled; err != nil {
		t.Fatal(err)
	}

	if got := h.run(grandchildID).Status; got != RunCancelled {
		t.Fatalf("grandchild = %s, want cancelled", got)
	}
}

func TestASignalDoesNotWaitForATransactionalStep(t *testing.T) {
	h := newHarness(t)
	createLedger(h)
	reached := newGate()
	flow := Define("busy-ledger", func(wf *Context, _ struct{}) (string, error) {
		if _, err := wf.TxStep("long-write", func(ctx context.Context, tx *sql.Tx) (bool, error) {
			if _, err := tx.ExecContext(ctx, `INSERT INTO ledger (entry) VALUES ('busy')`); err != nil {
				return false, err
			}
			return true, reached.wait(ctx)
		}); err != nil {
			return "", err
		}
		decision, err := wf.Receive(approved, Forever)
		return decision.By, err
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	reached.awaitReached(t)

	sent := make(chan error, 1)
	go func() { sent <- h.client.Signal(h.ctx, run.ID, approved, approval{By: "during"}) }()
	select {
	case err := <-sent:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Signal waited for the transactional step")
	}
	reached.release()

	if got := mustResult(h, run); got != "during" {
		t.Fatalf("result = %q", got)
	}
}

func TestPurgeKeepsTheChildrenOfAnUnfinishedParent(t *testing.T) {
	h := newHarness(t)
	parent := Define("slow-reader", func(wf *Context, _ struct{}) (int, error) {
		child := wf.Start("double", double, 4)
		if err := wf.Sleep("before-reading", time.Hour); err != nil {
			return 0, err
		}
		return child.Wait()
	})
	h.startWorker(parent, double)
	run := mustStart(h, parent, struct{}{})
	childID := h.waitForStep(run.ID, "double", StepWaiting).ChildRunID
	h.waitForStatus(childID, RunSucceeded)
	h.waitForStatus(run.ID, RunWaiting)

	deleted, err := h.client.Purge(h.ctx, PurgeParams{})
	if err != nil || deleted != 0 {
		t.Fatalf("deleted = %d, err = %v", deleted, err)
	}
	h.fastForward(run.ID)

	if got := mustResult(h, run); got != 8 {
		t.Fatalf("result = %d", got)
	}
}

func TestARunWhoseJobTheQueueGaveUpOnFails(t *testing.T) {
	h := newHarness(t)
	flow := Define("abandoned-job", func(wf *Context, _ struct{}) (int, error) { return 1, nil })
	run := mustStart(h, flow, struct{}{})
	h.exec(`UPDATE pgqueue_jobs SET status = 'failed', last_error = 'reaped: visibility timeout exceeded'
		WHERE id = (SELECT job_id FROM pgworkflow_runs WHERE id = $1)`, run.ID)
	h.startWorkerWith(workerOptions{config: WorkerConfig{ReapInterval: 50 * time.Millisecond}}, flow)

	_, err := result(h, run)

	runErr := requireRunError(t, err)
	if !strings.Contains(runErr.Message, "the queue gave up on the run's job: reaped") {
		t.Fatalf("message = %q", runErr.Message)
	}
}

func TestAStepRetryKeepsTheWorkflowValuesItLeavesZero(t *testing.T) {
	h := newHarness(t)
	calls := newCounter()
	flow := Define("partial-override", func(wf *Context, _ struct{}) (string, error) {
		return wf.Step("flaky", func(context.Context) (string, error) {
			calls.add("flaky")
			return "", errorString("no")
		}, Retry{Backoff: time.Millisecond})
	}, Retry{MaxAttempts: 5})
	h.startWorker(flow)

	_, _ = result(h, mustStart(h, flow, struct{}{}))

	if calls.get("flaky") != 5 {
		t.Fatalf("attempts = %d, want the workflow's 5", calls.get("flaky"))
	}
}

func TestTheFirstExecutionSeesWhatReplaysSee(t *testing.T) {
	h := newHarness(t)
	type withHidden struct {
		Shown  string `json:"shown"`
		Hidden string `json:"-"`
	}
	flow := Define("json-shaped", func(wf *Context, _ struct{}) (string, error) {
		value, err := wf.Step("shape", func(context.Context) (withHidden, error) {
			return withHidden{Shown: "kept", Hidden: "dropped"}, nil
		})
		return value.Shown + "/" + value.Hidden, err
	})
	h.startWorker(flow)

	if got := mustResult(h, mustStart(h, flow, struct{}{})); got != "kept/" {
		t.Fatalf("first execution saw %q", got)
	}
}

type errorString string

func (e errorString) Error() string { return string(e) }
