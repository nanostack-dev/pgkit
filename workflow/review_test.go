package workflow

import (
	"context"
	"database/sql"
	"fmt"
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
	h.waitForLockWaiters(1)
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
	run := mustStart(h, flow, struct{}{})

	awaitFailure(h, run)

	if calls.get("flaky") != 5 {
		t.Fatalf("attempts = %d, want the workflow's 5", calls.get("flaky"))
	}
	h.requireStep(run.ID, "flaky", StepFailed, 5)
}

func TestTheFirstExecutionSeesWhatReplaysSee(t *testing.T) {
	h := newHarness(t)
	type withHidden struct {
		Shown  string `json:"shown"`
		Hidden string `json:"-"`
	}
	observed := make(chan string, 4)
	flow := Define("json-shaped", func(wf *Context, _ struct{}) (string, error) {
		plain, err := wf.Step("plain", func(context.Context) (withHidden, error) {
			return withHidden{Shown: "kept", Hidden: "dropped"}, nil
		})
		if err != nil {
			return "", err
		}
		transactional, err := wf.TxStep("transactional", func(context.Context, *sql.Tx) (withHidden, error) {
			return withHidden{Shown: "kept", Hidden: "dropped"}, nil
		})
		if err != nil {
			return "", err
		}
		observed <- plain.Shown + "/" + plain.Hidden + " " + transactional.Shown + "/" + transactional.Hidden
		return "", wf.Sleep("replay", time.Hour)
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	h.waitForStatus(run.ID, RunWaiting)
	h.fastForward(run.ID)
	mustResult(h, run)
	close(observed)

	var seen []string
	for observation := range observed {
		seen = append(seen, observation)
	}
	if len(seen) != 2 || seen[0] != "kept/ kept/" || seen[1] != seen[0] {
		t.Fatalf("first execution and replay saw %q", seen)
	}
}

type errorString string

func (e errorString) Error() string { return string(e) }

// waitForLockWaiters waits until n sessions of the test database wait for a lock,
// proving a concurrent transaction is blocked where the test expects it.
func (h *harness) waitForLockWaiters(n int) {
	h.t.Helper()
	h.eventually(fmt.Sprintf("%d session(s) wait for a lock", n), func() bool {
		return h.queryInt(`SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`) >= n
	})
}

func TestAStepResultHoldingANULCharacterFailsWithoutRetry(t *testing.T) {
	h := newHarness(t)
	calls := newCounter()
	flow := Define("nul-result", func(wf *Context, _ struct{}) (string, error) {
		return wf.Step("binary", func(context.Context) (string, error) {
			calls.add("binary")
			return "a\x00b", nil
		})
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})

	runErr := mustFail(h, run)

	if calls.get("binary") != 1 || !strings.Contains(runErr.Message, "NUL character") {
		t.Fatalf("calls = %d, message = %q", calls.get("binary"), runErr.Message)
	}
	h.requireStep(run.ID, "binary", StepFailed, 1)
}

func TestStartRefusesAnInputHoldingANULCharacter(t *testing.T) {
	h := newHarness(t)
	flow := Define("nul-input", func(wf *Context, in string) (string, error) { return in, nil })
	_, err := h.client.Start(h.ctx, flow, "a\x00b")
	requireErrorIs(t, err, errUnstorableNUL)
	if checkStorable([]byte(`"a\\u0000"`)) != nil {
		t.Fatal("an escaped backslash before u0000 was taken for a NUL")
	}
}

func TestATxStepWhoseCheckpointFailsKeepsNoWrites(t *testing.T) {
	h := newHarness(t)
	createLedger(h)
	h.exec(`CREATE FUNCTION refuse_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'checkpoint refused'; END $$`)
	h.exec(`CREATE TRIGGER refuse_checkpoint BEFORE UPDATE ON pgworkflow_steps FOR EACH ROW
		WHEN (NEW.name = 'record' AND NEW.status = 'succeeded') EXECUTE FUNCTION refuse_checkpoint()`)
	calls := newCounter()
	flow := Define("ledger-checkpoint", func(wf *Context, _ struct{}) (int, error) {
		return wf.TxStep("record", func(ctx context.Context, tx *sql.Tx) (int, error) {
			_, err := tx.ExecContext(ctx, `INSERT INTO ledger (entry) VALUES ('once')`)
			return calls.add("record"), err
		})
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	h.eventually("the refused checkpoint failed the activation", func() bool {
		return h.queryInt(`SELECT count(*) FROM pgqueue_jobs job JOIN pgworkflow_runs run ON run.job_id = job.id
			WHERE run.id = $1 AND job.status = 'pending' AND job.last_error LIKE '%checkpoint refused%'`, run.ID) == 1
	})

	if h.queryInt(`SELECT count(*) FROM ledger`) != 0 {
		t.Fatal("the write committed without its checkpoint")
	}
	h.exec(`DROP TRIGGER refuse_checkpoint ON pgworkflow_steps`)
	h.wakeJob(run.ID)

	mustResult(h, run)
	if h.queryInt(`SELECT count(*) FROM ledger`) != 1 {
		t.Fatalf("ledger entries = %d, want exactly one", h.queryInt(`SELECT count(*) FROM ledger`))
	}
	if calls.get("record") < 2 {
		t.Fatal("the step did not run again after its checkpoint was refused")
	}
}

func TestCancelWaitsForATransactionalStepInProgress(t *testing.T) {
	h := newHarness(t)
	createLedger(h)
	reached := newGate()
	flow := Define("cancel-during-tx", func(wf *Context, _ struct{}) (string, error) {
		if _, err := wf.TxStep("record", func(ctx context.Context, tx *sql.Tx) (bool, error) {
			if _, err := tx.ExecContext(ctx, `INSERT INTO ledger (entry) VALUES ('committed')`); err != nil {
				return false, err
			}
			return true, reached.wait(ctx)
		}); err != nil {
			return "", err
		}
		return "", wf.Sleep("after", time.Hour)
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	reached.awaitReached(t)

	cancelled := make(chan error, 1)
	go func() { cancelled <- h.client.Cancel(h.ctx, run.ID) }()
	h.waitForLockWaiters(1)
	select {
	case err := <-cancelled:
		t.Fatalf("Cancel returned before the transactional step ended: %v", err)
	default:
	}
	reached.release()

	if err := <-cancelled; err != nil {
		t.Fatal(err)
	}
	_, err := result(h, run)
	requireErrorIs(t, err, ErrCancelled)
	if h.queryInt(`SELECT count(*) FROM ledger`) != 1 {
		t.Fatal("the transactional step's effect was lost or repeated")
	}
	h.requireStep(run.ID, "record", StepSucceeded, 1)
}
