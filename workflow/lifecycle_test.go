package workflow

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCancelAWaitingRun(t *testing.T) {
	h := newHarness(t)
	flow := Define("waiting-to-cancel", func(wf *Context, _ struct{}) (string, error) {
		return "", wf.Sleep("forever", time.Hour)
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	h.waitForStatus(run.ID, RunWaiting)

	if err := h.client.Cancel(h.ctx, run.ID); err != nil {
		t.Fatal(err)
	}

	_, err := result(h, run)
	requireErrorIs(t, err, ErrCancelled)
	if runErr := requireRunError(t, err); runErr.Status != RunCancelled {
		t.Fatalf("run error = %+v", runErr)
	}
	h.eventually("the parked job is settled", func() bool {
		return h.queryInt(`SELECT count(*) FROM pgqueue_jobs WHERE status = 'done'`) == 1
	})
}

func TestCancelInterruptsARunningStep(t *testing.T) {
	h := newHarness(t)
	reached := newGate()
	calls := newCounter()
	flow := Define("interrupted", func(wf *Context, _ struct{}) (string, error) {
		return wf.Step("long-call", func(ctx context.Context) (string, error) {
			calls.add("long-call")
			return "", reached.wait(ctx)
		}, fastRetry)
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	reached.awaitReached(t)

	started := time.Now()
	if err := h.client.Cancel(h.ctx, run.ID); err != nil {
		t.Fatal(err)
	}

	_, err := result(h, run)
	requireErrorIs(t, err, ErrCancelled)
	h.eventually("the job is settled", func() bool {
		return h.queryInt(`SELECT count(*) FROM pgqueue_jobs WHERE status = 'done'`) == 1
	})
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("cancellation took %v to reach the step", elapsed)
	}
	if calls.get("long-call") != 1 {
		t.Fatalf("the interrupted step was retried: %d calls", calls.get("long-call"))
	}
}

func TestCancelAPendingRun(t *testing.T) {
	h := newHarness(t)
	calls := newCounter()
	flow := Define("never-started", func(wf *Context, _ struct{}) (int, error) {
		return wf.Step("work", func(context.Context) (int, error) { return calls.add("work"), nil })
	})
	run := mustStart(h, flow, struct{}{})

	if err := h.client.Cancel(h.ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	h.startWorker(flow)

	h.eventually("the job is settled", func() bool {
		return h.queryInt(`SELECT count(*) FROM pgqueue_jobs WHERE status = 'done'`) == 1
	})
	if calls.get("work") != 0 || h.run(run.ID).Status != RunCancelled {
		t.Fatal("a cancelled pending run executed")
	}
}

func TestCancellingTwiceIsHarmless(t *testing.T) {
	h := newHarness(t)
	flow := Define("cancel-twice", func(wf *Context, _ struct{}) (int, error) { return 1, nil })
	run := mustStart(h, flow, struct{}{})

	if err := h.client.Cancel(h.ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	if err := h.client.Cancel(h.ctx, run.ID); err != nil {
		t.Fatalf("second cancel: %v", err)
	}
}

func TestCancellingAFinishedRunFails(t *testing.T) {
	h := newHarness(t)
	flow := Define("already-done", func(wf *Context, _ struct{}) (int, error) { return 1, nil })
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	mustResult(h, run)

	requireErrorIs(t, h.client.Cancel(h.ctx, run.ID), ErrRunFinished)
}

func TestCancellingAnUnknownRunFails(t *testing.T) {
	h := newHarness(t)
	requireErrorIs(t, h.client.Cancel(h.ctx, "missing"), ErrRunNotFound)
}

func TestARunThatOutlivesItsTimeoutFails(t *testing.T) {
	h := newHarness(t)
	flow := Define("too-slow", func(wf *Context, _ struct{}) (string, error) {
		return "", wf.Sleep("forever", time.Hour)
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{}, Timeout(300*time.Millisecond))

	_, err := result(h, run)

	requireErrorIs(t, err, ErrTimeout)
	if errors.Is(err, ErrCancelled) {
		t.Fatal("a timeout matched ErrCancelled")
	}
	info := h.run(run.ID)
	if !info.TimedOut || info.Status != RunFailed || info.DeadlineAt.IsZero() {
		t.Fatalf("run = %+v", info)
	}
}

func TestARunTimeoutInterruptsTheRunningStep(t *testing.T) {
	h := newHarness(t)
	reached := newGate()
	flow := Define("stuck", func(wf *Context, _ struct{}) (string, error) {
		return wf.Step("hang", func(ctx context.Context) (string, error) {
			return "", reached.wait(ctx)
		})
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{}, Timeout(500*time.Millisecond))
	reached.awaitReached(t)

	_, err := result(h, run)

	requireErrorIs(t, err, ErrTimeout)
}

func TestARunWhoseTimeoutPassedBeforeItStartedFails(t *testing.T) {
	h := newHarness(t)
	calls := newCounter()
	flow := Define("late-start", func(wf *Context, _ struct{}) (int, error) {
		return wf.Step("work", func(context.Context) (int, error) { return calls.add("work"), nil })
	})
	run := mustStart(h, flow, struct{}{}, Timeout(time.Millisecond))
	time.Sleep(50 * time.Millisecond)
	h.startWorker(flow)

	_, err := result(h, run)

	requireErrorIs(t, err, ErrTimeout)
	if calls.get("work") != 0 {
		t.Fatal("a run past its timeout started work")
	}
}

func TestRetryResumesAFailedRunFromItsCheckpoints(t *testing.T) {
	h := newHarness(t)
	calls := newCounter()
	healthy := make(chan struct{})
	flow := Define("resumable", func(wf *Context, _ struct{}) (string, error) {
		account, err := wf.Step("open-account", func(context.Context) (string, error) {
			calls.add("open-account")
			return "acct-1", nil
		})
		if err != nil {
			return "", err
		}
		card, err := wf.Step("issue-card", func(context.Context) (string, error) {
			calls.add("issue-card")
			select {
			case <-healthy:
				return "card-1", nil
			default:
				return "", errors.New("card service down")
			}
		}, NoRetry)
		if err != nil {
			return "", err
		}
		return account + "/" + card, nil
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	_, err := result(h, run)
	requireRunError(t, err)

	close(healthy)
	if err := h.client.Retry(h.ctx, run.ID); err != nil {
		t.Fatal(err)
	}

	if got := mustResult(h, run); got != "acct-1/card-1" {
		t.Fatalf("result = %q", got)
	}
	if calls.get("open-account") != 1 || calls.get("issue-card") != 2 {
		t.Fatalf("open-account = %d, issue-card = %d", calls.get("open-account"), calls.get("issue-card"))
	}
	if step := h.step(run.ID, "issue-card"); step.Attempts != 1 {
		t.Fatalf("the retried step kept its old attempts: %+v", step)
	}
}

func TestRetryResumesAFailedChild(t *testing.T) {
	h := newHarness(t)
	healthy := make(chan struct{})
	fragile := Define("fragile-child", func(wf *Context, _ struct{}) (string, error) {
		return wf.Step("work", func(context.Context) (string, error) {
			select {
			case <-healthy:
				return "child ok", nil
			default:
				return "", errors.New("not yet")
			}
		}, NoRetry)
	})
	parent := Define("parent-of-fragile", func(wf *Context, _ struct{}) (string, error) {
		return wf.Call("child", fragile, struct{}{})
	})
	h.startWorker(parent, fragile)
	run := mustStart(h, parent, struct{}{})
	_, err := result(h, run)
	requireRunError(t, err)

	close(healthy)
	if err := h.client.Retry(h.ctx, run.ID); err != nil {
		t.Fatal(err)
	}

	if got := mustResult(h, run); got != "child ok" {
		t.Fatalf("result = %q", got)
	}
	if got := h.queryInt(`SELECT count(*) FROM pgworkflow_runs WHERE parent_run_id = $1`, run.ID); got != 1 {
		t.Fatalf("retrying started %d children instead of resuming one", got)
	}
}

func TestRetryResumesACancelledRun(t *testing.T) {
	h := newHarness(t)
	flow := Define("resumed-after-cancel", func(wf *Context, _ struct{}) (string, error) {
		decision, err := wf.Receive(approved, Forever)
		return decision.By, err
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	h.waitForStatus(run.ID, RunWaiting)
	if err := h.client.Cancel(h.ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	h.waitForStatus(run.ID, RunCancelled)

	if err := h.client.Retry(h.ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	h.waitForStatus(run.ID, RunWaiting)
	if err := h.client.Signal(h.ctx, run.ID, approved, approval{By: "back"}); err != nil {
		t.Fatal(err)
	}

	if got := mustResult(h, run); got != "back" {
		t.Fatalf("result = %q", got)
	}
}

func TestRetryClearsTheRunTimeout(t *testing.T) {
	h := newHarness(t)
	flow := Define("retry-timeout", func(wf *Context, _ struct{}) (string, error) {
		decision, err := wf.Receive(approved, Forever)
		return decision.By, err
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{}, Timeout(200*time.Millisecond))
	_, err := result(h, run)
	requireErrorIs(t, err, ErrTimeout)

	if err := h.client.Retry(h.ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	h.waitForStatus(run.ID, RunWaiting)

	info := h.run(run.ID)
	if !info.DeadlineAt.IsZero() || info.TimedOut || info.Error != "" {
		t.Fatalf("run = %+v", info)
	}
}

func TestRetryRefusesARunThatHasNotFailed(t *testing.T) {
	h := newHarness(t)
	flow := Define("not-failed", func(wf *Context, _ struct{}) (int, error) { return 1, nil })
	pending := mustStart(h, flow, struct{}{})
	requireErrorIs(t, h.client.Retry(h.ctx, pending.ID), ErrRunNotRetryable)

	h.startWorker(flow)
	mustResult(h, pending)
	requireErrorIs(t, h.client.Retry(h.ctx, pending.ID), ErrRunNotRetryable)
	requireErrorIs(t, h.client.Retry(h.ctx, "missing"), ErrRunNotFound)
}

func TestARunFailedByNonDeterminismCanBeRetriedAfterAFix(t *testing.T) {
	h := newHarness(t)
	var deployed, fixed atomic.Bool
	flow := Define("fixed-later", func(wf *Context, _ struct{}) (string, error) {
		if deployed.Load() && !fixed.Load() {
			return "", wf.Sleep("lookup", time.Millisecond)
		}
		if _, err := wf.Step("lookup", func(context.Context) (int, error) { return 1, nil }); err != nil {
			return "", err
		}
		return "consistent", wf.Sleep("wait", time.Hour)
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	h.waitForStatus(run.ID, RunWaiting)
	deployed.Store(true)
	h.fastForward(run.ID)
	_, err := result(h, run)
	if runErr := requireRunError(t, err); !strings.Contains(runErr.Message, "recorded as a step") {
		t.Fatalf("message = %q", runErr.Message)
	}

	fixed.Store(true)
	if err := h.client.Retry(h.ctx, run.ID); err != nil {
		t.Fatal(err)
	}

	if got := mustResult(h, run); got != "consistent" {
		t.Fatalf("result = %q", got)
	}
}
