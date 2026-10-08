package workflow

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var fastRetry = Retry{MaxAttempts: 3, Backoff: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond}

func TestAFailingStepIsRetriedUntilItSucceeds(t *testing.T) {
	h := newHarness(t)
	calls := newCounter()
	flow := Define("flaky", func(wf *Context, _ struct{}) (string, error) {
		return wf.Step("call-api", func(context.Context) (string, error) {
			if calls.add("call-api") < 3 {
				return "", errors.New("503")
			}
			return "ok", nil
		}, fastRetry)
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})

	if got := mustResult(h, run); got != "ok" {
		t.Fatalf("result = %q", got)
	}
	if calls.get("call-api") != 3 {
		t.Fatalf("calls = %d", calls.get("call-api"))
	}
	if step := h.step(run.ID, "call-api"); step.Attempts != 3 || step.Status != StepSucceeded || step.Error != "" {
		t.Fatalf("step = %+v", step)
	}
}

func TestAStepFailsAfterItsLastAttempt(t *testing.T) {
	h := newHarness(t)
	calls := newCounter()
	flow := Define("hopeless", func(wf *Context, _ struct{}) (string, error) {
		return wf.Step("call-api", func(context.Context) (string, error) {
			return "", fmt.Errorf("attempt %d refused", calls.add("call-api"))
		}, fastRetry)
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})

	_, err := result(h, run)

	runErr := requireRunError(t, err)
	want := `workflow: step "call-api" failed after 3 attempt(s): attempt 3 refused`
	if runErr.Message != want {
		t.Fatalf("message = %q, want %q", runErr.Message, want)
	}
	if step := h.step(run.ID, "call-api"); step.Status != StepFailed || step.Attempts != 3 || step.Error != "attempt 3 refused" {
		t.Fatalf("step = %+v", step)
	}
}

func TestStepErrorIsAStepErrorValue(t *testing.T) {
	h := newHarness(t)
	type observed struct {
		Step     string
		Attempts int
		Message  string
	}
	flow := Define("inspecting", func(wf *Context, _ struct{}) (observed, error) {
		_, err := wf.Step("charge", func(context.Context) (int, error) { return 0, errors.New("card declined") }, NoRetry)
		var stepErr *StepError
		if !errors.As(err, &stepErr) {
			return observed{}, fmt.Errorf("unexpected error %T", err)
		}
		return observed{stepErr.Step, stepErr.Attempts, stepErr.Message}, nil
	})
	h.startWorker(flow)

	got := mustResult(h, mustStart(h, flow, struct{}{}))

	if got != (observed{"charge", 1, "card declined"}) {
		t.Fatalf("step error = %+v", got)
	}
}

func TestANonRetryableErrorFailsTheStepAtOnce(t *testing.T) {
	h := newHarness(t)
	calls := newCounter()
	flow := Define("final", func(wf *Context, _ struct{}) (string, error) {
		return wf.Step("validate", func(context.Context) (string, error) {
			calls.add("validate")
			return "", NonRetryable(errors.New("invalid card number"))
		}, fastRetry)
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})

	_, err := result(h, run)

	requireFailed(t, err)
	if calls.get("validate") != 1 {
		t.Fatalf("calls = %d", calls.get("validate"))
	}
	if step := h.step(run.ID, "validate"); step.Attempts != 1 || step.Error != "invalid card number" {
		t.Fatalf("step = %+v", step)
	}
}

func TestNoRetryAttemptsAStepOnce(t *testing.T) {
	h := newHarness(t)
	calls := newCounter()
	flow := Define("single-shot", func(wf *Context, _ struct{}) (string, error) {
		return wf.Step("once", func(context.Context) (string, error) {
			calls.add("once")
			return "", errors.New("no")
		}, NoRetry)
	})
	h.startWorker(flow)

	_, _ = result(h, mustStart(h, flow, struct{}{}))

	if calls.get("once") != 1 {
		t.Fatalf("calls = %d", calls.get("once"))
	}
}

func TestStepsAreAttemptedThreeTimesByDefault(t *testing.T) {
	h := newHarness(t)
	calls := newCounter()
	flow := Define("default-retry", func(wf *Context, _ struct{}) (string, error) {
		return wf.Step("default", func(context.Context) (string, error) {
			calls.add("default")
			return "", errors.New("no")
		})
	}, Retry{Backoff: time.Millisecond})
	h.startWorker(flow)

	_, _ = result(h, mustStart(h, flow, struct{}{}))

	if calls.get("default") != defaultMaxAttempts {
		t.Fatalf("calls = %d, want %d", calls.get("default"), defaultMaxAttempts)
	}
}

func TestAWorkflowRetryAppliesToItsStepsUnlessAStepOverridesIt(t *testing.T) {
	h := newHarness(t)
	calls := newCounter()
	flow := Define("retry-defaults", func(wf *Context, _ struct{}) (string, error) {
		_, _ = wf.Step("inherits", func(context.Context) (string, error) {
			calls.add("inherits")
			return "", errors.New("no")
		})
		_, _ = wf.Step("overrides", func(context.Context) (string, error) {
			calls.add("overrides")
			return "", errors.New("no")
		}, Retry{MaxAttempts: 4, Backoff: time.Millisecond})
		return "done", nil
	}, Retry{MaxAttempts: 2, Backoff: time.Millisecond})
	h.startWorker(flow)

	mustResult(h, mustStart(h, flow, struct{}{}))

	if calls.get("inherits") != 2 || calls.get("overrides") != 4 {
		t.Fatalf("inherits = %d, overrides = %d", calls.get("inherits"), calls.get("overrides"))
	}
}

func TestALongBackoffSuspendsTheRunUntilTheRetryIsDue(t *testing.T) {
	h := newHarness(t)
	calls := newCounter()
	flow := Define("patient", func(wf *Context, _ struct{}) (string, error) {
		return wf.Step("call-api", func(context.Context) (string, error) {
			if calls.add("call-api") == 1 {
				return "", errors.New("rate limited")
			}
			return "ok", nil
		}, Retry{MaxAttempts: 2, Backoff: time.Hour})
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})

	waiting := h.waitForStatus(run.ID, RunWaiting)
	step := h.step(run.ID, "call-api")
	if step.Status != StepRetrying || step.Error != "rate limited" || step.Attempts != 1 {
		t.Fatalf("step = %+v", step)
	}
	if delay := time.Until(step.WakeAt); delay < 59*time.Minute {
		t.Fatalf("retry due in %v, want about an hour", delay)
	}
	if !waiting.WakeAt.Equal(step.WakeAt) {
		t.Fatalf("run wakes at %v, step retries at %v", waiting.WakeAt, step.WakeAt)
	}

	h.fastForward(run.ID)

	if got := mustResult(h, run); got != "ok" {
		t.Fatalf("result = %q", got)
	}
	if calls.get("call-api") != 2 {
		t.Fatalf("calls = %d", calls.get("call-api"))
	}
}

func TestAShortBackoffRetriesWithoutSuspendingTheRun(t *testing.T) {
	h := newHarness(t)
	calls := newCounter()
	flow := Define("eager", func(wf *Context, _ struct{}) (string, error) {
		return wf.Step("call-api", func(context.Context) (string, error) {
			if calls.add("call-api") < 3 {
				return "", errors.New("blip")
			}
			return "ok", nil
		}, Retry{MaxAttempts: 3, Backoff: 20 * time.Millisecond})
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})

	mustResult(h, run)

	if lease := h.lease(run.ID); lease != 1 {
		t.Fatalf("the run was activated %d times, want once", lease)
	}
}

func TestATimeoutBoundsEachAttempt(t *testing.T) {
	h := newHarness(t)
	calls := newCounter()
	flow := Define("slow", func(wf *Context, _ struct{}) (string, error) {
		return wf.Step("call-api", func(ctx context.Context) (string, error) {
			if calls.add("call-api") == 1 {
				<-ctx.Done()
				return "", ctx.Err()
			}
			return "fast enough", nil
		}, Timeout(50*time.Millisecond), fastRetry)
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})

	if got := mustResult(h, run); got != "fast enough" {
		t.Fatalf("result = %q", got)
	}
	if calls.get("call-api") != 2 {
		t.Fatalf("calls = %d", calls.get("call-api"))
	}
}

func TestAPanickingAttemptIsAFailedAttempt(t *testing.T) {
	h := newHarness(t)
	calls := newCounter()
	flow := Define("panicky-step", func(wf *Context, _ struct{}) (string, error) {
		return wf.Step("parse", func(context.Context) (string, error) {
			if calls.add("parse") == 1 {
				panic("nil map")
			}
			return "parsed", nil
		}, fastRetry)
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})

	if got := mustResult(h, run); got != "parsed" {
		t.Fatalf("result = %q", got)
	}
	if step := h.step(run.ID, "parse"); step.Attempts != 2 {
		t.Fatalf("step = %+v", step)
	}
}

func TestAResultThatCannotBeEncodedFailsTheStepWithoutRetry(t *testing.T) {
	h := newHarness(t)
	calls := newCounter()
	flow := Define("unencodable-step", func(wf *Context, _ struct{}) (string, error) {
		_, err := wf.Step("channel", func(context.Context) (chan int, error) {
			calls.add("channel")
			return make(chan int), nil
		}, fastRetry)
		return "", err
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})

	_, err := result(h, run)

	requireFailed(t, err)
	if calls.get("channel") != 1 || !strings.Contains(h.step(run.ID, "channel").Error, "encode chan int result") {
		t.Fatalf("calls = %d, step = %+v", calls.get("channel"), h.step(run.ID, "channel"))
	}
}

func TestAnAttemptThatNeverReportedBackCountsAsFailed(t *testing.T) {
	h := newHarness(t)
	calls := newCounter()
	flow := Define("crashed", func(wf *Context, _ struct{}) (int, error) {
		return wf.Step("charge", func(context.Context) (int, error) { return calls.add("charge"), nil }, fastRetry)
	})
	run := mustStart(h, flow, struct{}{})
	h.exec(`INSERT INTO pgworkflow_steps (run_id, name, kind, status, attempts) VALUES ($1, 'charge', 'step', 'running', 1)`, run.ID)
	h.startWorker(flow)

	mustResult(h, run)

	if step := h.step(run.ID, "charge"); step.Attempts != 2 || step.Status != StepSucceeded {
		t.Fatalf("step = %+v", step)
	}
}

func TestAnAttemptThatNeverReportedBackFailsTheStepWhenItWasTheLast(t *testing.T) {
	h := newHarness(t)
	flow := Define("crashed-last", func(wf *Context, _ struct{}) (int, error) {
		return wf.Step("charge", func(context.Context) (int, error) { return 1, nil }, NoRetry)
	})
	run := mustStart(h, flow, struct{}{})
	h.exec(`INSERT INTO pgworkflow_steps (run_id, name, kind, status, attempts) VALUES ($1, 'charge', 'step', 'running', 1)`, run.ID)
	h.startWorker(flow)

	_, err := result(h, run)

	if runErr := requireRunError(t, err); !strings.Contains(runErr.Message, errAttemptLost.Error()) {
		t.Fatalf("message = %q", runErr.Message)
	}
}

func TestAWorkflowCanRecoverFromAFailedStep(t *testing.T) {
	h := newHarness(t)
	calls := newCounter()
	flow := Define("compensating", func(wf *Context, in order) (string, error) {
		if _, err := wf.Step("reserve-stock", func(context.Context) (bool, error) { calls.add("reserve"); return true, nil }); err != nil {
			return "", err
		}
		_, err := wf.Step("charge", func(context.Context) (int, error) { return 0, errors.New("card declined") }, NoRetry)
		if stepErr, failed := errors.AsType[*StepError](err); failed {
			if _, err := wf.Step("release-stock", func(context.Context) (bool, error) { calls.add("release"); return true, nil }); err != nil {
				return "", err
			}
			if err := wf.Sleep("settle", 10*time.Millisecond); err != nil {
				return "", err
			}
			return "cancelled order " + in.ID + ": " + stepErr.Message, nil
		}
		return "charged", err
	})
	h.startWorker(flow)

	got := mustResult(h, mustStart(h, flow, order{ID: "o-3"}))

	if got != "cancelled order o-3: card declined" {
		t.Fatalf("result = %q", got)
	}
	if calls.get("reserve") != 1 || calls.get("release") != 1 {
		t.Fatalf("reserve = %d, release = %d", calls.get("reserve"), calls.get("release"))
	}
}

func TestReplaysReturnRecordedResultsWithoutRunningStepsAgain(t *testing.T) {
	h := newHarness(t)
	calls := newCounter()
	executions := atomic.Int32{}
	flow := Define("replayed", func(wf *Context, _ struct{}) (string, error) {
		executions.Add(1)
		token, err := wf.Step("token", func(context.Context) (string, error) {
			return fmt.Sprintf("token-%d", calls.add("token")), nil
		})
		if err != nil {
			return "", err
		}
		if err := wf.Sleep("pause", time.Hour); err != nil {
			return "", err
		}
		return token, nil
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	h.waitForStatus(run.ID, RunWaiting)

	h.fastForward(run.ID)

	if got := mustResult(h, run); got != "token-1" {
		t.Fatalf("result = %q", got)
	}
	if calls.get("token") != 1 || executions.Load() != 2 {
		t.Fatalf("token calls = %d, executions = %d", calls.get("token"), executions.Load())
	}
}

func TestARepeatedStepNameIsNumbered(t *testing.T) {
	h := newHarness(t)
	flow := Define("looping", func(wf *Context, items []string) ([]string, error) {
		var shipped []string
		for _, item := range items {
			label, err := wf.Step("ship", func(context.Context) (string, error) { return "shipped " + item, nil })
			if err != nil {
				return nil, err
			}
			shipped = append(shipped, label)
		}
		return shipped, nil
	})
	h.startWorker(flow)
	run := mustStart(h, flow, []string{"a", "b", "c"})

	got := mustResult(h, run)

	if strings.Join(got, "|") != "shipped a|shipped b|shipped c" {
		t.Fatalf("result = %v", got)
	}
	var names []string
	for _, step := range h.steps(run.ID) {
		names = append(names, step.Name)
	}
	if strings.Join(names, ",") != "ship,ship#2,ship#3" {
		t.Fatalf("steps = %v", names)
	}
}

func TestAnInvalidStepNameIsAnError(t *testing.T) {
	h := newHarness(t)
	flow := Define("badly-named", func(wf *Context, name string) (string, error) {
		_, err := wf.Step(name, func(context.Context) (int, error) { return 1, nil })
		return fmt.Sprint(errors.Is(err, ErrInvalidName)), nil
	})
	h.startWorker(flow)

	for _, name := range []string{"", "  ", "with#hash"} {
		if got := mustResult(h, mustStart(h, flow, name)); got != "true" {
			t.Fatalf("name %q was accepted", name)
		}
	}
}

func TestReusingAStepNameForAnotherOperationFailsTheRun(t *testing.T) {
	h := newHarness(t)
	var deployed atomic.Bool
	flow := Define("changed-shape", func(wf *Context, _ struct{}) (string, error) {
		if deployed.Load() {
			if err := wf.Sleep("lookup", time.Millisecond); err != nil {
				return "", err
			}
			return "new code", nil
		}
		if _, err := wf.Step("lookup", func(context.Context) (int, error) { return 1, nil }); err != nil {
			return "", err
		}
		return "", wf.Sleep("wait", time.Hour)
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	h.waitForStatus(run.ID, RunWaiting)

	deployed.Store(true)
	h.fastForward(run.ID)

	_, err := result(h, run)
	runErr := requireRunError(t, err)
	if !strings.Contains(runErr.Message, `"lookup" was recorded as a step, the code now runs it as a sleep`) {
		t.Fatalf("message = %q", runErr.Message)
	}
}

func TestSwallowingANonDeterminismErrorStillFailsTheRun(t *testing.T) {
	h := newHarness(t)
	var deployed atomic.Bool
	flow := Define("swallowing", func(wf *Context, _ struct{}) (string, error) {
		if deployed.Load() {
			_ = wf.Sleep("lookup", time.Millisecond)
			return "pretended success", nil
		}
		if _, err := wf.Step("lookup", func(context.Context) (int, error) { return 1, nil }); err != nil {
			return "", err
		}
		return "", wf.Sleep("wait", time.Hour)
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	h.waitForStatus(run.ID, RunWaiting)

	deployed.Store(true)
	h.fastForward(run.ID)

	_, err := result(h, run)
	requireFailed(t, err)
}

func TestARecordedResultThatNoLongerDecodesFailsTheRun(t *testing.T) {
	h := newHarness(t)
	var deployed atomic.Bool
	flow := Define("changed-type", func(wf *Context, _ struct{}) (string, error) {
		if deployed.Load() {
			_, err := wf.Step("lookup", func(context.Context) (struct{ Count int }, error) { return struct{ Count int }{}, nil })
			return "new code", err
		}
		if _, err := wf.Step("lookup", func(context.Context) (string, error) { return "old shape", nil }); err != nil {
			return "", err
		}
		return "", wf.Sleep("wait", time.Hour)
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	h.waitForStatus(run.ID, RunWaiting)

	deployed.Store(true)
	h.fastForward(run.ID)

	_, err := result(h, run)
	if runErr := requireRunError(t, err); !strings.Contains(runErr.Message, `the value recorded for "lookup" does not decode`) {
		t.Fatalf("message = %q", runErr.Message)
	}
}

func TestIdempotencyKeyIdentifiesTheStepAcrossAttempts(t *testing.T) {
	h := newHarness(t)
	keys := make(chan string, 10)
	calls := newCounter()
	flow := Define("idempotent", func(wf *Context, _ struct{}) (string, error) {
		for range 2 {
			if _, err := wf.Step("pay", func(ctx context.Context) (bool, error) {
				keys <- IdempotencyKey(ctx)
				if calls.add(IdempotencyKey(ctx)) == 1 {
					return false, errors.New("timeout")
				}
				return true, nil
			}, fastRetry); err != nil {
				return "", err
			}
		}
		return "", nil
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	mustResult(h, run)
	close(keys)

	var seen []string
	for key := range keys {
		seen = append(seen, key)
	}
	want := []string{run.ID + "/pay", run.ID + "/pay", run.ID + "/pay#2", run.ID + "/pay#2"}
	if strings.Join(seen, ",") != strings.Join(want, ",") {
		t.Fatalf("keys = %v, want %v", seen, want)
	}
	if IdempotencyKey(context.Background()) != "" {
		t.Fatal("IdempotencyKey outside a step is not empty")
	}
}

func createLedger(h *harness) {
	h.t.Helper()
	h.exec(`CREATE TABLE ledger (id SERIAL PRIMARY KEY, entry TEXT NOT NULL)`)
}

func TestTxStepCommitsItsWritesWithItsCheckpoint(t *testing.T) {
	h := newHarness(t)
	createLedger(h)
	flow := Define("ledger", func(wf *Context, entry string) (int, error) {
		return wf.TxStep("record", func(ctx context.Context, tx *sql.Tx) (int, error) {
			var id int
			err := tx.QueryRowContext(ctx, `INSERT INTO ledger (entry) VALUES ($1) RETURNING id`, entry).Scan(&id)
			return id, err
		})
	})
	h.startWorker(flow)
	run := mustStart(h, flow, "deposit")

	id := mustResult(h, run)

	if h.queryInt(`SELECT count(*) FROM ledger WHERE id = $1 AND entry = 'deposit'`, id) != 1 {
		t.Fatal("the ledger entry is missing")
	}
	if step := h.step(run.ID, "record"); step.Status != StepSucceeded || string(step.Output) != fmt.Sprint(id) {
		t.Fatalf("step = %+v", step)
	}
}

func TestTxStepRollsBackAFailedAttempt(t *testing.T) {
	h := newHarness(t)
	createLedger(h)
	calls := newCounter()
	flow := Define("ledger-retry", func(wf *Context, _ struct{}) (int, error) {
		return wf.TxStep("record", func(ctx context.Context, tx *sql.Tx) (int, error) {
			attempt := calls.add("record")
			if _, err := tx.ExecContext(ctx, `INSERT INTO ledger (entry) VALUES ($1)`, fmt.Sprint("attempt ", attempt)); err != nil {
				return 0, err
			}
			if attempt == 1 {
				return 0, errors.New("validation failed after the insert")
			}
			return attempt, nil
		}, fastRetry)
	})
	h.startWorker(flow)

	mustResult(h, mustStart(h, flow, struct{}{}))

	if h.queryInt(`SELECT count(*) FROM ledger`) != 1 || h.queryInt(`SELECT count(*) FROM ledger WHERE entry = 'attempt 2'`) != 1 {
		t.Fatal("the failed attempt's write survived")
	}
}

func TestTxStepIsNotRepeatedOnReplay(t *testing.T) {
	h := newHarness(t)
	createLedger(h)
	flow := Define("ledger-replay", func(wf *Context, _ struct{}) (string, error) {
		if _, err := wf.TxStep("record", func(ctx context.Context, tx *sql.Tx) (bool, error) {
			_, err := tx.ExecContext(ctx, `INSERT INTO ledger (entry) VALUES ('once')`)
			return true, err
		}); err != nil {
			return "", err
		}
		return "done", wf.Sleep("pause", time.Hour)
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	h.waitForStatus(run.ID, RunWaiting)

	h.fastForward(run.ID)
	mustResult(h, run)

	if h.queryInt(`SELECT count(*) FROM ledger`) != 1 {
		t.Fatal("the transactional step ran twice")
	}
}

func TestTxStepThatFailsForGoodLeavesNoWrites(t *testing.T) {
	h := newHarness(t)
	createLedger(h)
	flow := Define("ledger-final", func(wf *Context, _ struct{}) (int, error) {
		return wf.TxStep("record", func(ctx context.Context, tx *sql.Tx) (int, error) {
			if _, err := tx.ExecContext(ctx, `INSERT INTO ledger (entry) VALUES ('doomed')`); err != nil {
				return 0, err
			}
			return 0, NonRetryable(errors.New("rejected"))
		})
	})
	h.startWorker(flow)

	_, err := result(h, mustStart(h, flow, struct{}{}))

	requireFailed(t, err)
	if h.queryInt(`SELECT count(*) FROM ledger`) != 0 {
		t.Fatal("a failed transactional step left its write")
	}
}

func TestAsyncStepsRunConcurrently(t *testing.T) {
	h := newHarness(t)
	bothStarted := make(chan struct{})
	var started atomic.Int32
	flow := Define("parallel", func(wf *Context, _ struct{}) (string, error) {
		wait := func(name string) func(context.Context) (string, error) {
			return func(ctx context.Context) (string, error) {
				if started.Add(1) == 2 {
					close(bothStarted)
				}
				select {
				case <-bothStarted:
					return name, nil
				case <-ctx.Done():
					return "", ctx.Err()
				}
			}
		}
		profile := wf.Async("profile", wait("profile"), Timeout(10*time.Second))
		orders := wf.Async("orders", wait("orders"), Timeout(10*time.Second))
		p, err := profile.Wait()
		if err != nil {
			return "", err
		}
		o, err := orders.Wait()
		return p + "+" + o, err
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})

	if got := mustResult(h, run); got != "profile+orders" {
		t.Fatalf("result = %q", got)
	}
	for _, name := range []string{"profile", "orders"} {
		if step := h.step(run.ID, name); step.Status != StepSucceeded {
			t.Fatalf("step = %+v", step)
		}
	}
}

func TestAsyncResultsAreReplayed(t *testing.T) {
	h := newHarness(t)
	calls := newCounter()
	flow := Define("parallel-replay", func(wf *Context, _ struct{}) (int, error) {
		futures := []*Future[int]{}
		for i := range 3 {
			futures = append(futures, wf.Async("part", func(context.Context) (int, error) {
				calls.add(fmt.Sprint("part", i))
				return i * 10, nil
			}))
		}
		sum := 0
		for _, future := range futures {
			value, err := future.Wait()
			if err != nil {
				return 0, err
			}
			sum += value
		}
		return sum, wf.Sleep("pause", time.Hour)
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	h.waitForStatus(run.ID, RunWaiting)

	h.fastForward(run.ID)

	if got := mustResult(h, run); got != 30 {
		t.Fatalf("result = %d", got)
	}
	for i := range 3 {
		if calls.get(fmt.Sprint("part", i)) != 1 {
			t.Fatalf("part %d ran %d times", i, calls.get(fmt.Sprint("part", i)))
		}
	}
}

func TestARunWaitsForAsyncStepsItDidNotWaitFor(t *testing.T) {
	h := newHarness(t)
	flow := Define("fire-and-forget", func(wf *Context, _ struct{}) (string, error) {
		wf.Async("audit", func(context.Context) (string, error) {
			time.Sleep(100 * time.Millisecond)
			return "audited", nil
		})
		return "returned early", nil
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})

	mustResult(h, run)

	if step := h.step(run.ID, "audit"); step.Status != StepSucceeded {
		t.Fatalf("the run finished before its async step: %+v", step)
	}
}

func TestAsyncStepFailuresSurfaceThroughWait(t *testing.T) {
	h := newHarness(t)
	flow := Define("parallel-failure", func(wf *Context, _ struct{}) (string, error) {
		_, err := wf.Async("broken", func(context.Context) (int, error) { return 0, errors.New("down") }, NoRetry).Wait()
		if stepErr, failed := errors.AsType[*StepError](err); failed {
			return stepErr.Message, nil
		}
		return "", err
	})
	h.startWorker(flow)

	if got := mustResult(h, mustStart(h, flow, struct{}{})); got != "down" {
		t.Fatalf("result = %q", got)
	}
}

func TestAnAsyncStepWithAnInvalidNameFailsItsFuture(t *testing.T) {
	h := newHarness(t)
	flow := Define("parallel-invalid", func(wf *Context, _ struct{}) (bool, error) {
		_, err := wf.Async("bad#name", func(context.Context) (int, error) { return 1, nil }).Wait()
		return errors.Is(err, ErrInvalidName), nil
	})
	h.startWorker(flow)

	if !mustResult(h, mustStart(h, flow, struct{}{})) {
		t.Fatal("an invalid async step name was accepted")
	}
}
